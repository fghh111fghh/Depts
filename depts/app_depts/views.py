import os
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional

import openpyxl
from django.conf import settings
from django.contrib import messages
from django.db.models import F, FloatField, Q, QuerySet, Sum
from django.db.models.functions import Coalesce
from django.http import HttpRequest, HttpResponseRedirect, HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.utils import timezone
from django.views import View
from django.views.generic import DetailView, ListView
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, TableStyle, Table, Paragraph

from .models import Record, Transaction, TransactionType


class RecordsListView(ListView):
    """
    Отображает список долговых обязательств с расширенной фильтрацией и аннотациями.
    """
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'
    paginate_by: int = 12

    def get_queryset(self) -> QuerySet:
        """
        Формирует QuerySet с аннотацией баланса и применением фильтров.

        Returns:
            QuerySet: Отфильтрованные и упорядоченные записи.
        """
        # Параметры из GET-запроса
        sort_param: str = self.request.GET.get('sort', '')
        search_query: str = self.request.GET.get('q', '')
        creditor_type: str = self.request.GET.get('creditor_type', '')
        show_paid_local: bool = self.request.GET.get('show_paid') == '1'

        accrual_types: List[str] = [
            TransactionType.ACCRUAL,
            TransactionType.INTEREST,
            TransactionType.PENALTY
        ]
        payment_types: List[str] = [
            TransactionType.PAYMENT,
            TransactionType.WRITE_OFF
        ]

        # Аннотация баланса для точной сортировки на уровне БД
        queryset: QuerySet = Record.objects.select_related('creditor').annotate(
            annotated_accrued=Coalesce(
                Sum('transactions__amount', filter=Q(transactions__type__in=accrual_types)),
                0.0, output_field=FloatField()
            ),
            annotated_payments=Coalesce(
                Sum('transactions__amount', filter=Q(transactions__type__in=payment_types)),
                0.0, output_field=FloatField()
            ),
            current_debt_balance=F('annotated_accrued') - F('annotated_payments')
        )

        # Применение фильтров
        if not show_paid_local:
            queryset = queryset.filter(is_paid=False)
        if creditor_type:
            queryset = queryset.filter(creditor__creditor_type=creditor_type)
        if search_query:
            queryset = queryset.filter(
                Q(name__icontains=search_query) |
                Q(creditor__name__icontains=search_query) |
                Q(note__icontains=search_query)
            )

        # Определение порядка сортировки
        order_list: List[str] = ['is_paid']
        if sort_param == 'creditor':
            order_list.append('creditor__name')
        elif sort_param == 'amount':
            order_list.append('-current_debt_balance')
        elif sort_param == 'end_date':
            order_list.append('end_date')
        else:
            order_list.append('-time_create')

        return queryset.order_by(*order_list)

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Добавляет статистические данные и параметры фильтрации в контекст.
        """
        context: Dict[str, Any] = super().get_context_data(**kwargs)
        today: timezone.now = timezone.now().date()

        active_records: QuerySet = Record.objects.filter(is_paid=False)

        # Глобальный прогресс по всем транзакциям
        all_tr: QuerySet = Transaction.objects.all()
        t_acc: float = all_tr.filter(
            type__in=[TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        ).aggregate(total=Sum('amount'))['total'] or 1.0

        t_pay: float = all_tr.filter(
            type__in=[TransactionType.PAYMENT, TransactionType.WRITE_OFF]
        ).aggregate(total=Sum('amount'))['total'] or 0.0

        # Вычисление общего баланса (итерируемся по активным записям)
        total_balance: float = sum(r.balance for r in active_records)
        context['records_all'] = Record.objects.all().select_related('creditor')
        context.update({
            'total_unpaid_amount': round(total_balance, 2),
            'overall_progress': round((float(t_pay) / float(t_acc)) * 100, 1),
            'creditors_count': active_records.values('creditor').distinct().count(),
            'records_count': active_records.count(),
            'overdue_count': active_records.filter(end_date__lt=today).count(),

            # Параметры для UI
            'current_sort': self.request.GET.get('sort', ''),
            'search_query': self.request.GET.get('q', ''),
            'current_type': self.request.GET.get('creditor_type', ''),
            'show_paid': self.request.GET.get('show_paid') == '1',
            'today': today,
        })
        return context


class RecordDetailView(DetailView):
    """
    Детальное представление долга с историей всех финансовых операций.
    """
    model = Record
    template_name = 'app_depts/record_detail.html'
    context_object_name = 'record'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Формирует детальную финансовую информацию по конкретной записи.
        """
        context: Dict[str, Any] = super().get_context_data(**kwargs)
        record: Record = self.object

        context.update({
            'transactions': record.transactions.all(),
            'today': timezone.now().date(),
            'total_accrued_val': record.total_accrued,
            'total_paid_val': record.total_paid,
            'progress_val': record.progress_percent,
        })
        return context


def quick_payment(request: HttpRequest, slug: str) -> HttpResponseRedirect:
    """
    Обработчик для быстрого внесения платежа из списка или модального окна.

    Args:
        request: Объект HTTP-запроса.
        slug: Уникальный слаг записи долга.

    Returns:
        HttpResponseRedirect: Перенаправление на предыдущую страницу.
    """
    if request.method == 'POST':
        record = get_object_or_404(Record, slug=slug)
        amount_raw = request.POST.get('amount')
        comment = request.POST.get('comment', '')

        if amount_raw:
            try:
                # 1. Очистка строки: убираем пробелы и заменяем запятую на точку
                clean_amount = amount_raw.strip().replace(' ', '').replace(',', '.')

                # 2. Преобразование в Decimal
                amount = Decimal(clean_amount).quantize(
                    Decimal('0.00'),
                    rounding=ROUND_HALF_UP
                )

                if amount > 0:
                    # Создаем транзакцию.
                    # Проверь поле в модели: если там 'note', используй note=comment
                    Transaction.objects.create(
                        record=record,
                        type=TransactionType.PAYMENT,
                        amount=amount,
                        date=timezone.now().date(),
                        comment=comment or "Быстрая оплата"
                    )

                    record.update_status()
                    messages.success(request, f"Платеж {amount} ₽ успешно зачислен")
                else:
                    messages.error(request, "Сумма должна быть больше нуля")

            except (InvalidOperation, ValueError, TypeError) as e:
                # Выводим ошибку в консоль для отладки, если что-то пойдет не так
                print(f"Ошибка парсинга суммы: {e}")
                messages.error(request, f"Ошибка: сумма '{amount_raw}' введена некорректно")

    return redirect(request.META.get('HTTP_REFERER', 'app_depts:records_list'))


class RecordFilterMixin:
    """Логика фильтрации для экспорта данных."""

    def get_filtered_queryset(self):
        queryset = Record.objects.select_related('creditor').all()

        q = self.request.GET.get('q', '')
        c_type = self.request.GET.get('creditor_type', '')
        show_paid = self.request.GET.get('show_paid') == '1'

        if q:
            queryset = queryset.filter(
                Q(name__icontains=q) |
                Q(creditor__name__icontains=q) |
                Q(note__icontains=q)
            )

        if c_type:
            queryset = queryset.filter(creditor__creditor_type=c_type)

        if not show_paid:
            queryset = queryset.filter(is_paid=False)

        return queryset


class ExportExcelView(RecordFilterMixin, View):
    def get(self, request, *args, **kwargs):
        queryset = self.get_filtered_queryset()

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Список долгов"

        # Список заголовков (Примечание в конце)
        headers = [
            'Название',
            'Кредитор',
            'Тип орг.',
            'Категория',
            'Начислено',
            'Выплачено',
            'Остаток',
            'Статус',
            'Открыт',
            'Окончание',
            'Примечание'
        ]
        ws.append(headers)

        # Жирный шрифт для шапки
        for cell in ws[1]:
            cell.font = openpyxl.styles.Font(bold=True)

        for obj in queryset:
            row = [
                obj.name,
                obj.creditor.name,
                obj.creditor.get_creditor_type_display(),
                obj.get_loan_type_display(),
                float(obj.total_accrued),  # Сумма начислений (property)
                float(obj.total_paid),  # Выплачено (property)
                float(obj.balance),  # Остаток (property)
                "Закрыт" if obj.is_paid else "Активен",
                obj.start_date.strftime('%d.%m.%Y') if obj.start_date else '-',
                obj.end_date.strftime('%d.%m.%Y') if obj.end_date else '-',
                obj.note  # Примечание из BaseEntity
            ]
            ws.append(row)

        # Настройка ширины колонок
        for col in ws.columns:
            max_length = 0
            column_letter = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            ws.column_dimensions[column_letter].width = max_length + 2

        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=debts_export.xlsx'
        wb.save(response)
        return response


class ExportPdfView(RecordFilterMixin, View):
    def get(self, request, *args, **kwargs):
        queryset = self.get_filtered_queryset()

        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = 'attachment; filename=debts_report.pdf'

        # 1. Регистрация шрифта (используем твой файл arialmt.ttf)
        font_path = os.path.join(settings.BASE_DIR, 'static', 'fonts', 'arialmt.ttf')
        pdfmetrics.registerFont(TTFont('ArialMT', font_path))

        # 2. Создаем стиль для ВСЕХ текстов в таблице
        # Это критически важно: здесь принудительно ставим ArialMT
        table_text_style = ParagraphStyle(
            'RusStyle',
            fontName='ArialMT',
            fontSize=8,
            leading=10,
            alignment=0,  # По левому краю
        )

        # Стиль для заголовков (чуть крупнее)
        header_style = ParagraphStyle(
            'HeaderStyle',
            fontName='ArialMT',
            fontSize=10,
            textColor=colors.whitesmoke,
            alignment=1,  # По центру
        )

        doc = SimpleDocTemplate(
            response,
            pagesize=landscape(A4),
            leftMargin=15, rightMargin=15, topMargin=20, bottomMargin=20
        )
        elements = []

        # Заголовки (тоже оборачиваем в Paragraph, чтобы не было квадратов в шапке)
        headers = [
            Paragraph('Название', header_style),
            Paragraph('Кредитор', header_style),
            Paragraph('Категория', header_style),
            Paragraph('Начислено', header_style),
            Paragraph('Выплачено', header_style),
            Paragraph('Остаток', header_style),
            Paragraph('Статус', header_style),
            Paragraph('Окончание', header_style),
            Paragraph('Примечание', header_style)
        ]

        data = [headers]

        for obj in queryset:
            # Оборачиваем КАЖДОЕ поле, где есть русский текст или спецсимволы
            data.append([
                Paragraph(obj.name or "-", table_text_style),
                Paragraph(obj.creditor.name or "-", table_text_style),
                Paragraph(obj.get_loan_type_display() or "-", table_text_style),
                f"{obj.total_accrued:,.2f}",  # Числа обычно отображаются нормально
                f"{obj.total_paid:,.2f}",
                f"{obj.balance:,.2f}",
                Paragraph("Закрыт" if obj.is_paid else "Активен", table_text_style),
                obj.end_date.strftime('%d.%m.%Y') if obj.end_date else '-',
                Paragraph(obj.note or "-", table_text_style)
            ])

        # Ширина колонок (всего 780-800)
        col_widths = [100, 90, 80, 70, 70, 70, 60, 70, 175]

        table = Table(data, colWidths=col_widths)

        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.dodgerblue),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            # Дополнительно прописываем шрифт для всей таблицы на случай пустых ячеек
            ('FONTNAME', (0, 0), (-1, -1), 'ArialMT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))

        elements.append(table)
        doc.build(elements)
        return response