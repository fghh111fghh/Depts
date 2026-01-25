from typing import Any, Dict, List, Optional

from django.contrib import messages
from django.db.models import F, FloatField, Q, QuerySet, Sum
from django.db.models.functions import Coalesce
from django.http import HttpRequest, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect
from django.utils import timezone
from django.views.generic import DetailView, ListView

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
        record: Record = get_object_or_404(Record, slug=slug)
        amount_raw: Optional[str] = request.POST.get('amount')
        comment: str = request.POST.get('comment', '')

        if amount_raw:
            try:
                # Обработка возможной запятой в качестве разделителя
                amount: float = float(amount_raw.replace(',', '.'))
                if amount > 0:
                    Transaction.objects.create(
                        record=record,
                        type=TransactionType.PAYMENT,
                        amount=amount,
                        date=timezone.now().date(),
                        comment=comment or "Быстрая оплата"
                    )
                    # Принудительный пересчет статуса is_paid
                    record.update_status()
                    messages.success(request, f"Платеж {amount} ₽ успешно зачислен")
            except (ValueError, TypeError):
                messages.error(request, "Ошибка: введена некорректная сумма")

    return redirect(request.META.get('HTTP_REFERER', 'app_depts:records_list'))