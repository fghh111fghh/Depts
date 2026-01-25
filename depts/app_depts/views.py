from django.shortcuts import get_object_or_404, redirect
from django.views.generic import ListView, DetailView
from django.db.models import Sum, Q, F, FloatField
from django.db.models.functions import Coalesce
from django.utils import timezone
from django.contrib import messages

# Импорт твоих моделей
from .models import Record, Transaction, TransactionType


class RecordsListView(ListView):
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'
    paginate_by = 12

    def get_queryset(self):
        # 1. Получаем параметры (одиночная сортировка)
        sort_param = self.request.GET.get('sort', '')
        search_query = self.request.GET.get('q', '')
        creditor_type = self.request.GET.get('creditor_type', '')
        show_paid_local = self.request.GET.get('show_paid') == '1'

        accrual_types = [TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        payment_types = [TransactionType.PAYMENT, TransactionType.WRITE_OFF]

        # 2. Аннотация баланса для точной сортировки
        queryset = Record.objects.select_related('creditor').annotate(
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

        # 3. Фильтрация
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

        # 4. Одиночная сортировка
        order_list = ['is_paid']
        if sort_param == 'creditor':
            order_list.append('creditor__name')
        elif sort_param == 'amount':
            order_list.append('-current_debt_balance')
        elif sort_param == 'end_date':
            order_list.append('end_date')
        else:
            order_list.append('-time_create')

        return queryset.order_by(*order_list)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        today = timezone.now().date()

        active_records = Record.objects.filter(is_paid=False)
        total_balance = sum(r.balance for r in active_records)

        # Расчет глобального прогресса
        all_tr = Transaction.objects.all()
        t_acc = all_tr.filter(type__in=[
            TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY
        ]).aggregate(Sum('amount'))['amount__sum'] or 1
        t_pay = all_tr.filter(type__in=[
            TransactionType.PAYMENT, TransactionType.WRITE_OFF
        ]).aggregate(Sum('amount'))['amount__sum'] or 0

        context.update({
            'total_unpaid_amount': round(total_balance, 2),
            'overall_progress': round((t_pay / t_acc) * 100, 1),
            'creditors_count': active_records.values('creditor').distinct().count(),
            'records_count': active_records.count(),
            'overdue_count': active_records.filter(end_date__lt=today).count(),

            # Параметры интерфейса
            'current_sort': self.request.GET.get('sort', ''),
            'search_query': self.request.GET.get('q', ''),
            'current_type': self.request.GET.get('creditor_type', ''),
            'show_paid': self.request.GET.get('show_paid') == '1',
            'today': today,
            'records_all': Record.objects.select_related('creditor').all(),
        })
        return context


class RecordDetailView(DetailView):
    model = Record
    template_name = 'app_depts/record_detail.html'
    context_object_name = 'record'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        record = self.object

        # Получаем транзакции (у тебя в модели уже стоит ordering = ['-date', '-id'])
        context['transactions'] = record.transactions.all()
        context['today'] = timezone.now().date()

        # Используем твои методы из models.py для расчетов
        context['total_accrued_val'] = record.total_accrued
        context['total_paid_val'] = record.total_paid
        context['progress_val'] = record.progress_percent  # Твой метод из модели

        return context


def quick_payment(request, slug):
    """Метод для быстрой оплаты прямо из списка или деталей"""
    if request.method == 'POST':
        record = get_object_or_404(Record, slug=slug)
        amount = request.POST.get('amount')

        if amount and float(amount) > 0:
            Transaction.objects.create(
                record=record,
                type=TransactionType.PAYMENT,
                amount=amount,
                date=timezone.now().date()
            )
            # Вызываем метод модели для обновления статуса is_paid
            record.update_status()
            messages.success(request, f"Платеж {amount} ₽ успешно зачислен")

    # Возвращаем пользователя туда, откуда он пришел
    return redirect(request.META.get('HTTP_REFERER', 'app_depts:records_list'))