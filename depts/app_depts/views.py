from django.shortcuts import get_object_or_404, redirect
from django.views.generic import ListView, DetailView
from django.db.models import Sum, Q
from django.utils import timezone
from django.contrib import messages
from .models import Record, Transaction, TransactionType


class RecordsListView(ListView):
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'
    paginate_by = 6  # По 6 карточек на страницу

    def get_queryset(self):
        sort_by = self.request.GET.get('sort', 'is_paid')
        search_query = self.request.GET.get('q', '')

        # Используем твои типы транзакций для аннотации суммы начислений
        queryset = Record.objects.select_related('creditor').annotate(
            total_debt_amount=Sum(
                'transactions__amount',
                filter=Q(transactions__type__in=[
                    TransactionType.ACCRUAL,
                    TransactionType.INTEREST,
                    TransactionType.PENALTY
                ])
            )
        )

        if search_query:
            # ПОИСК: теперь ищет и по твоему полю 'note' из BaseEntity
            queryset = queryset.filter(
                Q(name__icontains=search_query) |
                Q(creditor__name__icontains=search_query) |
                Q(note__icontains=search_query)
            )

        # Сортировка: сначала активные (is_paid=False), потом по выбранному полю
        if sort_by == 'amount':
            queryset = queryset.order_by('is_paid', '-total_debt_amount')
        elif sort_by == 'end_date':
            queryset = queryset.order_by('is_paid', 'end_date')
        elif sort_by == 'name':
            queryset = queryset.order_by('is_paid', 'name')
        else:
            # Сортировка по дате добавления из твоего BaseEntity
            queryset = queryset.order_by('is_paid', '-time_create')

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        today = timezone.now().date()

        # Для выпадающих подсказок в поиске (все записи)
        context['records_all'] = Record.objects.select_related('creditor').all()

        active_records = Record.objects.filter(is_paid=False)
        # Считаем баланс через твой @property balance (суммируем в цикле для простоты)
        total_balance = sum(r.balance for r in active_records)

        # РАСЧЕТ ГЛОБАЛЬНОГО ПРОГРЕССА (используем твои TransactionType)
        accrual_types = [TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        pay_types = [TransactionType.PAYMENT, TransactionType.WRITE_OFF]

        all_tr = Transaction.objects.all()
        total_accrued = all_tr.filter(type__in=accrual_types).aggregate(Sum('amount'))['amount__sum'] or 1
        total_paid = all_tr.filter(type__in=pay_types).aggregate(Sum('amount'))['amount__sum'] or 0

        overall_progress = (total_paid / total_accrued) * 100

        context.update({
            'total_unpaid_amount': round(total_balance, 2),
            'overall_progress': round(overall_progress, 1),
            'creditors_count': Record.objects.values('creditor').distinct().count(),
            'overdue_count': active_records.filter(end_date__lt=today).count(),
            'current_sort': self.request.GET.get('sort', ''),
            'search_query': self.request.GET.get('q', ''),
            'today': today,
        })
        return context


class RecordDetailView(DetailView):
    model = Record
    template_name = 'app_depts/record_detail.html'
    context_object_name = 'record'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Сортировка транзакций от новых к старым
        context['transactions'] = self.object.transactions.all().order_by('-date', '-id')
        return context


def quick_payment(request, slug):
    """Метод для быстрой оплаты из модального окна на главной"""
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
            messages.success(request, f"Успешно: {amount} ₽ зачислено в счет {record.name}")
    return redirect(request.META.get('HTTP_REFERER', 'app_depts:records_list'))