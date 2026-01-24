from django.shortcuts import get_object_or_404, redirect
from django.views.generic import ListView, DetailView
from django.db.models import Sum, Q
from django.utils import timezone
from django.contrib import messages
from .models import Record, Transaction, TransactionType, CreditorType


class RecordsListView(ListView):
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'
    paginate_by = 6

    def get_queryset(self):
        # Получаем параметры из GET-запроса
        sort_by = self.request.GET.get('sort', 'time_create')
        search_query = self.request.GET.get('q', '')
        creditor_type = self.request.GET.get('creditor_type', '')
        # Флаг: показывать ли оплаченные (по умолчанию False)
        show_paid = self.request.GET.get('show_paid') == '1'

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

        # 1. ЛОГИКА СКРЫТИЯ: если не просим показать все, фильтруем только активные
        if not show_paid:
            queryset = queryset.filter(is_paid=False)

        # 2. Фильтрация по типу кредитора
        if creditor_type:
            queryset = queryset.filter(creditor__creditor_type=creditor_type)

        # 3. Поиск
        if search_query:
            queryset = queryset.filter(
                Q(name__icontains=search_query) |
                Q(creditor__name__icontains=search_query) |
                Q(note__icontains=search_query)
            )

        # 4. Сортировка
        if sort_by == 'amount':
            queryset = queryset.order_by('is_paid', '-total_debt_amount')
        elif sort_by == 'end_date':
            queryset = queryset.order_by('is_paid', 'end_date')
        elif sort_by == 'name':
            queryset = queryset.order_by('is_paid', 'name')
        else:
            queryset = queryset.order_by('is_paid', '-time_create')

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        today = timezone.now().date()

        # Подсказки для поиска (всегда все записи для автокомплита)
        context['records_all'] = Record.objects.select_related('creditor').all()

        # ДАННЫЕ ДЛЯ ШАПКИ (считаем только по реальным долгам)
        active_records = Record.objects.filter(is_paid=False)
        total_balance = sum(r.balance for r in active_records)

        # ГЛОБАЛЬНЫЙ ПРОГРЕСС (по всей истории)
        accrual_types = [TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        pay_types = [TransactionType.PAYMENT, TransactionType.WRITE_OFF]

        all_tr = Transaction.objects.all()
        total_accrued = all_tr.filter(type__in=accrual_types).aggregate(Sum('amount'))['amount__sum'] or 1
        total_paid = all_tr.filter(type__in=pay_types).aggregate(Sum('amount'))['amount__sum'] or 0
        overall_progress = (total_paid / total_accrued) * 100

        context.update({
            'total_unpaid_amount': round(total_balance, 2),
            'overall_progress': round(overall_progress, 1),
            'creditors_count': active_records.values('creditor').distinct().count(),
            'records_count': active_records.count(),
            'overdue_count': active_records.filter(end_date__lt=today).count(),

            # Параметры для сохранения состояния фильтров в шаблоне
            'current_sort': self.request.GET.get('sort', ''),
            'search_query': self.request.GET.get('q', ''),
            'current_type': self.request.GET.get('creditor_type', ''),
            'show_paid': self.request.GET.get('show_paid') == '1',
            'today': today,
        })
        return context


class RecordDetailView(DetailView):
    model = Record
    template_name = 'app_depts/record_detail.html'
    context_object_name = 'record'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = self.object.transactions.all().order_by('-date', '-id')
        return context


def quick_payment(request, slug):
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
            record.update_status()
            messages.success(request, f"Успешно: {amount} ₽ зачислено в счет {record.name}")
    return redirect(request.META.get('HTTP_REFERER', 'app_depts:records_list'))