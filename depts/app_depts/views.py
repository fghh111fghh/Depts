from django.db.models import Sum, Q
from django.views.generic import ListView
from .models import Record, TransactionType


class RecordsListView(ListView):
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'

    def get_queryset(self):
        # Получаем тип сортировки из URL
        sort_by = self.request.GET.get('sort', 'is_paid')

        # Аннотируем сумму для сортировки по "Сумме"
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

        if sort_by == 'amount':
            return queryset.order_by('is_paid', '-total_debt_amount')
        elif sort_by == 'end_date':
            return queryset.order_by('is_paid', 'end_date')
        elif sort_by == 'name':
            return queryset.order_by('is_paid', 'creditor__name')

        # По умолчанию: сначала активные, затем по дате старта
        return queryset.order_by('is_paid', '-start_date')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Считаем остаток по всем открытым долгам для шапки
        active_records = Record.objects.filter(is_paid=False)
        total_balance = sum(r.balance for r in active_records)

        context['total_unpaid_amount'] = round(total_balance, 2)
        context['current_sort'] = self.request.GET.get('sort', '')
        return context