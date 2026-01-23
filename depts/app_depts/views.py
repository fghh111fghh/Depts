from django.views.generic import ListView
from django.db.models import Sum
from .models import Record


class RecordsListView(ListView):
    model = Record
    template_name = 'app_depts/records_list.html'
    context_object_name = 'records'

    def get_queryset(self):
        # Получаем параметр сортировки из URL, по умолчанию 'is_paid' (сначала долги)
        sort_by = self.request.GET.get('sort', 'is_paid')

        # Базовая выборка с оптимизацией
        queryset = Record.objects.select_related('creditor')

        # Логика сортировки
        if sort_by == 'end_date':
            return queryset.order_by('is_paid', 'end_date')  # По дате окончания
        elif sort_by == 'amount':
            return queryset.order_by('is_paid', '-amount')  # По сумме (от большей)
        elif sort_by == 'name':
            return queryset.order_by('is_paid', 'creditor__name')  # По алфавиту кредитора

        # Сортировка по умолчанию
        return queryset.order_by('is_paid', '-start_date')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Сохраняем общую сумму
        total_unpaid = Record.objects.filter(is_paid=False).aggregate(Sum('amount'))['amount__sum'] or 0
        context['total_unpaid_amount'] = total_unpaid

        # Передаем текущую сортировку в шаблон, чтобы подсветить активную кнопку
        context['current_sort'] = self.request.GET.get('sort', '')
        return context
