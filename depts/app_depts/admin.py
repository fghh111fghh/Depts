from django.contrib import admin
from django.utils.html import format_html
from .models import SRO, Creditor, Record, Transaction


# --- Инлайны (Транзакции внутри Долга) ---

class TransactionInline(admin.TabularInline):
    model = Transaction
    extra = 1
    fields = ('date', 'type', 'amount', 'comment')
    ordering = ('-date',)


# --- Настройка Кредиторов ---

@admin.register(Creditor)
class CreditorAdmin(admin.ModelAdmin):
    list_display = ('name', 'creditor_type', 'sro', 'phone', 'get_records_count')
    list_filter = ('creditor_type', 'sro')
    search_fields = ('name', 'phone')
    prepopulated_fields = {"slug": ("name",)}

    def get_records_count(self, obj):
        return obj.records.count()

    get_records_count.short_description = "Кол-во долгов"


# --- Настройка Записей Долгов (Центральная часть) ---

@admin.register(Record)
class RecordAdmin(admin.ModelAdmin):
    list_display = (
        'name', 'creditor', 'display_balance',
        'display_progress', 'start_date', 'is_paid'
    )
    list_filter = ('is_paid', 'loan_type', 'creditor__creditor_type', 'creditor')
    search_fields = ('name', 'creditor__name')
    readonly_fields = ('display_full_balance', 'display_progress_bar')
    inlines = [TransactionInline]

    fieldsets = (
        ('Основная информация', {
            'fields': (('name', 'slug'), ('creditor', 'loan_type'), ('start_date', 'end_date'), 'is_paid')
        }),
        ('Финансовое состояние', {
            'fields': ('display_full_balance', 'display_progress_bar'),
        }),
        ('Дополнительно', {
            'classes': ('collapse',),
            'fields': ('note',),
        }),
    )

    # --- Красивое отображение баланса в списке ---
    def display_balance(self, obj):
        balance = obj.balance
        color = "green" if balance <= 0 else "red"
        return format_html(
            '<b style="color: {};">{} р.</b>',
            color, balance
        )

    display_balance.short_description = "Остаток"
    display_balance.admin_order_field = 'is_paid'  # Сортировка по статусу

    # --- Прогресс-бар в списке ---
    def display_progress(self, obj):
        percent = obj.progress_percent
        color = "#28a745" if percent == 100 else "#007bff"
        return format_html(
            '''
            <div style="width: 100px; background: #eee; border-radius: 4px; overflow: hidden;">
                <div style="width: {}px; background: {}; height: 12px;"></div>
            </div>
            <small>{}%</small>
            ''',
            percent, color, percent
        )

    display_progress.short_description = "Прогресс"

    # --- Детальный баланс в карточке ---
    def display_full_balance(self, obj):
        return format_html(
            "Начислено: <b>{} р.</b> | Выплачено: <b>{} р.</b> | Остаток: <b style='color:red;'>{} р.</b>",
            obj.total_accrued, obj.total_paid, obj.balance
        )

    display_full_balance.short_description = "Сводка по счетам"

    def display_progress_bar(self, obj):
        return format_html("Текущий прогресс погашения: <b>{}%</b>", obj.progress_percent)

    display_progress_bar.short_description = "Прогресс (текст)"


# --- Настройка Транзакций (как отдельный лог) ---

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('date', 'record', 'type', 'display_amount', 'comment')
    list_filter = ('type', 'date', 'record__creditor')
    search_fields = ('record__name', 'comment')
    date_hierarchy = 'date'

    def display_amount(self, obj):
        color = "red" if obj.type in ['ACCRUAL', 'INTEREST', 'PENALTY'] else "green"
        prefix = "+" if color == "red" else "-"
        return format_html('<b style="color: {};">{}{} р.</b>', color, prefix, obj.amount)

    display_amount.short_description = "Сумма"


# Простая регистрация СРО
admin.site.register(SRO)