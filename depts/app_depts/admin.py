from django.contrib import admin
from django.utils.html import format_html
from .models import SRO, Creditor, Record, Transaction


# --- Инлайны (Транзакции внутри Долга) ---

class TransactionInline(admin.TabularInline):
    model = Transaction
    extra = 1  # Количество пустых строк для новых транзакций
    fields = ('date', 'type', 'amount', 'comment')
    # Удобная сортировка: новые транзакции будут сверху списка внутри долга
    ordering = ('-date', '-id')
    # Чтобы случайно не удалить важную транзакцию при быстром редактировании
    show_change_link = True


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
    # Добавляем инлайны сюда!
    inlines = [TransactionInline]

    # Автоматическое создание slug на основе имени и кредитора в админке не всегда удобно,
    # так как модель сама делает это в save() с использованием time.time().
    # Поэтому поле slug лучше оставить только для чтения или скрыть.
    readonly_fields = ('display_full_balance', 'display_progress_bar', 'slug')

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

    # --- Прогресс-бар в списке ---
    def display_progress(self, obj):
        percent = obj.progress_percent
        # Зеленый если закрыт, оранжевый если процесс идет
        color = "#28a745" if percent >= 100 else "#ff9f43"
        return format_html(
            '''
            <div style="width: 100px; background: #eee; border-radius: 4px; overflow: hidden; border: 1px solid #ccc;">
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
            "<span style='font-size: 1.1em;'>"
            "Начислено: <b style='color:#2c3e50;'>{} р.</b> | "
            "Выплачено: <b style='color:#27ae60;'>{} р.</b> | "
            "Остаток: <b style='color:#e74c3c;'>{} р.</b>"
            "</span>",
            obj.total_accrued, obj.total_paid, obj.balance
        )

    display_full_balance.short_description = "Сводка по счетам"

    def display_progress_bar(self, obj):
        return format_html("Текущий уровень погашения: <b>{}%</b>", obj.progress_percent)

    display_progress_bar.short_description = "Прогресс (текст)"


# --- Настройка Транзакций ---

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('date', 'record', 'type', 'display_amount', 'comment')
    list_filter = ('type', 'date', 'record__creditor')
    search_fields = ('record__name', 'comment')
    date_hierarchy = 'date'
    # Чтобы удобно было выбирать долг, если транзакций много
    autocomplete_fields = ['record']

    def display_amount(self, obj):
        # Красный для начислений (увеличивают долг), зеленый для оплат (уменьшают)
        is_accrual = obj.type in ['ACCRUAL', 'INTEREST', 'PENALTY']
        color = "#e74c3c" if is_accrual else "#27ae60"
        prefix = "+" if is_accrual else "-"
        return format_html('<b style="color: {};">{}{} р.</b>', color, prefix, obj.amount)

    display_amount.short_description = "Сумма"


# Регистрация СРО
@admin.register(SRO)
class SROAdmin(admin.ModelAdmin):
    list_display = ('name', 'time_create')
    prepopulated_fields = {"slug": ("name",)}