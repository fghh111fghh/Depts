from typing import Optional, Any

from django.contrib import admin
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.html import format_html

from .models import SRO, Creditor, Record, Transaction


# --- Инлайны (Транзакции внутри Долга) ---

class TransactionInline(admin.TabularInline):
    """
    Позволяет редактировать транзакции прямо на странице записи долга.
    """
    model = Transaction
    extra: int = 1
    fields: tuple = ('date', 'type', 'amount', 'comment')
    ordering: tuple = ('-date', '-id')
    show_change_link: bool = True


# --- Настройка СРО ---

@admin.register(SRO)
class SROAdmin(admin.ModelAdmin):
    """
    Администрирование саморегулируемых организаций.
    """
    list_display: tuple = ('name', 'display_contacts', 'time_create')
    prepopulated_fields: dict = {"slug": ("name",)}
    search_fields: tuple = ('name', 'phone')

    fieldsets: tuple = (
        ('Основная информация', {
            'fields': ('name', 'slug', 'note')
        }),
        ('Контактные данные', {
            'fields': ('phone', 'website'),
        }),
    )

    @admin.display(description="Контакты")
    def display_contacts(self, obj: SRO) -> Any:
        """Отображает телефон и ссылку на сайт в списке."""
        phone: str = obj.phone if obj.phone else ""
        website: str = format_html(
            '<a href="{0}" target="_blank" style="margin-left:10px;">🌐 Сайт</a>',
            obj.website
        ) if obj.website else ""
        return format_html('<span>{} {}</span>', phone, website)


# --- Настройка Кредиторов ---

@admin.register(Creditor)
class CreditorAdmin(admin.ModelAdmin):
    """
    Администрирование кредиторов (Банки, МФО и др.).
    """
    list_display: tuple = (
        'name', 'creditor_type', 'sro',
        'display_phone', 'display_website', 'get_records_count'
    )
    list_filter: tuple = ('creditor_type', 'sro')
    search_fields: tuple = ('name', 'phone')
    prepopulated_fields: dict = {"slug": ("name",)}

    fieldsets: tuple = (
        ('Организация', {
            'fields': (('name', 'slug'), ('creditor_type', 'sro'))
        }),
        ('Контакты', {
            'fields': (('phone', 'website'),),
        }),
        ('Дополнительно', {
            'classes': ('collapse',),
            'fields': ('note',),
        }),
    )

    @admin.display(description="Телефон")
    def display_phone(self, obj: Creditor) -> Any:
        """Отображает кликабельный номер телефона."""
        if not obj.phone:
            return "-"
        return format_html('<a href="tel:{0}">{0}</a>', obj.phone)

    @admin.display(description="Сайт")
    def display_website(self, obj: Creditor) -> Any:
        """Отображает иконку-ссылку на сайт."""
        if not obj.website:
            return "-"
        return format_html('<a href="{0}" target="_blank">🔗 Перейти</a>', obj.website)

    @admin.display(description="Кол-во долгов")
    def get_records_count(self, obj: Creditor) -> int:
        """Считает количество связанных долгов."""
        return obj.records.count()


# --- Настройка Записей Долгов ---

@admin.register(Record)
class RecordAdmin(admin.ModelAdmin):
    """
    Центральная модель: управление записями о долгах.
    """
    list_display: tuple = (
        'name', 'creditor', 'display_balance',
        'display_progress', 'start_date', 'is_paid'
    )
    list_filter: tuple = ('is_paid', 'loan_type', 'creditor__creditor_type', 'creditor')
    search_fields: tuple = ('name', 'creditor__name')
    inlines: list = [TransactionInline]
    readonly_fields: tuple = ('display_full_balance', 'display_progress_bar', 'slug')

    fieldsets: tuple = (
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

    @admin.display(description="Остаток")
    def display_balance(self, obj: Record) -> Any:
        """Цветовое выделение баланса (зеленый/красный)."""
        balance = obj.balance
        color: str = "green" if balance <= 0 else "red"
        return format_html('<b style="color: {};">{} р.</b>', color, balance)

    @admin.display(description="Прогресс")
    def display_progress(self, obj: Record) -> Any:
        """Визуальный прогресс-бар погашения."""
        percent: float = obj.progress_percent
        color: str = "#28a745" if percent >= 100 else "#ff9f43"
        return format_html(
            '<div style="width: 100px; background: #eee; border-radius: 4px; border: 1px solid #ccc;">'
            '<div style="width: {0}px; background: {1}; height: 12px;"></div>'
            '</div><small>{2}%</small>',
            percent, color, percent
        )

    @admin.display(description="Сводка по счетам")
    def display_full_balance(self, obj: Record) -> Any:
        """Детальный отчет по суммам в карточке редактирования."""
        return format_html(
            "<span style='font-size: 1.1em;'>"
            "Начислено: <b style='color:#2c3e50;'>{0} р.</b> | "
            "Выплачено: <b style='color:#27ae60;'>{1} р.</b> | "
            "Остаток: <b style='color:#e74c3c;'>{2} р.</b>"
            "</span>",
            obj.total_accrued, obj.total_paid, obj.balance
        )

    @admin.display(description="Прогресс (текст)")
    def display_progress_bar(self, obj: Record) -> Any:
        """Текстовое отображение уровня погашения."""
        return format_html("Текущий уровень погашения: <b>{}%</b>", obj.progress_percent)


# --- Настройка Транзакций ---

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    """
    Управление финансовыми операциями.
    """
    list_display: tuple = ('date', 'record', 'type', 'display_amount', 'comment')
    list_filter: tuple = ('type', 'date', 'record__creditor')
    search_fields: tuple = ('record__name', 'comment')
    date_hierarchy: str = 'date'
    autocomplete_fields: list = ['record']

    @admin.display(description="Сумма")
    def display_amount(self, obj: Transaction) -> Any:
        """Цветовое отображение суммы (приход/расход)."""
        accrual_list: list = ['ACCRUAL', 'INTEREST', 'PENALTY']
        is_accrual: bool = obj.type in accrual_list
        color: str = "#e74c3c" if is_accrual else "#27ae60"
        prefix: str = "+" if is_accrual else "-"
        return format_html('<b style="color: {};">{}{} р.</b>', color, prefix, obj.amount)

    def get_queryset(self, request: HttpRequest) -> QuerySet:
        """Оптимизация запросов через select_related."""
        return super().get_queryset(request).select_related('record', 'record__creditor')