from django.contrib import admin
from django.utils.html import format_html
from django.db.models import QuerySet
from typing import Any
from .models import SRO, Creditor, Record

# --- Настройки заголовков ---
admin.site.site_header = "Панель управления долгами"
admin.site.site_title = "Контроль Дебиторки"
admin.site.index_title = "Добро пожаловать в базу данных"


# --- Инлайны ---

class RecordInline(admin.TabularInline):
    """Позволяет редактировать записи долгов прямо на странице Кредитора."""
    model = Record
    extra = 1
    fields = ('name', 'amount', 'start_date', 'end_date', 'is_paid')
    show_change_link = True


# --- Базовый класс для подключения маски телефона ---

class BaseOrganizationAdmin(admin.ModelAdmin):
    """
    Базовый класс для СРО и Кредиторов.
    Подключает только наш локальный скрипт маски.
    """
    prepopulated_fields = {'slug': ('name',)}

    class Media:
        # Теперь здесь только наш файл, без внешних библиотек
        js = ('app_depts/js/phone_mask.js',)


# --- Основные классы админки ---

@admin.register(SRO)
class SROAdmin(BaseOrganizationAdmin):
    list_display = ('name', 'is_active', 'time_create', 'count_creditors')
    list_editable = ('is_active',)
    search_fields = ('name',)

    def count_creditors(self, obj: SRO) -> int:
        return obj.creditors.count()

    count_creditors.short_description = "Кол-во кредиторов"


@admin.register(Creditor)
class CreditorAdmin(BaseOrganizationAdmin):
    list_display = ('name', 'sro', 'is_active', 'website_link', 'total_debt')
    list_filter = ('sro', 'is_active')
    search_fields = ('name', 'note')
    inlines = [RecordInline]

    def website_link(self, obj: Creditor) -> Any:
        if obj.website:
            return format_html('<a href="{0}" target="_blank">{0}</a>', obj.website)
        return "-"

    website_link.short_description = "Сайт"

    def total_debt(self, obj: Creditor) -> str:
        # Считаем только активные (неоплаченные) долги
        total = sum(r.amount for r in obj.records.filter(is_paid=False))
        return f"{total} руб."

    total_debt.short_description = "Долг (актив)"


@admin.register(Record)
class RecordAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'creditor',
        'amount',
        'start_date',
        'end_date',
        'is_paid',
        'colored_status'
    )
    list_editable = ('is_paid',)
    list_filter = ('is_paid', 'creditor', 'start_date')
    search_fields = ('name', 'creditor__name', 'note')
    prepopulated_fields = {'slug': ('name',)}

    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'creditor', 'amount', 'is_paid')
        }),
        ('Даты и сроки', {
            'fields': ('start_date', 'end_date', 'slug')
        }),
        ('Дополнительно', {
            'classes': ('collapse',),
            'fields': ('note', 'is_active'),
        }),
    )

    def colored_status(self, obj: Record) -> Any:
        if obj.is_paid:
            return format_html('<b style="color: #28a745;">✅ Оплачен</b>')
        return format_html('<b style="color: #dc3545;">❌ Активен</b>')

    colored_status.short_description = "Статус"

    def get_queryset(self, request: Any) -> QuerySet:
        return super().get_queryset(request).select_related('creditor')