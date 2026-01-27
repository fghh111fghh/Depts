"""
Конфигурация URL для приложения app_depts.

Определяет маршруты для:
- Списка всех долговых обязательств.
- Детальной информации по конкретному долгу.
- Функционала быстрой оплаты через модальное окно.
"""

from django.urls import path
from . import views

# Имя приложения для использования в пространстве имен (namespace)
app_name: str = 'app_depts'

urlpatterns = [
    # Главная страница: отображает карточки со списком долгов и фильтрацией
    path(
        '',
        views.RecordsListView.as_view(),
        name='records_list'
    ),

    # Страница деталей: история транзакций и подробная статистика по конкретному slug
    path(
        'record/<slug:slug>/',
        views.RecordDetailView.as_view(),
        name='records_detail'
    ),

    # API-обработчик: выполняет сохранение быстрой оплаты из модального окна
    path(
        'quick-payment/<slug:slug>/',
        views.quick_payment,
        name='quick_payment'
    ),
path('export/excel/', views.ExportExcelView.as_view(), name='export_excel'),
path('export/pdf/', views.ExportPdfView.as_view(), name='export_pdf'),
]