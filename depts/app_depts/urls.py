from django.urls import path
from . import views

app_name = 'app_depts'

urlpatterns = [
    # Главная страница со списком
    path('', views.RecordsListView.as_view(), name='records_list'),

    # Страница деталей (DetailView)
    path('record/<slug:slug>/', views.RecordDetailView.as_view(), name='records_detail'),

    # Обработчик быстрой оплаты (функция)
    path('quick-payment/<slug:slug>/', views.quick_payment, name='quick_payment'),
]