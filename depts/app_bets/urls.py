
from django.urls import path
from . import views

# Имя приложения для использования в пространстве имен (namespace)
app_name: str = 'app_bets'

urlpatterns = [
    # Главная страница приложения
    path('', views.AnalyzeView.as_view(), name='bets_maim'),
]