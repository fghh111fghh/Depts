from django.urls import path
from . import views


# Имя приложения для использования в пространстве имен (namespace)
app_name: str = 'app_main'

urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
]