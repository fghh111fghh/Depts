from django.urls import path
from . import views

urlpatterns = [
    path('', views.RecordsListView.as_view(), name='records_list'),
]