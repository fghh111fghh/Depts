from django.urls import path
from . import views

app_name = 'app_depts'
urlpatterns = [
    path('', views.RecordsListView.as_view(), name='records_list'),
]