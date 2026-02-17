
from django.urls import path
from . import views

# Имя приложения для использования в пространстве имен (namespace)
app_name: str = 'app_bets'

urlpatterns = [
    # Главная страница приложения
    path('', views.AnalyzeView.as_view(), name='bets_maim'),
    path('upload-csv/', views.UploadCSVView.as_view(), name='upload_csv'),
    path('cleaned/', views.CleanedTemplateView.as_view(), name='cleaned'),
    path('export/excel/', views.ExportBetsExcelView.as_view(), name='export_excel'),
    path('export_cleaned/', views.ExportCleanedExcelView.as_view(), name='export_cleaned'),
]