
from django.urls import path
from . import views

# Имя приложения для использования в пространстве имен (namespace)
app_name: str = 'app_bets'

urlpatterns = [
    # Главная страница приложения
    path('', views.AnalyzeView.as_view(), name='bets_maim'),
    path('autocomplete/team/', views.TeamAutocomplete.as_view(), name='team-autocomplete'),
    path('autocomplete/league/', views.LeagueAutocomplete.as_view(), name='league-autocomplete'),
    path('autocomplete/sport/', views.SportAutocomplete.as_view(), name='sport-autocomplete'),
    path('autocomplete/country/', views.CountryAutocomplete.as_view(), name='country-autocomplete'),
    path('upload-csv/', views.UploadCSVView.as_view(), name='upload_csv'),
    path('cleaned/', views.CleanedTemplateView.as_view(), name='cleaned'),
    path('export/excel/', views.ExportBetsExcelView.as_view(), name='export_excel'),
    path('export_cleaned/', views.ExportCleanedExcelView.as_view(), name='export_cleaned'),
    path('records/', views.BetRecordsView.as_view(), name='records'),
    path('records/bulk-action/', views.bulk_bet_action, name='bulk_bet_action'),
    path('records/export-excel/', views.export_bets_excel, name='export_bets_excel'),
    path('bet/add/', views.BetCreateView.as_view(), name='bet_create'),
]