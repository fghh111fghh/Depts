from django.urls import reverse

def records_url(request):
    """
    Добавляет в контекст URL страницы учёта ставок.
    """
    return {
        'records_url': reverse('app_bets:records')
    }