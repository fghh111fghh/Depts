from django.urls import reverse

def records_url(request):
    """
    Добавляет в контекст URL страницы учёта ставок.
    """
    return {
        'records_url': reverse('app_bets:records')
    }

# Константы меню можно определить прямо здесь
BETS_MENU_ITEMS = [
    {
        'title': 'Главная',
        'url_name': 'app_bets:bets_maim',
    },
    {
        'title': 'Сигналы',
        'url_name': 'app_bets:cleaned',
    },
    {
        'title': 'Учет',
        'url_name': 'app_bets:records',
    },
{
        'title': 'Статистика',
        'url_name': 'app_bets:stats',
    },
]


def bets_menu(request):
    """
    Контекстный процессор для меню приложения ставок
    """
    current_url_name = request.resolver_match.view_name if request.resolver_match else ''

    menu_items = []
    for item in BETS_MENU_ITEMS:
        menu_item = item.copy()
        menu_item['is_active'] = (current_url_name == item['url_name'])
        menu_items.append(menu_item)

    return {
        'bets_menu_items': menu_items,
    }