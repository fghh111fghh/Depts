def site_modules(request):
    modules = [
        {
            'title': 'Финансовые обязательства',
            'app_namespace': 'app_depts',
            'desc': 'Учет задолженностей, графики выплат и аналитика долгов.',
            'icon': 'fa-wallet',
            'url_name': 'app_depts:records_list',
            'status': 'active',
            'status_text': 'Доступно'
        },
        {
            'title': 'Учет ставок',
            'app_namespace': 'app_bets',
            'desc': 'Анализ спортивных событий и статистика по алгоритму.',
            'icon': 'fa-chart-line',
            'url_name': 'app_bets:bets_maim',
            'status': 'active',
            'status_text': 'Доступно'
        },
        {
            'title': 'Складской учет',
            'app_namespace': 'app_warehouse',  # Лучше указать имя вместо None
            'desc': 'Инвентаризация, остатки товаров и логистика.',
            'icon': 'fa-boxes-stacked',
            'url_name': None,
            'status': 'wait',
            'status_text': 'В разработке'
        },
    ]

    current_module_title = "Личная экосистема"  # Значение по умолчанию

    if request.resolver_match:
        ns = request.resolver_match.app_name
        for m in modules:
            if m.get('app_namespace') == ns:
                current_module_title = m['title']  # Забираем название из списка выше
                break

    return {
        'site_modules': modules,
        'active_module_title': current_module_title,
    }