def site_modules(request):
    """
    Глобальный список модулей для всей экосистемы.
    """
    modules = [
        {
            'title': 'Финансовые обязательства',
            'desc': 'Учет задолженностей, графики выплат и аналитика долгов.',
            'icon': 'fa-wallet',
            'url_name': 'app_depts:records_list',
            'status': 'active',
            'status_text': 'Доступно'
        },
        {
            'title': 'Учет ставок',
            'desc': 'Анализ спортивных событий и статистика по алгоритму.',
            'icon': 'fa-chart-line',
            'url_name': None,
            'status': 'wait',
            'status_text': 'В разработке'
        },
        {
            'title': 'Складской учет',
            'desc': 'Инвентаризация, остатки товаров и логистика.',
            'icon': 'fa-boxes-stacked',
            'url_name': None,
            'status': 'wait',
            'status_text': 'В разработке'
        },
        # Чтобы добавить новый модуль, просто допиши еще один словарь здесь
    ]
    return {'site_modules': modules}