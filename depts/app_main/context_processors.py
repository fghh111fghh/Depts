from .constants import site_texts


def site_modules(request):
    modules = [
        {
            'title': 'Финансовые обязательства',
            'app_namespace': 'app_depts',
            'desc': 'Учет задолженностей, графики выплат и аналитика долгов.',
            'icon': 'dollar',
            'url_name': 'app_depts:records_list',
            'status': 'active',
            'status_text': 'Доступно'
        },
        {
            'title': 'Учет ставок',
            'app_namespace': 'app_bets',
            'desc': 'Анализ спортивных событий и статистика по алгоритму.',
            'icon': 'book-open',
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

    current_module_title = f'{site_texts.SITE_NAME_FIRST} {site_texts.SITE_NAME_SECOND}'
    # Значение по умолчанию

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



def site_texts_processor(request):
    return {
        # Шапка
        'site_name_first': site_texts.SITE_NAME_FIRST,
        'site_name_second': site_texts.SITE_NAME_SECOND,
        'admin_text': site_texts.ADMIN_BUTTON_TEXT,

        # Главная страница
        'index_page_title': site_texts.INDEX_PAGE_TITLE,
        'index_page_subtitle': site_texts.INDEX_PAGE_SUBTITLE,

        # Подвал
        'footer_copyright_year': site_texts.FOOTER_COPYRIGHT_YEAR,
        'footer_site_name': site_texts.FOOTER_SITE_NAME,
        'footer_tech_info': site_texts.FOOTER_TECH_INFO,
    }