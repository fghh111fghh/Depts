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
{
        'title': 'Калькулятор Келли',
        'url_name': 'app_bets:develop',
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

"""
Текстовые константы для главной страницы приложения ставок
"""

# Заголовки и метки
HEADER_TITLES = {
    'VECTOR_ANALYSIS': 'Векторный Анализ',
    'SORT_LABEL': 'Сортировка:',
    'DEFAULT_SORT': 'По умолч.',
    'BTTS_DESC': 'ОЗ ↓',
    'OVER25_DESC': 'б2.5 ↓',
    'TWINS_DESC': 'Близнецы ↓',
    'PATTERN_DESC': 'История ↓',
}

# Кнопки действий
ACTION_BUTTONS = {
    'EXPORT_EXCEL': 'В Excel',
    'CALCULATE': 'Рассчитать вероятность',
    'SAVE_ALIAS': 'Сохранить привязку',
    'SYNC_DB': 'Синхронизировать базу',
}

# Счетчики и статистика
COUNTER_LABELS = {
    'MATCHES_CALCULATED': 'Рассчитано матчей:',
    'LEARN_TEAMS': 'Привязать команды:',
}

# Раздел обучения
LEARNING_SECTION = {
    'TITLE': 'Обучение системы',
    'SELECT_TEAM': '-- выбери из базы --',
}

# CSV секция
CSV_SECTION = {
    'LABEL': 'Локальная папка (import_data):',
    'DESCRIPTION': 'Положить CSV файлы в папку import_data и нажать кнопку.',
}

# Результаты импорта
IMPORT_RESULTS = {
    'TITLE': 'Результат импорта',
    'DETAILS': 'Детали:',
    'ADDED': 'Добавлено:',
    'SKIPPED': 'Пропущено:',
    'ERRORS': 'Ошибок:',
}

# Поле ввода
INPUT_LABELS = {
    'DATA_INPUT': 'Ввод данных:',
    'PLACEHOLDER': 'Пример формата:\nРеал Овьедо\nМалага\n2.10\n3.10\n3.80',
}

# Пуассон и анализ
ANALYSIS_LABELS = {
    'POISSON': 'Пуассон:',
    'TWINS': 'Близнецы (кэфы)',
    'BTTS': 'Обе забьют',
    'TOTAL_OVER': 'Тотал > 2.5',
    'HISTORICAL_TOTAL': 'История ТБ>2.5',
    'HISTORICAL_PATTERN': 'Исторический шаблон (серия x4)',
    'H2H_TITLE': 'Личные встречи (в этом статусе)',
    'NO_DATA': 'Нет данных',
    'NO_TWINS_DATA': 'Нет данных по близнецам',
    'NO_PATTERN_DATA': 'Нет данных',
    'NO_H2H_DATA': 'Игр в таком статусе не найдено',
}

# Коэффициенты
ODDS_LABELS = {
    'P1': 'P1',
    'X': 'X',
    'P2': 'P2',
}

# Пустое состояние
EMPTY_STATE = {
    'TEXT': 'Введите данные матча для начала анализа',
}

# Вердикты и предсказания
PREDICTION_TEXTS = {
    'OVER': 'ТБ 2.5',
    'UNDER': 'ТМ 2.5',
    'FIFTY': '50/50',
}

def bets_texts(request):
    """
    Контекстный процессор для текстовых констант
    """
    return {
        'header_titles': HEADER_TITLES,
        'action_buttons': ACTION_BUTTONS,
        'counter_labels': COUNTER_LABELS,
        'learning_section': LEARNING_SECTION,
        'csv_section': CSV_SECTION,
        'import_results': IMPORT_RESULTS,
        'input_labels': INPUT_LABELS,
        'analysis_labels': ANALYSIS_LABELS,
        'odds_labels': ODDS_LABELS,
        'empty_state': EMPTY_STATE,
        'prediction_texts': PREDICTION_TEXTS,
    }


