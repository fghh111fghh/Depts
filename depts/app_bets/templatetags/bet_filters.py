from django import template

register = template.Library()

@register.filter
def thousand_separator(value):
    """Добавляет пробел как разделитель тысяч и округляет до целого (для сумм)"""
    try:
        value = float(value)
        # Округляем до целого
        rounded = round(value)
        # Добавляем пробелы между тысячами
        return f"{rounded:,}".replace(",", " ")
    except (ValueError, TypeError):
        return value

@register.filter
def round_to_hundreds(value):
    """Округляет число до сотен (например, 5742 -> 5700, 6858 -> 6900)"""
    try:
        value = float(value)
        rounded = round(value / 100) * 100
        return f"{rounded:,}".replace(",", " ")
    except (ValueError, TypeError):
        return value

@register.filter
def format_percent(value):
    """Форматирует процент с одним знаком после запятой, без разделителей тысяч"""
    try:
        value = float(value)
        return f"{value:.1f}"
    except (ValueError, TypeError):
        return value