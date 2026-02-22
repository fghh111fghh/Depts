from constants import site_texts


def debts_texts_processor(request):
    """
    Контекстный процессор для текстов приложения app_depts
    """
    return {
        'debt_stats_labels': site_texts.DEBTS_STATS_LABELS,
        'debt_buttons': site_texts.DEBTS_BUTTONS,
        'debt_card_texts': site_texts.DEBTS_CARD_TEXTS,
        'debt_modal_texts': site_texts.DEBTS_MODAL_TEXTS,
        'debt_empty_state': site_texts.DEBTS_EMPTY_STATE,
        'debt_pagination': site_texts.DEBTS_PAGINATION,
        'debt_detail_texts': site_texts.DEBT_DETAIL_TEXTS,
    }