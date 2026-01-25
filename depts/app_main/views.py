from typing import Any, Dict
from django.views.generic import TemplateView


class IndexView(TemplateView):
    """
    Представление для главной страницы портала.

    Служит точкой входа и отображает приветственный интерфейс или
    общую навигацию по проектам (футбольный анализ, учет долгов и т.д.).
    """
    template_name: str = 'app_main/index.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Добавляет дополнительные данные в контекст главной страницы.
        """
        context: Dict[str, Any] = super().get_context_data(**kwargs)
        # Здесь можно будет добавить ссылки на модули или общую статистику
        context['page_title'] = "Главная страница управления"
        return context