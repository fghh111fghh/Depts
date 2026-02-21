from django.apps import AppConfig


class AppBetsConfig(AppConfig):
    name = 'app_bets'
    verbose_name = 'Ставки'

    def ready(self):
        import app_bets.signals  # noqa
