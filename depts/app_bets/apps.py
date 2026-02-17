from django.apps import AppConfig


class AppBetsConfig(AppConfig):
    name = 'app_bets'

    def ready(self):
        import app_bets.signals  # noqa
