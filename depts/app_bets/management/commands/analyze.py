from django.core.management.base import BaseCommand
from app_bets.models import Match, Team, TeamAlias, League, Season
from django.utils import timezone
from decimal import Decimal
from django.db.models import Q


class Command(BaseCommand):
    help = 'Анализ будущего матча по названиям команд и коэффициентам'

    def add_arguments(self, parser):
        parser.add_argument('--home', type=str, required=True, help='Хозяева')
        parser.add_argument('--away', type=str, required=True, help='Гости')
        parser.add_argument('--h_odd', type=float, required=True, help='Кэф на П1')
        parser.add_argument('--d_odd', type=float, required=True, help='Кэф на Х')
        parser.add_argument('--a_odd', type=float, required=True, help='Кэф на П2')
        parser.add_argument('--league_id', type=int, required=True, help='ID лиги')

    def get_team_smart(self, name):
        """Исправленный метод: определяем clean_alias перед использованием"""
        if not name:
            return None
        # Вот эта строка должна быть обязательно:
        clean_alias = " ".join(name.split()).lower()

        alias = TeamAlias.objects.filter(name=clean_alias).select_related('team').first()
        return alias.team if alias else None

    def handle(self, *args, **options):
        home_team = self.get_team_smart(options['home'])
        away_team = self.get_team_smart(options['away'])
        league = League.objects.filter(pk=options['league_id']).first()
        # Берем последний созданный сезон как текущий
        season = Season.objects.order_by('-start_date').first()

        if not home_team or not away_team or not league:
            self.stdout.write(self.style.ERROR("Ошибка: Проверь названия команд или ID лиги."))
            return

        # Создаем виртуальный объект матча для вызова его методов
        match = Match(
            home_team=home_team,
            away_team=away_team,
            league=league,
            season=season,
            date=timezone.now(),
            odds_home=Decimal(str(options['h_odd'])),
            odds_draw=Decimal(str(options['d_odd'])),
            odds_away=Decimal(str(options['a_odd'])),
        )

        self.stdout.write(self.style.SUCCESS(f"\n=== АНАЛИЗ: {home_team.name} - {away_team.name} ==="))

        # 1. Близнецы
        self.stdout.write(self.style.HTTP_INFO("--- [1] БЛИЗНЕЦЫ (ПО СТРАНЕ) ---"))
        twins = match.get_twins()
        if twins:
            for t in twins[:5]:
                self.stdout.write(
                    f"  {t.date.strftime('%Y')} | {t.home_score_reg}:{t.away_score_reg} | Кэфы: {t.odds_home}-{t.odds_draw}-{t.odds_away}")
        else:
            self.stdout.write("  Нет совпадений.")

        # 2. Пуассон
        self.stdout.write(self.style.HTTP_INFO("\n--- [2] ПУАССОН ---"))
        poisson_data = match.calculate_poisson_lambda()
        if isinstance(poisson_data, dict):
            probs = match.get_poisson_probabilities()
            top_scores = sorted(probs.items(), key=lambda x: x[1], reverse=True)[:3]
            self.stdout.write(f"  Ожидание голов: {poisson_data['home_lambda']} - {poisson_data['away_lambda']}")
            self.stdout.write(f"  Топ счета: {', '.join([f'{k} ({v}%)' for k, v in top_scores])}")
        else:
            self.stdout.write(f"  {poisson_data}")

        # 3. Исторический шаблон
        self.stdout.write(self.style.HTTP_INFO("\n--- [3] ИСТОРИЧЕСКИЙ ШАБЛОН ---"))
        pattern = match.get_historical_pattern_report()
        if isinstance(pattern, dict):
            self.stdout.write(f"  Форма: {pattern['pattern']}")
            self.stdout.write(f"  Найдено: {pattern['matches_count']} матчей")
            self.stdout.write(
                f"  Исходы: П1 {pattern['outcomes']['P1']}% | X {pattern['outcomes']['X']}% | П2 {pattern['outcomes']['P2']}%")
        else:
            self.stdout.write(f"  {pattern}")

        self.stdout.write(self.style.SUCCESS("\n=== АНАЛИЗ ЗАВЕРШЕН ===\n"))