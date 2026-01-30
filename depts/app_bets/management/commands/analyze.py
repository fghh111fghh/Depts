from django.core.management.base import BaseCommand
from app_bets.models import Match, TeamAlias, League, Season
from django.utils import timezone
from decimal import Decimal
from django.db.models import Q

class Command(BaseCommand):
    help = 'Полный анализ матча: Близнецы, Пуассон, H2H, Шаблоны'

    def add_arguments(self, parser):
        parser.add_argument('--home', type=str, required=True)
        parser.add_argument('--away', type=str, required=True)
        parser.add_argument('--h_odd', type=float, required=True)
        parser.add_argument('--d_odd', type=float, required=True)
        parser.add_argument('--a_odd', type=float, required=True)
        parser.add_argument('--league_id', type=int, required=True)

    def get_team_smart(self, name):
        clean_alias = " ".join(name.split()).lower()
        alias = TeamAlias.objects.filter(name=clean_alias).select_related('team').first()
        return alias.team if alias else None

    def handle(self, *args, **options):
        home_team = self.get_team_smart(options['home'])
        away_team = self.get_team_smart(options['away'])
        league = League.objects.filter(pk=options['league_id']).first()
        season = Season.objects.order_by('-start_date').first()

        if not home_team or not away_team or not league:
            self.stdout.write(self.style.ERROR("Ошибка: Команды или Лига не найдены."))
            return

        match = Match(
            home_team=home_team, away_team=away_team, league=league, season=season,
            date=timezone.now(),
            odds_home=Decimal(str(options['h_odd'])),
            odds_draw=Decimal(str(options['d_odd'])),
            odds_away=Decimal(str(options['a_odd'])),
        )

        self.stdout.write(self.style.SUCCESS(f"\n=== АНАЛИЗ: {home_team.name} - {away_team.name} ==="))

        # 1. БЛИЗНЕЦЫ
        self.stdout.write(self.style.MIGRATE_LABEL("--- [1] БЛИЗНЕЦЫ (ПО СТРАНЕ) ---"))
        twins = match.get_twins()
        if twins.exists():
            for t in twins[:10]:
                self.stdout.write(f"  {t.date.strftime('%d.%m.%Y')} | {t.home_team.name} {t.home_score_reg}:{t.away_score_reg} {t.away_team.name} | Кэфы: {t.odds_home}-{t.odds_draw}-{t.odds_away}")
        else:
            self.stdout.write("  Близнецы не найдены.")

        # 2. ПУАССОН
        self.stdout.write(self.style.MIGRATE_LABEL("\n--- [2] ПУАССОН ---"))
        res = match.calculate_poisson_lambda()
        if isinstance(res, dict):
            probs = match.get_poisson_probabilities()
            top = sorted(probs.items(), key=lambda x: x[1], reverse=True)[:3]
            self.stdout.write(f"  Ожидание: {res['home_lambda']} - {res['away_lambda']}")
            self.stdout.write(f"  Вероятные счета: {', '.join([f'{k} ({v}%)' for k, v in top])}")
        else:
            self.stdout.write(f"  {res}")

        # 3. H2H (Личные встречи)
        self.stdout.write(self.style.MIGRATE_LABEL("\n--- [3] H2H (ЛИЧНЫЕ ВСТРЕЧИ) ---"))
        h2h = Match.objects.filter(
            (Q(home_team=home_team) & Q(away_team=away_team)) |
            (Q(home_team=away_team) & Q(away_team=home_team)),
            home_score_reg__isnull=False
        ).order_by('-date')
        if h2h.exists():
            for m in h2h:
                self.stdout.write(f"  {m.date.strftime('%d.%m.%Y')}: {m.home_team.name} {m.home_score_reg}:{m.away_score_reg} {m.away_team.name}")
        else:
            self.stdout.write("  Встреч не найдено.")

        # 4. ИСТОРИЧЕСКИЙ ШАБЛОН
        self.stdout.write(self.style.MIGRATE_LABEL("\n--- [4] ИСТОРИЧЕСКИЙ ШАБЛОН ---"))
        pattern = match.get_historical_pattern_report()
        if isinstance(pattern, dict):
            self.stdout.write(f"  Найдено: {pattern['matches_count']} матчей")
            for h_game in pattern['history']:
                self.stdout.write(f"    - {h_game}")
            self.stdout.write(f"  Итог: П1 {pattern['outcomes']['P1']}% | X {pattern['outcomes']['X']}% | П2 {pattern['outcomes']['P2']}%")
        else:
            self.stdout.write(f"  {pattern}")

        # # 5. ВЕКТОРНЫЙ СИНТЕЗ
        # self.stdout.write(self.style.MIGRATE_LABEL("\n--- [5] ИТОГОВЫЙ ВЕКТОР (СИНТЕЗ) ---"))
        # verdict = match.get_vector_synthesis()
        # self.stdout.write(self.style.SUCCESS(f"  ВЕРДИКТ: {verdict}"))

        self.stdout.write(self.style.SUCCESS("\n=== АНАЛИЗ ЗАВЕРШЕН ===\n"))