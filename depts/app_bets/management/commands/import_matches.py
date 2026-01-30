import csv
from datetime import datetime
from decimal import Decimal
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.timezone import make_aware
from app_bets.models import Match, TeamAlias, Season, League


class Command(BaseCommand):
    help = 'Этап 3: Финальный импорт матчей (с защитой от ошибок формата)'

    # Маппинг лиг согласно твоим ID в базе
    DIV_TO_LEAGUE_NAME = {
        'E0': 'АПЛ',
        'E1': 'Чемпионшип',
        'D1': 'Бундеслига',
        'D2': 'Бундеслига 2',
        'SP1': 'Ла Лига',
        'SP2': 'Сегунда',
        'I1': 'Серия А',
        'I2': 'Серия Б',
        'F1': 'Лига 1',
        'F2': 'Лига 2',
        'RU1': 'РПЛ',
        'N1': 'Эредивизи',
    }

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str)

    def get_team_by_alias(self, name):
        # Здесь мы создаем переменную clean_alias
        clean_alias = " ".join(name.split()).lower()
        # И здесь же её используем
        alias = TeamAlias.objects.filter(name=clean_alias).select_related('team').first()
        return alias.team if alias else None

    def get_season_by_date(self, dt):
        return Season.objects.filter(start_date__lte=dt.date(), end_date__gte=dt.date()).first()

    def parse_score(self, val):
        """Превращает '2.0', '2' или '2,0' в целое число 2"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return 0
        return int(float(str(val).replace(',', '.')))

    def parse_odd(self, val):
        """Безопасно парсит коэффициент в Decimal"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return Decimal('1.01')
        try:
            return Decimal(str(val).replace(',', '.')).quantize(Decimal('0.01'))
        except:
            return Decimal('1.01')

    @transaction.atomic
    def handle(self, *args, **options):
        file_path = options['csv_file']
        count = 0
        skipped_teams = 0
        errors = 0

        self.stdout.write(self.style.SUCCESS(f"Запуск импорта матчей из {file_path}..."))

        with open(file_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=',')

            for row in reader:
                try:
                    # 1. Поиск лиги по названию
                    div_code = row.get('Div')
                    league_name = self.DIV_TO_LEAGUE_NAME.get(div_code)

                    if not league_name:
                        continue

                    # Ищем объект лиги в базе по имени
                    league = League.objects.filter(name=league_name).first()
                    if not league:
                        self.stdout.write(self.style.WARNING(f"Лига '{league_name}' не найдена в базе!"))
                        continue

                    # 2. Дата и Сезон
                    date_str = row['Date'].strip()
                    try:
                        dt = datetime.strptime(date_str, '%d/%m/%Y')
                    except ValueError:
                        dt = datetime.strptime(date_str, '%d/%m/%y')

                    season = self.get_season_by_date(dt)
                    if not season:
                        continue

                    # 3. Поиск команд
                    home_team = self.get_team_by_alias(row['HomeTeam'])
                    away_team = self.get_team_by_alias(row['AwayTeam'])

                    if not home_team or not away_team:
                        skipped_teams += 1
                        continue

                    # Проверка на дубликат (по дате и хозяевам)
                    dt_aware = make_aware(dt)
                    if Match.objects.filter(date=dt_aware, home_team=home_team).exists():
                        continue

                    # 4. Сбор коэффициентов (Приоритет: Avg -> B365 -> PS)
                    odd_h = self.parse_odd(row.get('AvgH') or row.get('B365H') or row.get('PSH'))
                    odd_d = self.parse_odd(row.get('AvgD') or row.get('B365D') or row.get('PSD'))
                    odd_a = self.parse_odd(row.get('AvgA') or row.get('B365A') or row.get('PSA'))

                    # 5. Сбор голов
                    h_goal = self.parse_score(row['FTHG'])
                    a_goal = self.parse_score(row['FTAG'])

                    # 6. Сохранение в БД
                    Match.objects.create(
                        season=season,
                        league=league,
                        date=dt_aware,
                        home_team=home_team,
                        away_team=away_team,
                        home_score_reg=h_goal,
                        away_score_reg=a_goal,
                        home_score_final=h_goal,
                        away_score_final=a_goal,
                        odds_home=odd_h,
                        odds_draw=odd_d,
                        odds_away=odd_a,
                        finish_type='REG'
                    )

                    count += 1
                    if count % 1000 == 0:
                        self.stdout.write(f"Успешно загружено: {count} матчей...")

                except Exception as e:
                    errors += 1
                    # Выводим предупреждение, но продолжаем цикл
                    self.stdout.write(
                        self.style.WARNING(f"Пропуск строки ({row.get('Date')} {row.get('HomeTeam')}): {e}"))

        self.stdout.write(self.style.SUCCESS(f"\nФИНАЛЬНЫЙ ОТЧЕТ:"))
        self.stdout.write(f"- Добавлено новых матчей: {count}")
        self.stdout.write(f"- Пропущено (не найдены команды): {skipped_teams}")
        self.stdout.write(f"- Ошибок в данных: {errors}")