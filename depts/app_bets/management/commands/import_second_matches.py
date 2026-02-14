import os
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.timezone import make_aware, get_current_timezone

from app_bets.models import Match, TeamAlias, Season, League, Country, Sport
from app_bets.constants import ParsingConstants


class Command(BaseCommand):
    help = 'Импорт матчей из папки second_matches (структура: div_название_лиги_страна/сезон.csv)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            type=str,
            default='second_matches',
            help='Путь к папке с данными (по умолчанию: second_matches)'
        )
        parser.add_argument(
            '--create-leagues',
            action='store_true',
            help='Создавать новые лиги, если их нет в БД'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Пробный запуск без сохранения в БД'
        )

    @staticmethod
    def get_team_by_alias(name):
        """Поиск команды по псевдониму"""
        if not name or str(name).strip() == "":
            return None
        clean_alias = " ".join(name.split()).lower()
        alias = TeamAlias.objects.filter(name=clean_alias).select_related('team').first()
        return alias.team if alias else None

    @staticmethod
    def get_or_create_team(name, sport, country):
        """Создать новую команду, если не найдена"""
        from app_bets.models import Team, TeamAlias

        # Нормализуем имя
        clean_name = " ".join(name.split()).strip()
        if not clean_name:
            return None

        # Проверяем, может команда уже есть в TeamAlias
        team = Command.get_team_by_alias(clean_name)
        if team:
            return team

        # Проверяем, может команда уже есть в Team по точному названию
        team = Team.objects.filter(
            name__iexact=clean_name,
            sport=sport,
            country=country
        ).first()

        if team:
            # Создаем псевдоним для будущих импортов
            TeamAlias.objects.get_or_create(
                name=clean_name.lower(),
                defaults={'team': team}
            )
            return team

        # Создаем новую команду
        team = Team.objects.create(
            name=clean_name,
            sport=sport,
            country=country
        )
        # Создаем псевдоним
        TeamAlias.objects.create(
            name=clean_name.lower(),
            team=team
        )
        return team

    @staticmethod
    def get_season_by_date(dt):
        """Определяем сезон по дате матча"""
        return Season.objects.filter(
            start_date__lte=dt.date(),
            end_date__gte=dt.date()
        ).first()

    @staticmethod
    def parse_score(val):
        """Превращает строку в число голов"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return 0
        try:
            return int(float(str(val).replace(',', '.')))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def parse_odd(val):
        """Безопасно парсит коэффициент в Decimal"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return Decimal('1.01')
        try:
            return Decimal(str(val).replace(',', '.')).quantize(Decimal('0.01'))
        except (ValueError, TypeError, Decimal.InvalidOperation):
            return Decimal('1.01')

    @staticmethod
    def parse_folder_name(folder_name):
        """
        Разбирает имя папки формата "div_название_лиги_страна"
        Например: "B4_Высшая_лига_Малайзия" ->
            div_code = "B4"
            league_name = "Высшая лига"
            country_name = "Малайзия"
        """
        parts = folder_name.split('_')

        if len(parts) < 3:
            # Если меньше 3 частей, не можем корректно разобрать
            return None, folder_name, None

        # Первая часть - div_code
        div_code = parts[0]

        # Последняя часть - страна
        country_name = parts[-1]

        # Всё что между - название лиги (объединяем через пробел)
        league_name = ' '.join(parts[1:-1])

        return div_code, league_name, country_name

    def get_or_create_league(self, div_code, league_name, country_name, create_if_missing=False):
        """
        Получает или создает лигу по div коду, названию и стране
        """
        # 1. Сначала ищем по div_code (самый надёжный способ)
        if div_code:
            league = League.objects.filter(external_id=div_code).first()
            if league:
                return league, False

        # 2. Затем ищем по названию и стране (чтобы различать одинаковые названия в разных странах)
        if country_name:
            country = Country.objects.filter(name__iexact=country_name).first()
            if country:
                league = League.objects.filter(
                    name__iexact=league_name,
                    country=country
                ).first()
                if league:
                    return league, False

        # 3. Пытаемся найти по названию без страны (на всякий случай)
        league = League.objects.filter(name__iexact=league_name).first()
        if league:
            return league, False

        # 4. Если нужно создать новую лигу
        if create_if_missing and country_name:
            # Получаем или создаем страну
            country, _ = Country.objects.get_or_create(name=country_name)

            # Спорт - по умолчанию футбол
            sport = Sport.objects.filter(name=Sport.Name.FOOTBALL).first()
            if not sport:
                sport = Sport.objects.create(
                    name=Sport.Name.FOOTBALL,
                    has_draw=True
                )

            # Создаем лигу
            league = League.objects.create(
                name=league_name,
                sport=sport,
                country=country,
                external_id=div_code
            )
            return league, True

        return None, False

    def process_csv_file(self, file_path, folder_name, stats, create_leagues, dry_run):
        """Обрабатывает один CSV файл"""
        # Разбираем имя папки на div, лигу и страну
        div_code, league_name, country_name = self.parse_folder_name(folder_name)

        if not league_name or not country_name:
            self.stdout.write(self.style.WARNING(
                f"\n⚠️ Папка {folder_name} имеет неверный формат. "
                f"Ожидается: div_название_лиги_страна (например B4_Высшая_лига_Малайзия)"
            ))
            return

        season_name = os.path.basename(file_path).replace('.csv', '')

        self.stdout.write(f"\n📁 Обработка: {folder_name} / {season_name}")
        self.stdout.write(f"   ├─ Div: {div_code}")
        self.stdout.write(f"   ├─ Лига: {league_name}")
        self.stdout.write(f"   └─ Страна: {country_name}")

        # Пробуем разные кодировки
        encodings_to_try = ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251', 'latin-1', 'iso-8859-1']

        for encoding in encodings_to_try:
            try:
                with open(file_path, mode='r', encoding=encoding) as f:
                    # Пробуем прочитать первые несколько строк
                    sample = f.read(1024)
                    f.seek(0)
                    # Если дошли сюда - кодировка подходит
                    reader = csv.DictReader(f, delimiter=',')
                    self.stdout.write(f"   ✅ Используется кодировка: {encoding}")

                    # Обрабатываем строки
                    for row_num, row in enumerate(reader, start=1):
                        try:
                            self.process_row(row, stats, create_leagues, dry_run, div_code, league_name, country_name)

                            if stats['processed_matches'] % 100 == 0 and stats['processed_matches'] > 0:
                                self.stdout.write(f"  ✅ Обработано матчей: {stats['processed_matches']}")

                        except Exception as e:
                            stats['errors'] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"  ⚠️ Ошибка в строке {row_num}: {str(e)[:100]}"
                                )
                            )
                    break  # Прерываем цикл кодировок, если успешно прочитали

            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"   ❌ Ошибка при чтении файла: {e}"))
                break

    def process_row(self, row, stats, create_leagues, dry_run, div_code, league_name, country_name):
        """Обрабатывает одну строку CSV"""

        # 1. Получаем или создаем лигу
        league, is_new = self.get_or_create_league(
            div_code,
            league_name,
            country_name,
            create_if_missing=create_leagues
        )

        if not league:
            stats['skipped_league'] += 1
            return

        if is_new:
            stats['new_leagues'] += 1
            self.stdout.write(self.style.SUCCESS(
                f"   ✨ Создана новая лига: {league.name} ({league.country.name})"
            ))

        # 2. Дата и время
        date_str = row.get('Date', '').strip()
        time_str = row.get('Time', '12:00').strip()

        try:
            dt = datetime.strptime(f"{date_str} {time_str}", '%d/%m/%Y %H:%M')
        except ValueError:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", '%d/%m/%y %H:%M')
            except ValueError:
                # Если время не указано, берем только дату
                dt = datetime.strptime(date_str, '%d/%m/%Y')

        # 3. Сезон
        season = self.get_season_by_date(dt)
        if not season:
            # Если сезон не найден, пропускаем
            self.stdout.write(self.style.WARNING(
                f"  ⚠️ Сезон для даты {dt.date()} не найден, матч пропущен"
            ))
            return

        # 4. Команды
        home_team_name = row.get('HomeTeam', '').strip()
        away_team_name = row.get('AwayTeam', '').strip()

        home_team = self.get_team_by_alias(home_team_name)
        away_team = self.get_team_by_alias(away_team_name)

        # Если команды не найдены, пытаемся создать
        if (not home_team or not away_team) and create_leagues:
            if not home_team:
                home_team = self.get_or_create_team(
                    home_team_name,
                    league.sport,
                    league.country
                )
                if home_team:
                    stats['new_teams'] += 1
                    self.stdout.write(f"   ✨ Создана команда: {home_team_name}")
            if not away_team:
                away_team = self.get_or_create_team(
                    away_team_name,
                    league.sport,
                    league.country
                )
                if away_team:
                    stats['new_teams'] += 1
                    self.stdout.write(f"   ✨ Создана команда: {away_team_name}")

        if not home_team or not away_team:
            stats['skipped_teams'] += 1
            return

        # 5. Проверка на дубликат
        dt_aware = make_aware(dt, get_current_timezone())
        if Match.objects.filter(
                date=dt_aware,
                home_team=home_team,
                away_team=away_team
        ).exists():
            stats['duplicates'] += 1
            return

        # 6. Сбор коэффициентов
        odd_h = self.parse_odd(row.get('AvgH') or row.get('B365H') or row.get('PSH'))
        odd_d = self.parse_odd(row.get('AvgD') or row.get('B365D') or row.get('PSD'))
        odd_a = self.parse_odd(row.get('AvgA') or row.get('B365A') or row.get('PSA'))

        # 7. Счет
        h_goal = self.parse_score(row.get('FTHG', 0))
        a_goal = self.parse_score(row.get('FTAG', 0))

        # 8. Сохранение
        if not dry_run:
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

        stats['processed_matches'] += 1

    @transaction.atomic
    def handle(self, *args, **options):
        base_path = options['path']
        create_leagues = options['create_leagues']
        dry_run = options['dry_run']

        if not os.path.exists(base_path):
            self.stdout.write(self.style.ERROR(f"Папка {base_path} не найдена!"))
            return

        stats = {
            'total_files': 0,
            'processed_matches': 0,
            'skipped_teams': 0,
            'skipped_league': 0,
            'errors': 0,
            'new_leagues': 0,
            'new_teams': 0,
            'duplicates': 0
        }

        # Рекурсивно обходим все папки
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if not file.lower().endswith('.csv'):
                    continue

                file_path = os.path.join(root, file)

                # Получаем имя папки, в которой лежит файл
                folder_name = os.path.basename(root)

                stats['total_files'] += 1

                # Обрабатываем файл
                self.process_csv_file(file_path, folder_name, stats, create_leagues, dry_run)

        # Итоговый отчет
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
        self.stdout.write(self.style.SUCCESS("📊 ИТОГИ ИМПОРТА"))
        self.stdout.write("=" * 60)
        self.stdout.write(f"📁 Файлов обработано: {stats['total_files']}")
        self.stdout.write(f"⚽ Матчей добавлено: {stats['processed_matches']}")
        self.stdout.write(f"🔄 Дубликатов пропущено: {stats['duplicates']}")
        self.stdout.write(f"🏆 Новых лиг создано: {stats['new_leagues']}")
        self.stdout.write(f"👥 Новых команд создано: {stats['new_teams']}")
        self.stdout.write(f"⏭️ Пропущено (нет команд): {stats['skipped_teams']}")
        self.stdout.write(f"⏭️ Пропущено (нет лиги): {stats['skipped_league']}")
        self.stdout.write(f"❌ Ошибок: {stats['errors']}")

        if dry_run:
            self.stdout.write(self.style.WARNING("\n⚠️ Это был ПРОБНЫЙ запуск (dry-run). Данные не сохранены."))