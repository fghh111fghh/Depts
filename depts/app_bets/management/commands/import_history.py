import os
import csv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.core.exceptions import ValidationError

from app_bets.models import Match, Team, League, Season, Sport, Country, TeamAlias


class Command(BaseCommand):
    help = 'Импорт исторических данных из football_history_db_in_file.csv'

    def add_arguments(self, parser):
        parser.add_argument(
            'file_path',
            type=str,
            help='Путь к файлу CSV'
        )
        parser.add_argument(
            '--delimiter',
            type=str,
            default=';',
            help='Разделитель в CSV файле (по умолчанию ;)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Ограничить количество импортируемых строк (0 - без ограничений)'
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            help='Пропускать строки с ошибками и продолжать импорт'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Размер пачки для массового создания (по умолчанию 1000)'
        )

    def handle(self, *args, **options):
        file_path = options['file_path']
        delimiter = options['delimiter']
        limit = options['limit']
        skip_errors = options['skip_errors']
        batch_size = options['batch_size']

        if not os.path.exists(file_path):
            self.stderr.write(f"❌ Файл не найден: {file_path}")
            return

        self.stdout.write(f"📁 Импорт из: {file_path}")
        self.stdout.write(f"📊 Разделитель: '{delimiter}'")
        if limit > 0:
            self.stdout.write(f"⏱  Ограничение: {limit} строк")
        if skip_errors:
            self.stdout.write("⚠️  Режим пропуска ошибок включен")
        self.stdout.write(f"📦 Размер пачки: {batch_size}")

        # Сначала анализируем файл
        headers = self.analyze_file(file_path, delimiter)

        if not headers:
            self.stderr.write("❌ Не удалось прочитать заголовки")
            return

        # Подготовка данных
        div_mapping = self.get_div_mapping()
        sport = Sport.objects.get(name='football')

        # Импортируем
        stats = self.import_file(
            file_path, delimiter, headers, div_mapping,
            sport, limit, skip_errors, batch_size
        )

        # Вывод статистики
        self.stdout.write(f"\n{'=' * 60}")
        self.stdout.write("🎉 ИМПОРТ ЗАВЕРШЕН")
        self.stdout.write(f"{'=' * 60}")
        self.stdout.write(f"📊 СТАТИСТИКА:")
        self.stdout.write(f"  Всего строк в файле: {stats['total_rows']}")
        self.stdout.write(f"  Обработано строк: {stats['processed']}")
        self.stdout.write(f"  Успешно: {stats['added']}")
        self.stdout.write(f"  Дубликатов (пропущено): {stats['duplicates']}")
        self.stdout.write(f"  Ошибок: {stats['errors']}")
        self.stdout.write(f"  Создано команд: {stats['teams_created']}")
        self.stdout.write(f"  Создано псевдонимов: {stats['aliases_created']}")
        self.stdout.write(f"  Создано лиг: {stats['leagues_created']}")
        self.stdout.write(f"  Создано сезонов: {stats['seasons_created']}")

        self.stdout.write(f"\n✅ База данных обновлена!")

    def analyze_file(self, file_path, delimiter):
        """Анализирует файл и возвращает заголовки."""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                # Читаем первую строку
                first_line = f.readline().strip()
                headers = first_line.split(delimiter)

                self.stdout.write(f"📋 Заголовков: {len(headers)}")

                # Читаем первые 3 строки для проверки формата
                f.seek(0)
                reader = csv.reader(f, delimiter=delimiter)
                rows = []
                for i, row in enumerate(reader):
                    if i >= 3:
                        break
                    rows.append(row)

                if len(rows) >= 2:
                    self.stdout.write("📝 Проверка формата данных:")
                    self.stdout.write(f"  Заголовки: {rows[0][:8]}...")
                    self.stdout.write(f"  Строка 1: {rows[1][:8]}...")
                    if len(rows) > 2:
                        self.stdout.write(f"  Строка 2: {rows[2][:8]}...")

                # Проверяем ключевые поля
                required_fields = ['Div', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
                missing_fields = [field for field in required_fields if field not in headers]

                if missing_fields:
                    self.stderr.write(f"⚠️ Отсутствуют поля: {missing_fields}")
                    self.stdout.write(f"🔍 Доступные поля (первые 15): {headers[:15]}")
                    return None

                self.stdout.write(f"✅ Все необходимые поля присутствуют")
                return headers

        except Exception as e:
            self.stderr.write(f"❌ Ошибка анализа файла: {e}")
            return None

    def import_file(self, file_path, delimiter, headers, div_mapping, sport, limit, skip_errors, batch_size):
        """Импортирует файл."""
        stats = {
            'total_rows': 0,
            'processed': 0,
            'added': 0,
            'duplicates': 0,
            'errors': 0,
            'teams_created': 0,
            'aliases_created': 0,
            'leagues_created': 0,
            'seasons_created': 0
        }

        # Кэш для ускорения работы
        leagues_cache = {}
        countries_cache = {}
        seasons_cache = {}

        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=headers)
            next(reader)  # Пропускаем заголовки

            batch_matches = []

            for i, row in enumerate(reader, 1):
                if limit > 0 and stats['processed'] >= limit:
                    self.stdout.write(f"⏱  Достигнут лимит {limit} строк")
                    break

                stats['processed'] += 1
                stats['total_rows'] += 1

                # Показываем прогресс каждые 1000 строк
                if stats['processed'] % 1000 == 0:
                    self.stdout.write(f"  📊 Обработано: {stats['processed']} строк...")

                try:
                    # Обработка строки
                    match_data = self.process_row(
                        row, div_mapping, sport, i,
                        leagues_cache, countries_cache, seasons_cache
                    )

                    if match_data:
                        if match_data['status'] == 'duplicate':
                            stats['duplicates'] += 1
                        elif match_data['status'] == 'ready':
                            batch_matches.append(match_data['match_obj'])
                            stats['added'] += 1

                            # Сохраняем пачками
                            if len(batch_matches) >= batch_size:
                                self.save_batch(batch_matches)
                                batch_matches = []

                    else:
                        stats['errors'] += 1

                    if match_data and match_data.get('team_created'):
                        stats['teams_created'] += 1
                    if match_data and match_data.get('alias_created'):
                        stats['aliases_created'] += 1
                    if match_data and match_data.get('league_created'):
                        stats['leagues_created'] += 1
                    if match_data and match_data.get('season_created'):
                        stats['seasons_created'] += 1

                except Exception as e:
                    stats['errors'] += 1
                    if stats['errors'] <= 10:  # Показываем только первые 10 ошибок
                        self.stdout.write(f"❌ Строка {i}: {str(e)[:100]}")

                    if not skip_errors and stats['errors'] > 10:
                        self.stdout.write(f"⚠️  Слишком много ошибок. Используйте --skip-errors для продолжения")
                        break

                    continue

            # Сохраняем оставшиеся матчи
            if batch_matches:
                self.save_batch(batch_matches)

        return stats

    def process_row(self, row, div_mapping, sport, line_num,
                    leagues_cache, countries_cache, seasons_cache):
        """Обрабатывает одну строку."""
        try:
            # Извлекаем данные
            div_code = row.get('Div', '').strip()
            home_team_raw = row.get('HomeTeam', '').strip()
            away_team_raw = row.get('AwayTeam', '').strip()
            date_str = row.get('Date', '').strip()
            time_str = row.get('Time', '').strip() or '15:00'

            # Проверка обязательных полей
            if not div_code or not home_team_raw or not away_team_raw or not date_str:
                raise ValueError("Отсутствуют обязательные поля")

            # Получаем информацию о лиге
            league_info = div_mapping.get(div_code)
            if not league_info:
                raise ValueError(f"Неизвестный код лиги: {div_code}")

            # Получаем или создаем страну (с кэшированием)
            country_name = league_info['country']
            if country_name in countries_cache:
                country = countries_cache[country_name]
            else:
                country, created = Country.objects.get_or_create(name=country_name)
                countries_cache[country_name] = country

            # Получаем или создаем лигу (с кэшированием)
            cache_key = f"{div_code}_{country.id}"
            if cache_key in leagues_cache:
                league = leagues_cache[cache_key]
                league_created = False
            else:
                # Ищем лигу по названию и стране
                league_name = league_info['league']
                league = League.objects.filter(
                    name=league_name,
                    country=country,
                    sport=sport
                ).first()

                if not league:
                    # Создаем лигу с external_id
                    league = League.objects.create(
                        name=league_name,
                        sport=sport,
                        country=country,
                        external_id=div_code
                    )
                    league_created = True
                else:
                    # Обновляем external_id если его нет
                    if not league.external_id:
                        league.external_id = div_code
                        league.save()
                    league_created = False

                leagues_cache[cache_key] = league

            # Ищем или создаем команды
            home_team, team_created_home = self.get_or_create_team(
                home_team_raw, sport, country, line_num
            )
            away_team, team_created_away = self.get_or_create_team(
                away_team_raw, sport, country, line_num
            )

            # Парсим дату
            match_datetime = self.parse_date(date_str, time_str)
            if not match_datetime:
                raise ValueError(f"Неверный формат даты: {date_str} {time_str}")

            # Определяем сезон на основе даты
            season, season_created = self.get_season_from_date(
                match_datetime, seasons_cache
            )

            # Парсим счет
            home_score = self.parse_score(row.get('FTHG'))
            away_score = self.parse_score(row.get('FTAG'))

            # Проверяем на дубликат
            duplicate = self.check_duplicate(
                league, home_team, away_team, match_datetime, season
            )
            if duplicate:
                return {
                    'status': 'duplicate',
                    'team_created': team_created_home or team_created_away,
                    'league_created': league_created,
                    'season_created': season_created
                }

            # Парсим коэффициенты с округлением
            odds_home = self.parse_and_round_odds(row.get('B365H'), '2.00')
            odds_draw = self.parse_and_round_odds(row.get('B365D'), '3.50') if sport.has_draw else None
            odds_away = self.parse_and_round_odds(row.get('B365A'), '2.00')

            # Парсим дополнительные данные
            round_number = self.parse_round(row.get('Round'))

            # Создаем объект матча (но не сохраняем сразу)
            match = Match(
                home_team=home_team,
                away_team=away_team,
                date=match_datetime,
                home_score_reg=home_score,
                away_score_reg=away_score,
                home_score_final=home_score,
                away_score_final=away_score,
                league=league,
                season=season,
                finish_type='REG',
                odds_home=odds_home,
                odds_draw=odds_draw,
                odds_away=odds_away,
                round_number=round_number,
            )

            # Показываем созданный матч (первые 5)
            if line_num <= 5:
                self.stdout.write(f"✅ Строка {line_num}: Матч подготовлен: {home_team.name} vs {away_team.name}")

            return {
                'status': 'ready',
                'match_obj': match,
                'team_created': team_created_home or team_created_away,
                'league_created': league_created,
                'season_created': season_created,
                'alias_created': True if team_created_home or team_created_away else False
            }

        except Exception as e:
            if line_num <= 10:  # Показываем ошибки первых 10 строк
                self.stdout.write(f"❌ Строка {line_num}: {str(e)[:100]}")
            raise

    def save_batch(self, matches):
        """Сохраняет пачку матчей с обработкой ошибок."""
        if not matches:
            return

        saved_count = 0
        error_count = 0

        for match in matches:
            try:
                # Вызываем clean для валидации
                match.clean()
                match.save()
                saved_count += 1
            except ValidationError as e:
                error_count += 1
                if error_count <= 5:  # Показываем только первые 5 ошибок
                    self.stdout.write(f"⚠️  Ошибка валидации матча: {e}")
                # Пробуем сохранить без валидации (только для критичных полей)
                try:
                    match.save(force_insert=True)
                    saved_count += 1
                except:
                    pass
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    self.stdout.write(f"⚠️  Ошибка сохранения матча: {e}")

        if saved_count > 0:
            self.stdout.write(f"  💾 Сохранено {saved_count} матчей")
        if error_count > 0:
            self.stdout.write(f"  ⚠️  Ошибок при сохранении: {error_count}")

    def get_div_mapping(self):
        """Возвращает маппинг Div кодов."""
        return {
            'E0': {'league': 'АПЛ', 'country': 'Англия'},
            'E1': {'league': 'Чемпионшип', 'country': 'Англия'},
            'E2': {'league': 'Лига 1', 'country': 'Англия'},
            'E3': {'league': 'Лига 2', 'country': 'Англия'},

            'D1': {'league': 'Бундеслига', 'country': 'Германия'},
            'D2': {'league': 'Бундеслига 2', 'country': 'Германия'},

            'I1': {'league': 'Серия А', 'country': 'Италия'},
            'I2': {'league': 'Серия Б', 'country': 'Италия'},

            'SP1': {'league': 'Ла Лига', 'country': 'Испания'},
            'SP2': {'league': 'Сегунда', 'country': 'Испания'},

            'F1': {'league': 'Лига 1', 'country': 'Франция'},
            'F2': {'league': 'Лига 2', 'country': 'Франция'},

            'N1': {'league': 'Эредивизи', 'country': 'Нидерланды'},

            'B1': {'league': 'Жюпиле Лига', 'country': 'Бельгия'},
            'P1': {'league': 'Примейра Лига', 'country': 'Португалия'},
            'T1': {'league': 'Суперлига', 'country': 'Турция'},
            'SC0': {'league': 'Премьершип', 'country': 'Шотландия'},
            'SC1': {'league': 'Чемпионшип', 'country': 'Шотландия'},
            'SC2': {'league': 'Лига 1', 'country': 'Шотландия'},
            'SC3': {'league': 'Лига 2', 'country': 'Шотландия'},
        }

    def get_or_create_team(self, team_name, sport, country, line_num):
        """Ищет или создает команду."""
        if not team_name:
            return None, False

        team_name_clean = team_name.strip()

        # 1. Ищем по псевдонимам (приводим к нижнему регистру)
        cleaned_name = " ".join(team_name_clean.split()).lower()

        alias = TeamAlias.objects.filter(
            name=cleaned_name
        ).select_related('team').first()

        if alias and alias.team.sport == sport and alias.team.country == country:
            return alias.team, False

        # 2. Ищем по точному названию
        team = Team.objects.filter(
            name__iexact=team_name_clean,
            sport=sport,
            country=country
        ).first()

        if team:
            # Создаем псевдоним для будущего поиска
            if not TeamAlias.objects.filter(name=cleaned_name, team=team).exists():
                TeamAlias.objects.create(name=cleaned_name, team=team)
            return team, False

        # 3. Пробуем найти по частичному совпадению
        search_name = team_name_clean.replace(' ', '').replace('.', '').replace('-', '').lower()
        teams = Team.objects.filter(
            sport=sport,
            country=country
        )

        # Ищем вручную
        for t in teams:
            team_name_simple = t.name.replace(' ', '').replace('.', '').replace('-', '').lower()
            if team_name_simple == search_name:
                # Создаем псевдоним
                if not TeamAlias.objects.filter(name=cleaned_name, team=t).exists():
                    TeamAlias.objects.create(name=cleaned_name, team=t)
                return t, False

        # 4. Создаем новую команду
        try:
            team = Team.objects.create(
                name=team_name_clean,
                sport=sport,
                country=country
            )

            # Создаем псевдоним
            TeamAlias.objects.create(
                name=cleaned_name,
                team=team
            )

            return team, True

        except Exception as e:
            # Создаем команду с уникальным именем
            unique_name = f"{team_name_clean}_{country.id}"
            team = Team.objects.create(
                name=unique_name,
                sport=sport,
                country=country
            )
            TeamAlias.objects.create(
                name=cleaned_name,
                team=team
            )
            return team, True

    def parse_date(self, date_str, time_str):
        """Парсит дату в разных форматах."""
        if not date_str:
            return None

        # Стандартные форматы даты
        date_formats = [
            '%d/%m/%Y',  # 06/08/2004
            '%d.%m.%Y',  # 06.08.2004
            '%d/%m/%y',  # 06/08/04
            '%Y-%m-%d',  # 2004-08-06
            '%d/%m',  # 06/08 (без года)
        ]

        date_part = None

        for fmt in date_formats:
            try:
                date_part = datetime.strptime(date_str, fmt)
                # Если год не указан (формат %d/%m), добавляем 2004 для исторических данных
                if fmt == '%d/%m':
                    date_part = date_part.replace(year=2004)
                break
            except ValueError:
                continue

        if not date_part:
            return None

        # Обработка времени
        time_part = None
        if time_str and ':' in time_str:
            try:
                time_part = datetime.strptime(time_str, '%H:%M').time()
            except ValueError:
                try:
                    time_part = datetime.strptime(time_str, '%H.%M').time()
                except ValueError:
                    try:
                        time_part = datetime.strptime(time_str, '%H:%M:%S').time()
                    except ValueError:
                        time_part = None

        # Объединяем дату и время
        if time_part:
            match_datetime = datetime.combine(date_part.date(), time_part)
        else:
            match_datetime = datetime.combine(date_part.date(), datetime.strptime('15:00', '%H:%M').time())

        return timezone.make_aware(match_datetime)

    def get_season_from_date(self, match_datetime, seasons_cache):
        """Определяет сезон на основе даты."""
        year = match_datetime.year
        month = match_datetime.month

        # Для футбола: сезон обычно с июля по июнь
        if month >= 7:  # Июль-декабрь
            season_name = f"{year}/{year + 1}"
            start_date = datetime(year, 7, 1).date()
            end_date = datetime(year + 1, 6, 30).date()
        else:  # Январь-июнь
            season_name = f"{year - 1}/{year}"
            start_date = datetime(year - 1, 7, 1).date()
            end_date = datetime(year, 6, 30).date()

        # Проверяем кэш
        if season_name in seasons_cache:
            return seasons_cache[season_name], False

        # Ищем существующий сезон
        season = Season.objects.filter(name=season_name).first()
        season_created = False

        if not season:
            # Создаем сезон если его нет
            season = Season.objects.create(
                name=season_name,
                is_current=False,
                start_date=start_date,
                end_date=end_date
            )
            season_created = True

        seasons_cache[season_name] = season
        return season, season_created

    def parse_score(self, score_str):
        """Парсит счет."""
        if not score_str:
            return 0

        try:
            # Убираем .0 если есть
            score_clean = str(score_str).replace('.0', '')
            if not score_clean:
                return 0
            return int(float(score_clean))
        except (ValueError, TypeError):
            return 0

    def parse_and_round_odds(self, value, default):
        """Парсит коэффициенты и округляет до 2 знаков после запятой."""
        try:
            if value and str(value).strip():
                value_str = str(value).strip().replace(',', '.')
                # Округляем до 2 знаков после запятой
                odds = Decimal(value_str).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                if odds < Decimal('1.01'):
                    return Decimal(default).quantize(Decimal('0.01'))
                return odds
        except:
            pass

        return Decimal(default).quantize(Decimal('0.01'))

    def parse_round(self, round_str):
        """Парсит номер тура."""
        if not round_str:
            return None

        try:
            return int(float(round_str))
        except:
            return None

    def check_duplicate(self, league, home_team, away_team, match_datetime, season):
        """Проверяет, существует ли уже такой матч."""
        # Ищем матч с теми же командами в тот же день в той же лиге
        duplicates = Match.objects.filter(
            league=league,
            home_team=home_team,
            away_team=away_team,
            date__date=match_datetime.date(),
            season=season
        ).exists()

        return duplicates