# debug_import.py
import csv
import os
import sys
import django
from datetime import datetime
from decimal import Decimal

# Настройка Django
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'depts.settings')
django.setup()

from django.utils.timezone import make_aware
from app_bets.models import Match, TeamAlias, Season, League, Team, Country


class DebugImporter:
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

    def get_team_by_alias(self, name):
        """Ищем команду - УПРОЩЕННАЯ ВЕРСИЯ"""
        if not name:
            print(f"    ❌ Пустое название команды")
            return None

        print(f"    Поиск команды: '{name}'")

        # 1. Очищаем название
        clean_name = " ".join(name.split()).lower()
        print(f"    Очищенное: '{clean_name}'")

        # 2. Ищем в алиасах
        alias = TeamAlias.objects.filter(name=clean_name).select_related('team').first()
        if alias:
            print(f"    ✅ Найдена в алиасах: {alias.team.name}")
            return alias.team

        # 3. Ищем в основной таблице
        team = Team.objects.filter(name__iexact=clean_name).first()
        if team:
            print(f"    ✅ Найдена в Team: {team.name}")
            return team

        # 4. Ищем по частичному совпадению
        team_partial = Team.objects.filter(name__icontains=clean_name).first()
        if team_partial:
            print(f"    ⚠️ Частичное совпадение: {team_partial.name}")
            return team_partial

        print(f"    ❌ Команда не найдена")
        return None

    def get_season_by_date(self, dt):
        """Находим сезон по дате"""
        season = Season.objects.filter(start_date__lte=dt.date(), end_date__gte=dt.date()).first()
        if season:
            print(f"    ✅ Сезон найден: {season.name}")
        else:
            print(f"    ❌ Сезон не найден для даты {dt.date()}")
        return season

    def parse_score(self, val):
        """Безопасный парсинг счета"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return 0
        return int(float(str(val).replace(',', '.')))

    def parse_odd(self, val):
        """Безопасный парсинг коэффициента"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return Decimal('1.01')
        try:
            return Decimal(str(val).replace(',', '.')).quantize(Decimal('0.01'))
        except:
            return Decimal('1.01')

    def run(self, file_path):
        print(f"\n{'=' * 100}")
        print(f"ЗАПУСК ДЕБАГ ИМПОРТА ДЛЯ ФАЙЛА: {file_path}")
        print(f"{'=' * 100}")

        count = 0
        skipped_teams = 0
        errors = 0

        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, 1):
                print(f"\n{'=' * 80}")
                print(f"СТРОКА {row_num}")
                print(f"{'=' * 80}")

                try:
                    # 1. Поиск лиги
                    div_code = row.get('Div')
                    print(f"1. Div код: '{div_code}'")

                    league_name = self.DIV_TO_LEAGUE_NAME.get(div_code)
                    if not league_name:
                        print(f"   ❌ Лига не найдена для Div={div_code}")
                        errors += 1
                        continue

                    print(f"   ✅ Название лиги: {league_name}")

                    # Ищем лигу в базе
                    league = League.objects.filter(name=league_name).first()
                    if not league:
                        print(f"   ❌ Лига '{league_name}' не найдена в базе данных!")
                        errors += 1
                        continue

                    print(f"   ✅ Лига найдена в базе: {league.name} (ID: {league.id})")

                    # 2. Дата и сезон
                    date_str = row['Date'].strip()
                    print(f"2. Дата строка: '{date_str}'")

                    try:
                        dt = datetime.strptime(date_str, '%d/%m/%Y')
                    except ValueError:
                        dt = datetime.strptime(date_str, '%d/%m/%y')

                    print(f"   ✅ Дата парсится: {dt}")

                    season = self.get_season_by_date(dt)
                    if not season:
                        errors += 1
                        continue

                    # 3. Поиск команд
                    home_team_raw = row['HomeTeam']
                    away_team_raw = row['AwayTeam']

                    print(f"3. Поиск команд:")
                    print(f"   HomeTeam: '{home_team_raw}'")
                    print(f"   AwayTeam: '{away_team_raw}'")

                    home_team = self.get_team_by_alias(home_team_raw)
                    away_team = self.get_team_by_alias(away_team_raw)

                    if not home_team:
                        print(f"   ❌ Домашняя команда не найдена: {home_team_raw}")
                        skipped_teams += 1
                        continue

                    if not away_team:
                        print(f"   ❌ Гостевая команда не найдена: {away_team_raw}")
                        skipped_teams += 1
                        continue

                    print(f"   ✅ Команды найдены:")
                    print(f"      Домашняя: {home_team.name}")
                    print(f"      Гостевая: {away_team.name}")

                    # 4. Проверка дубликатов
                    dt_aware = make_aware(dt)
                    existing = Match.objects.filter(
                        date=dt_aware,
                        home_team=home_team,
                        away_team=away_team
                    ).exists()

                    if existing:
                        print(f"4. ⚠️ Матч уже существует, пропускаем")
                        continue

                    print(f"4. ✅ Матч не существует, создаем новый")

                    # 5. Парсинг коэффициентов и голов
                    odd_h = self.parse_odd(row.get('B365H'))
                    odd_d = self.parse_odd(row.get('B365D'))
                    odd_a = self.parse_odd(row.get('B365A'))

                    h_goal = self.parse_score(row['FTHG'])
                    a_goal = self.parse_score(row['FTAG'])

                    print(f"5. Данные матча:")
                    print(f"   Счет: {h_goal}:{a_goal}")
                    print(f"   Коэфы: {odd_h}/{odd_d}/{odd_a}")

                    # 6. Сохранение (ТОЛЬКО ДЛЯ ДЕБАГА - не сохраняем!)
                    print(f"6. 🚫 ТЕСТОВЫЙ РЕЖИМ - сохранение отключено")
                    # Match.objects.create(...)

                    count += 1
                    print(f"✅ Строка {row_num} обработана успешно")

                    # Останавливаемся после 3 строк для теста
                    if count >= 3:
                        print(f"\n🚫 Останавливаемся после 3 строк для теста")
                        break

                except Exception as e:
                    errors += 1
                    print(f"❌ ОШИБКА в строке {row_num}: {e}")
                    import traceback
                    traceback.print_exc()

        print(f"\n{'=' * 100}")
        print(f"ИТОГ ДЕБАГА:")
        print(f"- Успешно обработано: {count}")
        print(f"- Пропущено (команды): {skipped_teams}")
        print(f"- Ошибок: {errors}")
        print(f"{'=' * 100}")


if __name__ == "__main__":
    importer = DebugImporter()
    importer.run('import_data/E0.csv')