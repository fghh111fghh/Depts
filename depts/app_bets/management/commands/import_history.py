import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from app_bets.models import Season


class Command(BaseCommand):
    help = 'Этап 1: Заполнение таблицы Season на основе дат из CSV'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='football_history_db_in_file.csv')

    def handle(self, *args, **options):
        file_path = options['csv_file']
        found_seasons = set()

        self.stdout.write(self.style.HTTP_INFO("Сканирую файл для формирования списка сезонов..."))

        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                # В твоем файле разделитель точка с запятой
                reader = csv.DictReader(f, delimiter=';')

                for row in reader:
                    date_str = row.get('Date')
                    if not date_str:
                        continue

                    # Парсим дату (обработка форматов DD/MM/YYYY и DD/MM/YY)
                    try:
                        dt = datetime.strptime(date_str, '%d/%m/%Y')
                    except ValueError:
                        try:
                            dt = datetime.strptime(date_str, '%d/%m/%y')
                        except ValueError:
                            continue

                    # Футбольный цикл: июль - июнь
                    year = dt.year
                    if dt.month >= 7:
                        season_name = f"{year}/{year + 1}"
                        start_date = datetime(year, 7, 1).date()
                        end_date = datetime(year + 1, 6, 30).date()
                    else:
                        season_name = f"{year - 1}/{year}"
                        start_date = datetime(year - 1, 7, 1).date()
                        end_date = datetime(year, 6, 30).date()

                    if season_name not in found_seasons:
                        # get_or_create не перезапишет твой 2025/26, если он уже есть
                        season, created = Season.objects.get_or_create(
                            name=season_name,
                            defaults={
                                'start_date': start_date,
                                'end_date': end_date,
                                'is_current': False
                            }
                        )
                        if created:
                            self.stdout.write(f"Добавлен сезон: {season_name}")

                        found_seasons.add(season_name)

            self.stdout.write(self.style.SUCCESS(f"Готово! Обработано уникальных сезонов: {len(found_seasons)}"))

        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"Файл {file_path} не найден."))