import csv
from django.core.management.base import BaseCommand
from django.db import transaction
from app_bets.models import Team, TeamAlias, Sport, Country, League


class Command(BaseCommand):
    help = 'Этап 2: Интерактивный импорт команд под структуру БД'

    # Твои данные из таблиц Страны и Лиги
    DIV_CONFIG = {
        'E0': ('Англия', 1), 'E1': ('Англия', 2),
        'D1': ('Германия', 3), 'D2': ('Германия', 4),
        'SP1': ('Испания', 5), 'SP2': ('Испания', 6),
        'I1': ('Италия', 7), 'I2': ('Италия', 8),
        'F1': ('Франция', 9), 'F2': ('Франция', 10),
        'RUSSIA': ('Россия', 11), 'N1': ('Нидерланды', 12),
    }

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str)

    def get_or_create_team_interactive(self, original_name, sport, country_obj):
        clean_alias = " ".join(original_name.split()).lower()
        alias = TeamAlias.objects.filter(name=clean_alias).first()

        if alias:
            return alias.team

        self.stdout.write(self.style.WARNING(f"\nНОВАЯ КОМАНДА: '{original_name}' (Лига: {country_obj.name})"))
        user_input = input(f"Введите имя на русском (или Enter для '{original_name}'): ").strip()

        final_name = user_input if user_input else original_name

        with transaction.atomic():
            team, _ = Team.objects.get_or_create(
                name=final_name,
                sport=sport,
                country=country_obj
            )
            TeamAlias.objects.get_or_create(name=clean_alias, team=team)
            if user_input:
                TeamAlias.objects.get_or_create(name=user_input.lower(), team=team)
        return team

    def handle(self, *args, **options):
        file_path = options['csv_file']
        sport = Sport.objects.get(name='football')  # Убедись, что 'football' создан

        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    div_code = row.get('Div')
                    if div_code not in self.DIV_CONFIG:
                        continue

                    country_name, league_id = self.DIV_CONFIG[div_code]
                    country = Country.objects.get(name=country_name)

                    self.get_or_create_team_interactive(row['HomeTeam'], sport, country)
                    self.get_or_create_team_interactive(row['AwayTeam'], sport, country)

            self.stdout.write(self.style.SUCCESS("Готово! Команды и псевдонимы синхронизированы."))
        except KeyboardInterrupt:
            self.stdout.write("\nОстановка...")