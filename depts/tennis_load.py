import os

import pandas as pd
import django
from decimal import Decimal
from pathlib import Path
from django.db import transaction, IntegrityError

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'depts.settings')
django.setup()

from app_bets.models import Tournament, Player, TennisMatchATP, TennisMatchWTA


class TennisParser:
    """Парсер теннисных данных без дублей"""

    def __init__(self):
        self.stats = {
            'atp_matches': 0,
            'wta_matches': 0,
            'players': 0,
            'tournaments': 0,
            'errors': []
        }
        self.player_cache = {}
        self.tournament_cache = {}

    def get_player(self, name):
        """Получить или создать игрока"""
        if pd.isna(name) or not name:
            return None

        name = str(name).strip()

        if name in self.player_cache:
            return self.player_cache[name]

        player, created = Player.objects.get_or_create(name=name)

        if created:
            self.stats['players'] += 1

        self.player_cache[name] = player
        return player

    def get_tournament(self, name):
        """Получить или создать турнир"""
        if pd.isna(name) or not name:
            return None

        name = str(name).strip()

        if name in self.tournament_cache:
            return self.tournament_cache[name]

        tournament, created = Tournament.objects.get_or_create(name=name)

        if created:
            self.stats['tournaments'] += 1

        self.tournament_cache[name] = tournament
        return tournament

    def safe_int(self, value):
        """Безопасное преобразование в int"""
        if pd.isna(value):
            return None
        try:
            return int(float(str(value).strip()))
        except:
            return None

    def safe_decimal(self, value):
        """Безопасное преобразование в Decimal"""
        if pd.isna(value):
            return None
        try:
            return Decimal(str(value).replace(',', '.').strip())
        except:
            return None

    @transaction.atomic
    def parse_atp(self, file_path):
        """Парсинг ATP файла"""
        print(f"\n📁 Загрузка ATP: {file_path.name}")

        df = pd.read_excel(file_path)
        total = len(df)
        loaded = 0

        for idx, row in df.iterrows():
            try:
                # Проверка обязательных полей
                if pd.isna(row.get('Winner')) or pd.isna(row.get('Loser')):
                    continue

                # Получаем или создаем объекты
                tournament = self.get_tournament(row['Tournament'])
                winner = self.get_player(row['Winner'])
                loser = self.get_player(row['Loser'])

                if not tournament or not winner or not loser:
                    continue

                # Проверка на дубликат
                if TennisMatchATP.objects.filter(
                        tournament=tournament,
                        winner=winner,
                        loser=loser,
                        date=pd.to_datetime(row['Date']).date()
                ).exists():
                    continue

                # Создаем матч
                match = TennisMatchATP(
                    tournament=tournament,
                    date=pd.to_datetime(row['Date']).date(),
                    series=str(row.get('Series', '')),
                    surface=str(row.get('Surface', '')),
                    round=str(row.get('Round', '')),
                    best_of=self.safe_int(row.get('Best of')) or 3,
                    winner=winner,
                    loser=loser,
                    winner_rank=self.safe_int(row.get('WRank')),
                    loser_rank=self.safe_int(row.get('LRank')),
                    w1=self.safe_int(row.get('W1')),
                    l1=self.safe_int(row.get('L1')),
                    w2=self.safe_int(row.get('W2')),
                    l2=self.safe_int(row.get('L2')),
                    w3=self.safe_int(row.get('W3')),
                    l3=self.safe_int(row.get('L3')),
                    w4=self.safe_int(row.get('W4')),
                    l4=self.safe_int(row.get('L4')),
                    w5=self.safe_int(row.get('W5')),
                    l5=self.safe_int(row.get('L5')),
                    wsets=self.safe_int(row.get('Wsets')) or 0,
                    lsets=self.safe_int(row.get('Lsets')) or 0,
                    comment=str(row.get('Comment', '')),
                    b365w=self.safe_decimal(row.get('B365W')),
                    b365l=self.safe_decimal(row.get('B365L')),
                )
                match.save()
                loaded += 1

            except Exception as e:
                self.stats['errors'].append(f"ATP строка {idx + 2}: {type(e).__name__}")

        self.stats['atp_matches'] += loaded
        print(f"  ✅ Загружено: {loaded}/{total}")

    @transaction.atomic
    def parse_wta(self, file_path):
        """Парсинг WTA файла"""
        print(f"\n📁 Загрузка WTA: {file_path.name}")

        df = pd.read_excel(file_path)
        total = len(df)
        loaded = 0

        for idx, row in df.iterrows():
            try:
                # Проверка обязательных полей
                if pd.isna(row.get('Winner')) or pd.isna(row.get('Loser')):
                    continue

                # Получаем или создаем объекты
                tournament = self.get_tournament(row['Tournament'])
                winner = self.get_player(row['Winner'])
                loser = self.get_player(row['Loser'])

                if not tournament or not winner or not loser:
                    continue

                # Проверка на дубликат
                if TennisMatchWTA.objects.filter(
                        tournament=tournament,
                        winner=winner,
                        loser=loser,
                        date=pd.to_datetime(row['Date']).date()
                ).exists():
                    continue

                # Создаем матч
                match = TennisMatchWTA(
                    tournament=tournament,
                    date=pd.to_datetime(row['Date']).date(),
                    tier=str(row.get('Tier', '')),
                    surface=str(row.get('Surface', '')),
                    round=str(row.get('Round', '')),
                    best_of=self.safe_int(row.get('Best of')) or 3,
                    winner=winner,
                    loser=loser,
                    winner_rank=self.safe_int(row.get('WRank')),
                    loser_rank=self.safe_int(row.get('LRank')),
                    w1=self.safe_int(row.get('W1')),
                    l1=self.safe_int(row.get('L1')),
                    w2=self.safe_int(row.get('W2')),
                    l2=self.safe_int(row.get('L2')),
                    w3=self.safe_int(row.get('W3')),
                    l3=self.safe_int(row.get('L3')),
                    wsets=self.safe_int(row.get('Wsets')) or 0,
                    lsets=self.safe_int(row.get('Lsets')) or 0,
                    comment=str(row.get('Comment', '')),
                    b365w=self.safe_decimal(row.get('B365W')),
                    b365l=self.safe_decimal(row.get('B365L')),
                )
                match.save()
                loaded += 1

            except Exception as e:
                self.stats['errors'].append(f"WTA строка {idx + 2}: {type(e).__name__}")

        self.stats['wta_matches'] += loaded
        print(f"  ✅ Загружено: {loaded}/{total}")

    def run(self):
        """Запуск парсера"""
        base = Path('.')
        atp_file = base / 'Сборка ATP.xlsx'
        wta_file = base / 'Сборка WTA.xlsx'

        print("=" * 60)
        print("ПАРСЕР ТЕННИСНЫХ ДАННЫХ")
        print("=" * 60)

        if atp_file.exists():
            self.parse_atp(atp_file)
        else:
            self.stats['errors'].append("Файл Сборка ATP.xlsx не найден")

        if wta_file.exists():
            self.parse_wta(wta_file)
        else:
            self.stats['errors'].append("Файл Сборка WTA.xlsx не найден")

        print("\n" + "=" * 60)
        print("ИТОГИ")
        print("=" * 60)
        print(f"ATP матчей: {self.stats['atp_matches']}")
        print(f"WTA матчей: {self.stats['wta_matches']}")
        print(f"Игроков: {self.stats['players']}")
        print(f"Турниров: {self.stats['tournaments']}")

        if self.stats['errors']:
            print(f"\nОшибок: {len(self.stats['errors'])}")
            for err in self.stats['errors'][:5]:
                print(f"  • {err}")

        print("=" * 60)


if __name__ == "__main__":
    parser = TennisParser()
    parser.run()