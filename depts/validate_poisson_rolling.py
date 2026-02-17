# validate_poisson_rolling.py
# Скрипт для анализа калибровки модели Пуассона при разном количестве последних матчей команд

import pandas as pd
import numpy as np
import math
from datetime import datetime
from collections import defaultdict
from django.db.models import Q, Avg, Sum
from app_bets.models import Match, League, Team
import logging

logger = logging.getLogger(__name__)


class PoissonRollingValidator:
    """
    Валидатор с возможностью ограничивать статистику команд последними N матчами.
    """

    def __init__(self, csv_path, min_team_matches=3, target_league=None, filter_odds=True, last_matches=None):
        """
        :param csv_path: путь к CSV-файлу
        :param min_team_matches: минимальное количество матчей команды (используется только если last_matches=None)
        :param target_league: код лиги для фильтрации
        :param filter_odds: фильтровать строки по наличию коэффициентов
        :param last_matches: если указано, используется именно это количество последних матчей (игнорирует min_team_matches)
        """
        self.csv_path = csv_path
        self.min_team_matches = min_team_matches
        self.target_league = target_league
        self.filter_odds = filter_odds
        self.last_matches = last_matches
        self.results = defaultdict(list)
        self.stats = defaultdict(int)

    def load_csv(self):
        df = pd.read_csv(self.csv_path, encoding='utf-8', sep=';', low_memory=False)
        df.columns = [col.strip() for col in df.columns]

        if self.filter_odds:
            odds_cols = [
                'B365>2.5', 'Avg>2.5', 'Max>2.5', 'P>2.5',
                'B365C>2.5', 'PC>2.5', 'MaxC>2.5', 'AvgC>2.5'
            ]
            existing_odds_cols = [col for col in odds_cols if col in df.columns]
            if existing_odds_cols:
                df = df[df[existing_odds_cols].notna().any(axis=1)]
                print(f"После фильтрации по наличию коэффициентов осталось {len(df)} строк")
            else:
                print("Внимание: ни одна из колонок с коэффициентами не найдена в данных.")
        return df

    def find_match_in_db(self, row):
        self.stats['total_rows'] += 1
        div = row.get('Div')
        if pd.isna(div):
            self.stats['div_missing'] += 1
            return None
        if self.target_league and str(div).strip() != self.target_league:
            self.stats['filtered_by_league'] += 1
            return None

        try:
            league = League.objects.get(external_id=str(div).strip())
        except League.DoesNotExist:
            self.stats['league_not_found'] += 1
            return None

        date_val = row.get('Date')
        if pd.isna(date_val):
            self.stats['date_missing'] += 1
            return None
        date_str = str(date_val).strip()
        if not date_str:
            self.stats['date_empty'] += 1
            return None

        time_val = row.get('Time')
        if pd.isna(time_val):
            time_str = None
        else:
            time_str = str(time_val).strip()
            if time_str == '':
                time_str = None

        try:
            day, month, year = map(int, date_str.split('/'))
            if year < 100:
                year = 2000 + year
            match_date = datetime(year, month, day)
        except Exception as e:
            self.stats['date_parse_error'] += 1
            return None

        if time_str:
            try:
                hour, minute = map(int, time_str.split(':'))
                match_date = match_date.replace(hour=hour, minute=minute)
            except Exception as e:
                self.stats['time_parse_error'] += 1
                return None

        home_name = row.get('HomeTeam', '').strip()
        away_name = row.get('AwayTeam', '').strip()
        if not home_name or not away_name:
            self.stats['team_name_missing'] += 1
            return None

        def get_team(name):
            try:
                return Team.objects.get(
                    Q(aliases__name__iexact=name) | Q(name__iexact=name)
                )
            except Team.DoesNotExist:
                return None
            except Team.MultipleObjectsReturned:
                return Team.objects.filter(
                    Q(aliases__name__iexact=name) | Q(name__iexact=name)
                ).first()

        home_team = get_team(home_name)
        away_team = get_team(away_name)
        if not home_team:
            self.stats['home_team_not_found'] += 1
            return None
        if not away_team:
            self.stats['away_team_not_found'] += 1
            return None

        if time_str:
            match = Match.objects.filter(
                league=league,
                home_team=home_team,
                away_team=away_team,
                date__date=match_date.date(),
                date__hour=match_date.hour,
                date__minute=match_date.minute
            ).first()
            if not match:
                match = Match.objects.filter(
                    league=league,
                    home_team=home_team,
                    away_team=away_team,
                    date__date=match_date.date(),
                    date__hour=match_date.hour
                ).first()
        else:
            match = Match.objects.filter(
                league=league,
                home_team=home_team,
                away_team=away_team,
                date__date=match_date.date()
            ).first()

        if not match:
            self.stats['match_not_found'] += 1
            return None

        self.stats['match_found'] += 1
        return match

    def get_poisson_over_prob_before_date(self, match):
        league_matches_before = Match.objects.filter(
            league=match.league,
            season=match.season,
            date__lt=match.date,
            home_score_reg__isnull=False,
            away_score_reg__isnull=False
        )
        total_league = league_matches_before.count()
        if total_league < 5:
            self.stats['league_less_5'] += 1
            return None
        self.stats['league_ok'] += 1

        league_avg_home = league_matches_before.aggregate(avg=Avg('home_score_reg'))['avg'] or 1.2
        league_avg_away = league_matches_before.aggregate(avg=Avg('away_score_reg'))['avg'] or 1.0
        league_avg_home_conceded = league_avg_away
        league_avg_away_conceded = league_avg_home

        # --- Статистика хозяев (последние N домашних матчей до даты) ---
        home_home_qs = Match.objects.filter(
            league=match.league,
            season=match.season,
            home_team=match.home_team,
            date__lt=match.date,
            home_score_reg__isnull=False,
            away_score_reg__isnull=False
        ).order_by('-date')

        if self.last_matches is not None:
            home_home_qs = home_home_qs[:self.last_matches]
            required = self.last_matches
        else:
            required = self.min_team_matches

        home_home_data = list(home_home_qs.values('home_score_reg', 'away_score_reg'))
        if len(home_home_data) < required:
            self.stats['home_less_3'] += 1
            return None
        self.stats['home_ok'] += 1

        avg_home_scored = sum(d['home_score_reg'] for d in home_home_data) / len(home_home_data)
        avg_home_conceded = sum(d['away_score_reg'] for d in home_home_data) / len(home_home_data)

        # --- Статистика гостей (последние N гостевых матчей до даты) ---
        away_away_qs = Match.objects.filter(
            league=match.league,
            season=match.season,
            away_team=match.away_team,
            date__lt=match.date,
            home_score_reg__isnull=False,
            away_score_reg__isnull=False
        ).order_by('-date')

        if self.last_matches is not None:
            away_away_qs = away_away_qs[:self.last_matches]

        away_away_data = list(away_away_qs.values('home_score_reg', 'away_score_reg'))
        if len(away_away_data) < required:
            self.stats['away_less_3'] += 1
            return None
        self.stats['away_ok'] += 1

        avg_away_scored = sum(d['away_score_reg'] for d in away_away_data) / len(away_away_data)
        avg_away_conceded = sum(d['home_score_reg'] for d in away_away_data) / len(away_away_data)

        # Защита от нулевых значений
        avg_home_scored = max(avg_home_scored, 0.5)
        avg_home_conceded = max(avg_home_conceded, 0.5)
        avg_away_scored = max(avg_away_scored, 0.3)
        avg_away_conceded = max(avg_away_conceded, 0.5)
        league_avg_home = max(league_avg_home, 1.0)
        league_avg_away = max(league_avg_away, 0.8)
        league_avg_home_conceded = max(league_avg_home_conceded, 1.0)
        league_avg_away_conceded = max(league_avg_away_conceded, 0.8)

        # Сила атаки/обороны
        home_attack = avg_home_scored / league_avg_home
        away_defense = avg_away_conceded / league_avg_away_conceded
        away_attack = avg_away_scored / league_avg_away
        home_defense = avg_home_conceded / league_avg_home_conceded

        lambda_home = home_attack * away_defense * league_avg_home
        lambda_away = away_attack * home_defense * league_avg_away

        lambda_home = max(min(lambda_home, 3.5), 0.5)
        lambda_away = max(min(lambda_away, 3.0), 0.3)

        def poisson_prob(l, k):
            try:
                return (math.exp(-l) * (l ** k)) / math.factorial(k)
            except:
                return 0.0

        over_prob = 0.0
        for h in range(0, 11):
            for a in range(0, 11):
                if h + a > 2.5:
                    prob = poisson_prob(lambda_home, h) * poisson_prob(lambda_away, a)
                    over_prob += prob

        self.stats['pred_calculated'] += 1
        return over_prob

    def get_odds_from_row(self, row):
        for col in ['B365>2.5', 'Avg>2.5', 'Max>2.5', 'P>2.5', 'B365C>2.5', 'PC>2.5', 'MaxC>2.5', 'AvgC>2.5']:
            if col in row and pd.notna(row[col]):
                return float(row[col])
        self.stats['odds_missing'] += 1
        return None

    def process_row(self, row):
        match = self.find_match_in_db(row)
        if not match:
            return

        over_prob = self.get_poisson_over_prob_before_date(match)
        if over_prob is None:
            return

        fthg = row.get('FTHG')
        ftag = row.get('FTAG')
        if fthg is None or ftag is None:
            self.stats['score_missing'] += 1
            return
        actual = 1 if (fthg + ftag) > 2.5 else 0

        odds = self.get_odds_from_row(row)
        if odds is None:
            return

        self.results[match.league_id].append({
            'pred': over_prob,
            'actual': actual,
            'odds': odds
        })
        self.stats['success'] += 1

    def run(self):
        print("Загрузка CSV...")
        df = self.load_csv()
        total = len(df)
        print(f"Всего обрабатывается строк: {total}")
        processed = 0
        for idx, row in df.iterrows():
            self.process_row(row)
            processed += 1
            if processed % 1000 == 0:
                total_success = self.stats.get('success', 0)
                total_found = self.stats.get('match_found', 0)
                total_odds_missing = self.stats.get('odds_missing', 0)
                total_home_less = self.stats.get('home_less_3', 0)
                total_away_less = self.stats.get('away_less_3', 0)
                total_league_less = self.stats.get('league_less_5', 0)
                print(f"Обработано {processed}/{total} | найдено матчей: {total_found} | успех: {total_success} | нет кэфов: {total_odds_missing} | home<3: {total_home_less} | away<3: {total_away_less} | league<5: {total_league_less}")
        print("Обработка завершена.")
        self.print_stats()
        self.print_results()

    def print_stats(self):
        print("\n=== ДИАГНОСТИКА ===")
        for key, value in sorted(self.stats.items()):
            print(f"{key}: {value}")

    def print_results(self, bins=None, min_matches=1):
        if bins is None:
            bins = [30, 40, 50, 60, 70, 80, 90]

        if not self.results:
            print("Нет результатов для отображения.")
            return

        print("\n" + "=" * 90)
        print(f"КАЛИБРОВКА МОДЕЛИ ПУАССОНА (Тотал >2.5) ПО ЛИГАМ (last_matches={self.last_matches})".center(90))
        print("=" * 90)

        for league_id, records in self.results.items():
            if len(records) < min_matches:
                continue
            league = League.objects.get(id=league_id)
            print(f"\n📊 Лига: {league.name} ({league.country.name})  |  Матчей: {len(records)}")
            self._print_interval_table(records, bins, min_matches)

        print("\n" + "=" * 90)
        print(f"АГРЕГИРОВАННАЯ КАЛИБРОВКА (ВСЕ ЛИГИ) last_matches={self.last_matches}".center(90))
        print("=" * 90)

        all_records = []
        for recs in self.results.values():
            all_records.extend(recs)

        self._print_interval_table(all_records, bins, min_matches)

    def _print_interval_table(self, records, bins, min_matches):
        intervals = {f"{bins[i]}-{bins[i+1]}": [] for i in range(len(bins)-1)}
        intervals[f">{bins[-1]}"] = []

        for rec in records:
            p = rec['pred'] * 100
            if p < bins[0]:
                continue
            assigned = False
            for i in range(len(bins)-1):
                if bins[i] <= p < bins[i+1]:
                    intervals[f"{bins[i]}-{bins[i+1]}"].append(rec)
                    assigned = True
                    break
            if not assigned and p >= bins[-1]:
                intervals[f">{bins[-1]}"].append(rec)

        print(f"\n{'Интервал':>12} | {'Прогноз':>8} | {'Факт':>8} | {'Отклон':>8} | {'Ср.кэф':>8} | {'Ожид. рынок':>11} | {'Матчей':>8}")
        print(f"{'-'*12}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*11}-+-{'-'*8}")

        for interval in sorted(intervals.keys(), key=lambda x: self._interval_start(x)):
            group = intervals[interval]
            if len(group) < min_matches:
                continue

            outcomes = [r['actual'] for r in group]
            actual_freq = sum(outcomes) / len(outcomes) * 100
            mid_point = self._interval_mid(interval)
            error = actual_freq - mid_point

            avg_odds = np.mean([r['odds'] for r in group])
            implied_prob = 1 / avg_odds * 100

            arrow = "↑" if error > 0 else "↓" if error < 0 else "="

            print(
                f"  {interval:>12} | "
                f"{mid_point:6.1f}% | "
                f"{actual_freq:6.1f}% | "
                f"{error:+5.1f}%{arrow:1} | "
                f"{avg_odds:6.2f}  | "
                f"{implied_prob:6.1f}%      | "
                f"{len(group):8d}"
            )

    def _interval_mid(self, interval):
        if interval.startswith('>'):
            return float(interval[1:])
        low, high = map(float, interval.split('-'))
        return (low + high) / 2

    def _interval_start(self, interval):
        if interval.startswith('>'):
            return float(interval[1:])
        low, _ = map(float, interval.split('-'))
        return low


def run_poisson_rolling(csv_path='football_history_db_in_file.csv', target_league=None, last_matches_list=None):
    """
    Запускает валидацию для каждого значения last_matches в списке.
    Результаты выводятся на экран и сохраняются в CSV-файл с именем, содержащим значение.
    """
    import django
    django.setup()

    if last_matches_list is None:
        last_matches_list = [5, 6, 7, 8, 9, 10]

    for lm in last_matches_list:
        print("\n" + "=" * 90)
        print(f"ЗАПУСК ДЛЯ last_matches = {lm}".center(90))
        print("=" * 90)
        validator = PoissonRollingValidator(
            csv_path=csv_path,
            min_team_matches=3,  # не используется, т.к. передан last_matches
            target_league=target_league,
            filter_odds=True,
            last_matches=lm
        )
        validator.run()

        # Сохраняем агрегированные результаты в CSV
        if validator.results:
            all_records = []
            for recs in validator.results.values():
                all_records.extend(recs)
            if all_records:
                df_out = pd.DataFrame(all_records)
                df_out['last_matches'] = lm
                df_out.to_csv(f'calibration_lm_{lm}.csv', index=False, encoding='utf-8')
                print(f"\nРезультаты сохранены в calibration_lm_{lm}.csv")


if __name__ == "__main__":
    print("Скрипт для анализа калибровки при разном количестве последних матчей.")
    print("Используйте функцию run_poisson_rolling() для запуска.")