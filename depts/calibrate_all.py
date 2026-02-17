# calibrate_all.py
import os
import sys
import glob
import pickle
import pandas as pd
import csv
from collections import defaultdict
import django
from datetime import datetime

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'depts.settings')
django.setup()

# Импортируем необходимые модули проекта
from validate_poisson_historical import PoissonRollingValidator
from app_bets.models import League  # для получения названия лиги по ID


# ---------- Чтение и объединение файлов с подробным выводом ----------
def detect_separator(file_path, sample_size=4096):
    """Определяет разделитель CSV-файла."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(sample_size)
        if ';' in sample:
            lines = sample.splitlines()
            if len(lines) > 1:
                cols_count = len(lines[1].split(';'))
                if cols_count > 1:
                    return ';'
        if ',' in sample:
            lines = sample.splitlines()
            if len(lines) > 1:
                cols_count = len(lines[1].split(','))
                if cols_count > 1:
                    return ','
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
        except:
            return ','


def read_all_files(folder_path='for_calibration', required_cols=None):
    """
    Читает все CSV-файлы в папке и возвращает объединённый DataFrame.
    Выводит подробную информацию о каждом файле.
    """
    if required_cols is None:
        required_cols = ['Div', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']

    csv_files = glob.glob(os.path.join(folder_path, '**', '*.csv'), recursive=True)
    if not csv_files:
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

    print(f"\nНайдено CSV-файлов: {len(csv_files)}")
    print("=" * 60)

    data_frames = []
    total_rows = 0
    leagues_found = set()
    stats = {'ok': 0, 'warn': 0, 'error': 0}

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        try:
            sep = detect_separator(fpath)
            df = pd.read_csv(fpath, sep=sep, low_memory=False, encoding='utf-8', on_bad_lines='skip')
            df.columns = [col.strip() for col in df.columns]

            # Проверка обязательных колонок
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                print(f"⚠️ {fname}: пропущен – отсутствуют обязательные колонки {missing}")
                stats['warn'] += 1
                continue

            # Проверка наличия хотя бы одной колонки с коэффициентами на тотал
            odds_cols = [c for c in df.columns if ('>2.5' in c or '<2.5' in c)]
            if not odds_cols:
                print(f"⚠️ {fname}: пропущен – нет колонок с тоталом")
                stats['warn'] += 1
                continue

            # Парсинг даты
            try:
                df['Date_parsed'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
            except:
                df['Date_parsed'] = pd.to_datetime(df['Date'], errors='coerce')

            # Удаляем строки с некорректной датой
            before = len(df)
            df = df.dropna(subset=['Date_parsed'])
            after = len(df)
            if after == 0:
                print(f"⚠️ {fname}: пропущен – нет строк с корректной датой")
                stats['warn'] += 1
                continue

            if after < before:
                print(f"   {fname}: удалено {before - after} строк с некорректной датой")

            df['source_file'] = fname
            data_frames.append(df)
            total_rows += after
            leagues = df['Div'].unique()
            leagues_found.update(leagues)
            print(
                f"✅ {fname}: {after} строк, лиги {list(leagues)}, даты {df['Date_parsed'].min()} - {df['Date_parsed'].max()}")
            stats['ok'] += 1

        except Exception as e:
            print(f"❌ {fname}: ошибка – {e}")
            stats['error'] += 1

    print("=" * 60)
    print(f"Итог: OK: {stats['ok']}, WARN: {stats['warn']}, ERROR: {stats['error']}")

    if not data_frames:
        raise RuntimeError("Нет ни одного корректного файла для анализа.")

    combined = pd.concat(data_frames, ignore_index=True, sort=False)
    print(f"\nВсего объединено строк: {len(combined)}")
    unique_leagues = sorted([x for x in combined['Div'].unique() if isinstance(x, str)])
    print(f"Уникальные лиги: {unique_leagues}")
    return combined


def get_combined_data(pickle_path='combined_data.pkl', folder='for_calibration', force_reload=False):
    """
    Загружает объединённый DataFrame из pickle, если файл существует и force_reload=False.
    Иначе читает все файлы заново, сохраняет pickle и возвращает DataFrame.
    """
    if not force_reload and os.path.exists(pickle_path):
        print(f"\nЗагрузка объединённых данных из {pickle_path}...")
        with open(pickle_path, 'rb') as f:
            df = pickle.load(f)
        print(f"Загружено {len(df)} строк")
        print(f"Уникальные лиги: {sorted([x for x in df['Div'].unique() if isinstance(x, str)])}")
        return df
    else:
        if force_reload:
            print("\nПринудительное перечитывание всех файлов...")
        else:
            print(f"\nФайл {pickle_path} не найден. Чтение всех файлов...")
        df = read_all_files(folder)
        print(f"\nСохранение объединённых данных в {pickle_path}...")
        with open(pickle_path, 'wb') as f:
            pickle.dump(df, f)
        return df


# ---------- Модифицированный валидатор для ТБ и ТМ ----------
class PoissonRollingValidatorFlex(PoissonRollingValidator):
    """Расширенный валидатор, поддерживающий target='over' или 'under'."""

    def __init__(self, csv_path, min_team_matches=3, target_league=None, filter_odds=True, last_matches=None,
                 target='over'):
        super().__init__(csv_path, min_team_matches, target_league, filter_odds, last_matches)
        self.target = target

    def get_odds_from_row(self, row):
        """Извлекает коэффициент на ТБ или ТМ в зависимости от target."""
        if self.target == 'over':
            odds_candidates = ['BbMx>2.5', 'B365>2.5', 'Avg>2.5', 'Max>2.5', 'P>2.5',
                               'BbAv>2.5', 'B365C>2.5', 'PC>2.5', 'MaxC>2.5', 'AvgC>2.5']
        else:
            odds_candidates = ['BbMx<2.5', 'B365<2.5', 'Avg<2.5', 'Max<2.5', 'P<2.5',
                               'BbAv<2.5', 'B365C<2.5', 'PC<2.5', 'MaxC<2.5', 'AvgC<2.5']

        for col in odds_candidates:
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

        if self.target == 'over':
            pred_prob = over_prob
            fthg = row.get('FTHG')
            ftag = row.get('FTAG')
            if fthg is None or ftag is None:
                self.stats['score_missing'] += 1
                return
            actual = 1 if (fthg + ftag) > 2.5 else 0
        else:
            pred_prob = 1.0 - over_prob
            fthg = row.get('FTHG')
            ftag = row.get('FTAG')
            if fthg is None or ftag is None:
                self.stats['score_missing'] += 1
                return
            actual = 1 if (fthg + ftag) <= 2.5 else 0

        odds = self.get_odds_from_row(row)
        if odds is None:
            return

        self.results[match.league_id].append({
            'pred': pred_prob,
            'actual': actual,
            'odds': odds
        })
        self.stats['success'] += 1


# ---------- Сбор положительных EV ----------
def collect_positive_ev(validator_results, min_matches=10):
    """Из результатов валидации собирает интервалы с положительным EV."""
    bins = [30, 40, 50, 60, 70, 80, 90]
    positive = []
    league_cache = {}

    for league_id, records in validator_results.items():
        if len(records) < min_matches:
            continue
        if league_id not in league_cache:
            try:
                league = League.objects.get(id=league_id)
                league_cache[league_id] = f"{league.name} ({league.country.name})"
            except:
                league_cache[league_id] = f"Лига {league_id}"

        intervals = {f"{bins[i]}-{bins[i + 1]}": [] for i in range(len(bins) - 1)}
        intervals[f">{bins[-1]}"] = []
        for rec in records:
            p = rec['pred'] * 100
            if p < bins[0]:
                continue
            assigned = False
            for i in range(len(bins) - 1):
                if bins[i] <= p < bins[i + 1]:
                    intervals[f"{bins[i]}-{bins[i + 1]}"].append(rec)
                    assigned = True
                    break
            if not assigned and p >= bins[-1]:
                intervals[f">{bins[-1]}"].append(rec)

        for interval, group in intervals.items():
            if len(group) < min_matches:
                continue
            outcomes = [r['actual'] for r in group]
            actual_freq = sum(outcomes) / len(outcomes)
            avg_odds = sum(r['odds'] for r in group) / len(group)
            ev = actual_freq * avg_odds - 1
            if ev > 0:
                positive.append({
                    'league': league_cache[league_id],
                    'interval': interval,
                    'actual_%': round(actual_freq * 100, 1),
                    'avg_odds': round(avg_odds, 2),
                    'EV_%': round(ev * 100, 1),
                    'matches': len(group)
                })
    return positive


# ---------- Основной процесс ----------
def main():
    import argparse
    parser = argparse.ArgumentParser(description='Калибровка модели Пуассона')
    parser.add_argument('--reload', action='store_true', help='Принудительно перечитать все CSV-файлы')
    args = parser.parse_args()

    # Получаем объединённый DataFrame (из pickle или чтением)
    df = get_combined_data(pickle_path='combined_data.pkl', folder='for_calibration', force_reload=args.reload)

    # for target in ['over', 'under']:
    #     print(f"\n{'=' * 60}")
    #     print(f"КАЛИБРОВКА ДЛЯ ТОТАЛА {'БОЛЬШЕ' if target == 'over' else 'МЕНЬШЕ'} 2.5")
    #     print('=' * 60)
    #     for n in range(5, 11):
    #         print(f"\n--- last_matches = {n} ---")
    #         validator = PoissonRollingValidatorFlex(
    #             csv_path='dummy.csv',
    #             min_team_matches=3,
    #             target_league=None,
    #             filter_odds=True,
    #             last_matches=n,
    #             target=target
    #         )
    #         total_rows = len(df)
    #         processed = 0
    #         # Обрабатываем все строки с прогрессом
    #         for idx, row in df.iterrows():
    #             validator.process_row(row)
    #             processed += 1
    #             if processed % 10000 == 0:
    #                 print(f"  Обработано {processed}/{total_rows} строк, success: {validator.stats.get('success', 0)}")
    #
    #         print(f"  Обработка завершена. Всего success: {validator.stats.get('success', 0)}")
    #
    #         # Собираем положительные EV
    #         pos = collect_positive_ev(validator.results, min_matches=20)
    #         if pos:
    #             pos_df = pd.DataFrame(pos)
    #             pos_df = pos_df.sort_values(['league', 'interval'])
    #             print("\n  Положительные EV:")
    #             print(pos_df.to_string(index=False))
    #             filename = f'positive_ev_{target}_n{n}.csv'
    #             pos_df.to_csv(filename, index=False, encoding='utf-8-sig')
    #             print(f"  Результаты сохранены в {filename}")
    #         else:
    #             print("  Нет интервалов с положительным EV при min_matches=20.")


    for target in ['under']:
        print(f"\n{'=' * 60}")
        print(f"КАЛИБРОВКА ДЛЯ ТОТАЛА {'БОЛЬШЕ' if target == 'over' else 'МЕНЬШЕ'} 2.5")
        print('=' * 60)
        for n in range(9, 11):
            print(f"\n--- last_matches = {n} ---")
            validator = PoissonRollingValidatorFlex(
                csv_path='dummy.csv',
                min_team_matches=3,
                target_league=None,
                filter_odds=True,
                last_matches=n,
                target=target
            )
            total_rows = len(df)
            processed = 0
            # Обрабатываем все строки с прогрессом
            for idx, row in df.iterrows():
                validator.process_row(row)
                processed += 1
                if processed % 10000 == 0:
                    print(f"  Обработано {processed}/{total_rows} строк, success: {validator.stats.get('success', 0)}")

            print(f"  Обработка завершена. Всего success: {validator.stats.get('success', 0)}")

            # Собираем положительные EV
            pos = collect_positive_ev(validator.results, min_matches=20)
            if pos:
                pos_df = pd.DataFrame(pos)
                pos_df = pos_df.sort_values(['league', 'interval'])
                print("\n  Положительные EV:")
                print(pos_df.to_string(index=False))
                filename = f'positive_ev_{target}_n{n}.csv'
                pos_df.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"  Результаты сохранены в {filename}")
            else:
                print("  Нет интервалов с положительным EV при min_matches=20.")


if __name__ == "__main__":
    main()