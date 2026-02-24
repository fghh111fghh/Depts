#!/usr/bin/env python
import os
import sys
import csv
import pickle
import math
import chardet
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Константы для анализа
MIN_MATCHES = 3
MAX_MATCHES = 7

# Причины пропуска
SKIP_REASONS = {
    'NO_SCORE': 'Нет счета',
    'NO_ODDS': 'Нет коэффициентов',
    'INSUFFICIENT_HISTORY': 'Недостаточно истории',
    'INVALID_DATE': 'Некорректная дата',
    'ENCODING_ERROR': 'Ошибка кодировки',
    'OTHER': 'Другая ошибка'
}

PROBABILITY_BINS = [
    (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
    (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
    (50, 55), (55, 60), (60, 65), (65, 70), (70, 75),
    (75, 80), (80, 85), (85, 90), (90, 95), (95, 100)
]

# Фиксированные блоки с шагом 10% (без перекрытия)
ODDS_BINS = [
    (1.00, 1.10), (1.10, 1.21), (1.21, 1.33), (1.33, 1.46), (1.46, 1.61),
    (1.61, 1.77), (1.77, 1.95), (1.95, 2.14), (2.14, 2.35), (2.35, 2.59),
    (2.59, 2.85), (2.85, 3.13), (3.13, 3.44), (3.44, 3.78), (3.78, 4.16),
    (4.16, 4.58), (4.58, 5.04), (5.04, 5.54), (5.54, 6.09), (6.09, 6.70),
    (6.70, 7.37), (7.37, 8.11), (8.11, 8.92), (8.92, 9.81), (9.81, 10.79),
    (10.79, 11.87), (11.87, 13.06), (13.06, float('inf'))
]


def get_probability_bin(prob):
    """Определяет блок вероятности (5% интервалы)"""
    for low, high in PROBABILITY_BINS:
        if low <= prob < high:
            return f"{low}-{high}%"
    return "95-100%"


def get_odds_bin(odds):
    """Определяет фиксированный блок коэффициента с шагом 10%"""
    if odds is None:
        return None
    for low, high in ODDS_BINS:
        if low <= odds < high:
            if high == float('inf'):
                return f">{low:.2f}"
            return f"{low:.2f}-{high:.2f}"
    return f">{ODDS_BINS[-1][0]:.2f}"


def detect_delimiter(file_path):
    """Определяет разделитель в CSV файле"""
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        first_line = f.readline().strip()
        commas = first_line.count(',')
        semicolons = first_line.count(';')
        return ';' if semicolons > commas else ','


def detect_encoding(file_path):
    """Определяет кодировку файла"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # Читаем первые 10000 байт
        result = chardet.detect(raw_data)
        return result['encoding']


def get_odds_from_row(row, odds_type):
    """
    Ищет коэффициент в различных колонках (максимально расширенный поиск)

    Args:
        row: строка CSV
        odds_type: тип коэффициента ('H' - П1, 'D' - ничья, 'A' - П2, 'OVER' - ТБ2.5)

    Returns:
        float or None
    """
    # Максимально расширенный список всех возможных колонок с коэффициентами
    odds_mapping = {
        'H': [
            # Основные букмекеры
            'B365H', 'BWH', 'IWH', 'LBH', 'PSH', 'WHH', 'SJH', 'VCH',
            # Средние и максимальные
            'AvgH', 'MaxH', 'BbAvH', 'BbMxH',
            # Другие варианты
            'BFH', 'BFEH', 'PSCH', 'BWCH', 'BFCH', 'WHCH', '1XBH', 'MaxCH', 'AvgCH'
        ],
        'D': [
            'B365D', 'BWD', 'IWD', 'LBD', 'PSD', 'WHD', 'SJD', 'VCD',
            'AvgD', 'MaxD', 'BbAvD', 'BbMxD',
            'BFD', 'BFED', 'PSCD', 'BWCD', 'BFCD', 'WHCD', '1XBD', 'MaxCD', 'AvgCD'
        ],
        'A': [
            'B365A', 'BWA', 'IWA', 'LBA', 'PSA', 'WHA', 'SJA', 'VCA',
            'AvgA', 'MaxA', 'BbAvA', 'BbMxA',
            'BFA', 'BFEA', 'PSCA', 'BWCA', 'BFCA', 'WHCA', '1XBA', 'MaxCA', 'AvgCA'
        ],
        'OVER': [
            # Основные
            'B365>2.5', 'P>2.5', 'Max>2.5', 'Avg>2.5',
            # BetBrain
            'BbMx>2.5', 'BbAv>2.5',
            # Другие
            'BFE>2.5', 'BFEC>2.5', 'PC>2.5', 'MaxC>2.5', 'AvgC>2.5'
        ],
        'UNDER': [
            'B365<2.5', 'P<2.5', 'Max<2.5', 'Avg<2.5',
            'BbMx<2.5', 'BbAv<2.5',
            'BFE<2.5', 'BFEC<2.5', 'PC<2.5', 'MaxC<2.5', 'AvgC<2.5'
        ]
    }

    for col in odds_mapping.get(odds_type, []):
        if col in row:
            value = row.get(col, '').strip()
            if value and value != 'NA' and value != '':
                try:
                    return float(value.replace(',', '.'))
                except (ValueError, TypeError):
                    continue
    return None


def safe_int(value):
    """Безопасное преобразование в int"""
    if not value or str(value).strip() == '' or str(value).lower() == 'nan':
        return None
    try:
        return int(float(str(value).replace(',', '.')))
    except (ValueError, TypeError):
        return None


def parse_date(date_str):
    """Парсит дату из CSV файла"""
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    for fmt in ('%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


def get_poisson_probs(l_home, l_away):
    """Рассчитывает вероятности по Пуассону"""
    try:
        l_home = max(float(l_home), 0.1)
        l_away = max(float(l_away), 0.1)

        exp_home = math.exp(-l_home)
        exp_away = math.exp(-l_away)

        max_goals = 5
        factorials = [math.factorial(i) for i in range(max_goals + 1)]
        home_powers = [l_home ** i for i in range(max_goals + 1)]
        away_powers = [l_away ** i for i in range(max_goals + 1)]

        over25_yes = 0.0

        for h in range(max_goals + 1):
            p_h = (exp_home * home_powers[h]) / factorials[h]
            for a in range(max_goals + 1):
                p_a = (exp_away * away_powers[a]) / factorials[a]
                probability = p_h * p_a * 100

                if (h + a) > 2.5:
                    over25_yes += probability

        return {'over25_yes': over25_yes}

    except Exception:
        return {'over25_yes': 50.0}


def calculate_poisson_lambda_from_history(home_history, away_history, league_avg_home, league_avg_away):
    """
    Рассчитывает лямбды Пуассона на основе исторических данных
    """
    try:
        if len(home_history) < MIN_MATCHES or len(away_history) < MIN_MATCHES:
            return None

        # Защита от нулевых значений
        l_avg_home_goals = max(league_avg_home, 1.0)
        l_avg_away_goals = max(league_avg_away, 0.8)
        l_avg_home_conceded = l_avg_away_goals
        l_avg_away_conceded = l_avg_home_goals

        # Берем последние MAX_MATCHES матчей
        home_recent = home_history[-MAX_MATCHES:] if len(home_history) > MAX_MATCHES else home_history
        away_recent = away_history[-MAX_MATCHES:] if len(away_history) > MAX_MATCHES else away_history

        # Статистика хозяев
        h_avg_scored = sum(m['home_score'] for m in home_recent) / len(home_recent)
        h_avg_conceded = sum(m['away_score'] for m in home_recent) / len(home_recent)
        h_avg_scored = max(h_avg_scored, 0.5)
        h_avg_conceded = max(h_avg_conceded, 0.5)
        home_attack = h_avg_scored / l_avg_home_goals
        home_defense = h_avg_conceded / l_avg_home_conceded

        # Статистика гостей
        a_avg_scored = sum(m['away_score'] for m in away_recent) / len(away_recent)
        a_avg_conceded = sum(m['home_score'] for m in away_recent) / len(away_recent)
        a_avg_scored = max(a_avg_scored, 0.3)
        a_avg_conceded = max(a_avg_conceded, 0.5)
        away_attack = a_avg_scored / l_avg_away_goals
        away_defense = a_avg_conceded / l_avg_away_conceded

        lambda_home = home_attack * away_defense * l_avg_home_goals
        lambda_away = away_attack * home_defense * l_avg_away_goals

        lambda_home = max(min(lambda_home, 3.5), 0.5)
        lambda_away = max(min(lambda_away, 3.0), 0.3)

        return {
            'home_lambda': round(lambda_home, 2),
            'away_lambda': round(lambda_away, 2)
        }

    except Exception:
        return None


def analyze_csv_file(file_path):
    """
    Анализирует один CSV файл (полная версия из предыдущего скрипта)
    """
    print(f"\n--- Анализ файла: {os.path.basename(file_path)} ---")

    # Детальная статистика по пропускам
    skip_stats = defaultdict(int)

    # Проверяем существование файла
    if not os.path.exists(file_path):
        print(f"❌ Файл не найден")
        return None

    # Определяем кодировку
    try:
        encoding = detect_encoding(file_path)
        print(f"📄 Кодировка: {encoding}")
    except:
        encoding = 'utf-8-sig'
        print(f"📄 Используем кодировку по умолчанию: {encoding}")

    # Определяем разделитель
    try:
        delimiter = detect_delimiter(file_path)
        print(f"📊 Разделитель: '{delimiter}'")
    except Exception as e:
        print(f"❌ Ошибка определения разделителя: {e}")
        skip_stats['ENCODING_ERROR'] += 1
        return {
            'file_name': os.path.basename(file_path),
            'total_matches': 0,
            'analyzed': 0,
            'skipped': 0,
            'errors': 1,
            'skip_stats': dict(skip_stats),
            'predictions': [],
            'stats': {}
        }

    # Загружаем все данные из файла
    all_matches = []

    try:
        with open(file_path, mode='r', encoding=encoding, errors='replace') as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            fieldnames = reader.fieldnames
            if not fieldnames:
                print("❌ Файл пуст или не содержит заголовков")
                skip_stats['OTHER'] += 1
                return {
                    'file_name': os.path.basename(file_path),
                    'total_matches': 0,
                    'analyzed': 0,
                    'skipped': 0,
                    'errors': 1,
                    'skip_stats': dict(skip_stats),
                    'predictions': [],
                    'stats': {}
                }

            print(f"📋 Найдено колонок: {len(fieldnames)}")

            # Проверяем наличие обязательных колонок (только дата и команды)
            required_cols = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
            missing_cols = [col for col in required_cols if col not in fieldnames]

            if missing_cols:
                print(f"❌ Отсутствуют обязательные колонки: {missing_cols}")
                skip_stats['OTHER'] += 1
                return {
                    'file_name': os.path.basename(file_path),
                    'total_matches': 0,
                    'analyzed': 0,
                    'skipped': 0,
                    'errors': 1,
                    'skip_stats': dict(skip_stats),
                    'predictions': [],
                    'stats': {}
                }

            # Загружаем все строки
            for row in reader:
                date_str = row.get('Date', '').strip()
                dt = parse_date(date_str)
                if not dt:
                    skip_stats['INVALID_DATE'] += 1
                    continue

                # Получаем коэффициенты из разных колонок
                odds_h = get_odds_from_row(row, 'H')
                odds_d = get_odds_from_row(row, 'D')
                odds_a = get_odds_from_row(row, 'A')
                odds_over = get_odds_from_row(row, 'OVER')
                odds_under = get_odds_from_row(row, 'UNDER')

                # Если нашли хотя бы один коэффициент для тотала
                if odds_over is None and odds_under is not None:
                    # Конвертируем UNDER в OVER (1/UNDER)
                    try:
                        odds_over = 1.0 / odds_under
                    except:
                        pass

                all_matches.append({
                    'date': dt,
                    'date_str': date_str,
                    'home_team': row.get('HomeTeam', '').strip(),
                    'away_team': row.get('AwayTeam', '').strip(),
                    'fthg': safe_int(row.get('FTHG')),
                    'ftag': safe_int(row.get('FTAG')),
                    'odds_h': odds_h,
                    'odds_d': odds_d,
                    'odds_a': odds_a,
                    'odds_over': odds_over,
                    'odds_under': odds_under
                })

            # Сортируем по дате
            all_matches.sort(key=lambda x: x['date'])

    except Exception as e:
        print(f"❌ Ошибка при чтении файла: {e}")
        skip_stats['ENCODING_ERROR'] += 1
        return {
            'file_name': os.path.basename(file_path),
            'total_matches': 0,
            'analyzed': 0,
            'skipped': 0,
            'errors': 1,
            'skip_stats': dict(skip_stats),
            'predictions': [],
            'stats': {}
        }

    total_rows = len(all_matches)
    print(f"📊 Загружено строк: {total_rows}")

    # Результаты анализа
    stats = defaultdict(lambda: {'hits': 0, 'total': 0})
    predictions = []

    analyzed = 0
    errors = 0

    # Обрабатываем с конца (последние матчи первыми)
    for idx in range(total_rows - 1, -1, -1):
        match = all_matches[idx]

        try:
            if match['fthg'] is None or match['ftag'] is None:
                skip_stats['NO_SCORE'] += 1
                continue

            total_goals = match['fthg'] + match['ftag']

            if not match['odds_h'] or not match['odds_over']:
                skip_stats['NO_ODDS'] += 1
                continue

            # Получаем историю команд из ПРОШЛЫХ матчей
            home_history = []
            away_history = []

            for prev_idx in range(idx):
                prev_match = all_matches[prev_idx]
                if prev_match['fthg'] is not None and prev_match['ftag'] is not None:
                    if prev_match['home_team'] == match['home_team']:
                        home_history.append({
                            'home_score': prev_match['fthg'],
                            'away_score': prev_match['ftag']
                        })
                    if prev_match['away_team'] == match['away_team']:
                        away_history.append({
                            'home_score': prev_match['fthg'],
                            'away_score': prev_match['ftag']
                        })

            # Средние по лиге
            all_prev_matches = all_matches[:idx]
            if all_prev_matches:
                all_home_goals = [m['fthg'] for m in all_prev_matches if m['fthg'] is not None]
                all_away_goals = [m['ftag'] for m in all_prev_matches if m['ftag'] is not None]
                league_avg_home = sum(all_home_goals) / len(all_home_goals) if all_home_goals else 1.2
                league_avg_away = sum(all_away_goals) / len(all_away_goals) if all_away_goals else 1.0
            else:
                league_avg_home = 1.2
                league_avg_away = 1.0

            # Рассчитываем лямбды
            lambda_result = calculate_poisson_lambda_from_history(
                home_history, away_history,
                league_avg_home, league_avg_away
            )

            if lambda_result is None:
                skip_stats['INSUFFICIENT_HISTORY'] += 1
                continue

            lambda_home = lambda_result['home_lambda']
            lambda_away = lambda_result['away_lambda']

            # Получаем вероятность
            probs = get_poisson_probs(lambda_home, lambda_away)
            over25_prob = probs['over25_yes']

            # Определяем блоки
            odds_h_bin = get_odds_bin(match['odds_h'])
            odds_over_bin = get_odds_bin(match['odds_over'])
            prob_bin = get_probability_bin(over25_prob)

            # Ключ для статистики
            key = (odds_h_bin, odds_over_bin, prob_bin)

            # Обновляем статистику
            stats[key]['total'] += 1
            if total_goals > 2.5:
                stats[key]['hits'] += 1

            predictions.append({
                'date': match['date_str'],
                'home_team': match['home_team'],
                'away_team': match['away_team'],
                'fthg': match['fthg'],
                'ftag': match['ftag'],
                'total_goals': total_goals,
                'odds_h': match['odds_h'],
                'odds_over': match['odds_over'],
                'odds_h_bin': odds_h_bin,
                'odds_over_bin': odds_over_bin,
                'over25_prob': over25_prob,
                'prob_bin': prob_bin,
                'hit': total_goals > 2.5
            })

            analyzed += 1

        except Exception as e:
            print(f"❌ Ошибка при обработке строки: {e}")
            skip_stats['OTHER'] += 1
            errors += 1

    total_skipped = sum(skip_stats.values())

    print(f"   ✅ Проанализировано: {analyzed}")
    print(f"   ⚠️ Пропущено: {total_skipped}")
    for reason, count in skip_stats.items():
        if count > 0:
            print(f"      - {SKIP_REASONS[reason]}: {count}")

    return {
        'file_name': os.path.basename(file_path),
        'total_matches': total_rows,
        'analyzed': analyzed,
        'skipped': total_skipped,
        'errors': errors,
        'skip_stats': dict(skip_stats),
        'predictions': predictions,
        'stats': dict(stats)
    }


def analyze_folder(folder_path):
    """
    Анализирует все CSV файлы в указанной папке
    """
    print("\n" + "=" * 80)
    print(f"АНАЛИЗ ПАПКИ: {folder_path}")
    print("=" * 80)

    # Проверяем существование папки
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        print(f"❌ Папка не найдена: {folder_path}")
        return None

    # Находим все CSV файлы в папке
    csv_files = list(Path(folder_path).glob('*.csv'))

    if not csv_files:
        print(f"❌ В папке нет CSV файлов")
        return None

    print(f"📁 Найдено CSV файлов: {len(csv_files)}")

    # Общая статистика по всем файлам в папке
    folder_stats = {
        'folder_name': os.path.basename(folder_path),
        'folder_path': folder_path,
        'total_files': len(csv_files),
        'processed_files': 0,
        'files_with_errors': 0,
        'total_matches': 0,
        'total_analyzed': 0,
        'total_skipped': 0,
        'total_errors': 0,
        'files': [],
        'combined_stats': defaultdict(lambda: {'hits': 0, 'total': 0}),
        'combined_skip_stats': defaultdict(int),
        'predictions': []
    }

    # Обрабатываем каждый файл
    for csv_file in sorted(csv_files):
        print(f"\n{'=' * 60}")
        print(f"ОБРАБОТКА ФАЙЛА: {csv_file.name}")
        print('=' * 60)

        file_result = analyze_csv_file(str(csv_file))

        if file_result:
            folder_stats['processed_files'] += 1
            folder_stats['total_matches'] += file_result['total_matches']
            folder_stats['total_analyzed'] += file_result['analyzed']
            folder_stats['total_skipped'] += file_result['skipped']
            folder_stats['total_errors'] += file_result['errors']

            if file_result['errors'] > 0:
                folder_stats['files_with_errors'] += 1

            folder_stats['files'].append({
                'file_name': file_result['file_name'],
                'total_matches': file_result['total_matches'],
                'analyzed': file_result['analyzed'],
                'skipped': file_result['skipped'],
                'errors': file_result['errors'],
                'skip_stats': file_result.get('skip_stats', {})
            })

            # Объединяем статистику по пропускам
            for reason, count in file_result.get('skip_stats', {}).items():
                folder_stats['combined_skip_stats'][reason] += count

            # Объединяем статистику по блокам
            for key, data in file_result['stats'].items():
                folder_stats['combined_stats'][key]['total'] += data['total']
                folder_stats['combined_stats'][key]['hits'] += data['hits']

            # Сохраняем последние предсказания
            folder_stats['predictions'].extend(file_result['predictions'][-10:])

    # Ограничим предсказания
    if len(folder_stats['predictions']) > 100:
        folder_stats['predictions'] = folder_stats['predictions'][-100:]

    return folder_stats


def print_folder_stats(stats):
    """Выводит статистику по папке"""
    print("\n" + "=" * 80)
    print(f"СТАТИСТИКА ПО ПАПКЕ: {stats['folder_name']}")
    print("=" * 80)

    print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего файлов: {stats['total_files']}")
    print(f"   Обработано файлов: {stats['processed_files']}")
    print(f"   Файлов с ошибками: {stats['files_with_errors']}")
    print(f"   Всего матчей: {stats['total_matches']}")
    print(f"   ✅ Проанализировано: {stats['total_analyzed']}")
    print(f"   ⚠️ Пропущено: {stats['total_skipped']}")
    print(f"   ❌ Ошибок: {stats['total_errors']}")

    if stats['total_matches'] > 0:
        analyzed_percent = (stats['total_analyzed'] / stats['total_matches']) * 100
        print(f"\n📈 Процент проанализированных: {analyzed_percent:.1f}%")

    print("\n📊 ПРИЧИНЫ ПРОПУСКА:")
    total_skipped = stats['total_skipped']
    for reason_code, reason_name in SKIP_REASONS.items():
        count = stats['combined_skip_stats'].get(reason_code, 0)
        if count > 0:
            percent = (count / total_skipped) * 100 if total_skipped > 0 else 0
            print(f"   {reason_name}: {count} ({percent:.1f}%)")

    print("\n" + "=" * 80)
    print("СТАТИСТИКА ПО ФАЙЛАМ:")
    print("=" * 80)

    for f in stats['files']:
        analyzed_percent = (f['analyzed'] / f['total_matches']) * 100 if f['total_matches'] > 0 else 0
        error_mark = " ❌" if f['errors'] > 0 else ""
        print(f"{f['file_name']}{error_mark}: {f['analyzed']}/{f['total_matches']} = {analyzed_percent:.1f}%")

        # Детали по пропускам для файла
        if f.get('skip_stats'):
            for reason, count in f['skip_stats'].items():
                if count > 0:
                    print(f"      - {SKIP_REASONS[reason]}: {count}")

    if stats['combined_stats']:
        print("\n" + "=" * 80)
        print("ОБЪЕДИНЕННАЯ СТАТИСТИКА ПО БЛОКАМ")
        print("=" * 80)

        def sort_key(key):
            odds_h_bin, odds_over_bin, prob_bin = key
            odds_h_val = float(odds_h_bin.split('-')[0]) if odds_h_bin and '-' in odds_h_bin else 0
            odds_over_val = float(odds_over_bin.split('-')[0]) if odds_over_bin and '-' in odds_over_bin else 0
            prob_val = int(prob_bin.split('-')[0]) if prob_bin != '95-100%' else 95
            return (odds_h_val, odds_over_val, prob_val)

        sorted_keys = sorted(stats['combined_stats'].keys(), key=sort_key)

        total_analyzed = 0
        for key in sorted_keys:
            data = stats['combined_stats'][key]
            if data['total'] > 0:
                odds_h_bin, odds_over_bin, prob_bin = key
                hit_rate = (data['hits'] / data['total']) * 100
                print(
                    f"П1:{odds_h_bin} | ТБ:{odds_over_bin} | {prob_bin}: {data['hits']}/{data['total']} = {hit_rate:.1f}%")
                total_analyzed += data['total']

        print(f"\n📊 Всего учтено в статистике: {total_analyzed} матчей")


def find_first_csv_file():
    """Находит первый попавшийся CSV файл в папке ../all_matches/"""
    base_dir = Path(__file__).parent.parent
    all_matches_dir = base_dir / 'all_matches'

    if not all_matches_dir.exists():
        print(f"❌ Папка не найдена: {all_matches_dir}")
        return None

    for league_dir in all_matches_dir.iterdir():
        if league_dir.is_dir():
            return str(league_dir)

    print("❌ В папке all_matches нет подпапок с лигами")
    return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Анализ CSV файлов в папке')
    parser.add_argument('folder', nargs='?', default=None, help='Путь к папке с CSV файлами')

    args = parser.parse_args()

    # Если папка не указана, ищем ../all_matches/первую_попавшуюся_папку
    if not args.folder:
        args.folder = find_first_csv_file()
        if args.folder:
            print(f"📁 Выбрана папка: {args.folder}")
        else:
            print("❌ Не удалось найти папку для анализа")
            return

    # Анализируем папку
    stats = analyze_folder(args.folder)

    if stats and stats['processed_files'] > 0:
        # Выводим статистику
        print_folder_stats(stats)

        # Сохраняем результаты
        base_dir = Path(__file__).parent.parent
        output_dir = base_dir / 'analysis_results'
        output_dir.mkdir(exist_ok=True)

        folder_name = stats['folder_name'].replace(' ', '_').replace('/', '_')
        output_file = output_dir / f"{folder_name}_analysis.pkl"

        # Преобразуем ключи для сохранения
        save_stats = {}
        for key, value in stats['combined_stats'].items():
            odds_h_bin, odds_over_bin, prob_bin = key
            str_key = (str(odds_h_bin), str(odds_over_bin), str(prob_bin))
            save_stats[str_key] = value

        stats_for_save = {
            'folder_name': stats['folder_name'],
            'folder_path': stats['folder_path'],
            'total_files': stats['total_files'],
            'processed_files': stats['processed_files'],
            'files_with_errors': stats['files_with_errors'],
            'total_matches': stats['total_matches'],
            'total_analyzed': stats['total_analyzed'],
            'total_skipped': stats['total_skipped'],
            'total_errors': stats['total_errors'],
            'skip_stats': dict(stats['combined_skip_stats']),
            'files': stats['files'],
            'combined_stats': save_stats,
            'predictions': stats['predictions']
        }

        with open(output_file, 'wb') as f:
            pickle.dump(stats_for_save, f)

        print(f"\n💾 Результаты сохранены в: {output_file}")
        print("\n" + "🎯" * 10)
        print("АНАЛИЗ ПАПКИ ЗАВЕРШЕН")
        print("🎯" * 10)
    else:
        print("\n❌ НЕ УДАЛОСЬ ОБРАБОТАТЬ НИ ОДИН ФАЙЛ")


if __name__ == "__main__":
    main()