#!/usr/bin/env python
import os
import sys
import csv
import pickle
import math
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Константы для анализа
MIN_MATCHES = 3
MAX_MATCHES = 7

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


def safe_int(value):
    """Безопасное преобразование в int"""
    if not value or str(value).strip() == '' or str(value).lower() == 'nan':
        return None
    try:
        return int(float(str(value).replace(',', '.')))
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """Безопасное преобразование в float"""
    if not value or str(value).strip() == '' or str(value).lower() == 'nan':
        return None
    try:
        return float(str(value).replace(',', '.'))
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
    Анализирует один CSV файл полностью из файла, без БД
    """
    print("\n" + "=" * 80)
    print(f"АНАЛИЗ ФАЙЛА: {file_path}")
    print("=" * 80)

    # Проверяем существование файла
    if not os.path.exists(file_path):
        print(f"❌ Файл не найден: {file_path}")
        return None

    print(f"📁 Размер файла: {os.path.getsize(file_path)} байт")

    # Определяем разделитель
    delimiter = detect_delimiter(file_path)
    print(f"📊 Определен разделитель: '{delimiter}'")

    # Загружаем все данные из файла
    all_matches = []

    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            fieldnames = reader.fieldnames
            print(f"📋 Найдено колонок: {len(fieldnames)}")

            # Проверяем наличие необходимых колонок
            required_cols = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'AvgH', 'Avg>2.5']
            missing_cols = [col for col in required_cols if col not in fieldnames]

            if missing_cols:
                print(f"❌ Отсутствуют обязательные колонки: {missing_cols}")
                return None

            # Загружаем все строки в хронологическом порядке (от старых к новым)
            for row in reader:
                date_str = row.get('Date', '').strip()
                dt = parse_date(date_str)
                if not dt:
                    print(f"⚠️ Пропускаем строку с некорректной датой: {date_str}")
                    continue

                all_matches.append({
                    'date': dt,
                    'date_str': date_str,
                    'home_team': row.get('HomeTeam', '').strip(),
                    'away_team': row.get('AwayTeam', '').strip(),
                    'fthg': safe_int(row.get('FTHG')),
                    'ftag': safe_int(row.get('FTAG')),
                    'odds_h': safe_float(row.get('AvgH')),
                    'odds_over': safe_float(row.get('Avg>2.5'))
                })

            # Сортируем по дате (на всякий случай)
            all_matches.sort(key=lambda x: x['date'])

    except Exception as e:
        print(f"❌ Ошибка при чтении файла: {e}")
        return None

    total_rows = len(all_matches)
    print(f"\n📊 Загружено матчей: {total_rows}")

    # Результаты анализа
    stats = defaultdict(lambda: {'hits': 0, 'total': 0})
    predictions = []

    analyzed = 0
    skipped = 0
    errors = 0

    # Обрабатываем с конца (последние матчи первыми)
    for idx in range(total_rows - 1, -1, -1):
        match = all_matches[idx]
        match_num = idx + 1
        print(f"\n--- МАТЧ #{match_num} (с конца: {total_rows - idx}) ---")

        try:
            print(f"📅 Дата: {match['date_str']}")
            print(f"⚽ {match['home_team']} vs {match['away_team']}")

            if match['fthg'] is None or match['ftag'] is None:
                print("⚠️ Нет данных о счете - пропускаем")
                skipped += 1
                continue

            total_goals = match['fthg'] + match['ftag']
            print(f"🎯 Счет: {match['fthg']}:{match['ftag']} (тотал: {total_goals})")

            if not match['odds_h'] or not match['odds_over']:
                print("⚠️ Нет коэффициентов П1 или ТБ2.5 - пропускаем")
                skipped += 1
                continue

            # Получаем историю команд из ПРОШЛЫХ матчей (с меньшими индексами)
            home_history = []
            away_history = []

            for prev_idx in range(idx):  # только матчи до текущего
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

            print(f"📊 История команд до этого матча:")
            print(f"   {match['home_team']}: {len(home_history)} домашних матчей")
            print(f"   {match['away_team']}: {len(away_history)} гостевых матчей")

            # Рассчитываем средние по лиге (по всем матчам ДО текущего)
            all_prev_matches = all_matches[:idx]
            if all_prev_matches:
                all_home_goals = [m['fthg'] for m in all_prev_matches if m['fthg'] is not None]
                all_away_goals = [m['ftag'] for m in all_prev_matches if m['ftag'] is not None]
                league_avg_home = sum(all_home_goals) / len(all_home_goals) if all_home_goals else 1.2
                league_avg_away = sum(all_away_goals) / len(all_away_goals) if all_away_goals else 1.0
            else:
                league_avg_home = 1.2
                league_avg_away = 1.0

            print(f"📊 Средние по лиге (по {len(all_prev_matches)} предыдущим матчам):")
            print(f"   Голов хозяев: {league_avg_home:.2f}, гостей: {league_avg_away:.2f}")

            # Рассчитываем лямбды
            lambda_result = calculate_poisson_lambda_from_history(
                home_history, away_history,
                league_avg_home, league_avg_away
            )

            if lambda_result is None:
                print(f"⚠️ Недостаточно истории (минимум {MIN_MATCHES} матчей) - пропускаем")
                skipped += 1
                continue

            lambda_home = lambda_result['home_lambda']
            lambda_away = lambda_result['away_lambda']

            print(f"📊 Лямбды Пуассона: {lambda_home:.2f} (хозяева) : {lambda_away:.2f} (гости)")

            # Получаем вероятность
            probs = get_poisson_probs(lambda_home, lambda_away)
            over25_prob = probs['over25_yes']

            print(f"📈 Вероятность ТБ2.5: {over25_prob:.1f}%")

            # Определяем блоки
            odds_h_bin = get_odds_bin(match['odds_h'])
            odds_over_bin = get_odds_bin(match['odds_over'])
            prob_bin = get_probability_bin(over25_prob)

            print(f"   Блок П1: {odds_h_bin}")
            print(f"   Блок ТБ: {odds_over_bin}")
            print(f"   Блок вероятности: {prob_bin}")

            # Ключ для статистики
            key = (odds_h_bin, odds_over_bin, prob_bin)

            # Обновляем статистику
            stats[key]['total'] += 1
            if total_goals > 2.5:
                stats[key]['hits'] += 1
                print(f"✅ РЕЗУЛЬТАТ: Тотал >2.5 - ДА")
            else:
                print(f"❌ РЕЗУЛЬТАТ: Тотал >2.5 - НЕТ")

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
            print(f"✅ Матч обработан (#{analyzed})")
            print("-" * 40)

        except Exception as e:
            print(f"❌ Ошибка при обработке: {e}")
            import traceback
            traceback.print_exc()
            errors += 1

    # Формируем результаты
    results = {
        'file_name': os.path.basename(file_path),
        'total_matches': total_rows,
        'analyzed': analyzed,
        'skipped': skipped,
        'errors': errors,
        'predictions': predictions,
        'stats': dict(stats)
    }

    # Выводим итоговую статистику
    print("\n" + "=" * 80)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 80)
    print(f"📊 Всего матчей в файле: {total_rows}")
    print(f"✅ Проанализировано: {analyzed}")
    print(f"⚠️ Пропущено: {skipped}")
    print(f"❌ Ошибок: {errors}")

    if analyzed > 0:
        print(f"\n📈 Процент проанализированных: {(analyzed / total_rows) * 100:.1f}%")

        print("\n" + "=" * 80)
        print("СТАТИСТИКА ПО ТРЕМ БЛОКАМ")
        print("=" * 80)

        def sort_key(key):
            odds_h_bin, odds_over_bin, prob_bin = key
            odds_h_val = float(odds_h_bin.split('-')[0]) if odds_h_bin and '-' in odds_h_bin else 0
            odds_over_val = float(odds_over_bin.split('-')[0]) if odds_over_bin and '-' in odds_over_bin else 0
            prob_val = int(prob_bin.split('-')[0]) if prob_bin != '95-100%' else 95
            return (odds_h_val, odds_over_val, prob_val)

        sorted_keys = sorted(stats.keys(), key=sort_key)

        total_analyzed = 0
        for key in sorted_keys:
            data = stats[key]
            if data['total'] > 0:
                odds_h_bin, odds_over_bin, prob_bin = key
                hit_rate = (data['hits'] / data['total']) * 100
                print(
                    f"П1:{odds_h_bin} | ТБ:{odds_over_bin} | {prob_bin}: {data['hits']}/{data['total']} = {hit_rate:.1f}%")
                total_analyzed += data['total']

        print(f"\n📊 Всего учтено в статистике: {total_analyzed} матчей")

    return results


def find_first_csv_file():
    """Находит первый попавшийся CSV файл в папке ../all_matches/"""
    base_dir = Path(__file__).parent.parent
    all_matches_dir = base_dir / 'all_matches'

    if not all_matches_dir.exists():
        print(f"❌ Папка не найдена: {all_matches_dir}")
        return None

    for league_dir in all_matches_dir.iterdir():
        if not league_dir.is_dir():
            continue

        csv_files = list(league_dir.glob('*.csv'))
        if csv_files:
            return str(csv_files[0])

    print("❌ CSV файлы не найдены")
    return None


def main():
    print("\n" + "🚀" * 10)
    print("ЗАПУСК АНАЛИЗА CSV ФАЙЛА")
    print("🚀" * 10 + "\n")

    file_path = find_first_csv_file()

    if not file_path:
        print("❌ Не удалось найти CSV файл")
        return

    print(f"📁 Найден файл: {file_path}")

    result = analyze_csv_file(file_path)

    if result:
        base_dir = Path(__file__).parent.parent
        output_dir = base_dir / 'analysis_results'
        output_dir.mkdir(exist_ok=True)

        file_stem = Path(file_path).stem
        output_file = output_dir / f"{file_stem}_analysis.pkl"

        with open(output_file, 'wb') as f:
            result_for_save = {
                'file_name': result['file_name'],
                'total_matches': result['total_matches'],
                'analyzed': result['analyzed'],
                'skipped': result['skipped'],
                'errors': result['errors'],
                'stats': dict(result['stats']),
                'predictions': result['predictions'][-100:]
            }
            pickle.dump(result_for_save, f)

        print(f"\n💾 Результаты сохранены в: {output_file}")
        print("\n" + "🎯" * 10)
        print("АНАЛИЗ ЗАВЕРШЕН")
        print("🎯" * 10)
    else:
        print("\n❌ АНАЛИЗ НЕ УДАЛСЯ")


if __name__ == "__main__":
    main()