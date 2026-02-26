
import os
import csv
import pickle
import math
import chardet
from datetime import datetime
from pathlib import Path

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


def detect_encoding(file_path):
    """Определяет кодировку файла"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        return result['encoding']


def get_odds_from_row(row, odds_type):
    """
    Ищет коэффициент в различных колонках
    """
    odds_mapping = {
        'H': [
            'B365H', 'BWH', 'IWH', 'LBH', 'PSH', 'WHH', 'SJH', 'VCH',
            'AvgH', 'MaxH', 'BbAvH', 'BbMxH',
            'BFH', 'BFEH', 'PSCH', 'BWCH', 'BFCH', 'WHCH', '1XBH', 'MaxCH', 'AvgCH'
        ],
        'OVER': [
            'B365>2.5', 'P>2.5', 'Max>2.5', 'Avg>2.5',
            'BbMx>2.5', 'BbAv>2.5',
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

        l_avg_home_goals = max(league_avg_home, 1.0)
        l_avg_away_goals = max(league_avg_away, 0.8)
        l_avg_home_conceded = l_avg_away_goals
        l_avg_away_conceded = l_avg_home_goals

        home_recent = home_history[-MAX_MATCHES:] if len(home_history) > MAX_MATCHES else home_history
        away_recent = away_history[-MAX_MATCHES:] if len(away_history) > MAX_MATCHES else away_history

        h_avg_scored = sum(m['home_score'] for m in home_recent) / len(home_recent)
        h_avg_conceded = sum(m['away_score'] for m in home_recent) / len(home_recent)
        h_avg_scored = max(h_avg_scored, 0.5)
        h_avg_conceded = max(h_avg_conceded, 0.5)
        home_attack = h_avg_scored / l_avg_home_goals
        home_defense = h_avg_conceded / l_avg_home_conceded

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


def analyze_league_folder(folder_path):
    """
    Анализирует все CSV файлы в папке одной лиги
    Возвращает статистику по лиге и мета-информацию
    """
    league_name = os.path.basename(folder_path)
    print(f"\n{'=' * 60}")
    print(f"ЛИГА: {league_name}")
    print(f"{'=' * 60}")

    csv_files = list(Path(folder_path).glob('*.csv'))
    total_files = len(csv_files)
    print(f"📁 Найдено файлов: {total_files}")

    if total_files == 0:
        print(f"❌ Нет CSV файлов")
        return None, None

    processed_files = 0
    all_stats = {}
    total_league_matches = 0
    total_analyzed_matches = 0

    for csv_file in sorted(csv_files):
        print(f"\n--- Файл: {csv_file.name} ---")

        try:
            encoding = detect_encoding(str(csv_file))
            print(f"   📄 Кодировка: {encoding}")
        except:
            encoding = 'utf-8-sig'
            print(f"   📄 Кодировка: {encoding} (по умолчанию)")

        try:
            delimiter = detect_delimiter(str(csv_file))
            print(f"   📊 Разделитель: '{delimiter}'")
        except Exception as e:
            print(f"   ❌ Ошибка определения разделителя: {e}")
            continue

        all_matches = []

        try:
            with open(csv_file, mode='r', encoding=encoding, errors='replace') as f:
                reader = csv.DictReader(f, delimiter=delimiter)

                fieldnames = reader.fieldnames
                if not fieldnames:
                    print("   ❌ Файл пуст или не содержит заголовков")
                    continue

                required_cols = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
                missing_cols = [col for col in required_cols if col not in fieldnames]

                if missing_cols:
                    print(f"   ❌ Отсутствуют обязательные колонки: {missing_cols}")
                    continue

                for row in reader:
                    date_str = row.get('Date', '').strip()
                    dt = parse_date(date_str)
                    if not dt:
                        continue

                    odds_h = get_odds_from_row(row, 'H')
                    odds_over = get_odds_from_row(row, 'OVER')

                    if odds_over is None:
                        odds_under = get_odds_from_row(row, 'UNDER')
                        if odds_under is not None and odds_under > 0:
                            odds_over = 1.0 / odds_under

                    if odds_h is not None and odds_over is not None:
                        all_matches.append({
                            'date': dt,
                            'date_str': date_str,
                            'home_team': row.get('HomeTeam', '').strip(),
                            'away_team': row.get('AwayTeam', '').strip(),
                            'fthg': safe_int(row.get('FTHG')),
                            'ftag': safe_int(row.get('FTAG')),
                            'odds_h': odds_h,
                            'odds_over': odds_over
                        })

                all_matches.sort(key=lambda x: x['date'])

        except Exception as e:
            print(f"   ❌ Ошибка при чтении файла: {e}")
            continue

        file_matches = len(all_matches)
        total_league_matches += file_matches
        print(f"   📊 Загружено матчей с коэффициентами: {file_matches}")

        file_stats = {}
        analyzed = 0

        for idx in range(file_matches - 1, -1, -1):
            match = all_matches[idx]

            try:
                if match['fthg'] is None or match['ftag'] is None:
                    continue

                total_goals = match['fthg'] + match['ftag']

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

                all_prev_matches = all_matches[:idx]
                if all_prev_matches:
                    all_home_goals = [m['fthg'] for m in all_prev_matches if m['fthg'] is not None]
                    all_away_goals = [m['ftag'] for m in all_prev_matches if m['ftag'] is not None]
                    league_avg_home = sum(all_home_goals) / len(all_home_goals) if all_home_goals else 1.2
                    league_avg_away = sum(all_away_goals) / len(all_away_goals) if all_away_goals else 1.0
                else:
                    league_avg_home = 1.2
                    league_avg_away = 1.0

                lambda_result = calculate_poisson_lambda_from_history(
                    home_history, away_history,
                    league_avg_home, league_avg_away
                )

                if lambda_result is None:
                    continue

                lambda_home = lambda_result['home_lambda']
                lambda_away = lambda_result['away_lambda']

                probs = get_poisson_probs(lambda_home, lambda_away)
                over25_prob = probs['over25_yes']

                odds_h_bin = get_odds_bin(match['odds_h'])
                odds_over_bin = get_odds_bin(match['odds_over'])
                prob_bin = get_probability_bin(over25_prob)

                key = (odds_h_bin, odds_over_bin, prob_bin)

                if key not in file_stats:
                    file_stats[key] = {'total': 0, 'hits': 0}

                file_stats[key]['total'] += 1
                if total_goals > 2.5:
                    file_stats[key]['hits'] += 1

                analyzed += 1

            except Exception:
                continue

        print(f"   ✅ Проанализировано матчей в файле: {analyzed}")
        total_analyzed_matches += analyzed

        # Объединяем статистику из файла в общую
        for key, data in file_stats.items():
            if key not in all_stats:
                all_stats[key] = {'total': 0, 'hits': 0}
            all_stats[key]['total'] += data['total']
            all_stats[key]['hits'] += data['hits']

        processed_files += 1

    # Мета-информация по лиге
    league_info = {
        'name': league_name,
        'total_files': total_files,
        'processed_files': processed_files,
        'total_matches': total_league_matches,
        'analyzed_matches': total_analyzed_matches,
        'blocks_count': len(all_stats)
    }

    print(f"\n📊 ИТОГО ПО ЛИГЕ {league_name}:")
    print(f"   Обработано файлов: {processed_files}/{total_files}")
    print(f"   Всего матчей в файлах: {total_league_matches}")
    print(f"   Проанализировано матчей: {total_analyzed_matches}")
    print(f"   Получено блоков: {len(all_stats)}")

    return league_info, all_stats


def main():
    print("\n" + "🚀" * 10)
    print("ЗАПУСК АНАЛИЗА ВСЕХ ЛИГ")
    print("🚀" * 10 + "\n")

    base_dir = Path(__file__).parent.parent
    all_matches_dir = base_dir / 'all_matches'

    if not all_matches_dir.exists():
        print(f"❌ Папка не найдена: {all_matches_dir}")
        return

    # Находим все папки с лигами
    league_folders = [f for f in all_matches_dir.iterdir() if f.is_dir()]

    if not league_folders:
        print(f"❌ В папке {all_matches_dir} нет подпапок с лигами")
        return

    print(f"📁 Найдено лиг: {len(league_folders)}")

    # Словари для общей статистики
    all_leagues_stats = {}
    all_leagues_info = []

    # Анализируем каждую лигу
    for folder in sorted(league_folders):
        league_info, league_stats = analyze_league_folder(str(folder))

        if league_stats:
            all_leagues_stats[league_info['name']] = league_stats
            all_leagues_info.append(league_info)

    if all_leagues_stats:
        # Сохраняем результаты
        output_dir = base_dir / 'analysis_results'
        output_dir.mkdir(exist_ok=True)

        output_file = output_dir / 'all_leagues_complete_stats.pkl'

        with open(output_file, 'wb') as f:
            pickle.dump(all_leagues_stats, f)

        print(f"\n💾 Полные результаты сохранены в: {output_file}")

        # Сохраняем также сводную информацию
        summary_file = output_dir / 'summary_info.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("СВОДНАЯ ИНФОРМАЦИЯ ПО ВСЕМ ЛИГАМ\n")
            f.write("=" * 50 + "\n\n")

            for info in all_leagues_info:
                f.write(f"Лига: {info['name']}\n")
                f.write(f"  Файлов: {info['processed_files']}/{info['total_files']}\n")
                f.write(f"  Всего матчей: {info['total_matches']}\n")
                f.write(f"  Проанализировано: {info['analyzed_matches']}\n")
                f.write(f"  Блоков: {info['blocks_count']}\n\n")

        print(f"📊 Сводная информация сохранена в: {summary_file}")

        # Выводим итоговую статистику
        print("\n" + "=" * 80)
        print("ИТОГОВАЯ СТАТИСТИКА ПО ВСЕМ ЛИГАМ")
        print("=" * 80)

        total_analyzed = 0
        total_matches = 0

        for info in all_leagues_info:
            print(f"\n📊 {info['name']}:")
            print(f"   Файлов: {info['processed_files']}/{info['total_files']}")
            print(f"   Всего матчей: {info['total_matches']}")
            print(f"   Проанализировано: {info['analyzed_matches']}")
            print(f"   Блоков: {info['blocks_count']}")

            total_matches += info['total_matches']
            total_analyzed += info['analyzed_matches']

        print("\n" + "-" * 40)
        print(f"ВСЕГО ПО ВСЕМ ЛИГАМ:")
        print(f"   Всего матчей: {total_matches}")
        print(f"   Проанализировано: {total_analyzed}")

        print("\n" + "🎯" * 10)
        print("АНАЛИЗ ВСЕХ ЛИГ ЗАВЕРШЕН")
        print("🎯" * 10)
    else:
        print("\n❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ СТАТИСТИКУ НИ ПО ОДНОЙ ЛИГЕ")


if __name__ == "__main__":
    main()