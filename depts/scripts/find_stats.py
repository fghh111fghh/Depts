#!/usr/bin/env python
import pickle
from pathlib import Path

# Функции для определения блоков (копируем из основного скрипта)
PROBABILITY_BINS = [
    (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
    (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
    (50, 55), (55, 60), (60, 65), (65, 70), (70, 75),
    (75, 80), (80, 85), (85, 90), (90, 95), (95, 100)
]

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


def load_pkl_file(file_path):
    """Загружает данные из PKL файла"""
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        print(f"✅ Файл загружен: {file_path}")
        return data
    except Exception as e:
        print(f"❌ Ошибка загрузки файла: {e}")
        return None


def find_stats_for_matches(league_stats, matches_data):
    """
    Находит total и hits для каждого матча
    """
    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТЫ ПОИСКА ПО БЛОКАМ")
    print("=" * 80)

    results = []

    for i, match in enumerate(matches_data, 1):
        league = match['league']
        p1 = match['p1']
        tb = match['tb']
        prob = match['prob']

        # Определяем блоки
        p1_bin = get_odds_bin(p1)
        tb_bin = get_odds_bin(tb)
        prob_bin = get_probability_bin(prob)

        key = (p1_bin, tb_bin, prob_bin)

        print(f"\n--- Матч #{i} ---")
        print(f"Исходные данные: П1={p1}, ТБ={tb}, вероятность={prob}%")
        print(f"Блоки: П1:{p1_bin} | ТБ:{tb_bin} | {prob_bin}")

        # Ищем в статистике лиги
        if league in league_stats:
            if key in league_stats[league]:
                data = league_stats[league][key]
                hit_rate = (data['hits'] / data['total']) * 100 if data['total'] > 0 else 0
                print(f"✅ НАЙДЕНО!")
                print(f"   total: {data['total']}")
                print(f"   hits: {data['hits']}")
                print(f"   hit_rate: {hit_rate:.1f}%")

                results.append({
                    'match': i,
                    'league': league,
                    'p1': p1,
                    'tb': tb,
                    'prob': prob,
                    'p1_bin': p1_bin,
                    'tb_bin': tb_bin,
                    'prob_bin': prob_bin,
                    'total': data['total'],
                    'hits': data['hits'],
                    'hit_rate': hit_rate
                })
            else:
                print(f"❌ Ключ не найден в статистике лиги")

                # Поиск похожих ключей для отладки
                print("   Похожие ключи в данных:")
                similar_found = 0
                for existing_key in league_stats[league].keys():
                    if p1_bin in existing_key[0] or tb_bin in existing_key[1]:
                        print(f"     {existing_key[0]} | {existing_key[1]} | {existing_key[2]}")
                        similar_found += 1
                        if similar_found >= 3:
                            break
        else:
            print(f"❌ Лига '{league}' не найдена в данных")

    return results


def print_summary(results):
    """Выводит сводную таблицу результатов"""
    if not results:
        print("\n❌ Нет результатов для отображения")
        return

    print("\n" + "=" * 80)
    print("СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ")
    print("=" * 80)
    print(f"{'№':<3} {'П1 блок':<15} {'ТБ блок':<15} {'Вероятность':<12} {'total':<8} {'hits':<8} {'hit_rate':<8}")
    print("-" * 80)

    for r in results:
        print(
            f"{r['match']:<3} {r['p1_bin']:<15} {r['tb_bin']:<15} {r['prob_bin']:<12} {r['total']:<8} {r['hits']:<8} {r['hit_rate']:<6.1f}%")


def main():
    print("\n" + "🚀" * 10)
    print("ПОИСК ДАННЫХ ПО БЛОКАМ")
    print("🚀" * 10 + "\n")

    # Путь к PKL файлу
    base_dir = Path(__file__).parent.parent
    pkl_file = base_dir / 'analysis_results' / 'all_leagues_complete_stats.pkl'

    # Загружаем данные
    league_stats = load_pkl_file(pkl_file)

    if not league_stats:
        # Пробуем найти другой файл
        alt_file = base_dir / 'analysis_results' / 'all_leagues_stats.pkl'
        league_stats = load_pkl_file(alt_file)

        if not league_stats:
            print("❌ Не удалось загрузить данные")
            return

    # Данные для поиска
    matches_data = [
        {'league': 'Чемпионшип Англия', 'p1': 2.41, 'tb': 2.15, 'prob': 20.13},
        {'league': 'Чемпионшип Англия', 'p1': 1.91, 'tb': 2.40, 'prob': 50.14},
        {'league': 'Чемпионшип Англия', 'p1': 1.50, 'tb': 1.69, 'prob': 65.35},
        {'league': 'Чемпионшип Англия', 'p1': 2.06, 'tb': 2.04, 'prob': 82.73},
        {'league': 'Чемпионшип Англия', 'p1': 1.80, 'tb': 2.07, 'prob': 34.49},
        {'league': 'Чемпионшип Англия', 'p1': 3.23, 'tb': 2.13, 'prob': 46.89},
        {'league': 'Чемпионшип Англия', 'p1': 2.53, 'tb': 2.02, 'prob': 45.10},
        {'league': 'Чемпионшип Англия', 'p1': 1.68, 'tb': 1.69, 'prob': 17.38},
    ]

    # Ищем статистику
    results = find_stats_for_matches(league_stats, matches_data)

    # Выводим сводную таблицу
    print_summary(results)

    print("\n" + "🎯" * 10)
    print("ПОИСК ЗАВЕРШЕН")
    print("🎯" * 10)


if __name__ == "__main__":
    main()