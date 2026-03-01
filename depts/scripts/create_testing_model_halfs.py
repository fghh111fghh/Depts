# ============================================================================
# СОЗДАНИЕ МОДЕЛИ ПРОГНОЗИРОВАНИЯ НА ВСЕХ ДАННЫХ (15 ЛИГ, 20+ ЛЕТ)
# ============================================================================

import os
import csv
import pickle
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import random

print("\n" + "=" * 80)
print("СОЗДАНИЕ МОДЕЛИ ПРОГНОЗИРОВАНИЯ НА 15 ЛИГАХ")
print("=" * 80 + "\n")

# ============================================================================
# 1. ОПРЕДЕЛЯЕМ ПУТИ
# ============================================================================

base_dir = Path(r'c:\Users\admin\Desktop\Новая папка\Depts\depts\all_matches')
league_folders = [f for f in base_dir.iterdir() if f.is_dir()]

print(f"📁 Найдено лиг: {len(league_folders)}")
for folder in league_folders:
    print(f"  - {folder.name}")

# ============================================================================
# 2. СБОР СТАТИСТИКИ ПО ВСЕМ ЛИГАМ
# ============================================================================

print("\n" + "=" * 80)
print("2. СБОР СТАТИСТИКИ ПО ВСЕМ ЛИГАМ")
print("=" * 80)


def extract_league_name(filepath):
    """Извлекает название лиги из пути"""
    return filepath.parent.name


def process_csv_file(filepath, stats):
    """Обрабатывает один CSV файл"""
    league_name = extract_league_name(filepath)

    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    # Проверяем наличие всех нужных полей
                    if not all(k in row for k in ['FTHG', 'FTAG', 'HTHG', 'HTAG', 'B365H', 'B365A']):
                        continue

                    # Основные данные
                    fthg = int(row['FTHG'])
                    ftag = int(row['FTAG'])
                    hthg = int(row['HTHG'])
                    htag = int(row['HTAG'])

                    # Коэффициенты
                    odds_h = float(row['B365H']) if row['B365H'] else 0
                    odds_a = float(row['B365A']) if row['B365A'] else 0

                    if odds_h == 0 or odds_a == 0:
                        continue

                    # Голы по таймам
                    first_half = hthg + htag
                    second_half = (fthg + ftag) - first_half

                    # Результат сравнения таймов
                    if second_half > first_half:
                        half_result = 'SECOND'
                    elif first_half > second_half:
                        half_result = 'FIRST'
                    else:
                        half_result = 'EQUAL'

                    # Определяем фаворита
                    if odds_h < odds_a:
                        fav_odds = odds_h
                        fav_type = 'HOME'
                    else:
                        fav_odds = odds_a
                        fav_type = 'AWAY'

                    # Сохраняем матч для тестовой выборки
                    match_data = {
                        'league': league_name,
                        'date': row.get('Date', ''),
                        'home': row.get('HomeTeam', ''),
                        'away': row.get('AwayTeam', ''),
                        'odds_h': odds_h,
                        'odds_a': odds_a,
                        'fav_odds': fav_odds,
                        'fav_type': fav_type,
                        'first_half': first_half,
                        'second_half': second_half,
                        'half_result': half_result,
                        'total_goals': fthg + ftag
                    }

                    # Добавляем в статистику
                    stats['all_matches'].append(match_data)

                except (ValueError, KeyError):
                    continue

    except Exception as e:
        print(f"  Ошибка в файле {filepath.name}: {e}")


# Собираем все матчи
all_matches = []
total_files = 0

for league_folder in league_folders:
    csv_files = list(league_folder.glob('*.csv'))
    total_files += len(csv_files)
    print(f"\n📊 Обработка лиги: {league_folder.name} ({len(csv_files)} файлов)")

    stats = {'all_matches': []}
    for csv_file in csv_files:
        process_csv_file(csv_file, stats)

    all_matches.extend(stats['all_matches'])
    print(f"  ✅ Загружено матчей: {len(stats['all_matches'])}")

print(f"\n📊 ВСЕГО ЗАГРУЖЕНО МАТЧЕЙ: {len(all_matches)} из {total_files} файлов")

# ============================================================================
# 3. РАЗДЕЛЕНИЕ НА ОБУЧАЮЩУЮ И ТЕСТОВУЮ ВЫБОРКИ
# ============================================================================

print("\n" + "=" * 80)
print("3. РАЗДЕЛЕНИЕ НА ОБУЧАЮЩУЮ И ТЕСТОВУЮ ВЫБОРКИ")
print("=" * 80)

# Сортируем по дате (если есть)
# all_matches.sort(key=lambda x: x['date'])  # раскомментировать если есть даты

# Берем последние 100 матчей для теста
test_matches = all_matches[-100:]
train_matches = all_matches[:-100]

print(f"📊 Обучающая выборка: {len(train_matches)} матчей")
print(f"📊 Тестовая выборка: {len(test_matches)} матчей")

# ============================================================================
# 4. СОЗДАНИЕ МОДЕЛИ НА ОБУЧАЮЩЕЙ ВЫБОРКЕ
# ============================================================================

print("\n" + "=" * 80)
print("4. СОЗДАНИЕ МОДЕЛИ НА ОБУЧАЮЩЕЙ ВЫБОРКЕ")
print("=" * 80)

# Статистика по лигам
league_stats = defaultdict(lambda: {
    'total': 0,
    'second_more': 0,
    'first_more': 0,
    'equal': 0,
    'total_goals': 0
})

# Статистика по коэффициентам
odds_stats = defaultdict(lambda: {
    'total': 0,
    'second_more': 0,
    'first_more': 0,
    'equal': 0
})

# Статистика по фаворитам
fav_stats = defaultdict(lambda: {
    'total': 0,
    'second_more': 0,
    'first_more': 0
})

for match in train_matches:
    league = match['league']
    result = match['half_result']
    fav_odds = match['fav_odds']

    # Статистика по лигам
    league_stats[league]['total'] += 1
    league_stats[league]['total_goals'] += match['total_goals']
    if result == 'SECOND':
        league_stats[league]['second_more'] += 1
    elif result == 'FIRST':
        league_stats[league]['first_more'] += 1
    else:
        league_stats[league]['equal'] += 1

    # Статистика по коэффициентам
    if fav_odds < 1.5:
        range_key = '<1.5'
    elif fav_odds < 2.0:
        range_key = '1.5-2.0'
    elif fav_odds < 2.5:
        range_key = '2.0-2.5'
    elif fav_odds < 3.0:
        range_key = '2.5-3.0'
    else:
        range_key = '>3.0'

    odds_stats[range_key]['total'] += 1
    if result == 'SECOND':
        odds_stats[range_key]['second_more'] += 1
    elif result == 'FIRST':
        odds_stats[range_key]['first_more'] += 1
    else:
        odds_stats[range_key]['equal'] += 1

    # Статистика по фаворитам (дома/в гостях)
    fav_stats[match['fav_type']]['total'] += 1
    if result == 'SECOND':
        fav_stats[match['fav_type']]['second_more'] += 1
    elif result == 'FIRST':
        fav_stats[match['fav_type']]['first_more'] += 1

# Нормализация
print("\n📊 СТАТИСТИКА ПО ЛИГАМ:")
for league, stats in sorted(league_stats.items(), key=lambda x: x[1]['total'], reverse=True):
    total = stats['total']
    if total > 0:
        pct_second = stats['second_more'] / total * 100
        pct_first = stats['first_more'] / total * 100
        pct_equal = stats['equal'] / total * 100
        avg_goals = stats['total_goals'] / total
        print(f"  {league[:25]:<25} "
              f"2-й>{pct_second:5.1f}% | "
              f"1-й>{pct_first:5.1f}% | "
              f"равно{pct_equal:5.1f}% | "
              f"ср.голы {avg_goals:.2f}")

print("\n📊 СТАТИСТИКА ПО КОЭФФИЦИЕНТАМ:")
for range_key, stats in sorted(odds_stats.items()):
    total = stats['total']
    if total > 0:
        pct_second = stats['second_more'] / total * 100
        pct_first = stats['first_more'] / total * 100
        print(f"  {range_key:>6}: {total:6d} матчей, 2-й>{pct_second:5.1f}%, 1-й>{pct_first:5.1f}%")

print("\n📊 СТАТИСТИКА ПО ФАВОРИТАМ:")
for fav_type, stats in fav_stats.items():
    total = stats['total']
    if total > 0:
        pct_second = stats['second_more'] / total * 100
        print(f"  {fav_type}: {total:6d} матчей, 2-й>{pct_second:5.1f}%")


# ============================================================================
# 5. ФУНКЦИЯ ПРОГНОЗА
# ============================================================================

def predict(match, league_stats, odds_stats, fav_stats):
    """
    Прогнозирует результат сравнения таймов
    Возвращает: 'SECOND', 'FIRST' или 'EQUAL'
    """
    league = match['league']
    fav_odds = match['fav_odds']
    fav_type = match['fav_type']

    # Базовые вероятности из статистики лиги
    league_data = league_stats.get(league, {
        'second_more': 0, 'first_more': 0, 'equal': 0, 'total': 1
    })
    total = max(league_data['total'], 1)

    prob_second_league = league_data['second_more'] / total * 100
    prob_first_league = league_data['first_more'] / total * 100
    prob_equal_league = league_data['equal'] / total * 100

    # Коррекция по коэффициентам
    if fav_odds < 1.5:
        odds_data = odds_stats.get('<1.5', {'second_more': 0, 'total': 1})
    elif fav_odds < 2.0:
        odds_data = odds_stats.get('1.5-2.0', {'second_more': 0, 'total': 1})
    elif fav_odds < 2.5:
        odds_data = odds_stats.get('2.0-2.5', {'second_more': 0, 'total': 1})
    elif fav_odds < 3.0:
        odds_data = odds_stats.get('2.5-3.0', {'second_more': 0, 'total': 1})
    else:
        odds_data = odds_stats.get('>3.0', {'second_more': 0, 'total': 1})

    odds_total = max(odds_data['total'], 1)
    prob_second_odds = odds_data['second_more'] / odds_total * 100

    # Коррекция по типу фаворита
    fav_data = fav_stats.get(fav_type, {'second_more': 0, 'total': 1})
    fav_total = max(fav_data['total'], 1)
    prob_second_fav = fav_data['second_more'] / fav_total * 100

    # Взвешенная вероятность (50% лига, 30% коэффициенты, 20% тип фаворита)
    prob_second = (prob_second_league * 0.5 +
                   prob_second_odds * 0.3 +
                   prob_second_fav * 0.2)

    prob_first = prob_first_league * 0.5 + (100 - prob_second_odds) * 0.3 + (100 - prob_second_fav) * 0.2
    prob_equal = prob_equal_league * 0.5 + 0  # сложнее, но для простоты так

    # Нормализация
    total_prob = prob_second + prob_first + prob_equal
    prob_second = prob_second / total_prob * 100
    prob_first = prob_first / total_prob * 100
    prob_equal = prob_equal / total_prob * 100

    # Определяем прогноз
    if prob_second > prob_first and prob_second > prob_equal:
        prediction = 'SECOND'
        confidence = prob_second / 100
    elif prob_first > prob_second and prob_first > prob_equal:
        prediction = 'FIRST'
        confidence = prob_first / 100
    else:
        prediction = 'EQUAL'
        confidence = prob_equal / 100

    return {
        'prediction': prediction,
        'confidence': round(confidence, 2),
        'probs': {
            'second': round(prob_second, 1),
            'first': round(prob_first, 1),
            'equal': round(prob_equal, 1)
        }
    }


# ============================================================================
# 6. ТЕСТИРОВАНИЕ НА ПОСЛЕДНИХ 100 МАТЧАХ
# ============================================================================

print("\n" + "=" * 80)
print("6. ТЕСТИРОВАНИЕ МОДЕЛИ НА 100 МАТЧАХ")
print("=" * 80)

results = []
for match in test_matches:
    pred = predict(match, league_stats, odds_stats, fav_stats)
    actual = match['half_result']

    correct = (pred['prediction'] == actual)
    results.append({
        'match': f"{match['home']} - {match['away']}",
        'league': match['league'],
        'actual': actual,
        'prediction': pred['prediction'],
        'confidence': pred['confidence'],
        'correct': correct,
        'probs': pred['probs']
    })

# Общая статистика
correct_total = sum(1 for r in results if r['correct'])
accuracy = correct_total / len(results) * 100

print(f"\n📊 ОБЩАЯ ТОЧНОСТЬ: {accuracy:.1f}% ({correct_total}/{len(results)})")

# По лигам
league_results = defaultdict(lambda: {'total': 0, 'correct': 0})
for r in results:
    league_results[r['league']]['total'] += 1
    if r['correct']:
        league_results[r['league']]['correct'] += 1

print("\n📊 ТОЧНОСТЬ ПО ЛИГАМ:")
for league, stats in sorted(league_results.items(), key=lambda x: x[1]['total'], reverse=True):
    if stats['total'] >= 3:
        acc = stats['correct'] / stats['total'] * 100
        print(f"  {league[:25]:<25} {acc:5.1f}% ({stats['correct']}/{stats['total']})")

# По уровню уверенности
conf_levels = [
    (0.7, 1.0, "Очень высокая"),
    (0.6, 0.7, "Высокая"),
    (0.5, 0.6, "Средняя"),
    (0, 0.5, "Низкая")
]

print("\n📊 ТОЧНОСТЬ ПО УРОВНЮ УВЕРЕННОСТИ:")
for low, high, name in conf_levels:
    filtered = [r for r in results if low <= r['confidence'] < high]
    if filtered:
        acc = sum(1 for r in filtered if r['correct']) / len(filtered) * 100
        print(f"  {name:15} ({low:.1f}-{high:.1f}): {len(filtered):3d} матчей, точность {acc:5.1f}%")

# ============================================================================
# 7. РАСЧЕТ ПОТЕНЦИАЛЬНОЙ ПРИБЫЛИ
# ============================================================================

print("\n" + "=" * 80)
print("7. РАСЧЕТ ПОТЕНЦИАЛЬНОЙ ПРИБЫЛИ")
print("=" * 80)

# Моделируем ставки с дробью Келли 0.25
bank = 100000
kelly_fraction = 0.25
stakes = []

for r in results:
    if r['confidence'] > 0.55:  # Ставим только при высокой уверенности
        # Расчет ставки (упрощенный)
        prob = r['probs'][r['prediction'].lower()] / 100
        odds = 2.0  # средний коэффициент

        if prob > 0.5:
            kelly = (prob * odds - 1) / (odds - 1)
            kelly = max(0, min(kelly, 1))
            stake = bank * kelly * kelly_fraction
            stake = round(stake / 100) * 100

            if r['correct']:
                profit = stake * (odds - 1)
            else:
                profit = -stake

            bank += profit
            stakes.append({
                'stake': stake,
                'profit': profit,
                'bank': bank
            })

if stakes:
    final_bank = stakes[-1]['bank']
    total_profit = final_bank - 100000
    roi = total_profit / 100000 * 100

    print(f"\n💰 Начальный банк: 100 000 ₽")
    print(f"💰 Конечный банк: {final_bank:,.0f} ₽")
    print(f"💰 Прибыль: {total_profit:+,.0f} ₽ ({roi:+.1f}%)")
    print(f"📊 Сделано ставок: {len(stakes)}")

# ============================================================================
# 8. СОХРАНЕНИЕ МОДЕЛИ
# ============================================================================

print("\n" + "=" * 80)
print("8. СОХРАНЕНИЕ МОДЕЛИ")
print("=" * 80)

model = {
    'league_stats': dict(league_stats),
    'odds_stats': dict(odds_stats),
    'fav_stats': dict(fav_stats),
    'test_results': results,
    'accuracy': accuracy
}

model_path = Path(r'c:\Users\admin\Desktop\Новая папка\Depts\depts\analysis_results\halves_model.pkl')
model_path.parent.mkdir(exist_ok=True)

with open(model_path, 'wb') as f:
    pickle.dump(model, f)

print(f"💾 Модель сохранена в: {model_path}")
print(f"📊 Точность модели: {accuracy:.1f}%")

# ============================================================================
# 9. ВЫВОД ПРИМЕРОВ ПРОГНОЗОВ
# ============================================================================

print("\n" + "=" * 80)
print("9. ПРИМЕРЫ ПРОГНОЗОВ (ПЕРВЫЕ 20)")
print("=" * 80)

print(f"\n{'Матч':<40} {'Лига':<20} {'Факт':<8} {'Прогноз':<8} {'Уверенность':<12} {'Результат'}")
print("-" * 100)

for r in results[:20]:
    match_short = r['match'][:38] if len(r['match']) > 38 else r['match']
    mark = '✅' if r['correct'] else '❌'
    print(f"{match_short:<40} {r['league'][:18]:<20} {r['actual']:<8} {r['prediction']:<8} "
          f"{r['confidence']:<12.2f} {mark}")

print("\n" + "=" * 80)
print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)