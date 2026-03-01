import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
import pickle


class FootballPredictor:
    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.all_matches = []
        self.teams = defaultdict(list)
        self.patterns = self.load_patterns()
        self.h2h_stats = {}
        self.cycle_stats = {}

    def load_patterns(self):
        """Загружает или создает паттерны"""
        patterns_file = Path('team_patterns.pkl')
        if patterns_file.exists():
            with open(patterns_file, 'rb') as f:
                return pickle.load(f)
        return {}

    def analyze_team_cycles(self, team_matches):
        """Анализирует циклы команды"""
        if len(team_matches) < 3:
            return {}

        results = []
        for m in team_matches[-5:]:  # последние 5 матчей
            if m['result'] == 'W':
                results.append('W')
            elif m['result'] == 'D':
                results.append('D')
            else:
                results.append('L')

        current_pattern = ''.join(results)

        # Ищем этот паттерн в истории
        pattern_matches = []
        for i in range(len(team_matches) - len(results)):
            past_results = [m['result'] for m in team_matches[i:i + len(results)]]
            past_pattern = ''.join(past_results)

            if past_pattern == current_pattern and i + len(results) < len(team_matches):
                next_match = team_matches[i + len(results)]
                pattern_matches.append(next_match)

        if len(pattern_matches) < 5:
            return {}

        # Анализируем, что было после этого паттерна
        stats = {
            'total': len(pattern_matches),
            'win': sum(1 for m in pattern_matches if m['result'] == 'W'),
            'draw': sum(1 for m in pattern_matches if m['result'] == 'D'),
            'loss': sum(1 for m in pattern_matches if m['result'] == 'L'),
            'over25': sum(1 for m in pattern_matches if m['total_goals'] > 2.5),
            'btts': sum(1 for m in pattern_matches if m['goals_for'] > 0 and m['goals_against'] > 0),
            'avg_goals': sum(m['total_goals'] for m in pattern_matches) / len(pattern_matches)
        }

        return stats

    def analyze_h2h(self, team1, team2):
        """Анализирует личные встречи"""
        key = tuple(sorted([team1, team2]))
        if key in self.h2h_stats:
            return self.h2h_stats[key]

        matches = []
        for match in self.all_matches:
            if (match['home_team'] == team1 and match['away_team'] == team2) or \
                    (match['home_team'] == team2 and match['away_team'] == team1):
                matches.append(match)

        if len(matches) < 3:
            return {}

        stats = {
            'total': len(matches),
            'home_wins': sum(1 for m in matches if m['result'] == 'H'),
            'away_wins': sum(1 for m in matches if m['result'] == 'A'),
            'draws': sum(1 for m in matches if m['result'] == 'D'),
            'over25': sum(1 for m in matches if m['total_goals'] > 2.5),
            'btts': sum(1 for m in matches if m['fthg'] > 0 and m['ftag'] > 0),
            'avg_goals': sum(m['total_goals'] for m in matches) / len(matches)
        }

        self.h2h_stats[key] = stats
        return stats

    def get_team_form(self, team, last_n=5):
        """Возвращает форму команды за последние N матчей"""
        team_matches = self.teams.get(team, [])
        if len(team_matches) < last_n:
            return []

        recent = team_matches[-last_n:]
        form = []
        for m in recent:
            if m['result'] == 'W':
                form.append('W')
            elif m['result'] == 'D':
                form.append('D')
            else:
                form.append('L')

        return form

    def analyze_match(self, home_team, away_team):
        """Анализирует конкретный матч и выдает прогнозы"""

        predictions = []

        # Получаем историю команд
        home_matches = self.teams.get(home_team, [])
        away_matches = self.teams.get(away_team, [])

        if len(home_matches) < 5 or len(away_matches) < 5:
            return []

        # 1. Анализ циклов хозяев
        home_cycles = self.analyze_team_cycles(home_matches)
        if home_cycles and home_cycles['total'] > 10:
            win_prob = home_cycles['win'] / home_cycles['total'] * 100
            if win_prob > 55:
                predictions.append({
                    'type': 'home_win_cycle',
                    'probability': win_prob,
                    'confidence': home_cycles['total'],
                    'description': f"{home_team} выигрывала после этого паттерна в {win_prob:.0f}% случаев"
                })

        # 2. Анализ циклов гостей
        away_cycles = self.analyze_team_cycles(away_matches)
        if away_cycles and away_cycles['total'] > 10:
            away_win_prob = away_cycles['win'] / away_cycles['total'] * 100
            if away_win_prob > 55:
                predictions.append({
                    'type': 'away_win_cycle',
                    'probability': away_win_prob,
                    'confidence': away_cycles['total'],
                    'description': f"{away_team} выигрывала после этого паттерна в {away_win_prob:.0f}% случаев"
                })

        # 3. Анализ личных встреч
        h2h = self.analyze_h2h(home_team, away_team)
        if h2h and h2h['total'] >= 5:
            # Тотал
            if h2h['over25'] / h2h['total'] * 100 > 55:
                predictions.append({
                    'type': 'over25',
                    'probability': h2h['over25'] / h2h['total'] * 100,
                    'confidence': h2h['total'],
                    'description': f"В {h2h['over25']}/{h2h['total']} матчах был ТБ 2.5"
                })
            elif h2h['over25'] / h2h['total'] * 100 < 45:
                predictions.append({
                    'type': 'under25',
                    'probability': (1 - h2h['over25'] / h2h['total']) * 100,
                    'confidence': h2h['total'],
                    'description': f"В {h2h['total'] - h2h['over25']}/{h2h['total']} матчах был ТМ 2.5"
                })

            # Обе забьют
            if h2h['btts'] / h2h['total'] * 100 > 55:
                predictions.append({
                    'type': 'btts_yes',
                    'probability': h2h['btts'] / h2h['total'] * 100,
                    'confidence': h2h['total'],
                    'description': f"Обе забивали в {h2h['btts']}/{h2h['total']} матчах"
                })
            elif h2h['btts'] / h2h['total'] * 100 < 45:
                predictions.append({
                    'type': 'btts_no',
                    'probability': (1 - h2h['btts'] / h2h['total']) * 100,
                    'confidence': h2h['total'],
                    'description': f"Обе не забивали в {h2h['total'] - h2h['btts']}/{h2h['total']} матчах"
                })

        # 4. Анализ формы
        home_form = self.get_team_form(home_team, 3)
        away_form = self.get_team_form(away_team, 3)

        if home_form == ['W', 'W', 'W']:
            # Команда выиграла 3 подряд
            predictions.append({
                'type': 'home_win_streak',
                'probability': 55,
                'confidence': 2667,
                'description': f"{home_team} выиграла 3 подряд (паттерн WWW)"
            })

        if away_form == ['L', 'L', 'L']:
            # Команда проиграла 3 подряд - жди ничью?
            predictions.append({
                'type': 'away_draw_after_losses',
                'probability': 27,
                'confidence': 500,
                'description': f"{away_team} проиграла 3 подряд"
            })

        return predictions

    def predict_todays_matches(self, matches_list):
        """Прогнозирует сегодняшние матчи"""

        print("\n" + "=" * 80)
        print("📊 ПРОГНОЗЫ НА СЕГОДНЯШНИЕ МАТЧИ")
        print("=" * 80)

        results = []
        for match in matches_list:
            home = match['home']
            away = match['away']
            league = match.get('league', 'Неизвестно')

            print(f"\n⚽ {home} - {away} ({league})")
            print("-" * 50)

            predictions = self.analyze_match(home, away)

            if predictions:
                # Сортируем по вероятности
                predictions.sort(key=lambda x: x['probability'], reverse=True)

                for p in predictions[:3]:  # топ-3 прогноза
                    print(f"  🎯 {p['type']}: {p['probability']:.0f}% ({p['description']})")

                    # Сохраняем для вывода
                    results.append({
                        'match': f"{home} - {away}",
                        'league': league,
                        'prediction': p['type'],
                        'probability': p['probability'],
                        'confidence': p['confidence']
                    })
            else:
                print("  ❌ Нет надежных прогнозов")

        return results


# ============================================================================
# ЗАГРУЗКА ДАННЫХ
# ============================================================================

def load_data(predictor):
    """Загружает все матчи в память"""

    print("📥 Загрузка исторических данных...")

    for league_folder in predictor.data_path.iterdir():
        if not league_folder.is_dir():
            continue

        league_name = league_folder.name

        for csv_file in league_folder.glob('*.csv'):
            try:
                with open(csv_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            date_str = row.get('Date', '')
                            if not date_str:
                                continue

                            # Парсим дату
                            for fmt in ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d']:
                                try:
                                    match_date = datetime.strptime(date_str, fmt)
                                    break
                                except:
                                    continue
                            else:
                                continue

                            match = {
                                'date': match_date,
                                'league': league_name,
                                'home_team': row.get('HomeTeam', '').strip(),
                                'away_team': row.get('AwayTeam', '').strip(),
                                'fthg': int(row.get('FTHG', 0)),
                                'ftag': int(row.get('FTAG', 0)),
                                'total_goals': int(row.get('FTHG', 0)) + int(row.get('FTAG', 0)),
                                'result': 'H' if int(row.get('FTHG', 0)) > int(row.get('FTAG', 0)) else
                                'A' if int(row.get('FTAG', 0)) > int(row.get('FTHG', 0)) else 'D'
                            }

                            predictor.all_matches.append(match)

                        except (ValueError, KeyError):
                            continue
            except:
                continue

    # Сортируем по дате
    predictor.all_matches.sort(key=lambda x: x['date'])

    # Группируем по командам
    for match in predictor.all_matches:
        predictor.teams[match['home_team']].append({
            'date': match['date'],
            'opponent': match['away_team'],
            'venue': 'home',
            'result': 'W' if match['result'] == 'H' else 'D' if match['result'] == 'D' else 'L',
            'goals_for': match['fthg'],
            'goals_against': match['ftag'],
            'total_goals': match['total_goals'],
            'league': match['league']
        })

        predictor.teams[match['away_team']].append({
            'date': match['date'],
            'opponent': match['home_team'],
            'venue': 'away',
            'result': 'W' if match['result'] == 'A' else 'D' if match['result'] == 'D' else 'L',
            'goals_for': match['ftag'],
            'goals_against': match['fthg'],
            'total_goals': match['total_goals'],
            'league': match['league']
        })

    # Сортируем матчи каждой команды
    for team in predictor.teams:
        predictor.teams[team].sort(key=lambda x: x['date'])

    print(f"✅ Загружено {len(predictor.all_matches)} матчей")
    print(f"✅ Найдено {len(predictor.teams)} команд")


# ============================================================================
# ЗАПУСК ПРОГНОЗЕРА
# ============================================================================

if __name__ == "__main__":

    # Создаем прогнозер
    predictor = FootballPredictor(r'c:\Users\admin\Desktop\Новая папка\Depts\depts\all_matches')

    # Загружаем данные
    load_data(predictor)

    # ВВЕДИТЕ СЕГОДНЯШНИЕ МАТЧИ СЮДА
    today_matches = [
        # Формат: {'home': 'Команда1', 'away': 'Команда2', 'league': 'Лига'}
        {'home': 'Арсенал', 'away': 'Челси', 'league': 'АПЛ Англия'},
        {'home': 'Real Madrid', 'away': 'Barcelona', 'league': 'Ла Лига Испания'},
    ]

    # Получаем прогнозы
    results = predictor.predict_todays_matches(today_matches)

    print("\n" + "=" * 80)
    print("📋 ИТОГОВЫЕ РЕКОМЕНДАЦИИ")
    print("=" * 80)

    for r in results:
        print(f"\n⚽ {r['match']}")
        print(f"   🎯 Ставка: {r['prediction']}")
        print(f"   📊 Вероятность: {r['probability']:.0f}%")
        print(f"   📈 Достоверность: {r['confidence']} матчей в статистике")