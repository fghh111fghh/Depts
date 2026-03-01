import csv
import os
from pathlib import Path
from collections import defaultdict
import statistics


# ============================================================================
# АНАЛИЗ ВСЕХ ВОЗМОЖНЫХ РЫНКОВ ДЛЯ СТАВОК
# ============================================================================

class FootballStrategyAnalyzer:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.results = {}

    def analyze_all_markets(self):
        """Анализирует все возможные рынки для ставок"""

        markets = {
            '1x2_home': self.analyze_1x2_home,
            '1x2_draw': self.analyze_1x2_draw,
            '1x2_away': self.analyze_1x2_away,
            'over15': self.analyze_over15,
            'over25': self.analyze_over25,
            'over35': self.analyze_over35,
            'btts': self.analyze_btts,
            'btts_first_half': self.analyze_btts_first_half,
            'btts_second_half': self.analyze_btts_second_half,
            'first_half_over05': self.analyze_first_half_over05,
            'first_half_over15': self.analyze_first_half_over15,
            'second_half_over05': self.analyze_second_half_over05,
            'second_half_over15': self.analyze_second_half_over15,
            'second_half_more_goals': self.analyze_second_half_more,
            'first_half_more_goals': self.analyze_first_half_more,
            'home_win_to_nil': self.analyze_home_win_to_nil,
            'away_win_to_nil': self.analyze_away_win_to_nil,
            'both_halves_over05': self.analyze_both_halves_over05,
            'score_in_both_halves': self.analyze_score_in_both_halves,
        }

        for market_name, analyzer in markets.items():
            print(f"\n📊 Анализ рынка: {market_name}")
            stats = self.analyze_market(analyzer)
            if stats and stats['total'] > 100:
                self.results[market_name] = stats
                self.print_market_summary(market_name, stats)

        return self.find_best_strategies()

    def analyze_market(self, analyzer_func):
        """Запускает анализ для конкретного рынка по всем лигам"""

        league_folders = [f for f in self.base_path.iterdir() if f.is_dir()]
        all_stats = defaultdict(lambda: {
            'total': 0, 'yes': 0, 'no': 0, 'matches': []
        })

        for league_folder in league_folders:
            league_name = league_folder.name
            csv_files = list(league_folder.glob('*.csv'))

            for csv_file in csv_files:
                try:
                    with open(csv_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            try:
                                result = analyzer_func(row)
                                if result is not None:
                                    all_stats[league_name]['total'] += 1
                                    if result:
                                        all_stats[league_name]['yes'] += 1
                                    else:
                                        all_stats[league_name]['no'] += 1
                            except (ValueError, KeyError):
                                continue
                except Exception:
                    continue

        # Общие итоги
        total = sum(s['total'] for s in all_stats.values())
        yes = sum(s['yes'] for s in all_stats.values())

        if total == 0:
            return None

        return {
            'total': total,
            'yes': yes,
            'no': total - yes,
            'prob': yes / total * 100,
            'by_league': dict(all_stats)
        }

    # ========================================================================
    # АНАЛИЗАТОРЫ РАЗНЫХ РЫНКОВ
    # ========================================================================

    def analyze_1x2_home(self, row):
        """Победа хозяев"""
        fthg = int(row['FTHG'])
        ftag = int(row['FTAG'])
        return fthg > ftag

    def analyze_1x2_draw(self, row):
        """Ничья"""
        fthg = int(row['FTHG'])
        ftag = int(row['FTAG'])
        return fthg == ftag

    def analyze_1x2_away(self, row):
        """Победа гостей"""
        fthg = int(row['FTHG'])
        ftag = int(row['FTAG'])
        return ftag > fthg

    def analyze_over15(self, row):
        """Тотал больше 1.5"""
        return (int(row['FTHG']) + int(row['FTAG'])) > 1.5

    def analyze_over25(self, row):
        """Тотал больше 2.5"""
        return (int(row['FTHG']) + int(row['FTAG'])) > 2.5

    def analyze_over35(self, row):
        """Тотал больше 3.5"""
        return (int(row['FTHG']) + int(row['FTAG'])) > 3.5

    def analyze_btts(self, row):
        """Обе забьют"""
        return int(row['FTHG']) > 0 and int(row['FTAG']) > 0

    def analyze_btts_first_half(self, row):
        """Обе забьют в 1-м тайме"""
        return int(row['HTHG']) > 0 and int(row['HTAG']) > 0

    def analyze_btts_second_half(self, row):
        """Обе забьют во 2-м тайме"""
        first_home = int(row['HTHG'])
        first_away = int(row['HTAG'])
        total_home = int(row['FTHG'])
        total_away = int(row['FTAG'])

        second_home = total_home - first_home
        second_away = total_away - first_away

        return second_home > 0 and second_away > 0

    def analyze_first_half_over05(self, row):
        """1-й тайм тотал больше 0.5"""
        return (int(row['HTHG']) + int(row['HTAG'])) > 0.5

    def analyze_first_half_over15(self, row):
        """1-й тайм тотал больше 1.5"""
        return (int(row['HTHG']) + int(row['HTAG'])) > 1.5

    def analyze_second_half_over05(self, row):
        """2-й тайм тотал больше 0.5"""
        first = int(row['HTHG']) + int(row['HTAG'])
        total = int(row['FTHG']) + int(row['FTAG'])
        return (total - first) > 0.5

    def analyze_second_half_over15(self, row):
        """2-й тайм тотал больше 1.5"""
        first = int(row['HTHG']) + int(row['HTAG'])
        total = int(row['FTHG']) + int(row['FTAG'])
        return (total - first) > 1.5

    def analyze_second_half_more(self, row):
        """2-й тайм результативнее 1-го"""
        first = int(row['HTHG']) + int(row['HTAG'])
        total = int(row['FTHG']) + int(row['FTAG'])
        second = total - first
        return second > first

    def analyze_first_half_more(self, row):
        """1-й тайм результативнее 2-го"""
        first = int(row['HTHG']) + int(row['HTAG'])
        total = int(row['FTHG']) + int(row['FTAG'])
        second = total - first
        return first > second

    def analyze_home_win_to_nil(self, row):
        """Победа хозяев с сухим счетом"""
        return int(row['FTHG']) > 0 and int(row['FTAG']) == 0

    def analyze_away_win_to_nil(self, row):
        """Победа гостей с сухим счетом"""
        return int(row['FTAG']) > 0 and int(row['FTHG']) == 0

    def analyze_both_halves_over05(self, row):
        """Гол в обоих таймах"""
        first = int(row['HTHG']) + int(row['HTAG'])
        total = int(row['FTHG']) + int(row['FTAG'])
        second = total - first
        return first > 0 and second > 0

    def analyze_score_in_both_halves(self, row):
        """Команда забивает в обоих таймах"""
        home_first = int(row['HTHG']) > 0
        home_second = (int(row['FTHG']) - int(row['HTHG'])) > 0
        away_first = int(row['HTAG']) > 0
        away_second = (int(row['FTAG']) - int(row['HTAG'])) > 0

        return (home_first and home_second) or (away_first and away_second)

    def print_market_summary(self, market_name, stats):
        """Выводит сводку по рынку"""
        prob = stats['prob']
        print(f"  Всего матчей: {stats['total']:,}")
        print(f"  Проходимость: {prob:.1f}% ({stats['yes']}/{stats['total']})")

        # Оценка потенциальной прибыли при среднем кэфе 2.0
        roi = (prob / 100 * 2.0 - 1) * 100
        print(f"  ROI при кэфе 2.0: {roi:+.1f}%")

        # Лучшие лиги
        best_leagues = sorted(
            [(l, s['yes'] / s['total'] * 100) for l, s in stats['by_league'].items() if s['total'] > 100],
            key=lambda x: x[1], reverse=True
        )[:3]

        if best_leagues:
            print(f"  Лучшие лиги:")
            for league, p in best_leagues:
                print(f"    {league[:20]}: {p:.1f}%")

    def find_best_strategies(self):
        """Находит лучшие стратегии"""

        strategies = []
        for market, stats in self.results.items():
            # Для разных кэфов нужна разная проходимость
            required_prob = {
                '1x2_home': 1 / 2.5 * 100,  # ~40%
                '1x2_draw': 1 / 4.0 * 100,  # 25%
                '1x2_away': 1 / 3.5 * 100,  # ~28.5%
                'over15': 1 / 1.4 * 100,  # ~71.4%
                'over25': 1 / 1.9 * 100,  # ~52.6%
                'over35': 1 / 3.0 * 100,  # ~33.3%
                'btts': 1 / 2.0 * 100,  # 50%
            }.get(market, 50)

            actual_prob = stats['prob']

            if actual_prob > required_prob:
                roi = (actual_prob / 100 * 2.0 - 1) * 100
                strategies.append({
                    'market': market,
                    'prob': actual_prob,
                    'required': required_prob,
                    'edge': actual_prob - required_prob,
                    'roi': roi,
                    'total': stats['total']
                })

        # Сортируем по преимуществу
        strategies.sort(key=lambda x: x['edge'], reverse=True)

        print("\n" + "=" * 80)
        print("ЛУЧШИЕ СТРАТЕГИИ")
        print("=" * 80)

        for s in strategies[:10]:
            print(f"\n🎯 {s['market']}:")
            print(f"  Проходимость: {s['prob']:.1f}% (нужно {s['required']:.1f}%)")
            print(f"  Преимущество: +{s['edge']:.1f}%")
            print(f"  ROI при кэф 2.0: {s['roi']:+.1f}%")
            print(f"  Матчей в базе: {s['total']:,}")

        return strategies


# ============================================================================
# ЗАПУСК АНАЛИЗА
# ============================================================================

if __name__ == "__main__":
    base_path = r'c:\Users\admin\Desktop\Новая папка\Depts\depts\all_matches'
    analyzer = FootballStrategyAnalyzer(base_path)
    best_strategies = analyzer.analyze_all_markets()