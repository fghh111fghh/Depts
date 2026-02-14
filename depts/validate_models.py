# validate_models.py

from django.db.models import F, Q
from app_bets.models import Match, Season, AnalysisConstants
from datetime import datetime
from collections import defaultdict
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


class CalibrationValidator:
    """
    Проверка калибровки вероятностных прогнозов:
    - Тотал больше 2.5
    - Обе забьют (BTTS)
    """

    def __init__(self, matches_queryset):
        self.all_matches = matches_queryset.select_related(
            'league', 'season'
        ).exclude(
            home_score_reg__isnull=True
        ).exclude(
            away_score_reg__isnull=True
        ).order_by('date')

        print(f"Загружено {self.all_matches.count()} матчей")

        # Хранилища результатов: league_id -> модель -> список кортежей (прогноз%, реальный_исход)
        self.results = defaultdict(lambda: {
            'poisson_over': [],  # (pred_prob, actual: 1 если тотал>2.5, иначе 0)
            'poisson_btts': [],  # (pred_prob, actual: 1 если обе забили, иначе 0)
            'historical_over': []  # (pred_prob, actual: 1 если тотал>2.5, иначе 0)
        })

    def get_actual_over(self, match):
        """Фактический тотал больше 2.5?"""
        total = match.home_score_reg + match.away_score_reg
        return 1 if total > 2.5 else 0

    def get_actual_btts(self, match):
        """Фактически обе забили?"""
        return 1 if (match.home_score_reg > 0 and match.away_score_reg > 0) else 0

    def validate_match(self, match):
        """Обработка одного матча"""
        league_id = match.league_id

        # --- 1. Модель Пуассона ---
        try:
            probs = match.get_poisson_probabilities()
            if probs and isinstance(probs, dict):
                # Рассчитываем вероятности из полной сетки счетов
                over_prob = 0.0
                btts_prob = 0.0

                for score, prob in probs.items():
                    h, a = map(int, score.split(':'))
                    if h + a > 2.5:
                        over_prob += prob
                    if h > 0 and a > 0:
                        btts_prob += prob

                if over_prob > 0:
                    self.results[league_id]['poisson_over'].append({
                        'pred': over_prob,
                        'actual': self.get_actual_over(match)
                    })

                if btts_prob > 0:
                    self.results[league_id]['poisson_btts'].append({
                        'pred': btts_prob,
                        'actual': self.get_actual_btts(match)
                    })
        except Exception as e:
            pass

        # --- 2. Модель Исторического тотала ---
        try:
            insight = match.get_historical_total_insight()
            if insight and insight.get('synthetic'):
                synth = insight['synthetic']
                over_prob = synth.get('over_25', 0)

                if over_prob > 0:
                    self.results[league_id]['historical_over'].append({
                        'pred': over_prob,
                        'actual': self.get_actual_over(match)
                    })
        except Exception as e:
            pass

    def run_validation(self, batch_size=1000):
        """Запуск валидации"""
        total = self.all_matches.count()
        print(f"Начинаем обработку {total} матчей...")

        for match in tqdm(self.all_matches.iterator(chunk_size=batch_size), total=total):
            self.validate_match(match)

        print("Обработка завершена")
        return self.calculate_calibration()

    def calculate_calibration(self, bins=None):
        """
        Расчет калибровки по интервалам вероятности.

        bins: список порогов, например [50,55,60,65,70,75,80,85,90]
        """
        if bins is None:
            bins = [50, 55, 60, 65, 70, 75, 80, 85, 90]

        calibration_report = defaultdict(lambda: defaultdict(list))

        for league_id, models in self.results.items():
            for model_name, predictions in models.items():
                if not predictions:
                    continue

                # Группируем прогнозы по интервалам
                bins_data = {f"{bins[i]}-{bins[i + 1]}": [] for i in range(len(bins) - 1)}
                bins_data[f">{bins[-1]}"] = []  # для прогнозов выше 90%

                for pred in predictions:
                    p = pred['pred']
                    actual = pred['actual']

                    if p < bins[0]:
                        continue  # игнорируем прогнозы ниже 50%

                    # Определяем интервал
                    assigned = False
                    for i in range(len(bins) - 1):
                        if bins[i] <= p < bins[i + 1]:
                            bins_data[f"{bins[i]}-{bins[i + 1]}"].append(actual)
                            assigned = True
                            break

                    if not assigned and p >= bins[-1]:
                        bins_data[f">{bins[-1]}"].append(actual)

                # Рассчитываем фактическую частоту для каждого интервала
                for interval, outcomes in bins_data.items():
                    if len(outcomes) >= 5:  # минимум 5 матчей для статистики
                        actual_freq = sum(outcomes) / len(outcomes) * 100
                        mid_point = self._get_interval_mid(interval)

                        calibration_report[league_id][model_name].append({
                            'interval': interval,
                            'pred_mid': mid_point,
                            'actual_pct': round(actual_freq, 2),
                            'count': len(outcomes),
                            'error': round(actual_freq - mid_point, 2)  # отклонение
                        })

        return calibration_report

    def _get_interval_mid(self, interval):
        """Получить середину интервала"""
        if interval.startswith('>'):
            return float(interval[1:])
        low, high = map(float, interval.split('-'))
        return (low + high) / 2

    def print_results(self, min_matches=10):
        """Вывод результатов в консоль"""
        cal = self.calculate_calibration()

        print("\n" + "=" * 80)
        print("РЕЗУЛЬТАТЫ КАЛИБРОВКИ ПРОГНОЗОВ".center(80))
        print("=" * 80)

        for league_id, models in cal.items():
            print(f"\n📊 ЛИГА {league_id}")
            print("-" * 80)

            for model_name, intervals in models.items():
                # Красивое название модели
                if model_name == 'poisson_over':
                    model_display = "Пуассон (Тотал >2.5)"
                elif model_name == 'poisson_btts':
                    model_display = "Пуассон (Обе забьют)"
                elif model_name == 'historical_over':
                    model_display = "Исторический тотал"
                else:
                    model_display = model_name

                print(f"\n  {model_display}:")
                print(f"  {'Интервал':>12} | {'Прогноз':>8} | {'Факт':>8} | {'Отклон':>8} | {'Матчей':>8}")
                print(f"  {'-' * 12}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 8}")

                for data in sorted(intervals, key=lambda x: x['pred_mid']):
                    if data['count'] >= min_matches:
                        arrow = "↑" if data['error'] > 0 else "↓" if data['error'] < 0 else "="
                        print(
                            f"  {data['interval']:>12} | "
                            f"{data['pred_mid']:6.1f}% | "
                            f"{data['actual_pct']:6.1f}% | "
                            f"{data['error']:+5.1f}%{arrow:1} | "
                            f"{data['count']:8d}"
                        )

        # Общая статистика по всем лигам вместе
        self.print_aggregated_results(min_matches)

    def print_aggregated_results(self, min_matches=10):
        """Вывод агрегированных результатов по всем лигам"""
        print("\n" + "=" * 80)
        print("АГРЕГИРОВАННЫЕ РЕЗУЛЬТАТЫ (ВСЕ ЛИГИ)".center(80))
        print("=" * 80)

        # Собираем все прогнозы вместе
        all_predictions = {
            'poisson_over': [],
            'poisson_btts': [],
            'historical_over': []
        }

        for league_id, models in self.results.items():
            for model_name, predictions in models.items():
                if model_name in all_predictions:
                    all_predictions[model_name].extend(predictions)

        # Группируем по интервалам
        bins = [50, 55, 60, 65, 70, 75, 80, 85, 90]

        for model_name, predictions in all_predictions.items():
            if not predictions:
                continue

            # Красивое название модели
            if model_name == 'poisson_over':
                model_display = "Пуассон (Тотал >2.5)"
            elif model_name == 'poisson_btts':
                model_display = "Пуассон (Обе забьют)"
            elif model_name == 'historical_over':
                model_display = "Исторический тотал"
            else:
                model_display = model_name

            print(f"\n📈 {model_display}")
            print(f"{'Интервал':>12} | {'Прогноз':>8} | {'Факт':>8} | {'Отклон':>8} | {'Матчей':>8}")
            print(f"{'-' * 12}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 8}")

            # Группируем
            bins_data = {f"{bins[i]}-{bins[i + 1]}": [] for i in range(len(bins) - 1)}
            bins_data[f">{bins[-1]}"] = []

            for pred in predictions:
                p = pred['pred']
                actual = pred['actual']

                if p < bins[0]:
                    continue

                assigned = False
                for i in range(len(bins) - 1):
                    if bins[i] <= p < bins[i + 1]:
                        bins_data[f"{bins[i]}-{bins[i + 1]}"].append(actual)
                        assigned = True
                        break

                if not assigned and p >= bins[-1]:
                    bins_data[f">{bins[-1]}"].append(actual)

            # Выводим
            for interval, outcomes in bins_data.items():
                if len(outcomes) >= min_matches:
                    actual_freq = sum(outcomes) / len(outcomes) * 100
                    mid_point = self._get_interval_mid(interval)
                    error = actual_freq - mid_point
                    arrow = "↑" if error > 0 else "↓" if error < 0 else "="

                    print(
                        f"  {interval:>12} | "
                        f"{mid_point:6.1f}% | "
                        f"{actual_freq:6.1f}% | "
                        f"{error:+5.1f}%{arrow:1} | "
                        f"{len(outcomes):8d}"
                    )

    def save_to_csv(self, filename='calibration_results.csv'):
        """Сохраняет результаты в CSV"""
        cal = self.calculate_calibration()
        rows = []

        for league_id, models in cal.items():
            for model_name, intervals in models.items():
                for data in intervals:
                    rows.append({
                        'league_id': league_id,
                        'model': model_name,
                        'interval': data['interval'],
                        'predicted_mid': data['pred_mid'],
                        'actual_percent': data['actual_pct'],
                        'error': data['error'],
                        'matches': data['count']
                    })

        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"\nРезультаты сохранены в {filename}")
            return df
        else:
            print("Нет данных для сохранения")
            return None


def run_validation(start_date=None, end_date=None, league_id=None, min_matches=10):
    """
    Функция для запуска валидации из shell

    Примеры использования:
    run_validation()  # все матчи
    run_validation(league_id=10)  # только лига 10
    run_validation(start_date='2008-01-01', end_date='2008-12-31')  # только 2008 год
    """

    print("=" * 80)
    print("ЗАПУСК ВАЛИДАЦИИ МОДЕЛЕЙ".center(80))
    print("=" * 80)

    # Получаем все матчи
    matches = Match.objects.select_related(
        'league', 'season'
    ).exclude(
        home_score_reg__isnull=True
    ).exclude(
        away_score_reg__isnull=True
    )

    # Фильтры
    if league_id:
        matches = matches.filter(league_id=league_id)
        print(f"Фильтр по лиге: {league_id}")

    if start_date:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        matches = matches.filter(date__gte=start)
        print(f"Начальная дата: {start_date}")

    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d')
        matches = matches.filter(date__lte=end)
        print(f"Конечная дата: {end_date}")

    print(f"Всего матчей для анализа: {matches.count()}")
    print()

    # Создаем валидатор
    validator = CalibrationValidator(matches)

    # Запускаем
    validator.run_validation()

    # Выводим результаты
    validator.print_results(min_matches=min_matches)

    # Сохраняем в CSV
    validator.save_to_csv()

    return validator


# Если скрипт запускается напрямую
if __name__ == "__main__":
    print("Скрипт загружен. Используйте функцию run_validation() для запуска.")
    print("\nПримеры вызова:")
    print("  run_validation()  # все матчи")
    print("  run_validation(league_id=10)  # только лига 10")
    print("  run_validation(start_date='2008-01-01', end_date='2008-12-31')  # только 2008 год")