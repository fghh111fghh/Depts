"""
Модуль анализа футбольных матчей с использованием нескольких статистических методов.

Класс AnalyzeView предоставляет:
1. GET: отображение формы для ввода данных
2. POST: обработка и анализ введенных данных
3. Парсинг текстовых данных с коэффициентами и названиями команд
4. Анализ матчей методами: Пуассона, "близнецов", паттернов форм, личных встреч
5. Векторный синтез для формирования итогового вердикта
6. Сохранение алиасов команд для улучшения распознавания

Основные сохраняемые структуры данных в контексте шаблона:
- results: список словарей с результатами анализа каждого матча
- unknown_teams: множество команд, которые не удалось распознать
- raw_text: оригинальный текст, введенный пользователем
- all_teams: QuerySet всех команд для выпадающего списка
"""
import csv
import io
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional

from django.conf import settings
from django.db import transaction
from django.db.models import F
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.utils.timezone import make_aware, get_current_timezone
from django.views import View
import re
import math
import unicodedata
from django.views.generic import TemplateView

from app_bets import constants
from app_bets.constants import Outcome, ParsingConstants, AnalysisConstants, Messages
from app_bets.models import Team, TeamAlias, Season, Match, League

# Настройка логгера для мониторинга
logger = logging.getLogger(__name__)


class AnalyzeView(View):
    """
    View для анализа футбольных матчей на основе текстового ввода.

    Обрабатывает:
    - GET запросы: отображение пустой формы для ввода данных
    - POST запросы: обработка и анализ введенных данных

    Основные функции:
    - Парсинг строк с коэффициентами и названиями команд
    - Распознавание команд через основную базу и алиасы
    - Анализ исторических данных (паттерны форм, личные встречи)
    - Расчет вероятностей методом Пуассона
    - Поиск "близнецов" - исторических матчей с похожими коэффициентами
    - Формирование итогового вердикта на основе всех методов

    Сохраняет полную совместимость структуры выходных данных:
    results: [{
        'match': "Команда1 - Команда2",
        'league': "Название лиги",
        'poisson_l': "1.50 : 1.20",
        'poisson_top': [{'score': '1:0', 'prob': 12.34}, ...],
        'twins_count': 15,
        'twins_dist': "П1: 40% | X: 30% | П2: 30%",
        'pattern_data': "Текст или словарь с паттерном",
        'h2h_list': [{'date': '25.12.23', 'score': '2:1'}, ...],
        'h2h_total': 5,
        'verdict': "СИГНАЛ: П1"
    }]
    """

    template_name = 'app_bets/bets_main.html'

    def get(self, request):
        """
        Обрабатывает GET запрос - отображает пустую форму для ввода данных.

        Args:
            request: HttpRequest объект

        Returns:
            HttpResponse: Рендеренный шаблон с пустой формой
        """
        # Получаем все команды для выпадающего списка создания алиасов
        all_teams = Team.objects.all().order_by('name')

        # Рендерим пустую форму
        return render(request, self.template_name, {
            'results': [],  # Пустой список результатов
            'raw_text': '',  # Пустой текст
            'unknown_teams': [],  # Пустой список неизвестных команд
            'all_teams': all_teams,  # Все команды для выпадающего списка
        })

    @staticmethod
    def clean_team_name(name: str) -> str:
        """
        Очищает название команды от лишних символов для сравнения.

        Алгоритм:
        1. Нормализация Unicode
        2. Удаление временных меток (15:30, 21.45)
        3. Удаление спецсимволов и скобок
        4. Нормализация дефисов/тире
        5. Удаление лишних пробелов
        6. Приведение к нижнему регистру

        Args:
            name (str): Исходное название команды

        Returns:
            str: Очищенное название в нижнем регистре

        Пример:
            "Барселона (Испания) - 20:30" → "барселона испания"
        """
        if not name:
            return ""

        try:
            # Нормализация Unicode (объединение диакритических знаков)
            name = unicodedata.normalize('NFKC', str(name))

            # Удаление временных меток (15:30, 21.45, 20.30 и т.д.)
            name = re.sub(ParsingConstants.TIME_REGEX, '', name)

            # Удаление символов в скобках и спецсимволов
            name = re.sub(r'[^\w\s\d\-\']', ' ', name)

            # Удаление одиночных цифр в начале или конце
            name = re.sub(r'^\d+\s+|\s+\d+$', '', name)

            # Замена нескольких дефисов/тире на один пробел
            name = re.sub(r'[\-\–\—]+', ' ', name)

            # Удаление лишних пробелов
            name = ' '.join(name.split())

            return name.strip().lower()

        except Exception as e:
            logger.warning(f"Ошибка очистки названия команды '{name}': {e}")
            return str(name).strip().lower() if name else ""

    @staticmethod
    def get_poisson_probs(l_home: float, l_away: float) -> Dict:
        """
        Рассчитывает вероятности различных счетов по распределению Пуассона.
        Также вычисляет вероятность BTTS (обе забьют) и тотала > 2.5.

        Args:
            l_home (float): Лямбда для домашней команды
            l_away (float): Лямбда для гостевой команды

        Returns:
            Dict: Словарь с:
                - top_scores: топ-5 счетов
                - btts_yes: вероятность обе забьют
                - btts_no: вероятность не обе забьют
                - over25_yes: вероятность тотал > 2.5
                - over25_no: вероятность тотал < 2.5
        """
        probs = []
        btts_yes = 0.0
        btts_no = 0.0
        over25_yes = 0.0
        over25_no = 0.0

        try:
            # Защита от нулевых или отрицательных значений
            l_home = max(float(l_home), AnalysisConstants.POISSON_MIN_LAMBDA)
            l_away = max(float(l_away), AnalysisConstants.POISSON_MIN_LAMBDA)

            # Предварительный расчет экспонент для производительности
            exp_home = math.exp(-l_home)
            exp_away = math.exp(-l_away)

            # Используем константу из настроек
            max_goals = AnalysisConstants.POISSON_MAX_GOALS

            # Предвычисляем факториалы
            factorials = [math.factorial(i) for i in range(max_goals + 1)]

            # Предвычисляем степени для оптимизации
            home_powers = [l_home ** i for i in range(max_goals + 1)]
            away_powers = [l_away ** i for i in range(max_goals + 1)]

            # Рассчитываем вероятности для счетов
            for h in range(max_goals + 1):
                p_h = (exp_home * home_powers[h]) / factorials[h]
                for a in range(max_goals + 1):
                    p_a = (exp_away * away_powers[a]) / factorials[a]
                    probability = p_h * p_a * 100

                    # Собираем топ-5 вероятных счетов
                    if probability > AnalysisConstants.MIN_PROBABILITY:
                        probs.append({
                            'score': f"{h}:{a}",
                            'prob': round(probability, 2)
                        })

                    # Расчет для "обе забьют" (BTTS)
                    if h > 0 and a > 0:
                        btts_yes += probability
                    else:
                        btts_no += probability

                    # Расчет для "тотал > 2.5"
                    if (h + a) > 2.5:
                        over25_yes += probability
                    else:
                        over25_no += probability

            # Сортируем по убыванию вероятности и берем топ-5
            top_scores = sorted(probs, key=lambda x: x['prob'], reverse=True)[:5]

            # Нормализуем до 100%
            total_btss = btts_yes + btts_no
            total_over = over25_yes + over25_no

            if total_btss > 0:
                btts_yes = (btts_yes / total_btss) * 100
                btts_no = (btts_no / total_btss) * 100

            if total_over > 0:
                over25_yes = (over25_yes / total_over) * 100
                over25_no = (over25_no / total_over) * 100

            # Округляем
            btts_yes = round(btts_yes, 2)
            btts_no = round(btts_no, 2)
            over25_yes = round(over25_yes, 2)
            over25_no = round(over25_no, 2)

        except Exception as e:
            logger.error(f"Ошибка расчета вероятностей Пуассона: {e}")
            return {
                'top_scores': [],
                'btts_yes': 0.0,
                'btts_no': 0.0,
                'over25_yes': 0.0,
                'over25_no': 0.0
            }

        return {
            'top_scores': top_scores,
            'btts_yes': btts_yes,
            'btts_no': btts_no,
            'over25_yes': over25_yes,
            'over25_no': over25_no
        }

    def get_team_smart(self, name: str) -> Optional['Team']:
        """
        Интеллектуальный поиск команды по названию.

        Алгоритм поиска:
        1. Поиск точного совпадения в основной таблице команд
        2. Поиск в таблице алиасов (альтернативных названий)
        3. Возврат None если команда не найдена

        Args:
            name (str): Название команды для поиска

        Returns:
            Optional[Team]: Объект команды или None

        Важно: Использует очищенное название для поиска, но сохраняет
               оригинальное название в результатах анализа.
        """
        clean_name = self.clean_team_name(name)
        if not clean_name:
            return None

        # 1. Поиск в основной таблице команд (точное совпадение без учета регистра)
        team = Team.objects.filter(name__iexact=clean_name).first()
        if team:
            return team

        # 2. Поиск в таблице алиасов
        alias = TeamAlias.objects.filter(name__iexact=clean_name).select_related('team').first()
        if alias:
            return alias.team

        return None

    def _extract_team_names(self, lines: List[str], odds_index: int) -> List[str]:
        """
        Извлекает названия команд из строк перед коэффициентами.

        Алгоритм:
        1. Ищет строки выше коэффициентов
        2. Пропускает разделители, время, заголовки столбцов
        3. Проверяет валидность названий команд
        4. Возвращает список из 2 названий (гостевая, домашняя)

        Args:
            lines (List[str]): Список всех строк
            odds_index (int): Индекс строки с первым коэффициентом

        Returns:
            List[str]: Список из 2 названий команд или пустой список
        """
        names = []
        # Ищем до MAX_SEARCH_DEPTH строк выше коэффициентов
        search_depth = min(ParsingConstants.MAX_SEARCH_DEPTH, odds_index)

        for j in range(odds_index - 1, odds_index - search_depth - 1, -1):
            if j < 0:
                break

            row = lines[j].strip()

            # Если строка пустая, пропускаем но продолжаем поиск
            if not row:
                continue

            # Пропускаем разделители
            if row == '-':
                continue

            # Пропускаем время
            if re.match(ParsingConstants.TIME_REGEX, row):
                continue

            # Пропускаем заголовки столбцов
            if row.lower() in ParsingConstants.SKIP_KEYWORDS:
                continue

            # Пропускаем названия лиг и стран
            if any(keyword in row.lower() for keyword in ParsingConstants.LEAGUE_KEYWORDS):
                continue

            # Проверяем, может ли строка быть названием команды
            clean_name = self.clean_team_name(row)
            if (clean_name and
                    len(clean_name) >= AnalysisConstants.MIN_TEAM_NAME_LENGTH and
                    not re.match(ParsingConstants.DIGITS_ONLY_REGEX, clean_name) and
                    clean_name not in ParsingConstants.BLACKLIST):

                names.append(row)
                if len(names) == 2:
                    break

        return names

    def post(self, request):
        """
        Основной метод обработки POST-запроса для анализа матчей.

        Алгоритм:
        1. Обработка создания алиаса (если запрошено)
        2. Парсинг текста с матчами
        3. Определение текущего сезона
        4. Анализ каждого матча по отдельности
        5. Сбор нераспознанных команд
        6. Формирование контекста для шаблона

        Входные данные:
        - matches_text: многострочный текст с данными матчей
        - create_alias (опционально): флаг создания алиаса
        - alias_name, team_id: данные для создания алиаса

        Выходные данные (контекст для шаблона):
        - results: список результатов анализа (сохраняет оригинальную структуру)
        - raw_text: оригинальный текст пользователя
        - unknown_teams: список нераспознанных команд
        - all_teams: QuerySet всех команд для выпадающего списка
        """
        # Сохраняем оригинальный текст
        raw_text = request.POST.get('matches_text', '')

        # --- 1. ОБРАБОТКА СОЗДАНИЯ АЛИАСА ---
        if 'create_alias' in request.POST:
            alias_raw = request.POST.get('alias_name', '')
            t_id = request.POST.get('team_id')
            if alias_raw and t_id:
                try:
                    clean_n = self.clean_team_name(alias_raw)
                    with transaction.atomic():
                        TeamAlias.objects.update_or_create(
                            name=clean_n,
                            defaults={'team_id': t_id}
                        )
                    logger.info(Messages.ALIAS_CREATED.format(clean_n, t_id))
                except Exception as e:
                    error_msg = f"Ошибка сохранения алиаса: {e}"
                    logger.error(error_msg)
                    print(error_msg)

        # --- 2. ПОДГОТОВКА ДАННЫХ ДЛЯ АНАЛИЗА ---
        results = []
        unknown_teams = set()

        # Определяем текущий сезон
        season = Season.objects.filter(is_current=True).first() or Season.objects.order_by('-start_date').first()

        # Разбиваем текст на строки
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]

        if not lines:
            # Если нет данных, возвращаем пустой результат
            return render(request, self.template_name, {
                'results': results,
                'raw_text': raw_text,
                'unknown_teams': sorted(list(unknown_teams)),
                'all_teams': Team.objects.all().order_by('name'),
            })

        # --- 3. ПАРСИНГ И АНАЛИЗ МАТЧЕЙ ---
        skip_to = -1
        for i, line in enumerate(lines):
            # Пропускаем уже обработанные строки
            if i <= skip_to:
                continue

            # Проверяем, является ли строка коэффициентом
            if re.match(ParsingConstants.ODDS_REGEX, line):
                try:
                    # Парсим коэффициенты
                    h_odd = Decimal(line.replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
                    d_odd = Decimal(lines[i + 1].replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
                    a_odd = Decimal(lines[i + 2].replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
                    skip_to = i + 2

                    # Извлекаем названия команд
                    names = self._extract_team_names(lines, i)

                    if len(names) == 2:
                        away_raw, home_raw = names[0], names[1]

                        # Ищем команды
                        home_team = self.get_team_smart(home_raw)
                        away_team = self.get_team_smart(away_raw)

                        if home_team and away_team:
                            # Логирование найденного матча
                            logger.info(Messages.MATCH_FOUND.format(
                                home_team.name, away_team.name, h_odd, d_odd, a_odd
                            ))

                            # --- АНАЛИЗ МАТЧА ---

                            # Определяем лигу
                            ref = Match.objects.filter(home_team=home_team).select_related('league__country').first()
                            league = ref.league if ref else League.objects.filter(country=home_team.country).first()

                            # --- ШАБЛОНЫ (ОПТИМИЗИРОВАНО) ---
                            all_league_matches = list(
                                Match.objects.filter(league=league, home_score_reg__isnull=False).order_by('date'))
                            team_history = {}
                            match_patterns = {}
                            for m in all_league_matches:
                                h_id, a_id = m.home_team_id, m.away_team_id
                                h_f = "".join(team_history.get(h_id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]
                                a_f = "".join(team_history.get(a_id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]
                                if len(h_f) == AnalysisConstants.PATTERN_FORM_LENGTH and len(
                                        a_f) == AnalysisConstants.PATTERN_FORM_LENGTH:
                                    match_patterns[m.id] = (h_f, a_f)

                                # Определяем результат для каждой команды
                                if m.home_score_reg == m.away_score_reg:
                                    res_h = Outcome.DRAW
                                    res_a = Outcome.DRAW
                                elif m.home_score_reg > m.away_score_reg:
                                    res_h = Outcome.WIN
                                    res_a = Outcome.LOSE
                                else:
                                    res_h = Outcome.LOSE
                                    res_a = Outcome.WIN

                                team_history.setdefault(h_id, []).append(res_h)
                                team_history.setdefault(a_id, []).append(res_a)

                            # Получаем текущие формы команд
                            curr_h_form = "".join(team_history.get(home_team.id, []))[
                                          -AnalysisConstants.PATTERN_FORM_LENGTH:]
                            curr_a_form = "".join(team_history.get(away_team.id, []))[
                                          -AnalysisConstants.PATTERN_FORM_LENGTH:]

                            # Анализируем паттерны
                            pattern_res = Messages.PATTERN_INSUFFICIENT_DATA
                            p_hw, p_dw, p_aw, p_count = 0, 0, 0, 0

                            if len(curr_h_form) == AnalysisConstants.PATTERN_FORM_LENGTH and len(
                                    curr_a_form) == AnalysisConstants.PATTERN_FORM_LENGTH:
                                for m in all_league_matches:
                                    if match_patterns.get(m.id) == (curr_h_form, curr_a_form):
                                        p_count += 1
                                        if m.home_score_reg > m.away_score_reg:
                                            p_hw += 1
                                        elif m.home_score_reg == m.away_score_reg:
                                            p_dw += 1
                                        else:
                                            p_aw += 1

                                if p_count > 0:
                                    pattern_res = {
                                        'pattern': f"{curr_h_form} - {curr_a_form}",
                                        'count': p_count,
                                        'dist': f"П1: {round(p_hw / p_count * 100)}% | X: {round(p_dw / p_count * 100)}% | П2: {round(p_aw / p_count * 100)}%"
                                    }

                            # --- ПУАССОН И БЛИЗНЕЦЫ ---
                            m_obj = Match(
                                home_team=home_team,
                                away_team=away_team,
                                league=league,
                                season=season,
                                odds_home=h_odd
                            )
                            p_data = m_obj.calculate_poisson_lambda()
                            poisson_results = self.get_poisson_probs(p_data['home_lambda'], p_data['away_lambda'])
                            top_scores = poisson_results['top_scores']

                            # Поиск "близнецов" - матчей с похожими коэффициентами
                            tol = AnalysisConstants.TWINS_TOLERANCE_SMALL
                            twins_qs = Match.objects.filter(
                                league__country=league.country,
                                odds_home__range=(h_odd - tol, h_odd + tol),
                                odds_away__range=(a_odd - tol, a_odd + tol)
                            ).exclude(home_score_reg__isnull=True)

                            if twins_qs.count() == 0:
                                tol = AnalysisConstants.TWINS_TOLERANCE_LARGE
                                twins_qs = Match.objects.filter(
                                    league__country=league.country,
                                    odds_home__range=(h_odd - tol, h_odd + tol),
                                    odds_away__range=(a_odd - tol, a_odd + tol)
                                ).exclude(home_score_reg__isnull=True)

                            t_count = twins_qs.count()
                            t_dist, hw_t, dw_t, aw_t = Messages.TWINS_NO_DATA, 0, 0, 0
                            if t_count > 0:
                                hw_t = twins_qs.filter(home_score_reg__gt=F('away_score_reg')).count()
                                dw_t = twins_qs.filter(home_score_reg=F('away_score_reg')).count()
                                aw_t = twins_qs.filter(home_score_reg__lt=F('away_score_reg')).count()
                                t_dist = f"П1: {round(hw_t / t_count * 100)}% | X: {round(dw_t / t_count * 100)}% | П2: {round(aw_t / t_count * 100)}%"

                            # --- ЛИЧНЫЕ ВСТРЕЧИ ---
                            h2h_qs = Match.objects.filter(
                                home_team=home_team,
                                away_team=away_team
                            ).exclude(home_score_reg__isnull=True).order_by('-date')

                            h2h_list = [
                                {
                                    'date': m.date.strftime(Messages.DATE_FORMAT),
                                    'score': f"{m.home_score_reg}:{m.away_score_reg}"
                                }
                                for m in h2h_qs
                            ]

                            # --- ВЕКТОРНЫЙ СИНТЕЗ (ВЕСА) ---
                            v_p1, v_x, v_p2 = 0, 0, 0

                            # 1. Учитываем Пуассон
                            if top_scores:
                                ms = top_scores[0]['score'].split(':')
                                if int(ms[0]) > int(ms[1]):
                                    v_p1 += AnalysisConstants.POISSON_WEIGHT
                                elif int(ms[0]) == int(ms[1]):
                                    v_x += AnalysisConstants.POISSON_WEIGHT
                                else:
                                    v_p2 += AnalysisConstants.POISSON_WEIGHT

                            # 2. Учитываем близнецов
                            if t_count > 0:
                                if hw_t / t_count > AnalysisConstants.WIN_THRESHOLD:
                                    v_p1 += AnalysisConstants.TWINS_WEIGHT
                                if dw_t / t_count > AnalysisConstants.DRAW_THRESHOLD:
                                    v_x += AnalysisConstants.TWINS_WEIGHT
                                if aw_t / t_count > AnalysisConstants.WIN_THRESHOLD:
                                    v_p2 += AnalysisConstants.TWINS_WEIGHT

                            # 3. Учитываем паттерны
                            if isinstance(pattern_res, dict) and 'count' in pattern_res and pattern_res['count'] > 0:
                                if p_hw / p_count > AnalysisConstants.WIN_THRESHOLD:
                                    v_p1 += AnalysisConstants.PATTERN_WEIGHT
                                if p_dw / p_count > AnalysisConstants.DRAW_THRESHOLD:
                                    v_x += AnalysisConstants.PATTERN_WEIGHT
                                if p_aw / p_count > AnalysisConstants.WIN_THRESHOLD:
                                    v_p2 += AnalysisConstants.PATTERN_WEIGHT

                            # --- ФОРМИРОВАНИЕ ВЕРДИКТА ---
                            # Определяем правила в порядке приоритета
                            verdict_rules = [
                                (v_p1 >= AnalysisConstants.VERDICT_STRONG_THRESHOLD, Messages.VERDICT_SIGNAL_P1),
                                (v_p2 >= AnalysisConstants.VERDICT_STRONG_THRESHOLD, Messages.VERDICT_SIGNAL_P2),
                                (v_x >= AnalysisConstants.VERDICT_STRONG_THRESHOLD, Messages.VERDICT_SIGNAL_DRAW),
                                (v_p1 >= AnalysisConstants.VERDICT_WEAK_THRESHOLD, Messages.VERDICT_ACCENT_1X),
                                (v_p2 >= AnalysisConstants.VERDICT_WEAK_THRESHOLD, Messages.VERDICT_ACCENT_X2),
                            ]

                            # Ищем первое выполняющееся условие
                            verdict = Messages.VERDICT_NO_CLEAR_VECTOR  # Значение по умолчанию
                            for condition, verdict_text in verdict_rules:
                                if condition:
                                    verdict = verdict_text
                                    break

                            # --- СОХРАНЕНИЕ РЕЗУЛЬТАТА (ОРИГИНАЛЬНАЯ СТРУКТУРА) ---
                            results.append({
                                'match': f"{home_team.name} - {away_team.name}",
                                'league': league.name if league else "Unknown",
                                'poisson_l': f"{p_data['home_lambda']} : {p_data['away_lambda']}",
                                'poisson_top': top_scores,
                                'poisson_btts': {
                                    'yes': poisson_results['btts_yes'],
                                    'no': poisson_results['btts_no']
                                },
                                'poisson_over25': {
                                    'yes': poisson_results['over25_yes'],
                                    'no': poisson_results['over25_no']
                                },
                                'twins_count': t_count,
                                'twins_dist': t_dist,
                                'pattern_data': pattern_res,
                                'h2h_list': h2h_list,
                                'h2h_total': h2h_qs.count(),
                                'odds': (
                                    float(h_odd) if h_odd is not None else None,
                                    float(d_odd) if d_odd is not None else None,
                                    float(a_odd) if a_odd is not None else None
                                ),
                                'verdict': verdict
                            })
                        else:
                            # Сохраняем нераспознанные команды
                            if not home_team:
                                unknown_teams.add(home_raw.strip())
                            if not away_team:
                                unknown_teams.add(away_raw.strip())

                except (IndexError, ValueError, Exception) as e:
                    error_msg = f"Error processing line {i}: {e}"
                    logger.error(error_msg)
                    print(error_msg)
                    continue

        self.request.session['cleaned_results'] = []
        for el in results:
            if el['verdict'] == constants.Messages.VERDICT_SIGNAL_P1 or \
                    el['verdict'] == constants.Messages.VERDICT_SIGNAL_P2 or \
                    el['verdict'] == constants.Messages.VERDICT_SIGNAL_DRAW:
                self.request.session['cleaned_results'].append(el)
        # self.request.session['cleaned_results'] = results

        # Логирование общего количества найденных матчей
        logger.info(Messages.TOTAL_MATCHES.format(len(results)))

        # --- 4. ВОЗВРАТ РЕЗУЛЬТАТОВ (СОХРАНЯЕМ ОРИГИНАЛЬНУЮ СТРУКТУРУ) ---
        return render(request, self.template_name, {
            'results': results,
            'raw_text': raw_text,
            'unknown_teams': sorted(list(unknown_teams)),
            'all_teams': Team.objects.all().order_by('name'),
        })


class CleanedTemplateView(TemplateView):
    template_name = 'app_bets/cleaned.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['cleaned_results'] = self.request.session.get('cleaned_results')
        return context


class UploadCSVView(View):
    template_name = 'app_bets/bets_main.html'

    def post(self, request):
        # Получаем контекст из сессии или создаем новый
        context = {
            'results': [],
            'raw_text': '',
            'unknown_teams': [],
            'all_teams': Team.objects.all(),
            'import_status': 'success',
            'import_message': '',
            'import_added': 0,
            'import_skipped': 0,
            'import_errors': 0
        }

        try:
            # Проверяем, есть ли файл в запросе
            if 'csv_file' not in request.FILES:
                # Проверяем, не идет ли синхронизация из папки
                if 'sync_files' in request.POST:
                    return self.sync_from_folder(request, context)

                context['import_status'] = 'error'
                context['import_message'] = 'Файл не найден. Выберите CSV файл для загрузки.'
                return render(request, self.template_name, context)

            csv_file = request.FILES['csv_file']

            # Проверяем расширение файла
            if not csv_file.name.endswith('.csv'):
                context['import_status'] = 'error'
                context['import_message'] = 'Файл должен быть в формате CSV'
                return render(request, self.template_name, context)

            # Импортируем из файла
            return self.import_from_file(request, csv_file, context)

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка при обработке запроса: {str(e)}'
            return render(request, self.template_name, context)

    def sync_from_folder(self, request, context):
        """Синхронизация из папки import_data"""
        import_data_dir = 'import_data'

        try:
            if not os.path.exists(import_data_dir):
                context['import_status'] = 'error'
                context['import_message'] = f'Папка {import_data_dir} не найдена.'
                return render(request, self.template_name, context)

            csv_files = [f for f in os.listdir(import_data_dir) if f.endswith('.csv')]

            if not csv_files:
                context['import_status'] = 'warning'
                context['import_message'] = f'В папке {import_data_dir} не найдено CSV файлов.'
                return render(request, self.template_name, context)

            total_added = 0
            total_skipped = 0
            total_errors = 0

            for csv_file_name in csv_files:
                file_path = os.path.join(import_data_dir, csv_file_name)
                result = self.process_csv_file(file_path)
                total_added += result['added']
                total_skipped += result['skipped']
                total_errors += result['errors']

            context['import_added'] = total_added
            context['import_skipped'] = total_skipped
            context['import_errors'] = total_errors
            context['import_message'] = (
                f'СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА:\n'
                f'- Обработано файлов: {len(csv_files)}\n'
                f'- Добавлено матчей: {total_added}\n'
                f'- Пропущено (не найдены команды): {total_skipped}\n'
                f'- Ошибок в данных: {total_errors}'
            )

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка синхронизации: {str(e)}'

        return render(request, self.template_name, context)

    def import_from_file(self, request, csv_file, context):
        """Импорт из загруженного файла"""

        try:
            # Читаем файл
            try:
                file_content = csv_file.read().decode('utf-8-sig')
            except UnicodeDecodeError:
                csv_file.seek(0)
                file_content = csv_file.read().decode('latin-1')

            # Сохраняем файл временно
            import tempfile

            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name

            try:
                result = self.process_csv_file(tmp_path)
            finally:
                # Удаляем временный файл
                os.unlink(tmp_path)

            context['import_added'] = result['added']
            context['import_skipped'] = result['skipped']
            context['import_errors'] = result['errors']

            if result['added'] > 0:
                context['import_message'] = (
                    f'ИМПОРТ ЗАВЕРШЕН:\n'
                    f'- Добавлено матчей: {result["added"]}\n'
                    f'- Пропущено (не найдены команды): {result["skipped"]}\n'
                    f'- Ошибок в данных: {result["errors"]}'
                )
            else:
                context['import_status'] = 'warning'
                context['import_message'] = (
                    f'Нет новых матчей для добавления.\n'
                    f'Пропущено (не найдены команды): {result["skipped"]}\n'
                    f'Ошибок в данных: {result["errors"]}'
                )

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка при обработке файла: {str(e)}'

        return render(request, self.template_name, context)

    @transaction.atomic
    def process_csv_file(self, file_path):
        """Обработка CSV файла (общая функция для файла и папки)"""
        count = 0
        skipped_teams = 0
        errors = 0

        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=',')

                for row in reader:
                    try:
                        # 1. Поиск лиги по названию
                        div_code = row.get('Div')
                        league_name = ParsingConstants.DIV_TO_LEAGUE_NAME.get(div_code)

                        if not league_name:
                            continue

                        # Ищем объект лиги в базе по имени
                        league = League.objects.filter(name=league_name).first()
                        if not league:
                            continue

                        # 2. Дата и Сезон
                        date_str = row.get('Date', '').strip()
                        if not date_str:
                            continue

                        try:
                            dt = datetime.strptime(date_str, '%d/%m/%Y')
                        except ValueError:
                            try:
                                dt = datetime.strptime(date_str, '%d/%m/%y')
                            except ValueError:
                                continue

                        season = self.get_season_by_date(dt)
                        if not season:
                            continue

                        # 3. Поиск команд
                        home_team = self.get_team_by_alias(row.get('HomeTeam'))
                        away_team = self.get_team_by_alias(row.get('AwayTeam'))

                        if not home_team or not away_team:
                            skipped_teams += 1
                            continue

                        # Проверка на дубликат (по дате и хозяевам)
                        dt_aware = make_aware(dt, get_current_timezone())
                        if Match.objects.filter(date=dt_aware, home_team=home_team).exists():
                            continue

                        # 4. Сбор коэффициентов (Приоритет: Avg -> B365 -> PS)
                        odd_h = self.parse_odd(row.get('AvgH') or row.get('B365H') or row.get('PSH'))
                        odd_d = self.parse_odd(row.get('AvgD') or row.get('B365D') or row.get('PSD'))
                        odd_a = self.parse_odd(row.get('AvgA') or row.get('B365A') or row.get('PSA'))

                        # 5. Сбор голов
                        h_goal = self.parse_score(row.get('FTHG'))
                        a_goal = self.parse_score(row.get('FTAG'))

                        # 6. Сохранение в БД
                        Match.objects.create(
                            season=season,
                            league=league,
                            date=dt_aware,
                            home_team=home_team,
                            away_team=away_team,
                            home_score_reg=h_goal,
                            away_score_reg=a_goal,
                            home_score_final=h_goal,
                            away_score_final=a_goal,
                            odds_home=odd_h,
                            odds_draw=odd_d,
                            odds_away=odd_a,
                            finish_type='REG'
                        )

                        count += 1

                    except Exception as e:
                        errors += 1
                        continue

        except Exception as e:
            errors += 1

        return {
            'added': count,
            'skipped': skipped_teams,
            'errors': errors
        }

    @staticmethod
    def get_team_by_alias(name):
        if not name:
            return None
        clean_alias = " ".join(str(name).split()).lower()
        alias = TeamAlias.objects.filter(name=clean_alias).select_related('team').first()
        return alias.team if alias else None

    @staticmethod
    def get_season_by_date(dt):
        return Season.objects.filter(start_date__lte=dt.date(), end_date__gte=dt.date()).first()

    @staticmethod
    def parse_score(val):
        """Превращает '2.0', '2' или '2,0' в целое число 2"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return 0
        try:
            return int(float(str(val).replace(',', '.')))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def parse_odd(val):
        """Безопасно парсит коэффициент в Decimal"""
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return Decimal('1.01')
        try:
            return Decimal(str(val).replace(',', '.')).quantize(Decimal('0.01'))
        except:
            return Decimal('1.01')
