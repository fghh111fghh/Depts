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
import logging
import os
from decimal import Decimal
from typing import List, Dict, Optional

from django.conf import settings
from django.db import transaction
from django.db.models import F
from django.shortcuts import render, redirect
from django.views import View
import re
import math
import unicodedata

from app_bets.constants import Outcome, ParsingConstants, AnalysisConstants, Messages

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
    def get_poisson_probs(l_home: float, l_away: float) -> List[Dict]:
        """
        Рассчитывает вероятности различных счетов по распределению Пуассона.

        Формула Пуассона: P(k) = (e^(-λ) * λ^k) / k!
        где λ (лямбда) - среднее ожидаемое количество голов

        Args:
            l_home (float): Лямбда для домашней команды
            l_away (float): Лямбда для гостевой команды

        Returns:
            List[Dict]: Список из 5 самых вероятных счетов в формате
                       [{'score': '2:1', 'prob': 12.34}, ...]

        Пример расчета для λ_home=1.5, λ_away=1.0:
        P(1:0) = P(home=1) * P(away=0) ≈ 0.3347 * 0.3679 ≈ 0.1231 = 12.31%
        """
        probs = []
        try:
            # Защита от нулевых или отрицательных значений
            l_home = max(float(l_home), AnalysisConstants.POISSON_MIN_LAMBDA)
            l_away = max(float(l_away), AnalysisConstants.POISSON_MIN_LAMBDA)

            # Предварительный расчет экспонент для производительности
            exp_home = math.exp(-l_home)
            exp_away = math.exp(-l_away)

            # Рассчитываем вероятности для счетов до POISSON_MAX_GOALS голов
            for h in range(AnalysisConstants.POISSON_MAX_GOALS):
                p_h = (exp_home * (l_home ** h)) / math.factorial(h)
                for a in range(AnalysisConstants.POISSON_MAX_GOALS):
                    p_a = (exp_away * (l_away ** a)) / math.factorial(a)
                    probability = p_h * p_a * 100

                    # Сохраняем только вероятности выше минимального порога
                    if probability > AnalysisConstants.MIN_PROBABILITY:
                        probs.append({
                            'score': f"{h}:{a}",
                            'prob': round(probability, 2)
                        })
        except Exception as e:
            logger.error(f"Ошибка расчета вероятностей Пуассона: {e}")
            return []

        # Сортируем по убыванию вероятности и берем топ-5
        return sorted(probs, key=lambda x: x['prob'], reverse=True)[:5]

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
                            top_scores = self.get_poisson_probs(p_data['home_lambda'], p_data['away_lambda'])

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
                                'twins_count': t_count,
                                'twins_dist': t_dist,
                                'pattern_data': pattern_res,
                                'h2h_list': h2h_list,
                                'h2h_total': h2h_qs.count(),
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

        # Логирование общего количества найденных матчей
        logger.info(Messages.TOTAL_MATCHES.format(len(results)))

        # --- 4. ВОЗВРАТ РЕЗУЛЬТАТОВ (СОХРАНЯЕМ ОРИГИНАЛЬНУЮ СТРУКТУРУ) ---
        return render(request, self.template_name, {
            'results': results,
            'raw_text': raw_text,
            'unknown_teams': sorted(list(unknown_teams)),
            'all_teams': Team.objects.all().order_by('name'),
        })


# Импорты моделей (должны быть в конце во избежание циклических импортов)
from app_bets.models import Team, TeamAlias, Match, League, Season


class UploadCSVView(View):
    def post(self, request):
        if 'sync_files' in request.POST:
            stats = self.sync_local_files()
            from django.contrib import messages

            # Формируем основное сообщение
            msg = f"Добавлено: {stats['added']}, Обновлено: {stats['updated']}. "

            # Если есть неизвестные команды, добавляем их имена прямо в сообщение
            if stats['unknown_teams']:
                teams_list = ", ".join(list(stats['unknown_teams']))
                msg += f"НЕ ОПОЗНАНО ({len(stats['unknown_teams'])}): [{teams_list}]"

            if stats['unknown_leagues']:
                leagues_list = ", ".join(list(stats['unknown_leagues']))
                msg += f" | Неизвестные лиги: {leagues_list}"

            messages.success(request, msg)
            return redirect('app_bets:bets_maim')
        return redirect('app_bets:bets_maim')

    def sync_local_files(self):
        folder_path = os.path.join(settings.BASE_DIR, 'import_data')
        stats = {'added': 0, 'updated': 0, 'unknown_teams': set(), 'unknown_leagues': set()}

        # Текущий сезон
        current_season = Season.objects.filter(is_current=True).first() or Season.objects.last()
        analyzer = AnalyzeView()

        if not os.path.exists(folder_path):
            return stats

        for filename in os.listdir(folder_path):
            if not filename.endswith('.csv'): continue

            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        div_code = row.get('Div')
                        h_raw, a_raw = row.get('HomeTeam'), row.get('AwayTeam')
                        h_score, a_score = row.get('FTHG'), row.get('FTAG')

                        if not h_raw or h_score is None: continue

                        # Поиск лиги
                        league = League.objects.filter(external_id=div_code).first()
                        if not league:
                            stats['unknown_leagues'].add(div_code)
                            continue

                        # Поиск команд
                        home = analyzer.get_team_smart(h_raw)
                        away = analyzer.get_team_smart(a_raw)

                        if not home or not away:
                            if not home: stats['unknown_teams'].add(h_raw)
                            if not away: stats['unknown_teams'].add(a_raw)
                            continue

                        def get_dec(v):
                            try:
                                return Decimal(str(v).replace(',', '.')) if v else Decimal(0)
                            except:
                                return Decimal(0)

                        # Сохранение (ИСПРАВЛЕНО: добавлены final счета для валидации)
                        obj, created = Match.objects.update_or_create(
                            home_team=home,
                            away_team=away,
                            date=analyzer.parse_csv_date(row.get('Date')),
                            defaults={
                                'home_score_reg': int(h_score),
                                'away_score_reg': int(a_score),
                                'home_score_final': int(h_score),
                                'away_score_final': int(a_score),
                                'league': league,
                                'season': current_season,
                                'odds_home': get_dec(row.get('B365H')),
                                'odds_draw': get_dec(row.get('B365D')),
                                'odds_away': get_dec(row.get('B365A')),
                            }
                        )
                        if created:
                            stats['added'] += 1
                        else:
                            stats['updated'] += 1

                    except Exception as e:
                        print(f"Ошибка в {filename}: {e}")
        return stats
