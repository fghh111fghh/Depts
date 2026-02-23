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
import os
import pickle
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
import openpyxl
import pandas as pd
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.messages.views import SuccessMessageMixin
from django.core.cache import cache
from django.db import transaction
from django.db.models import F, Q, Sum, DecimalField
from django.db.models.functions import Coalesce
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.utils.timezone import make_aware, get_current_timezone
from django.views import View
import re
import math
import unicodedata
from django.views.decorators.cache import cache_page
from django.views.decorators.http import require_POST
from django.views.generic import TemplateView, CreateView, ListView
from openpyxl.styles import Font, PatternFill, Alignment
from dal import autocomplete
from app_bets.constants import Outcome, ParsingConstants, AnalysisConstants, Messages
from app_bets.forms import BetForm
from app_bets.models import Team, TeamAlias, Season, Match, League, Bet,Bank


class AnalyzeView(ListView):
    """
    Отображает список проанализированных матчей на основе загруженного текста.
    """
    template_name = 'app_bets/bets_main.html'
    context_object_name = 'results'
    paginate_by = 20

    def get_queryset(self):
        """
        Возвращает результаты из сессии (так как они не хранятся в БД).
        Для ListView ожидается QuerySet, но мы адаптируем под список.
        """
        # Получаем результаты из сессии
        results = self.request.session.get('results', [])

        # Применяем сортировку
        current_sort = self.get_current_sort()
        results = self.sort_results(results, current_sort)

        return results

    def get_context_data(self, **kwargs):
        """Добавляет дополнительные данные в контекст шаблона."""
        context = super().get_context_data(**kwargs)

        # Получаем параметры из сессии
        context.update({
            'raw_text': self.request.session.get('raw_text', ''),
            'unknown_teams': sorted(self.request.session.get('unknown_teams', [])),
            'all_teams': Team.objects.all().order_by('name'),
            'current_sort': self.get_current_sort(),
        })

        return context

    def get_current_sort(self) -> str:
        """Возвращает текущий параметр сортировки."""
        return self.request.GET.get('sort') or self.request.session.get('current_sort', 'default')

    def sort_results(self, results: List[Dict], sort_param: str) -> List[Dict]:
        """
        Сортирует результаты в соответствии с параметром сортировки.
        """
        if not results:
            return results

        # Сохраняем оригинальные результаты для сброса
        if sort_param == 'default':
            results[:] = [dict(r) for r in self.request.session.get('original_results', [])]
        elif sort_param == 'btts_desc':
            results.sort(key=lambda x: x['poisson_btts']['yes'], reverse=True)
        elif sort_param == 'over25_desc':
            results.sort(key=lambda x: x['poisson_over25']['yes'], reverse=True)
        elif sort_param == 'twins_p1_desc':
            results.sort(
                key=lambda x: max(
                    x.get('twins_data', {}).get('p1', 0) if x.get('twins_data') else 0,
                    x.get('twins_data', {}).get('p2', 0) if x.get('twins_data') else 0
                ),
                reverse=True
            )
        elif sort_param == 'pattern_p1_desc':
            results.sort(
                key=lambda x: max(
                    x.get('pattern_data', {}).get('p1', 0) if x.get('pattern_data') else 0,
                    x.get('pattern_data', {}).get('p2', 0) if x.get('pattern_data') else 0
                ),
                reverse=True
            )

        return results

    def post(self, request, *args, **kwargs):
        """
        Обрабатывает POST-запрос: парсинг текста и анализ матчей.
        """
        # Получаем параметры
        current_sort = request.POST.get('sort') or request.GET.get('sort') or request.session.get('current_sort',
                                                                                                  'default')
        request.session['current_sort'] = current_sort
        raw_text = request.POST.get('matches_text', '')

        # Обработка создания алиаса
        if 'create_alias' in request.POST:
            self._handle_alias_creation(request)

        # Если нет текста для анализа
        if not raw_text.strip():
            return self._render_empty_response(request, raw_text, current_sort)

        # Выполняем анализ
        results, unknown_teams = self._analyze_matches(request, raw_text)

        # Сохраняем в сессию
        request.session['results'] = results
        request.session['raw_text'] = raw_text
        request.session['unknown_teams'] = list(unknown_teams)

        if results:
            request.session['original_results'] = [dict(r) for r in results]

        # Применяем сортировку
        sorted_results = self.sort_results(results, current_sort)

        # Рендерим результат
        return render(request, self.template_name, {
            'results': sorted_results,
            'raw_text': raw_text,
            'unknown_teams': sorted(list(unknown_teams)),
            'all_teams': Team.objects.all().order_by('name'),
            'current_sort': current_sort,
        })

    def _handle_alias_creation(self, request):
        """Создает новый алиас для команды."""
        alias_raw = request.POST.get('alias_name', '')
        team_id = request.POST.get('team_id')

        if alias_raw and team_id:
            try:
                clean_name = self.clean_team_name(alias_raw)
                with transaction.atomic():
                    TeamAlias.objects.update_or_create(
                        name=clean_name,
                        defaults={'team_id': team_id}
                    )
            except Exception as e:
                print(f"Ошибка сохранения алиаса: {e}")

    def _render_empty_response(self, request, raw_text: str, current_sort: str):
        """Возвращает пустой ответ, когда нет данных для анализа."""
        return render(request, self.template_name, {
            'results': [],
            'raw_text': raw_text,
            'unknown_teams': [],
            'all_teams': Team.objects.all().order_by('name'),
            'current_sort': current_sort,
        })

    def _load_cached_data(self):
        """
        Загружает данные с кэшированием в памяти.
        Кэш обновляется раз в сутки.
        """
        cache_key = 'match_analysis_full_data'

        # Пробуем получить из кэша
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            print("Данные загружены из кэша")
            return cached_data

        print("Загружаем данные из БД в кэш (может занять некоторое время)...")

        # Загружаем все матчи с результатами
        all_matches = list(Match.objects.filter(
            home_score_reg__isnull=False
        ).select_related(
            'home_team', 'away_team', 'league', 'season'
        ).order_by('date'))

        # Индексируем матчи по лиге для быстрого доступа
        matches_by_league = {}
        for match in all_matches:
            if match.league_id not in matches_by_league:
                matches_by_league[match.league_id] = []
            matches_by_league[match.league_id].append(match)

        # Кэшируем все команды
        all_teams = {team.id: team for team in Team.objects.all()}

        # Кэшируем все алиасы
        all_aliases = {}
        for alias in TeamAlias.objects.all().select_related('team'):
            all_aliases[alias.name] = alias.team

        # Кэшируем все лиги
        all_leagues = {league.id: league for league in League.objects.all()}

        # Сохраняем в кэш на 24 часа (86400 секунд)
        data = (all_matches, matches_by_league, all_teams, all_aliases, all_leagues)
        cache.set(cache_key, data, 86400)  # 24 часа

        print(f"Загружено {len(all_matches)} матчей, {len(all_teams)} команд, {len(all_leagues)} лиг")
        return data

    def _analyze_matches(self, request, raw_text: str) -> Tuple[List[Dict], set]:
        """
        Основной метод анализа матчей с использованием кэшированных данных.
        """
        results = []
        unknown_teams = set()

        # Получаем текущий сезон
        season = Season.objects.filter(is_current=True).first() or Season.objects.order_by('-start_date').first()

        # Разбиваем текст на строки
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        if not lines:
            return results, unknown_teams

        # Загружаем данные из кэша (все матчи уже здесь!)
        all_matches, matches_by_league, all_teams_dict, all_aliases, all_leagues = self._load_cached_data()

        # Парсим и анализируем
        skip_to = -1
        for i, line in enumerate(lines):
            if i <= skip_to:
                continue

            if re.match(ParsingConstants.ODDS_REGEX, line):
                try:
                    match_data = self._parse_match_data(lines, i)
                    if not match_data:
                        continue

                    h_odd, d_odd, a_odd, skip_to, names = match_data

                    if len(names) != 2:
                        continue

                    away_raw, home_raw = names[0], names[1]

                    # Ищем команды в кэшированных данных
                    home_team, away_team = self._find_teams(
                        home_raw, away_raw, all_teams_dict, all_aliases
                    )

                    if home_team and away_team:
                        # Анализируем матч используя кэшированные данные
                        result = self._analyze_single_match(
                            home_team, away_team, season, all_matches, matches_by_league,
                            all_leagues, h_odd, d_odd, a_odd
                        )
                        if result:
                            results.append(result)
                    else:
                        if not home_team:
                            unknown_teams.add(home_raw.strip())
                        if not away_team:
                            unknown_teams.add(away_raw.strip())

                except (IndexError, ValueError, Exception) as e:
                    print(f"Error processing line {i}: {e}")
                    continue

        return results, unknown_teams

    def _parse_match_data(self, lines: List[str], index: int) -> Optional[Tuple]:
        """Парсит коэффициенты и названия команд из строк."""
        try:
            h_odd = Decimal(lines[index].replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
            d_odd = Decimal(lines[index + 1].replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
            a_odd = Decimal(lines[index + 2].replace(',', '.')).quantize(Decimal(Messages.DECIMAL_FORMAT))
            skip_to = index + 2

            names = self._extract_team_names(lines, index)

            return h_odd, d_odd, a_odd, skip_to, names
        except Exception as e:
            print(f"Ошибка парсинга данных матча: {e}")
            return None

    def _extract_team_names(self, lines: List[str], odds_index: int) -> List[str]:
        """Извлекает названия команд из строк перед коэффициентами."""
        names = []
        search_depth = min(ParsingConstants.MAX_SEARCH_DEPTH, odds_index)

        for j in range(odds_index - 1, odds_index - search_depth - 1, -1):
            if j < 0:
                break

            row = lines[j].strip()

            if (not row or row == '-' or
                    re.match(ParsingConstants.TIME_REGEX, row) or
                    row.lower() in ParsingConstants.SKIP_KEYWORDS or
                    any(keyword in row.lower() for keyword in ParsingConstants.LEAGUE_KEYWORDS)):
                continue

            clean_name = self.clean_team_name(row)
            if (clean_name and
                    len(clean_name) >= AnalysisConstants.MIN_TEAM_NAME_LENGTH and
                    not re.match(ParsingConstants.DIGITS_ONLY_REGEX, clean_name) and
                    clean_name not in ParsingConstants.BLACKLIST):

                names.append(row)
                if len(names) == 2:
                    break

        return names

    @staticmethod
    def clean_team_name(name: str) -> str:
        """Очищает название команды от лишних символов."""
        if not name:
            return ""
        try:
            name = unicodedata.normalize('NFKC', str(name))
            name = re.sub(ParsingConstants.TIME_REGEX, '', name)
            name = re.sub(r'[^\w\s\d\-\']', ' ', name)
            name = re.sub(r'^\d+\s+|\s+\d+$', '', name)
            name = re.sub(r'[\-\–\—]+', ' ', name)
            name = ' '.join(name.split())
            return name.strip().lower()
        except Exception as e:
            print(f"Ошибка очистки названия команды '{name}': {e}")
            return str(name).strip().lower() if name else ""

    def _find_teams(self, home_raw: str, away_raw: str, all_teams: Dict, all_aliases: Dict) -> Tuple[
        Optional[object], Optional[object]]:
        """Находит команды по их названиям."""
        clean_home = self.clean_team_name(home_raw)
        clean_away = self.clean_team_name(away_raw)

        home_team = self._find_team(clean_home, all_teams, all_aliases)
        away_team = self._find_team(clean_away, all_teams, all_aliases)

        return home_team, away_team

    def _find_team(self, clean_name: str, all_teams: Dict, all_aliases: Dict) -> Optional[object]:
        """Находит команду по очищенному названию."""
        # Поиск по алиасам
        if clean_name in all_aliases:
            return all_aliases[clean_name]

        # Поиск по точному совпадению имени
        for team in all_teams.values():
            if team.name.lower() == clean_name:
                return team

        return None

    def _analyze_single_match(self, home_team, away_team, season, all_matches, matches_by_league,
                              all_leagues, h_odd, d_odd, a_odd) -> Optional[Dict]:
        """
        Анализирует один матч: определяет лигу, считает паттерны, пуассон, близнецов.
        Использует кэшированные данные.
        """
        # Определяем лигу
        league = self._determine_league(
            home_team, away_team, season, all_matches, matches_by_league, all_leagues
        )

        if not league:
            return None

        league_matches = matches_by_league.get(league.id, [])

        # Анализируем исторический паттерн
        pattern_data, curr_h_form, curr_a_form = self._analyze_pattern(
            home_team, away_team, season, league_matches
        )

        # Считаем Пуассон
        poisson_results, p_data = self._calculate_poisson(
            home_team, away_team, league, season, h_odd
        )

        # Ищем близнецов
        twins_data, t_count = self._find_twins_matches(
            league_matches, h_odd, a_odd
        )

        # Получаем историю личных встреч
        h2h_list = self._get_h2h_matches(home_team, away_team)

        # Создаем объект матча для дополнительных расчетов
        m_obj = Match(
            home_team=home_team,
            away_team=away_team,
            league=league,
            season=season,
            odds_home=h_odd
        )
        historical_total_insight = m_obj.get_historical_total_insight()

        # Формируем результат
        return {
            'match': f"{home_team.name} - {away_team.name}",
            'league': league.name if league else "Unknown",
            'poisson_l': f"{p_data['home_lambda']:.2f} : {p_data['away_lambda']:.2f}",
            'poisson_top': poisson_results['top_scores'],
            'poisson_btts': {
                'yes': poisson_results['btts_yes'],
                'no': poisson_results['btts_no']
            },
            'poisson_over25': {
                'yes': poisson_results['over25_yes'],
                'no': poisson_results['over25_no']
            },
            'twins_count': t_count,
            'twins_data': twins_data,
            'pattern_data': pattern_data,
            'current_h_form': curr_h_form,
            'current_a_form': curr_a_form,
            'h2h_list': h2h_list,
            'h2h_total': len(h2h_list),
            'odds': (float(h_odd), float(d_odd), float(a_odd)),
            'historical_total': historical_total_insight.get('synthetic'),
        }

    def _determine_league(self, home_team, away_team, season, all_matches, matches_by_league, all_leagues):
        """
        Определяет лигу для пары команд.
        """
        league = None
        current_season_matches = [m for m in all_matches if m.season_id == season.id]

        # 1. По личным встречам в текущем сезоне
        for match in current_season_matches:
            if ((match.home_team_id == home_team.id and match.away_team_id == away_team.id) or
                (match.home_team_id == away_team.id and match.away_team_id == home_team.id)) and match.league:
                league = match.league
                print(f"Лига найдена по личным встречам в текущем сезоне: {league.name}")
                break

        # 2. По домашним матчам home_team в текущем сезоне
        if not league:
            for match in current_season_matches:
                if match.home_team_id == home_team.id and match.league:
                    league = match.league
                    print(f"Лига найдена по домашним матчам {home_team.name} в текущем сезоне: {league.name}")
                    break

        # 3. По гостевым матчам away_team в текущем сезоне
        if not league:
            for match in current_season_matches:
                if match.away_team_id == away_team.id and match.league:
                    league = match.league
                    print(f"Лига найдена по гостевым матчам {away_team.name} в текущем сезоне: {league.name}")
                    break

        # 4. По личным встречам в истории
        if not league:
            for match in all_matches:
                if ((match.home_team_id == home_team.id and match.away_team_id == away_team.id) or
                    (match.home_team_id == away_team.id and match.away_team_id == home_team.id)) and match.league:
                    league = match.league
                    print(f"Лига найдена по личным встречам в истории: {league.name}")
                    break

        # 5. По домашним матчам home_team в истории
        if not league:
            for match in all_matches:
                if match.home_team_id == home_team.id and match.league:
                    league = match.league
                    print(f"Лига найдена по домашним матчам {home_team.name} в истории: {league.name}")
                    break

        # 6. По гостевым матчам away_team в истории
        if not league:
            for match in all_matches:
                if match.away_team_id == away_team.id and match.league:
                    league = match.league
                    print(f"Лига найдена по гостевым матчам {away_team.name} в истории: {league.name}")
                    break

        # 7. По стране
        if not league and home_team.country:
            league = League.objects.filter(country=home_team.country).first()
            if league:
                print(f"Лига найдена по стране {home_team.country}: {league.name}")

        return league

    def _analyze_pattern(self, home_team, away_team, season, league_matches):
        """
        Анализирует исторические паттерны формы команд.
        """
        # Формируем историю команд только из матчей текущего сезона
        current_season_matches = [m for m in league_matches if m.season_id == season.id]
        sorted_current_season_matches = sorted(current_season_matches, key=lambda x: x.date)

        team_history_current = {}

        for m in sorted_current_season_matches:
            h_id, a_id = m.home_team_id, m.away_team_id

            if m.home_score_reg == m.away_score_reg:
                res_h = Outcome.DRAW
                res_a = Outcome.DRAW
            elif m.home_score_reg > m.away_score_reg:
                res_h = Outcome.WIN
                res_a = Outcome.LOSE
            else:
                res_h = Outcome.LOSE
                res_a = Outcome.WIN

            team_history_current.setdefault(h_id, []).append(res_h)
            team_history_current.setdefault(a_id, []).append(res_a)

        curr_h_form = "".join(team_history_current.get(home_team.id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]
        curr_a_form = "".join(team_history_current.get(away_team.id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]

        # Поиск совпадений паттернов во всей истории лиги
        all_league_matches_sorted = sorted(league_matches, key=lambda x: x.date)
        team_history_all = {}
        match_patterns_all = {}

        for m in all_league_matches_sorted:
            h_id, a_id = m.home_team_id, m.away_team_id

            h_f = "".join(team_history_all.get(h_id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]
            a_f = "".join(team_history_all.get(a_id, []))[-AnalysisConstants.PATTERN_FORM_LENGTH:]

            if len(h_f) == AnalysisConstants.PATTERN_FORM_LENGTH and len(a_f) == AnalysisConstants.PATTERN_FORM_LENGTH:
                match_patterns_all[m.id] = (h_f, a_f)

            if m.home_score_reg == m.away_score_reg:
                res_h = Outcome.DRAW
                res_a = Outcome.DRAW
            elif m.home_score_reg > m.away_score_reg:
                res_h = Outcome.WIN
                res_a = Outcome.LOSE
            else:
                res_h = Outcome.LOSE
                res_a = Outcome.WIN

            team_history_all.setdefault(h_id, []).append(res_h)
            team_history_all.setdefault(a_id, []).append(res_a)

        pattern_data = None
        p_hw, p_dw, p_aw, p_count = 0, 0, 0, 0

        if len(curr_h_form) == AnalysisConstants.PATTERN_FORM_LENGTH and len(
                curr_a_form) == AnalysisConstants.PATTERN_FORM_LENGTH:
            for m in league_matches:
                if match_patterns_all.get(m.id) == (curr_h_form, curr_a_form):
                    p_count += 1
                    if m.home_score_reg > m.away_score_reg:
                        p_hw += 1
                    elif m.home_score_reg == m.away_score_reg:
                        p_dw += 1
                    else:
                        p_aw += 1

            if p_count > 0:
                p1_pct = round(p_hw / p_count * 100)
                x_pct = round(p_dw / p_count * 100)
                p2_pct = round(p_aw / p_count * 100)

                total_pct = p1_pct + x_pct + p2_pct
                if total_pct != 100:
                    diff = 100 - total_pct
                    max_val = max(p1_pct, x_pct, p2_pct)
                    if p1_pct == max_val:
                        p1_pct += diff
                    elif x_pct == max_val:
                        x_pct += diff
                    else:
                        p2_pct += diff

                pattern_data = {
                    'pattern': f"{curr_h_form} - {curr_a_form}",
                    'count': p_count,
                    'p1': p1_pct,
                    'x': x_pct,
                    'p2': p2_pct
                }

        return pattern_data, curr_h_form, curr_a_form

    def _calculate_poisson(self, home_team, away_team, league, season, h_odd):
        """
        Рассчитывает вероятности по Пуассону.
        """
        m_obj = Match(
            home_team=home_team,
            away_team=away_team,
            league=league,
            season=season,
            odds_home=h_odd
        )
        p_data = m_obj.calculate_poisson_lambda_last_n(AnalysisConstants.LAMBDA_LAST_N)
        poisson_results = self.get_poisson_probs(p_data['home_lambda'], p_data['away_lambda'])

        return poisson_results, p_data

    def _find_twins_matches(self, league_matches, h_odd, a_odd):
        """
        Находит матчи-близнецы с похожими коэффициентами.
        """
        tol = AnalysisConstants.TWINS_TOLERANCE_SMALL
        twins_matches = []

        for m in league_matches:
            if m.odds_home and m.odds_away:  # Проверяем что коэффициенты есть
                h_diff = abs(float(m.odds_home) - float(h_odd))
                a_diff = abs(float(m.odds_away) - float(a_odd))
                if h_diff <= tol and a_diff <= tol:
                    twins_matches.append(m)

        if not twins_matches:
            tol = AnalysisConstants.TWINS_TOLERANCE_LARGE
            for m in league_matches:
                if m.odds_home and m.odds_away:
                    h_diff = abs(float(m.odds_home) - float(h_odd))
                    a_diff = abs(float(m.odds_away) - float(a_odd))
                    if h_diff <= tol and a_diff <= tol:
                        twins_matches.append(m)

        t_count = len(twins_matches)
        twins_data = None

        if t_count > 0:
            hw_t = sum(1 for m in twins_matches if m.home_score_reg > m.away_score_reg)
            dw_t = sum(1 for m in twins_matches if m.home_score_reg == m.away_score_reg)
            aw_t = sum(1 for m in twins_matches if m.home_score_reg < m.away_score_reg)

            total_with_results = hw_t + dw_t + aw_t

            if total_with_results > 0:
                p1_pct = round(hw_t / total_with_results * 100)
                x_pct = round(dw_t / total_with_results * 100)
                p2_pct = round(aw_t / total_with_results * 100)

                total_pct = p1_pct + x_pct + p2_pct
                if total_pct != 100:
                    diff = 100 - total_pct
                    max_val = max(p1_pct, x_pct, p2_pct)
                    if p1_pct == max_val:
                        p1_pct += diff
                    elif x_pct == max_val:
                        x_pct += diff
                    else:
                        p2_pct += diff

                twins_data = {
                    'count': t_count,
                    'p1': p1_pct,
                    'x': x_pct,
                    'p2': p2_pct
                }

        return twins_data, t_count

    def _get_h2h_matches(self, home_team, away_team):
        """
        Получает историю личных встреч команд.
        """
        h2h_queryset = Match.objects.filter(
            home_team=home_team,
            away_team=away_team
        ).select_related(
            'home_team', 'away_team'
        ).order_by('-date')[:10]

        h2h_list = []
        for m in h2h_queryset:
            h2h_list.append({
                'date': m.date.strftime(Messages.DATE_FORMAT),
                'score': f"{m.home_score_reg}:{m.away_score_reg}"
            })

        return h2h_list

    @staticmethod
    def get_poisson_probs(l_home: float, l_away: float) -> Dict:
        """
        Рассчитывает вероятности по Пуассону для различных исходов.
        """
        probs = []
        btts_yes = 0.0
        btts_no = 0.0
        over25_yes = 0.0
        over25_no = 0.0

        try:
            l_home = max(float(l_home), AnalysisConstants.POISSON_MIN_LAMBDA)
            l_away = max(float(l_away), AnalysisConstants.POISSON_MIN_LAMBDA)

            exp_home = math.exp(-l_home)
            exp_away = math.exp(-l_away)

            max_goals = AnalysisConstants.POISSON_MAX_GOALS
            factorials = [math.factorial(i) for i in range(max_goals + 1)]
            home_powers = [l_home ** i for i in range(max_goals + 1)]
            away_powers = [l_away ** i for i in range(max_goals + 1)]

            for h in range(max_goals + 1):
                p_h = (exp_home * home_powers[h]) / factorials[h]
                for a in range(max_goals + 1):
                    p_a = (exp_away * away_powers[a]) / factorials[a]
                    probability = p_h * p_a * 100

                    if probability > AnalysisConstants.MIN_PROBABILITY:
                        probs.append({
                            'score': f"{h}:{a}",
                            'prob': round(probability, 2)
                        })

                    if h > 0 and a > 0:
                        btts_yes += probability
                    else:
                        btts_no += probability

                    if (h + a) > 2.5:
                        over25_yes += probability
                    else:
                        over25_no += probability

            top_scores = sorted(probs, key=lambda x: x['prob'], reverse=True)[:5]

            total_btss = btts_yes + btts_no
            total_over = over25_yes + over25_no

            if total_btss > 0:
                btts_yes = (btts_yes / total_btss) * 100
                btts_no = (btts_no / total_btss) * 100

            if total_over > 0:
                over25_yes = (over25_yes / total_over) * 100
                over25_no = (over25_no / total_over) * 100

            btts_yes = round(btts_yes, 2)
            btts_no = round(btts_no, 2)
            over25_yes = round(over25_yes, 2)
            over25_no = round(over25_no, 2)

        except Exception as e:
            print(f"Ошибка расчета вероятностей Пуассона: {e}")
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

@method_decorator(cache_page(60 * 60 * 24), name='dispatch')  # кэш на 24 часа
class CleanedTemplateView(TemplateView):
    template_name = 'app_bets/cleaned.html'

    def get_league_mapping(self):
        leagues = League.objects.all()
        mapping = {}
        for league in leagues:
            mapping[league.name] = league.external_id
            mapping[f"{league.name} ({league.country.name})"] = league.external_id
        return mapping

    def get_calibration_data(self):
        pickle_path = os.path.join(settings.BASE_DIR, 'calibration_summary.pkl')
        if not os.path.exists(pickle_path):
            return None
        with open(pickle_path, 'rb') as f:
            df = pickle.load(f)
        league_map = self.get_league_mapping()
        df['external_id'] = df['league'].map(league_map)
        df = df.dropna(subset=['external_id'])
        return df

    def get_excel_matches(self):
        excel_path = os.path.join(settings.BASE_DIR, 'for_analyze_matches.xlsx')
        if not os.path.exists(excel_path):
            return None
        df = pd.read_excel(excel_path)
        required = ['Время', 'Хозяева', 'Гости', 'ТБ2,5', 'ТМ2,5']
        if not all(col in df.columns for col in required):
            return None
        return df

    def find_team(self, name):
        try:
            return Team.objects.get(Q(aliases__name__iexact=name) | Q(name__iexact=name))
        except Team.DoesNotExist:
            return None
        except Team.MultipleObjectsReturned:
            return Team.objects.filter(Q(aliases__name__iexact=name) | Q(name__iexact=name)).first()

    def get_league_for_team(self, team):
        last_match = Match.objects.filter(
            Q(home_team=team) | Q(away_team=team)
        ).select_related('league').order_by('-date').first()
        return last_match.league if last_match else None

    def calculate_probs_for_match(self, home_team, away_team, league, n_values):
        results = []
        for n in n_values:
            temp_match = Match(
                home_team=home_team,
                away_team=away_team,
                league=league,
                season=None
            )
            lambdas = temp_match.calculate_poisson_lambda_last_n(n=n)
            if 'error' in lambdas:
                continue
            over_prob = self.poisson_over_prob(lambdas['home_lambda'], lambdas['away_lambda'])
            results.append({
                'n': n,
                'over_prob': over_prob,
                'under_prob': 1 - over_prob,
                'home_lambda': lambdas['home_lambda'],
                'away_lambda': lambdas['away_lambda']
            })
        return results

    def poisson_over_prob(self, l_home, l_away, max_goals=10):
        import math
        def poisson(l, k):
            return math.exp(-l) * (l**k) / math.factorial(k)
        over = 0.0
        for h in range(max_goals+1):
            for a in range(max_goals+1):
                if h + a > 2.5:
                    over += poisson(l_home, h) * poisson(l_away, a)
        return over

    def find_calibration(self, calib_df, league, target, n, prob):
        league_code = league.external_id
        subset = calib_df[(calib_df['external_id'] == league_code) &
                          (calib_df['target'] == target) &
                          (calib_df['last_matches'] == n)]
        if subset.empty:
            return None, None
        prob_pct = prob * 100
        for _, row in subset.iterrows():
            interval = row['interval']
            if interval.startswith('>'):
                low = float(interval[1:])
                high = 100
            else:
                low, high = map(float, interval.split('-'))
            if low <= prob_pct < high:
                return row['actual_%'], interval
        return None, None

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        calib_df = self.get_calibration_data()
        excel_df = self.get_excel_matches()

        if calib_df is None or excel_df is None:
            context['error'] = 'Не удалось загрузить калибровочные данные или Excel-файл.'
            return context

        n_values = list(range(5, 11))
        analysis_results = []

        for idx, row in excel_df.iterrows():
            match_time = row['Время']
            if hasattr(match_time, 'strftime'):
                time_str = match_time.strftime('%H:%M')
            else:
                time_str = str(match_time)

            home_name = row['Хозяева']
            away_name = row['Гости']
            odds_over = float(row['ТБ2,5']) if not pd.isna(row['ТБ2,5']) else None
            odds_under = float(row['ТМ2,5']) if not pd.isna(row['ТМ2,5']) else None

            home_team = self.find_team(home_name)
            away_team = self.find_team(away_name)
            if not home_team or not away_team:
                continue

            league = self.get_league_for_team(home_team) or self.get_league_for_team(away_team)
            if not league:
                continue

            probs = self.calculate_probs_for_match(home_team, away_team, league, n_values)
            if not probs:
                continue

            best_ev = None
            best_target = None
            best_n = None
            best_actual = None
            best_interval = None
            best_prob = None
            best_odds = None

            for p in probs:
                actual_over, interval_over = self.find_calibration(calib_df, league, 'over', p['n'], p['over_prob'])
                if actual_over is not None and odds_over is not None:
                    ev_over = (actual_over / 100.0) * odds_over - 1
                    if ev_over > 0 and (best_ev is None or ev_over > best_ev):
                        best_ev = ev_over
                        best_target = 'over'
                        best_n = p['n']
                        best_actual = actual_over
                        best_interval = interval_over
                        best_prob = p['over_prob']
                        best_odds = odds_over

                actual_under, interval_under = self.find_calibration(calib_df, league, 'under', p['n'], p['under_prob'])
                if actual_under is not None and odds_under is not None:
                    ev_under = (actual_under / 100.0) * odds_under - 1
                    if ev_under > 0 and (best_ev is None or ev_under > best_ev):
                        best_ev = ev_under
                        best_target = 'under'
                        best_n = p['n']
                        best_actual = actual_under
                        best_interval = interval_under
                        best_prob = p['under_prob']
                        best_odds = odds_under

            if best_ev is not None:
                analysis_results.append({
                    'time': time_str,
                    'home': home_name,
                    'away': away_name,
                    'match': f"{home_name} - {away_name}",
                    'league': league.name,
                    'odds_over': odds_over,
                    'odds_under': odds_under,
                    'target': 'ТБ 2.5' if best_target == 'over' else 'ТМ 2.5',
                    'ev': round(best_ev * 100, 1),
                    'n': best_n,
                    'poisson_prob': round(best_prob * 100, 1),
                    'actual_prob': best_actual,
                    'interval': best_interval,
                    'recommended_odds': best_odds,
                    'home_team_id': home_team.id,
                    'away_team_id': away_team.id,
                    'league_id': league.id,
                    'target_code': best_target,
                })

        analysis_results.sort(key=lambda x: x['time'])
        self.request.session['cleaned_analysis_results'] = analysis_results
        context['analysis_results'] = analysis_results
        return context


class UploadCSVView(View):
    template_name = 'app_bets/bets_main.html'

    def post(self, request):
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
            if 'csv_file' not in request.FILES:
                if 'sync_files' in request.POST:
                    return self.sync_from_folder(request, context)

                context['import_status'] = 'error'
                context['import_message'] = 'Файл не найден. Выберите CSV файл для загрузки.'
                return render(request, self.template_name, context)

            csv_file = request.FILES['csv_file']

            if not csv_file.name.endswith('.csv'):
                context['import_status'] = 'error'
                context['import_message'] = 'Файл должен быть в формате CSV'
                return render(request, self.template_name, context)

            return self.import_from_file(request, csv_file, context)

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка при обработке запроса: {str(e)}'
            return render(request, self.template_name, context)

    def sync_from_folder(self, request, context):
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
            total_aliases = 0
            processed_files = 0
            details = []
            all_unknown_teams = set()

            self.request = request

            for csv_file_name in csv_files:
                file_path = os.path.join(import_data_dir, csv_file_name)
                result = self.process_csv_file(file_path)
                total_added += result['added']
                total_skipped += result['skipped']
                total_errors += result['errors']
                total_aliases += result.get('created_aliases', 0)
                processed_files += 1

                if 'unknown_teams_list' in result:
                    for team in result['unknown_teams_list']:
                        all_unknown_teams.add(team['name'])

                details.append(
                    f"{csv_file_name}: +{result['added']} "
                    f"(пропущено {result['skipped']}, "
                    f"ошибок {result['errors']}, "
                    f"алиасов {result.get('created_aliases', 0)})"
                )

            if all_unknown_teams:
                current_unknown = request.session.get('unknown_teams', [])
                request.session['unknown_teams'] = list(set(current_unknown + list(all_unknown_teams)))

            context['import_added'] = total_added
            context['import_skipped'] = total_skipped
            context['import_errors'] = total_errors
            context['import_aliases'] = total_aliases

            message_parts = [
                f'СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА:',
                f'✅ Обработано файлов: {processed_files}',
                f'✅ Добавлено матчей: {total_added}',
                f'⚠️ Пропущено: {total_skipped}',
                f'❌ Ошибок: {total_errors}',
                f'✨ Создано алиасов: {total_aliases}',
            ]

            if all_unknown_teams:
                message_parts.append(f'\n📝 Неизвестных команд: {len(all_unknown_teams)}')

            message_parts.append(f'\n📊 Детали по файлам:')
            message_parts.extend(details)

            context['import_message'] = '\n'.join(message_parts)

            if total_added == 0 and total_aliases == 0:
                context['import_status'] = 'warning'
            elif total_added > 0:
                context['import_status'] = 'success'
            else:
                context['import_status'] = 'info'

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка синхронизации: {str(e)}'
            import traceback
            traceback.print_exc()

        return render(request, self.template_name, context)

    def import_from_file(self, request, csv_file, context):
        try:
            try:
                file_content = csv_file.read().decode('utf-8-sig')
            except UnicodeDecodeError:
                csv_file.seek(0)
                file_content = csv_file.read().decode('latin-1')

            import tempfile

            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name

            try:
                result = self.process_csv_file(tmp_path)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

            context['import_added'] = result['added']
            context['import_skipped'] = result['skipped']
            context['import_errors'] = result['errors']

            if result['added'] > 0:
                context['import_message'] = (
                    f'ИМПОРТ ЗАВЕРШЕН:\n'
                    f'- Добавлено матчей: {result["added"]}\n'
                    f'- Пропущено: {result["skipped"]}\n'
                    f'- Ошибок: {result["errors"]}'
                )
            else:
                context['import_status'] = 'warning'
                context['import_message'] = (
                    f'Нет новых матчей для добавления.\n'
                    f'Пропущено: {result["skipped"]}\n'
                    f'Ошибок: {result["errors"]}'
                )

        except Exception as e:
            context['import_status'] = 'error'
            context['import_message'] = f'Ошибка при обработке файла: {str(e)}'

        return render(request, self.template_name, context)

    @transaction.atomic
    def process_csv_file(self, file_path):
        count = 0
        skipped = 0
        errors = 0
        processed = 0
        created_aliases = 0
        unknown_teams_list = []

        all_teams = {team.id: team for team in Team.objects.all()}
        all_teams_by_name = {team.name.lower(): team for team in Team.objects.all()}
        all_aliases = {}
        for alias in TeamAlias.objects.all().select_related('team'):
            all_aliases[alias.name] = alias.team
        all_leagues = {league.name: league for league in League.objects.all()}

        def find_team_smart(team_name, all_teams_dict, all_aliases_dict, all_teams_by_name_dict):
            nonlocal created_aliases, unknown_teams_list

            if not team_name:
                return None

            clean_name = self.clean_team_name(team_name)

            if clean_name in all_aliases_dict:
                return all_aliases_dict[clean_name]

            if clean_name in all_teams_by_name_dict:
                team = all_teams_by_name_dict[clean_name]
                alias, created = TeamAlias.objects.get_or_create(
                    name=clean_name,
                    defaults={'team': team}
                )
                if created:
                    created_aliases += 1
                    all_aliases_dict[clean_name] = team
                return team

            best_match = None
            best_score = 0

            for team_name_db, team in all_teams_by_name_dict.items():
                if clean_name in team_name_db:
                    score = len(clean_name) / len(team_name_db)
                    if score > best_score:
                        best_score = score
                        best_match = team
                elif team_name_db in clean_name:
                    score = len(team_name_db) / len(clean_name)
                    if score > best_score:
                        best_score = score
                        best_match = team

            if best_match and best_score > 0.6:
                alias, created = TeamAlias.objects.get_or_create(
                    name=clean_name,
                    defaults={'team': best_match}
                )
                if created:
                    created_aliases += 1
                    all_aliases_dict[clean_name] = best_match
                return best_match

            unknown_teams_list.append({
                'name': team_name,
                'clean_name': clean_name
            })
            return None

        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                first_line = f.readline()
                f.seek(0)

                delimiter = ';' if ';' in first_line else ','
                reader = csv.DictReader(f, delimiter=delimiter)

                for row in reader:
                    processed += 1
                    try:
                        div_code = row.get('Div', '').strip()
                        if not div_code:
                            skipped += 1
                            continue

                        league_name = ParsingConstants.DIV_TO_LEAGUE_NAME.get(div_code)
                        if not league_name:
                            skipped += 1
                            continue

                        league = all_leagues.get(league_name)
                        if not league:
                            skipped += 1
                            continue

                        date_str = row.get('Date', '').strip()
                        if not date_str:
                            skipped += 1
                            continue

                        try:
                            dt = datetime.strptime(date_str, '%d/%m/%Y')
                        except ValueError:
                            try:
                                dt = datetime.strptime(date_str, '%d/%m/%y')
                            except ValueError:
                                try:
                                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                                except ValueError:
                                    errors += 1
                                    continue

                        season = self.get_season_by_date(dt)
                        if not season:
                            season = Season.objects.filter(
                                start_date__lte=dt.date(),
                                end_date__gte=dt.date()
                            ).first()
                            if not season:
                                errors += 1
                                continue

                        home_team_name = row.get('HomeTeam', '').strip()
                        away_team_name = row.get('AwayTeam', '').strip()

                        if not home_team_name or not away_team_name:
                            skipped += 1
                            continue

                        home_team = find_team_smart(home_team_name, all_teams, all_aliases, all_teams_by_name)
                        away_team = find_team_smart(away_team_name, all_teams, all_aliases, all_teams_by_name)

                        if not home_team or not away_team:
                            skipped += 1
                            continue

                        dt_aware = make_aware(dt, get_current_timezone())

                        if Match.objects.filter(
                                date=dt_aware,
                                home_team=home_team,
                                away_team=away_team
                        ).exists():
                            skipped += 1
                            continue

                        odd_h = self.parse_odd(row.get('AvgH') or row.get('B365H') or row.get('PSH') or '1.01')
                        odd_d = self.parse_odd(row.get('AvgD') or row.get('B365D') or row.get('PSD') or '1.01')
                        odd_a = self.parse_odd(row.get('AvgA') or row.get('B365A') or row.get('PSA') or '1.01')
                        h_goal = self.parse_score(row.get('FTHG') or '0')
                        a_goal = self.parse_score(row.get('FTAG') or '0')

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

                    except Exception:
                        errors += 1
                        continue

        except Exception:
            errors += 1

        if unknown_teams_list and hasattr(self, 'request'):
            current_unknown = self.request.session.get('unknown_teams', [])
            new_unknown = list(set([item['name'] for item in unknown_teams_list]))
            self.request.session['unknown_teams'] = list(set(current_unknown + new_unknown))
        print(unknown_teams_list)
        return {
            'added': count,
            'skipped': skipped,
            'errors': errors,
            'created_aliases': created_aliases,
            'unknown_teams_list': unknown_teams_list,
            'unknown_teams': len(unknown_teams_list)
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
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return 0
        try:
            return int(float(str(val).replace(',', '.')))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def parse_odd(val):
        if not val or str(val).strip() == "" or str(val).lower() == 'nan':
            return Decimal('1.01')
        try:
            return Decimal(str(val).replace(',', '.')).quantize(Decimal('0.01'))
        except:
            return Decimal('1.01')

    @staticmethod
    def clean_team_name(name: str) -> str:
        if not name:
            return ""
        clean = " ".join(str(name).split()).lower()
        clean = re.sub(r'[^\w\s]', '', clean)
        return clean


class ExportBetsExcelView(View):
    """
    Экспорт отфильтрованных и отсортированных результатов в Excel.
    """

    def get(self, request, *args, **kwargs):
        # Получаем результаты из сессии (уже отсортированные)
        results = request.session.get('results', [])
        current_sort = request.session.get('current_sort', 'default')

        # Определяем название сортировки
        sort_names = {
            'default': 'По умолчанию',
            'btts_desc': 'ОЗ (убывание)',
            'over25_desc': 'б2.5 (убывание)',
            'twins_p1_desc': 'Близнецы (макс. П1/П2)',
            'pattern_p1_desc': 'История (макс. П1/П2)'
        }
        sort_name = sort_names.get(current_sort, 'По умолчанию')

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Анализ матчей"

        # Заголовок с информацией о сортировке
        ws.append([f"Сортировка: {sort_name}"])
        ws.append([])

        # Заголовки таблицы - оптимизированные, без дублей
        headers = [
            'Хозяева',
            'Гости',
            'Лига',
            'П1',
            'X',
            'П2',
            'ОЗ Да',
            'Тот2.5 Да',
            'Ист ТБ',
            'Синтез ТБ',
            'Оценка',
            'Близ П1',
            'Близ X',
            'Близ П2',
            'Ист П1',
            'Ист X',
            'Ист П2',
        ]
        ws.append(headers)

        # Жирный шрифт для заголовков
        for cell in ws[2]:
            cell.font = openpyxl.styles.Font(bold=True)

        # Заполняем данными
        for res in results:
            # Разбиваем match на хозяева и гости
            match_parts = res['match'].split(' - ', 1)
            home_team = match_parts[0] if len(match_parts) > 0 else ''
            away_team = match_parts[1] if len(match_parts) > 1 else ''

            # Коэффициенты
            odds = res.get('odds', (None, None, None))

            # Данные исторического тотала
            historical_total = res.get('historical_total', {})
            historical_tb = f"{historical_total.get('over_25', '')}%" if historical_total and historical_total.get(
                'over_25') else ''

            # Данные Пуассона по тоталу
            poisson_over25 = res.get('poisson_over25', {})
            poisson_tb = poisson_over25.get('yes', 0)

            # ------------------------------------------------------------
            # СИНТЕТИЧЕСКИЙ ПОКАЗАТЕЛЬ
            # ------------------------------------------------------------
            synthesis_tb = ''
            confidence = ''

            if historical_total and poisson_tb:
                hist_prob = historical_total.get('over_25', 0)

                if hist_prob:
                    # 1. БАЗОВЫЙ СИГНАЛ (приоритет Пуассона 60% / История 40%)
                    base = poisson_tb * 0.6 + hist_prob * 0.4

                    # 2. БОНУС ЗА СОГЛАСОВАННОСТЬ
                    if poisson_tb > 50 and hist_prob > 50:
                        p1 = poisson_tb / 100
                        p2 = hist_prob / 100
                        boost = 1.0 + (p1 * p2 * 0.3)
                        final = base * boost
                    else:
                        final = base

                    # 3. ШТРАФ ЗА ПРОТИВОРЕЧИЕ
                    if (poisson_tb > 70 and hist_prob < 40) or (hist_prob > 70 and poisson_tb < 40):
                        gap = abs(poisson_tb - hist_prob) / 100
                        penalty = 1.0 - (gap * 0.3)
                        final = final * penalty

                    # Ограничиваем шкалу 0-100
                    final = max(0, min(100, final))

                    synthesis_tb = f"{round(final, 1)}%"

                    # 4. УРОВЕНЬ УВЕРЕННОСТИ
                    if final >= 85:
                        confidence = "ВЫСОЧАЙШАЯ"
                    elif final >= 75:
                        confidence = "ВЫСОКАЯ"
                    elif final >= 65:
                        confidence = "ВЫШЕ СРЕДНЕГО"
                    elif final >= 55:
                        confidence = "СРЕДНЯЯ"
                    elif final >= 45:
                        confidence = "НИЗКАЯ"
                    else:
                        confidence = "СЛУЧАЙНАЯ"

            # Данные близнецов
            twins = res.get('twins_data', {})
            twins_p1 = twins.get('p1', '') if twins else ''
            twins_x = twins.get('x', '') if twins else ''
            twins_p2 = twins.get('p2', '') if twins else ''

            # Данные паттернов
            pattern = res.get('pattern_data', {})
            pattern_p1 = pattern.get('p1', '') if pattern else ''
            pattern_x = pattern.get('x', '') if pattern else ''
            pattern_p2 = pattern.get('p2', '') if pattern else ''

            row = [
                home_team,
                away_team,
                res.get('league', ''),
                odds[0] if odds[0] is not None else '',
                odds[1] if odds[1] is not None else '',
                odds[2] if odds[2] is not None else '',
                res.get('poisson_btts', {}).get('yes', ''),  # ОЗ Да
                poisson_over25.get('yes', ''),  # Тотал >2.5 Да
                historical_tb,  # Ист ТБ
                synthesis_tb,  # Синтез ТБ
                confidence,  # Оценка
                f"{twins_p1}%" if twins_p1 != '' else '',  # Близнецы (П1)
                f"{twins_x}%" if twins_x != '' else '',  # Близнецы (X)
                f"{twins_p2}%" if twins_p2 != '' else '',  # Близнецы (П2)
                f"{pattern_p1}%" if pattern_p1 != '' else '',  # История (П1)
                f"{pattern_x}%" if pattern_x != '' else '',  # История (X)
                f"{pattern_p2}%" if pattern_p2 != '' else '',  # История (П2)
            ]
            ws.append(row)

        # Настройка ширины колонок
        for col in ws.columns:
            max_length = 0
            column_letter = col[0].column_letter
            for cell in col:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            ws.column_dimensions[column_letter].width = min(max_length + 2, 30)

        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response[
            'Content-Disposition'] = f'attachment; filename=bets_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        wb.save(response)
        return response


class ExportCleanedExcelView(View):
    def get(self, request, *args, **kwargs):
        results = request.session.get('cleaned_analysis_results', [])
        if not results:
            # Если нет данных, можно вернуть пустой файл или ошибку
            return HttpResponse("Нет данных для экспорта", status=404)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Анализ матчей"

        headers = [
            'Время',
            'Хозяева',
            'Гости',
            'Лига',
            'Коэффициент',
            'Исход',
            'Прогноз Пуассона, %',
            'Фактическая вероятность, %',
            'EV, %'
        ]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = openpyxl.styles.Font(bold=True)

        for res in results:
            row = [
                res.get('time', ''),
                res.get('home', ''),
                res.get('away', ''),
                res.get('league', ''),
                res.get('recommended_odds', ''),
                res.get('target', ''),
                res.get('poisson_prob', ''),
                res.get('actual_prob', ''),
                res.get('ev', ''),
            ]
            ws.append(row)

        # автоширина
        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                if cell.value and len(str(cell.value)) > max_len:
                    max_len = len(str(cell.value))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 30)

        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        filename = f"cleaned_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        response['Content-Disposition'] = f'attachment; filename={filename}'
        wb.save(response)
        return response


class TeamAutocomplete(autocomplete.Select2QuerySetView):
    def get_queryset(self):
        qs = Team.objects.all()
        if self.q:
            # Поиск только по основному имени команды (без алиасов)
            qs = qs.filter(name__istartswith=self.q)
        return qs


class LeagueAutocomplete(autocomplete.Select2QuerySetView):
    def get_queryset(self):
        qs = League.objects.all()
        if self.q:
            qs = qs.filter(name__istartswith=self.q)
        return qs


class BetCreateView(SuccessMessageMixin, CreateView):
    model = Bet
    form_class = BetForm
    template_name = 'app_bets/bet_form.html'
    success_url = reverse_lazy('app_bets:cleaned')
    success_message = "Ставка на матч %(home_team)s - %(away_team)s (коэф. %(recommended_odds)s, сумма %(stake)s) успешно сохранена!"

    def get_initial(self):
        initial = super().get_initial()

        def safe_float(val):
            if val is None:
                return None
            val = str(val).replace(',', '.')
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        def safe_int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        # ID команд и лиги
        home_id = self.request.GET.get('home_team_id')
        if home_id:
            initial['home_team'] = safe_int(home_id)

        away_id = self.request.GET.get('away_team_id')
        if away_id:
            initial['away_team'] = safe_int(away_id)

        league_id = self.request.GET.get('league_id')
        if league_id:
            initial['league'] = safe_int(league_id)

        # Время матча
        match_time = self.request.GET.get('match_time')
        if match_time:
            initial['match_time'] = match_time

        # Коэффициенты (скрытые)
        odds_over = self.request.GET.get('odds_over')
        if odds_over:
            initial['odds_over'] = safe_float(odds_over)

        odds_under = self.request.GET.get('odds_under')
        if odds_under:
            initial['odds_under'] = safe_float(odds_under)

        # Рекомендуемый исход и коэффициент
        recommended_target = self.request.GET.get('recommended_target')
        if recommended_target:
            initial['recommended_target'] = recommended_target

        recommended_odds = self.request.GET.get('recommended_odds')
        if recommended_odds:
            initial['recommended_odds'] = safe_float(recommended_odds)

        # Вероятности и EV
        poisson_prob = self.request.GET.get('poisson_prob')
        if poisson_prob:
            initial['poisson_prob'] = safe_float(poisson_prob)

        actual_prob = self.request.GET.get('actual_prob')
        if actual_prob:
            initial['actual_prob'] = safe_float(actual_prob)

        ev = self.request.GET.get('ev')
        if ev:
            initial['ev'] = safe_float(ev)

        # n_last_matches (скрытое)
        n_last_matches = self.request.GET.get('n_last_matches')
        if n_last_matches:
            initial['n_last_matches'] = safe_int(n_last_matches)

        # Интервал
        interval = self.request.GET.get('interval')
        if interval:
            initial['interval'] = interval

        # Автоматические поля
        from .models import Bank
        from django.utils import timezone
        initial['bank_before'] = float(Bank.get_balance())
        initial['settled_at'] = timezone.now().date().isoformat()
        initial['fractional_kelly'] = 0.5

        # Расчёт начальной суммы ставки
        try:
            bank = initial.get('bank_before', 0)
            odds = initial.get('recommended_odds', 0)
            prob = initial.get('actual_prob', 0)
            fraction = initial.get('fractional_kelly', 0.5)

            if odds and prob and bank and fraction:
                p = float(prob) / 100
                k = float(odds)
                full_kelly = (p * k - 1) / (k - 1)
                limited_kelly = max(0, min(full_kelly, 1))
                stake = float(bank) * limited_kelly * float(fraction)
                initial['stake'] = round(stake / 100) * 100
            else:
                initial['stake'] = 0
        except Exception as e:
            print(f"Ошибка расчёта stake: {e}")
            initial['stake'] = 0

        return initial

    def get_success_message(self, cleaned_data):
        return self.success_message % {
            'home_team': self.object.home_team.name,
            'away_team': self.object.away_team.name,
            'recommended_odds': self.object.recommended_odds,
            'stake': self.object.stake,
        }

    def form_invalid(self, form):
        messages.error(self.request, 'Ошибка при сохранении ставки. Пожалуйста, исправьте ошибки в форме.')
        return super().form_invalid(form)


class BetRecordsView(LoginRequiredMixin, ListView):
    """
    View для отображения записей ставок в стиле админки
    """
    model = Bet
    template_name = 'app_bets/bet_records.html'
    context_object_name = 'bets'
    paginate_by = 20

    def get_queryset(self):
        """
        Получение queryset с фильтрацией и сортировкой
        """
        queryset = Bet.objects.select_related(
            'home_team',
            'away_team',
            'league__sport',
            'league__country'
        )

        # Фильтры
        # Поиск по командам или лиге
        search_query = self.request.GET.get('search', '')
        if search_query:
            queryset = queryset.filter(
                Q(home_team__name__icontains=search_query) |
                Q(away_team__name__icontains=search_query) |
                Q(league__name__icontains=search_query) |
                Q(league__country__name__icontains=search_query)
            )

        # Фильтр по дате начала
        date_from = self.request.GET.get('date_from', '')
        if date_from:
            try:
                date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
                queryset = queryset.filter(date_placed__date__gte=date_from)
            except ValueError:
                pass

        # Фильтр по дате окончания
        date_to = self.request.GET.get('date_to', '')
        if date_to:
            try:
                date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
                # Добавляем +1 день, чтобы включить весь день окончания
                date_to = datetime.combine(date_to, datetime.max.time())
                queryset = queryset.filter(date_placed__lte=date_to)
            except ValueError:
                pass

        # Фильтр по лиге
        league_id = self.request.GET.get('league', '')
        if league_id and league_id.isdigit():
            queryset = queryset.filter(league_id=league_id)

        # Фильтр по спорту
        sport_id = self.request.GET.get('sport', '')
        if sport_id and sport_id.isdigit():
            queryset = queryset.filter(league__sport_id=sport_id)

        # Фильтр по результату
        result = self.request.GET.get('result', '')
        if result and result != 'all':
            queryset = queryset.filter(result=result)

        # Фильтр по типу ставки (ТБ/ТМ)
        target = self.request.GET.get('target', '')
        if target and target != 'all':
            queryset = queryset.filter(recommended_target=target)

        # Фильтр по минимальной сумме
        min_amount = self.request.GET.get('min_amount', '')
        if min_amount and min_amount.replace('.', '', 1).isdigit():
            queryset = queryset.filter(stake__gte=Decimal(min_amount))

        # Фильтр по максимальной сумме
        max_amount = self.request.GET.get('max_amount', '')
        if max_amount and max_amount.replace('.', '', 1).isdigit():
            queryset = queryset.filter(stake__lte=Decimal(max_amount))

        # Фильтр по минимальному EV
        min_ev = self.request.GET.get('min_ev', '')
        if min_ev and min_ev.replace('-', '', 1).replace('.', '', 1).isdigit():
            queryset = queryset.filter(ev__gte=float(min_ev))

        # Фильтр по максимальному EV
        max_ev = self.request.GET.get('max_ev', '')
        if max_ev and max_ev.replace('-', '', 1).replace('.', '', 1).isdigit():
            queryset = queryset.filter(ev__lte=float(max_ev))

        # Сортировка
        sort_field = self.request.GET.get('sort', '-date_placed')
        valid_sort_fields = [
            'date_placed', '-date_placed',
            'stake', '-stake',
            'ev', '-ev',
            'profit', '-profit',
            'recommended_odds', '-recommended_odds',
            'home_team__name', '-home_team__name',
            'league__name', '-league__name',
        ]

        if sort_field in valid_sort_fields:
            queryset = queryset.order_by(sort_field)
        else:
            queryset = queryset.order_by('-date_placed')

        return queryset

    def get_context_data(self, **kwargs):
        """
        Добавление дополнительных данных в контекст
        """
        context = super().get_context_data(**kwargs)

        # Текущий баланс
        context['current_balance'] = Bank.get_balance()

        # Статистика по ставкам
        bets = self.get_queryset()
        context['total_bets'] = bets.count()
        context['total_stake'] = bets.aggregate(
            total=Coalesce(Sum('stake'), 0, output_field=DecimalField())
        )['total']
        context['total_profit'] = bets.aggregate(
            total=Coalesce(Sum('profit'), 0, output_field=DecimalField())
        )['total']

        # Количество по результатам
        context['wins_count'] = bets.filter(result=Bet.ResultChoices.WIN).count()
        context['losses_count'] = bets.filter(result=Bet.ResultChoices.LOSS).count()
        context['refunds_count'] = bets.filter(result=Bet.ResultChoices.REFUND).count()

        # ROI (возврат инвестиций)
        if context['total_stake'] > 0:
            context['roi'] = (context['total_profit'] / context['total_stake']) * 100
        else:
            context['roi'] = 0

        # Списки для фильтров
        context['leagues'] = League.objects.filter(bet__isnull=False).distinct().order_by('name')

        # Текущие значения фильтров для формы
        context['current_filters'] = {
            'search': self.request.GET.get('search', ''),
            'date_from': self.request.GET.get('date_from', ''),
            'date_to': self.request.GET.get('date_to', ''),
            'league': self.request.GET.get('league', ''),
            'sport': self.request.GET.get('sport', ''),
            'result': self.request.GET.get('result', ''),
            'target': self.request.GET.get('target', ''),
            'min_amount': self.request.GET.get('min_amount', ''),
            'max_amount': self.request.GET.get('max_amount', ''),
            'min_ev': self.request.GET.get('min_ev', ''),
            'max_ev': self.request.GET.get('max_ev', ''),
            'sort': self.request.GET.get('sort', '-date_placed'),
        }

        return context


@require_POST
@staff_member_required
def bulk_bet_action(request):
    """
    Обработка массовых действий со ставками
    """
    action = request.POST.get('action')
    bet_ids = request.POST.getlist('selected_bets')
    confirm = request.POST.get('confirm')

    if not bet_ids:
        messages.error(request, 'Не выбрано ни одной ставки')
        return redirect('app_bets:records')

    if action == 'delete':
        # Для удаления нужен confirm
        if confirm != 'true':
            # Сохраняем ID в сессии и показываем страницу подтверждения
            request.session['pending_bet_ids'] = bet_ids
            bets = Bet.objects.filter(id__in=bet_ids)
            return render(request, 'app_bets/confirm_bulk_delete.html', {
                'bets': bets,
                'count': bets.count(),
                'total_stake': bets.aggregate(total=Sum('stake'))['total'],
                'total_profit': bets.aggregate(total=Sum('profit'))['total'],
            })
        else:
            # Подтвержденное удаление - берем ID из POST или сессии
            if not bet_ids:
                bet_ids = request.session.pop('pending_bet_ids', [])

            if bet_ids:
                # Получаем все ставки для удаления
                bets_to_delete = Bet.objects.filter(id__in=bet_ids)

                # ВАЖНО: Вызываем delete() для каждой ставки отдельно,
                # чтобы сработал метод delete() модели и обновился банк
                deleted_count = 0
                total_profit_reverted = 0

                for bet in bets_to_delete:
                    profit = bet.profit
                    bet.delete()  # Здесь сработает метод delete() модели
                    if profit:
                        total_profit_reverted += profit
                    deleted_count += 1

                messages.success(
                    request,
                    f'Удалено {deleted_count} ставок. Баланс скорректирован на {abs(total_profit_reverted)} ₽'
                )
            else:
                messages.error(request, 'Не найдены ставки для удаления')

    elif action == 'mark_win':
        bets = Bet.objects.filter(id__in=bet_ids)
        for bet in bets:
            bet.result = Bet.ResultChoices.WIN
            bet.save()  # save() вызовет обновление банка
        messages.success(request, f'Отмечено как выигрыш: {bets.count()} ставок')

    elif action == 'mark_loss':
        bets = Bet.objects.filter(id__in=bet_ids)
        for bet in bets:
            bet.result = Bet.ResultChoices.LOSS
            bet.save()  # save() вызовет обновление банка
        messages.success(request, f'Отмечено как проигрыш: {bets.count()} ставок')

    elif action == 'mark_refund':
        bets = Bet.objects.filter(id__in=bet_ids)
        for bet in bets:
            bet.result = Bet.ResultChoices.REFUND
            bet.save()  # save() вызовет обновление банка
        messages.success(request, f'Отмечено как возврат: {bets.count()} ставок')

    return redirect('app_bets:records')


@staff_member_required
def export_bets_excel(request):
    """
    Экспорт ставок в Excel
    """
    # Получаем отфильтрованный queryset как в BetRecordsView
    queryset = Bet.objects.select_related(
        'home_team', 'away_team', 'league__sport', 'league__country'
    )

    # Применяем те же фильтры, что и в представлении
    # (копируем логику фильтрации из BetRecordsView.get_queryset)
    search_query = request.GET.get('search', '')
    if search_query:
        queryset = queryset.filter(
            Q(home_team__name__icontains=search_query) |
            Q(away_team__name__icontains=search_query) |
            Q(league__name__icontains=search_query) |
            Q(league__country__name__icontains=search_query)
        )

    date_from = request.GET.get('date_from', '')
    if date_from:
        try:
            date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
            queryset = queryset.filter(date_placed__date__gte=date_from)
        except ValueError:
            pass

    date_to = request.GET.get('date_to', '')
    if date_to:
        try:
            date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
            date_to = datetime.combine(date_to, datetime.max.time())
            queryset = queryset.filter(date_placed__lte=date_to)
        except ValueError:
            pass

    # Создаем Excel файл
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Ставки"

    # Заголовки
    headers = [
        'Дата', 'Время', 'Лига', 'Хозяева', 'Гости',
        'Ставка', 'Кэф', 'Сумма', 'Результат', 'Прибыль',
        'EV %', 'Вер. Пуассона', 'Факт. вер.', 'n матчей'
    ]

    # Стили для заголовков
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")

    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')

    # Данные
    for row_num, bet in enumerate(queryset, 2):
        ws.cell(row=row_num, column=1, value=bet.date_placed.strftime('%d.%m.%Y'))
        ws.cell(row=row_num, column=2, value=bet.match_time)
        ws.cell(row=row_num, column=3, value=str(bet.league))
        ws.cell(row=row_num, column=4, value=bet.home_team.name)
        ws.cell(row=row_num, column=5, value=bet.away_team.name)
        ws.cell(row=row_num, column=6, value=bet.get_recommended_target_display())
        ws.cell(row=row_num, column=7, value=float(bet.recommended_odds))
        ws.cell(row=row_num, column=8, value=float(bet.stake))
        ws.cell(row=row_num, column=9, value=bet.get_result_display())
        ws.cell(row=row_num, column=10, value=float(bet.profit) if bet.profit else 0)
        ws.cell(row=row_num, column=11, value=bet.ev)
        ws.cell(row=row_num, column=12, value=bet.poisson_prob)
        ws.cell(row=row_num, column=13, value=bet.actual_prob)
        ws.cell(row=row_num, column=14, value=bet.n_last_matches)

        # Форматирование чисел
        for col in [7, 8, 10]:
            ws.cell(row=row_num, column=col).number_format = '#,##0.00'
        for col in [11, 12, 13]:
            ws.cell(row=row_num, column=col).number_format = '0.00'

    # Автоширина колонок
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 30)
        ws.column_dimensions[column].width = adjusted_width

    # Создаем ответ
    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response[
        'Content-Disposition'] = f'attachment; filename=bets_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'

    wb.save(response)
    return response

