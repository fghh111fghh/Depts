import csv
import os
import re
import math
from datetime import datetime

import unicodedata
from decimal import Decimal

from django.conf import settings
from django.shortcuts import render, redirect
from django.views.generic import View
from django.db.models import F
from django.utils import timezone

from .constants import Outcome
from .models import Match, TeamAlias, League, Season, Team


class AnalyzeView(View):
    template_name = 'app_bets/bets_main.html'

    @staticmethod
    def clean_team_name(name: str) -> str:
        """
        Очищает название команды от лишних символов,
        приводит к единому формату для сравнения.

        Args:
            name (str): Исходное название команды

        Returns:
            str: Очищенное название в нижнем регистре
        """
        if not name:
            return ""

        try:
            # Нормализация Unicode
            name = unicodedata.normalize('NFKC', str(name))

            # Удаление временных меток (15:30, 21.45)
            name = re.sub(r'\b\d{1,2}[:\.]\d{2}\b', '', name)

            # Удаление символов в скобках и спецсимволов
            name = re.sub(r'[^\w\s\d\-]', ' ', name)

            # Замена нескольких дефисов/тире на один пробел
            name = re.sub(r'[\-\–\—]+', ' ', name)

            # Удаление лишних пробелов
            name = ' '.join(name.split())

            return name.strip().lower()

        except Exception as e:
            # Логирование ошибки в продакшене
            # logger.warning(f"Error cleaning team name '{name}': {e}")
            return str(name).strip().lower() if name else ""

    @staticmethod
    def get_poisson_probs(l_home: int | float, l_away: int | float) -> list:
        """
        Рассчитывает вероятности различных счетов (например, 2:1, 0:0 и т.д.)
        между домашней (home) и гостевой (away) командами, используя
        распределение Пуассона. Метод возвращает 5 самых вероятных счетов
        """
        probs = []
        try:
            # l_home, l_away λ (лямбда) - среднее ожидаемое количество голов
            l_home, l_away = float(l_home), float(l_away)
            # Минимальное значение для избежания ошибок
            if l_home <= 0: l_home = 0.01
            if l_away <= 0: l_away = 0.01
            # Вероятность что домашняя команда забьет h голов
            for h in range(5):
                # Вероятность что гостевая команда забьет a голов
                for a in range(5):
                    p_h = (math.exp(-l_home) * (l_home ** h)) / math.factorial(h)
                    p_a = (math.exp(-l_away) * (l_away ** a)) / math.factorial(a)
                    # Вероятность конкретного счета (h:a)
                    probs.append({'score': f"{h}:{a}", 'prob': p_h * p_a * 100})
        except:
            return []
        # Пример расчета Пуассона для счета 1:0
        # P(home=1) = (e ^ (-1.5) * 1.5 ^ 1) / 1! ≈ 0.3347
        # P(away=0) = (e ^ (-1.0) * 1.0 ^ 0) / 0! ≈ 0.3679
        # P(1: 0) = 0.3347 * 0.3679 ≈ 0.1231 = 12.31 %
        return sorted(probs, key=lambda x: x['prob'], reverse=True)[:5]

    @staticmethod
    def parse_csv_date(d_str: str) -> datetime:
        """
        Преобразует строку с датой из CSV в объект datetime Django с учетом
        часового пояса, поддерживая несколько форматов дат
        """
        if not d_str:
            return timezone.now()
        # Поддерживает два формата
        # %d / % m / % Y — полный год(например: "25/12/2023")
        # %d / % m / % y — короткий год(например: "25/12/23")
        for fmt in ('%d/%m/%Y', '%d/%m/%y'):
            try:
                # Преобразование строки в datetime
                # "25/12/2023" → datetime(2023, 12, 25, 0, 0)
                dt = datetime.strptime(d_str, fmt)
                # Если USE_TZ = True(используются часовые пояса)
                # dt.date() — получает только дату(без времени)
                # datetime.min.time() — минимальное время 00: 00:00
                # datetime.combine() — объединяет дату и время
                # timezone.make_aware() — добавляет часовой пояс
                # Результат: aware datetime с временем 00: 00 в текущем часовом поясе
                if settings.USE_TZ:
                    return timezone.make_aware(datetime.combine(dt.date(), datetime.min.time()))
                return dt
            except (ValueError, TypeError):
                continue
        return timezone.now()

    def get_team_smart(self, name: str) -> Team | None:
        """
        Ищет команду по названию, сначала проверяя точное совпадение с основной
        таблицей команд, затем с таблицей алиасов (альтернативных названий/синонимов).
        """
        clean_name = self.clean_team_name(name)
        # 1. По точному имени
        team = Team.objects.filter(name__iexact=clean_name).first()
        if team: return team

        # 2. По алиасу
        alias = TeamAlias.objects.filter(name__iexact=clean_name).first()
        if alias: return alias.team
        return None


    def post(self, request):
        raw_text = request.POST.get('matches_text', '')

        if 'create_alias' in request.POST:
            alias_raw = request.POST.get('alias_name', '')
            t_id = request.POST.get('team_id')
            if alias_raw and t_id:
                try:
                    clean_n = self.clean_team_name(alias_raw)
                    TeamAlias.objects.update_or_create(name=clean_n, defaults={'team_id': t_id})
                except Exception as e:
                    print(f"Ошибка сохранения алиаса: {e}")

        results = []
        unknown_teams = set()
        season = Season.objects.filter(is_current=True).first() or Season.objects.order_by('-start_date').first()
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]

        skip_to = -1
        for i, line in enumerate(lines):
            if i <= skip_to: continue
            if re.match(r'^\d+[\.,]\d+$', line):
                try:
                    h_odd = Decimal(line.replace(',', '.')).quantize(Decimal('0.00'))
                    d_odd = Decimal(lines[i + 1].replace(',', '.')).quantize(Decimal('0.00'))
                    a_odd = Decimal(lines[i + 2].replace(',', '.')).quantize(Decimal('0.00'))
                    skip_to = i + 2

                    names = []
                    for j in range(i - 1, -1, -1):
                        row = lines[j].strip()
                        if not self.clean_team_name(row) or row in ['-', 'vs', 'x', '1', '2']: continue
                        names.append(row)
                        if len(names) == 2: break

                    if len(names) == 2:
                        away_raw, home_raw = names[0], names[1]
                        home_team = self.get_team_smart(home_raw)
                        away_team = self.get_team_smart(away_raw)

                        if home_team and away_team:
                            ref = Match.objects.filter(home_team=home_team).select_related('league__country').first()
                            league = ref.league if ref else League.objects.filter(country=home_team.country).first()

                            # --- ШАБЛОНЫ (ОПТИМИЗИРОВАНО) ---
                            all_league_matches = list(
                                Match.objects.filter(league=league, home_score_reg__isnull=False).order_by('date'))
                            team_history = {}
                            match_patterns = {}
                            for m in all_league_matches:
                                h_id, a_id = m.home_team_id, m.away_team_id
                                h_f = "".join(team_history.get(h_id, []))[-4:]
                                a_f = "".join(team_history.get(a_id, []))[-4:]
                                if len(h_f) == 4 and len(a_f) == 4: match_patterns[m.id] = (h_f, a_f)
                                res_h = Outcome.DRAW if m.home_score_reg == m.away_score_reg else (
                                    Outcome.WIN if m.home_score_reg > m.away_score_reg else Outcome.LOSE)
                                res_a = Outcome.DRAW if m.home_score_reg == m.away_score_reg else (
                                    Outcome.WIN if m.away_score_reg > m.home_score_reg else Outcome.LOSE)
                                team_history.setdefault(h_id, []).append(res_h)
                                team_history.setdefault(a_id, []).append(res_a)

                            curr_h_form = "".join(team_history.get(home_team.id, []))[-4:]
                            curr_a_form = "".join(team_history.get(away_team.id, []))[-4:]

                            pattern_res = "Недостаточно данных"
                            p_hw, p_dw, p_aw, p_count = 0, 0, 0, 0
                            if len(curr_h_form) == 4 and len(curr_a_form) == 4:
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
                                    pattern_res = {'pattern': f"{curr_h_form} - {curr_a_form}", 'count': p_count,
                                                   'dist': f"П1: {round(p_hw / p_count * 100)}% | X: {round(p_dw / p_count * 100)}% | П2: {round(p_aw / p_count * 100)}%"}

                            # --- ПУАССОН И БЛИЗНЕЦЫ ---
                            m_obj = Match(home_team=home_team, away_team=away_team, league=league, season=season,
                                          odds_home=h_odd)
                            p_data = m_obj.calculate_poisson_lambda()
                            top_scores = self.get_poisson_probs(p_data['home_lambda'], p_data['away_lambda'])

                            tol = Decimal('0.05')
                            twins_qs = Match.objects.filter(league__country=league.country,
                                                            odds_home__range=(h_odd - tol, h_odd + tol),
                                                            odds_away__range=(a_odd - tol, a_odd + tol)).exclude(
                                home_score_reg__isnull=True)
                            if twins_qs.count() == 0:
                                tol = Decimal('0.10')
                                twins_qs = Match.objects.filter(league__country=league.country,
                                                                odds_home__range=(h_odd - tol, h_odd + tol),
                                                                odds_away__range=(a_odd - tol, a_odd + tol)).exclude(
                                    home_score_reg__isnull=True)

                            t_count = twins_qs.count()
                            t_dist, hw_t, dw_t, aw_t = "Нет данных", 0, 0, 0
                            if t_count > 0:
                                hw_t = twins_qs.filter(home_score_reg__gt=F('away_score_reg')).count()
                                dw_t = twins_qs.filter(home_score_reg=F('away_score_reg')).count()
                                aw_t = twins_qs.filter(home_score_reg__lt=F('away_score_reg')).count()
                                t_dist = f"П1: {round(hw_t / t_count * 100)}% | X: {round(dw_t / t_count * 100)}% | П2: {round(aw_t / t_count * 100)}%"

                            h2h_qs = Match.objects.filter(home_team=home_team, away_team=away_team).exclude(
                                home_score_reg__isnull=True).order_by('-date')
                            h2h_list = [
                                {'date': m.date.strftime('%d.%m.%y'), 'score': f"{m.home_score_reg}:{m.away_score_reg}"}
                                for m in h2h_qs]

                            # --- ВЕКТОРНЫЙ СИНТЕЗ (ВЕСА) ---
                            v_p1, v_x, v_p2 = 0, 0, 0
                            if top_scores:
                                ms = top_scores[0]['score'].split(':')
                                if int(ms[0]) > int(ms[1]):
                                    v_p1 += 0.2
                                elif int(ms[0]) == int(ms[1]):
                                    v_x += 0.2
                                else:
                                    v_p2 += 0.2
                            if t_count > 0:
                                if hw_t / t_count > 0.45: v_p1 += 0.4
                                if dw_t / t_count > 0.30: v_x += 0.4
                                if aw_t / t_count > 0.45: v_p2 += 0.4
                            if p_count > 0:
                                if p_hw / p_count > 0.45: v_p1 += 0.4
                                if p_dw / p_count > 0.30: v_x += 0.4
                                if p_aw / p_count > 0.45: v_p2 += 0.4

                            if v_p1 >= 0.6:
                                verdict = "СИГНАЛ: П1"
                            elif v_p2 >= 0.6:
                                verdict = "СИГНАЛ: П2"
                            elif v_x >= 0.6:
                                verdict = "СИГНАЛ: НИЧЬЯ"
                            elif v_p1 >= 0.4:
                                verdict = "АКЦЕНТ: 1X"
                            elif v_p2 >= 0.4:
                                verdict = "АКЦЕНТ: X2"
                            else:
                                verdict = "НЕТ ЧЕТКОГО ВЕКТОРА"

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
                            if not home_team: unknown_teams.add(home_raw.strip())
                            if not away_team: unknown_teams.add(away_raw.strip())
                except Exception as e:
                    print(f"Error: {e}")
                    continue

        return render(request, self.template_name, {
            'results': results, 'raw_text': raw_text, 'unknown_teams': sorted(list(unknown_teams)),
            'all_teams': Team.objects.all().order_by('name'),
        })

    def get(self, request):
        return render(request, self.template_name, {'all_teams': Team.objects.all().order_by('name')})


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
