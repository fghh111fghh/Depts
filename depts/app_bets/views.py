import re
import math
import unicodedata
from decimal import Decimal
from django.shortcuts import render
from django.views.generic import View
from django.db.models import F, Q
from django.utils import timezone
from .models import Match, TeamAlias, League, Season, Team


class AnalyzeView(View):
    template_name = 'app_bets/bets_main.html'

    def clean_team_name(self, name):
        """Очистка названия от мусора"""
        if not name: return ""
        name = unicodedata.normalize('NFKC', str(name))
        name = re.sub(r'\d{1,2}[:\.]\d{2}', '', name)
        name = re.sub(r'[^\w\s\d]', ' ', name)
        name = ' '.join(name.split())
        return name.strip().lower()

    def get_poisson_probs(self, l_home, l_away):
        probs = []
        try:
            l_home, l_away = float(l_home), float(l_away)
            if l_home <= 0: l_home = 0.01
            if l_away <= 0: l_away = 0.01
            for h in range(5):
                for a in range(5):
                    p_h = (math.exp(-l_home) * (l_home ** h)) / math.factorial(h)
                    p_a = (math.exp(-l_away) * (l_away ** a)) / math.factorial(a)
                    probs.append({'score': f"{h}:{a}", 'prob': p_h * p_a * 100})
        except:
            return []
        return sorted(probs, key=lambda x: x['prob'], reverse=True)[:5]

    def get_team_smart(self, name):
        search = self.clean_team_name(name)
        if not search: return None
        alias = TeamAlias.objects.filter(name__iexact=search).select_related('team').first()
        if alias: return alias.team
        return Team.objects.filter(Q(name__iexact=search) | Q(name__icontains=search)).first()

    def get_fast_form(self, team_id, date, matches_list, window=4):
        """Скоростное извлечение формы из списка матчей в памяти"""
        # Фильтруем матчи команды до конкретной даты
        past = [m for m in matches_list if (m.home_team_id == team_id or m.away_team_id == team_id) and m.date < date]
        # Берем последние window штук (они уже отсортированы по дате DESC в основном запросе)
        relevant = past[:window]

        if len(relevant) < window:
            return None

        form = []
        # Разворачиваем, чтобы идти от старых к новым (ППНВ)
        for m in reversed(relevant):
            is_home = (m.home_team_id == team_id)
            h, a = m.home_score_reg, m.away_score_reg
            if h == a:
                form.append('N')  # Ничья
            elif (is_home and h > a) or (not is_home and a > h):
                form.append('P')  # Победа
            else:
                form.append('V')  # Выигрыш (поражение в твоей терминологии В - Выигрыш соперника)
        return "".join(form)

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
                            m_obj = Match(home_team=home_team, away_team=away_team, league=league, season=season,
                                          odds_home=h_odd, odds_draw=d_odd, odds_away=a_odd, date=timezone.now())

                            # --- 1. ПУАССОН ---
                            p_data = m_obj.calculate_poisson_lambda()
                            top_scores = self.get_poisson_probs(p_data['home_lambda'], p_data['away_lambda'])

                            # --- 2. УМНЫЙ ПОИСК БЛИЗНЕЦОВ (ТВОЙ АЛГОРИТМ) ---
                            tol = Decimal('0.05')
                            twins_qs = Match.objects.filter(
                                league__country=league.country,
                                odds_home__range=(h_odd - tol, h_odd + tol),
                                odds_away__range=(a_odd - tol, a_odd + tol)
                            ).exclude(home_score_reg__isnull=True)

                            if twins_qs.count() == 0:
                                tol = Decimal('0.10')
                                twins_qs = Match.objects.filter(
                                    league__country=league.country,
                                    odds_home__range=(h_odd - tol, h_odd + tol),
                                    odds_away__range=(a_odd - tol, a_odd + tol)
                                ).exclude(home_score_reg__isnull=True)

                            t_count = twins_qs.count()
                            t_dist = "Нет данных"
                            if t_count > 0:
                                hw_t = twins_qs.filter(home_score_reg__gt=F('away_score_reg')).count()
                                dw_t = twins_qs.filter(home_score_reg=F('away_score_reg')).count()
                                aw_t = twins_qs.filter(home_score_reg__lt=F('away_score_reg')).count()
                                t_dist = f"П1: {round(hw_t / t_count * 100)}% | X: {round(dw_t / t_count * 100)}% | П2: {round(aw_t / t_count * 100)}%"

                            # --- 3. ИСТОРИЧЕСКИЙ ШАБЛОН (ППНВ) - СКОРОСТНОЙ ---
                            all_league_matches = list(Match.objects.filter(
                                league=league, home_score_reg__isnull=False
                            ).order_by('-date'))

                            h_form = self.get_fast_form(home_team.id, m_obj.date, all_league_matches)
                            a_form = self.get_fast_form(away_team.id, m_obj.date, all_league_matches)

                            pattern_res = "Недостаточно данных"
                            if h_form and a_form:
                                p_count, p_hw, p_dw, p_aw = 0, 0, 0, 0
                                for m in all_league_matches:
                                    f_h = self.get_fast_form(m.home_team_id, m.date, all_league_matches)
                                    f_a = self.get_fast_form(m.away_team_id, m.date, all_league_matches)
                                    if f_h == h_form and f_a == a_form:
                                        p_count += 1
                                        if m.home_score_reg > m.away_score_reg:
                                            p_hw += 1
                                        elif m.home_score_reg == m.away_score_reg:
                                            p_dw += 1
                                        else:
                                            p_aw += 1

                                if p_count > 0:
                                    pattern_res = {
                                        'pattern': f"{h_form} - {a_form}",
                                        'count': p_count,
                                        'dist': f"П1: {round(p_hw / p_count * 100)}% | X: {round(p_dw / p_count * 100)}% | П2: {round(p_aw / p_count * 100)}%"
                                    }

                            # --- 4. H2H ---
                            h2h_qs = Match.objects.filter(home_team=home_team, away_team=away_team).exclude(
                                home_score_reg__isnull=True).order_by('-date')
                            h2h_list = [
                                {'date': m.date.strftime('%d.%m.%y'), 'score': f"{m.home_score_reg}:{m.away_score_reg}"}
                                for m in h2h_qs]

                            results.append({
                                'match': f"{home_team.name} - {away_team.name}",
                                'league': league.name if league else "Unknown",
                                'poisson_l': f"{p_data['home_lambda']} : {p_data['away_lambda']}",
                                'poisson_top': top_scores,
                                'twins_count': t_count,
                                'twins_dist': t_dist,
                                'pattern_data': pattern_res,
                                'h2h_list': h2h_list,
                                'h2h_total': h2h_qs.count()
                            })
                        else:
                            if not home_team: unknown_teams.add(home_raw.strip())
                            if not away_team: unknown_teams.add(away_raw.strip())
                except Exception as e:
                    print(f"Ошибка парсинга: {e}")
                    continue

        return render(request, self.template_name, {
            'results': results, 'raw_text': raw_text, 'unknown_teams': sorted(list(unknown_teams)),
            'all_teams': Team.objects.all().order_by('name'),
        })

    def get(self, request):
        return render(request, self.template_name, {
            'all_teams': Team.objects.all().order_by('name'),
        })