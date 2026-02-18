import math
from django.db import models
from django.core.validators import MinValueValidator
from django.core.exceptions import ValidationError
from django.db.models import Q, Avg, Sum, F
from decimal import Decimal
from django.utils.timezone import is_naive, make_aware, get_current_timezone
from app_bets.constants import AnalysisConstants


class Sport(models.Model):
    """
    Справочник видов спорта.
    Центральный узел логики: определяет, бывает ли ничья в основное время.
    """

    class Name(models.TextChoices):
        FOOTBALL = 'football', 'Футбол'
        HOCKEY = 'hockey', 'Хоккей'
        TENNIS = 'tennis', 'Теннис'
        VOLLEYBALL = 'volleyball', 'Волейбол'
        BASKETBALL = 'basketball', 'Баскетбол'

    name = models.CharField(
        max_length=20,
        choices=Name.choices,
        unique=True,
        verbose_name="Вид спорта"
    )
    has_draw = models.BooleanField(
        default=True,
        verbose_name="Ничья в линии",
        help_text="Определяет, принимает ли букмекер ставки на 'Х' в основное время."
    )

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        verbose_name = "Вид спорта"
        verbose_name_plural = "Виды спорта"

    def save(self, *args, **kwargs):
        # Автоматическая корректировка для видов спорта без ничьих
        if self.name in [self.Name.TENNIS, self.Name.VOLLEYBALL]:
            self.has_draw = False
        super().save(*args, **kwargs)

    def __str__(self):
        return self.get_name_display()


class Country(models.Model):
    """Страны для фильтрации лиг и поиска 'близнецов' по региону."""
    name = models.CharField(max_length=100, unique=True, verbose_name="Название страны")

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        verbose_name_plural = "Страны"

    def __str__(self):
        return self.name


class League(models.Model):
    """Лиги и чемпионаты (бывшие Tournament)."""
    name = models.CharField(max_length=150, verbose_name="Название лиги")
    sport = models.ForeignKey(Sport, on_delete=models.CASCADE, verbose_name="Вид спорта", related_name="sport_leagues")
    country = models.ForeignKey(Country, on_delete=models.CASCADE, verbose_name="Страна", related_name="country_leagues")
    external_id = models.CharField(
        max_length=10,
        blank=True,
        null=True,
        unique=True,
        verbose_name="Внешний код (например, E0)"
    )

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        unique_together = ('name', 'sport', 'country')
        verbose_name = "Лига"
        verbose_name_plural = "Лиги"

    def get_season_averages(self, season=None):
        """
        Вычисляет средний тотал лиги за сезон.
        Нужно для Пуассона (L_HGS и L_AGS).
        """
        if not season:
            season = Season.objects.filter(is_current=True).first()
        if not season: return None # Защита если нет сезонов

        matches = self.matches.filter(season=season, home_score_reg__isnull=False)
        stats = matches.aggregate(
            avg_home_goals=Avg('home_score_reg'),
            avg_away_goals=Avg('away_score_reg'),
            total_matches=models.Count('id')
        )
        return stats

    def get_draw_frequency(self, season=None):
        """
        Считает процент ничьих в лиге за весь сезон.
        Помогает понять, является ли лига 'ничейной' по своей природе.
        """
        if not season:
            season = Season.objects.filter(is_current=True).first()

        total_matches = self.matches.filter(season=season, home_score_reg__isnull=False).count()
        if total_matches == 0:
            return 0

        draws = self.matches.filter(
            season=season,
            home_score_reg=models.F('away_score_reg')
        ).count()

        return round((draws / total_matches) * 100, 2)

    def check_round_anomaly(self, round_number, season=None):
        """
        Вспомогательный метод: анализирует конкретный тур на предмет отклонения от средних по лиге.
        """
        # 1. Получаем средние по лиге за сезон
        league_avg = self.get_season_averages(season)
        if not league_avg: return "NORMAL"
        avg_total = (league_avg['avg_home_goals'] or 0) + (league_avg['avg_away_goals'] or 0)

        # 2. Получаем средние за конкретный тур
        round_matches = self.matches.filter(league=self, season=season, round_number=round_number)
        round_avg = round_matches.aggregate(Avg('home_score_reg'), Avg('away_score_reg'))
        round_total = (round_avg['home_score_reg__avg'] or 0) + (round_avg['away_score_reg__avg'] or 0)

        # 3. Сравниваем: если в туре забили на 30% больше/меньше, чем обычно — это аномалия
        if avg_total > 0:
            diff = (round_total / avg_total)
            if diff > 1.3: return "HIGH_ANOMALY"
            if diff < 0.7: return "LOW_ANOMALY"

        return "NORMAL"

    def __str__(self):
        return f"{self.name} ({self.country.name})"


class Team(models.Model):
    """Каноническая запись команды или игрока (Мастер-запись)."""
    name = models.CharField(max_length=100, verbose_name="Каноническое название")
    sport = models.ForeignKey(Sport, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, on_delete=models.CASCADE)

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        unique_together = ('name', 'sport', 'country')
        verbose_name = "Команда / Игрок"
        verbose_name_plural = "Команды и Игроки"

    def __str__(self):
        return self.name


class TeamAlias(models.Model):
    """
    Синонимы названий для парсинга (напр. 'Man City' и 'Манчестер Сити' -> ID одной команды).
    """
    name = models.CharField(max_length=150, unique=True, verbose_name="Вариант из источника")
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="aliases")

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        verbose_name = "Псевдоним команды"
        verbose_name_plural = "Псевдонимы команд"

    def save(self, *args, **kwargs):
        # Глубокая очистка строки: убираем лишние пробелы и в нижний регистр
        if self.name:
            self.name = " ".join(self.name.split()).lower()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} -> {self.team.name}"


class Season(models.Model):
    """
    Сезоны (напр. '2025/2026').
    Обеспечивает изоляцию данных для аналитики.
    """
    name = models.CharField(max_length=20, unique=True, verbose_name="Название сезона")
    is_current = models.BooleanField(default=False, verbose_name="Текущий сезон")
    start_date = models.DateField(verbose_name="Дата начала (включительно)")
    end_date = models.DateField(verbose_name="Дата окончания (включительно)")

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
        verbose_name = "Сезон"
        verbose_name_plural = "Сезоны"
        ordering = ['-start_date']

    def clean(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValidationError("Дата начала сезона не может быть позже даты окончания.")

        # Гарантируем, что только один сезон помечен как текущий (для автоматики)
        if self.is_current:
            Season.objects.filter(is_current=True).exclude(pk=self.pk).update(is_current=False)

    def __str__(self):
        return self.name


class Match(models.Model):
    """
    Центральная модель для хранения данных и проведения анализа.
    Реализует разделение счета (основное время/финал) и валидацию по видам спорта.
    """

    class FinishType(models.TextChoices):
        REGULAR = 'REG', 'Основное время'
        OVERTIME = 'OT', 'Овертайм'
        PENALTIES = 'SO', 'Буллиты/Пенальти'

    season = models.ForeignKey(
        Season,
        on_delete=models.CASCADE,
        related_name="matches",
        verbose_name="Сезон",
        null=True, blank=True  # Null=True чтобы автоматика могла сработать в clean
    )
    league = models.ForeignKey(League, on_delete=models.CASCADE, related_name="matches")
    date = models.DateTimeField(verbose_name="Дата и время")
    round_number = models.PositiveIntegerField(null=True, blank=True, verbose_name="Тур")

    home_team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="home_matches")
    away_team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="away_matches")

    # Счета (PositiveIntegerField гарантирует значения >= 0)
    home_score_reg = models.PositiveIntegerField(null=True, blank=True, verbose_name="Осн. время (Дома)")
    away_score_reg = models.PositiveIntegerField(null=True, blank=True, verbose_name="Осн. время (Гости)")

    home_score_final = models.PositiveIntegerField(null=True, blank=True, verbose_name="Итоговый (Дома)")
    away_score_final = models.PositiveIntegerField(null=True, blank=True, verbose_name="Итоговый (Гости)")

    finish_type = models.CharField(
        max_length=3,
        choices=FinishType.choices,
        default=FinishType.REGULAR,
        verbose_name="Завершение игры"
    )

    # Коэффициенты (на ОСНОВНОЕ время)
    odds_home = models.DecimalField(
        max_digits=6, decimal_places=2,
        validators=[MinValueValidator(Decimal('1.01'))],
        verbose_name="Кэф П1"
    )
    odds_draw = models.DecimalField(
        max_digits=6, decimal_places=2,
        null=True, blank=True,
        validators=[MinValueValidator(Decimal('1.01'))],
        verbose_name="Кэф Х"
    )
    odds_away = models.DecimalField(
        max_digits=6, decimal_places=2,
        validators=[MinValueValidator(Decimal('1.01'))],
        verbose_name="Кэф П2"
    )

    # Технические поля для анализа
    is_anomaly = models.BooleanField(default=False, verbose_name="Аномальный результат")
    home_lineup = models.JSONField(null=True, blank=True, verbose_name="Состав (Дома)")
    away_lineup = models.JSONField(null=True, blank=True, verbose_name="Состав (Гости)")

    class Meta:
        indexes = [
            models.Index(fields=["odds_home", "odds_away"]),
        ]
        verbose_name = "Матч"
        verbose_name_plural = "Матчи"
        ordering = ['-date']

    # --- МАКСИМАЛЬНАЯ ВАЛИДАЦИЯ ---

    def __str__(self):
        return (f'{self.date} {self.home_team} - {self.away_team} {self.home_score_final}'
                f':{self.away_score_final}')

    def clean(self):
        """Логическая проверка данных перед сохранением в БД."""
        # 1. АВТОМАТИКА СЕЗОНА: Если сезон не указан, ищем активный по дате матча
        if not self.season:
            active_season = Season.objects.filter(
                start_date__lte=self.date.date(),
                end_date__gte=self.date.date()
            ).first()

            if not active_season:
                # Если по дате не нашли, берем тот, что помечен is_current=True
                active_season = Season.objects.filter(is_current=True).first()

            if active_season:
                self.season = active_season
            else:
                raise ValidationError("Не удалось определить сезон. Создайте сезон или укажите его вручную.")

        # 1. Проверка принадлежности к спорту
        if self.home_team.sport != self.league.sport or self.away_team.sport != self.league.sport:
            raise ValidationError("Команды должны принадлежать тому же виду спорта, что и лига.")
        # 2. ВАЛИДАЦИЯ ДАТЫ: Проверяем, попадает ли матч в период указанного сезона
        if self.season and (self.date.date() < self.season.start_date or self.date.date() > self.season.end_date):
            raise ValidationError(
                f"Дата матча ({self.date.date()}) не входит в диапазон сезона {self.season.name}.")
        # 2. Нельзя играть против самого себя
        if self.home_team == self.away_team:
            raise ValidationError("Команда не может играть сама с собой.")

        # 3. Валидация ничьих и исходов
        sport_has_draw = self.league.sport.has_draw

        if not sport_has_draw:
            if self.odds_draw is not None:
                raise ValidationError(f"В виде спорта '{self.league.sport}' рынок ничьих отсутствует.")
            if self.home_score_reg is not None and self.home_score_reg == self.away_score_reg:
                raise ValidationError(f"В виде спорта '{self.league.sport}' не может быть ничейного счета.")

        # 4. Валидация Овертаймов / Буллитов
        if self.home_score_reg is not None and self.away_score_reg is not None:
            if self.finish_type == self.FinishType.REGULAR:
                # Если игра в осн. время, счета обязаны совпадать
                if self.home_score_reg != self.home_score_final or self.away_score_reg != self.away_score_final:
                    raise ValidationError(
                        "В основное время итоговый счет должен совпадать со счетом основного времени.")
            else:
                # В хоккее/баскетболе если был ОТ, в основное время должна быть ничья
                if self.home_score_reg != self.away_score_reg:
                    raise ValidationError("Для овертайма или буллитов счет основного времени должен быть ничейным.")
                # После ОТ/Буллитов итоговой ничьей быть не может
                if self.home_score_final == self.away_score_final:
                    raise ValidationError("Итоговый счет после овертайма/буллитов не может быть ничейным.")

    def save(self, *args, **kwargs):
        # Если дата без часового пояса (naive)
        if is_naive(self.date):
            # Добавляем часовой пояс из настроек Django (Europe/Moscow)
            self.date = make_aware(self.date, get_current_timezone())
        # Если дата уже с часовым поясом (aware) - оставляем как есть
        self.full_clean()  # Принудительная валидация при любом способе сохранения
        super().save(*args, **kwargs)

    # --- МЕТОДЫ ТВОЕГО АЛГОРИТМА ---

    def get_twins(self, tolerance=Decimal('0.05')):
        """
        Поиск матчей-'близнецов' только по коэффициентам П1 и П2.
        """
        from decimal import Decimal

        # --- КРИТИЧЕСКАЯ ЗАЩИТА ---
        if not self.league_id:
            return Match.objects.none()
        if not self.league or not self.league.country_id:
            return Match.objects.none()
        if not self.odds_home or not self.odds_away:
            return Match.objects.none()

        try:
            # Гарантируем точность Decimal
            h_odd = Decimal(str(self.odds_home))
            a_odd = Decimal(str(self.odds_away))

            def perform_search(tol):
                # Ищем только по П1 и П2 в пределах допуска
                qs = Match.objects.filter(
                    league__country_id=self.league.country_id,
                    odds_home__range=(h_odd - tol, h_odd + tol),
                    odds_away__range=(a_odd - tol, a_odd + tol)
                )

                if self.id:
                    qs = qs.exclude(id=self.id)

                return qs.select_related('home_team', 'away_team', 'league').order_by('-date')

            # 1. Сначала ищем по стандартному допуску 0.05
            results = perform_search(tolerance)

            # 2. Если совсем ничего нет, расширяем до 0.10
            if not results.exists():
                results = perform_search(Decimal('0.10'))

            return results

        except Exception as e:
            print(f"Error in get_twins: {e}")
            return Match.objects.none()

    def get_h2h(self, limit=10):
        """История личных встреч (Head-to-Head)."""
        return Match.objects.filter(
            (Q(home_team=self.home_team) & Q(away_team=self.away_team)) |
            (Q(home_team=self.away_team) & Q(away_team=self.home_team)),
            date__lt=self.date,
            home_score_reg__isnull=False
        ).order_by('-date')[:limit]

    # --- МЕТОДЫ АНАЛИЗА АНОМАЛИЙ (КОРРЕКЦИЯ К СРЕДНЕМУ) ---

    def get_league_trends(self, window=3):
        """
        Анализирует последние N туров лиги перед текущим матчем.
        Возвращает статистику по 'верхам/низам' и количеству ничьих.
        """
        if not self.round_number or self.round_number <= window:
            return None

        # Берем матчи этой лиги за прошлые N туров
        past_rounds_matches = Match.objects.filter(
            league=self.league,
            season=self.season,
            round_number__range=(self.round_number - window, self.round_number - 1),
            home_score_reg__isnull=False
        )

        total_matches = past_rounds_matches.count()
        if total_matches == 0:
            return None

        # 1. Анализ ТБ 2.5 (Верх/Низ)
        # Суммируем голы в каждом матче и считаем количество игр с ТБ 2.5
        over_25_count = 0
        draw_count = 0

        for m in past_rounds_matches:
            if (m.home_score_reg + m.away_score_reg) > 2.5:
                over_25_count += 1
            if m.home_score_reg == m.away_score_reg:
                draw_count += 1

        over_percentage = (over_25_count / total_matches) * 100
        draw_percentage = (draw_count / total_matches) * 100

        return {
            'total_matches': total_matches,
            'over_25_percent': round(over_percentage, 2),
            'draw_percent': round(draw_percentage, 2),
            'is_high_anomaly': over_percentage > 70,  # Условный порог аномалии "Верх"
            'is_low_anomaly': over_percentage < 30,  # Условный порог аномалии "Низ"
            'is_draw_anomaly': draw_percentage > 40,  # Слишком много ничьих
        }

    def get_correction_vector(self):
        """
        Синтезирует финальный сигнал коррекции.
        Если прошлые туры были аномальными, возвращает направление 'отката'.
        """
        trends = self.get_league_trends(window=3)
        if not trends:
            return "Статистики недостаточно"

        corrections = []

        if trends['is_high_anomaly']:
            corrections.append("ОЖИДАЕТСЯ НИЗ (после серии верховых туров)")
        elif trends['is_low_anomaly']:
            corrections.append("ОЖИДАЕТСЯ ВЕРХ (после серии низовых туров)")

        if trends['is_draw_anomaly']:
            corrections.append("НИЧЬЯ МАЛОВЕРОЯТНА (коррекция после избытка ничьих)")

        return corrections if corrections else "В пределах нормы"

    def calculate_poisson_lambda(self):
        """
        Рассчитывает ожидаемое кол-во голов (лямбда) для хозяев и гостей.
        Условие: расчет только если у каждой команды >= 3 игр в сезоне.
        """
        try:
            # 1. Получаем данные лиги за сезон
            league_stats = self.league.get_season_averages(self.season)
            if not league_stats or league_stats['total_matches'] == 0:
                return {
                    'home_lambda': 1.2,
                    'away_lambda': 1.0,
                    'error': 'Нет статистики лиги'
                }

            # Средние показатели лиги
            l_avg_home_goals = Decimal(str(league_stats['avg_home_goals'] or 1.2))
            l_avg_away_goals = Decimal(str(league_stats['avg_away_goals'] or 1.0))
            l_avg_home_conceded = l_avg_away_goals
            l_avg_away_conceded = l_avg_home_goals

            # 2. Собираем статистику Хозяев
            home_team_matches = Match.objects.filter(
                league=self.league,
                season=self.season,
                home_team=self.home_team,
                home_score_reg__isnull=False
            )

            # 3. Собираем статистику Гостей
            away_team_matches = Match.objects.filter(
                league=self.league,
                season=self.season,
                away_team=self.away_team,
                away_score_reg__isnull=False
            )

            # Проверка на минимальное количество игр
            if home_team_matches.count() < 3 or away_team_matches.count() < 3:
                # Возвращаем дефолтные значения вместо строки!
                return {
                    'home_lambda': 1.2,
                    'away_lambda': 1.0,
                    'error': f'Недостаточно данных: хозяева {home_team_matches.count()}, гости {away_team_matches.count()}'
                }

            # Агрегация
            h_agg = home_team_matches.aggregate(s=Sum('home_score_reg'), c=Sum('away_score_reg'))
            a_agg = away_team_matches.aggregate(s=Sum('away_score_reg'), c=Sum('home_score_reg'))

            # Статистика Хозяев дома
            h_avg_scored = Decimal(str(h_agg['s'] or 0)) / home_team_matches.count()
            h_avg_conceded = Decimal(str(h_agg['c'] or 0)) / home_team_matches.count()

            # Статистика Гостей в гостях
            a_avg_scored = Decimal(str(a_agg['s'] or 0)) / away_team_matches.count()
            a_avg_conceded = Decimal(str(a_agg['c'] or 0)) / away_team_matches.count()

            # Защита от нулевых значений
            h_avg_scored = max(h_avg_scored, Decimal('0.5'))
            h_avg_conceded = max(h_avg_conceded, Decimal('0.5'))
            a_avg_scored = max(a_avg_scored, Decimal('0.5'))
            a_avg_conceded = max(a_avg_conceded, Decimal('0.5'))
            l_avg_home_goals = max(l_avg_home_goals, Decimal('1.0'))
            l_avg_away_goals = max(l_avg_away_goals, Decimal('0.8'))
            l_avg_home_conceded = max(l_avg_home_conceded, Decimal('1.0'))
            l_avg_away_conceded = max(l_avg_away_conceded, Decimal('0.8'))

            # 4. РАСЧЕТ СИЛЫ (АТАКА / ОБОРОНА)
            h_attack_strength = h_avg_scored / l_avg_home_goals
            a_defense_strength = a_avg_conceded / l_avg_away_conceded
            a_attack_strength = a_avg_scored / l_avg_away_goals
            h_defense_strength = h_avg_conceded / l_avg_home_conceded

            # 5. ИТОГОВАЯ ВЕРОЯТНОСТЬ ГОЛОВ (LYAMBDA)
            lambda_home = h_attack_strength * a_defense_strength * l_avg_home_goals
            lambda_away = a_attack_strength * h_defense_strength * l_avg_away_goals

            # Нормализация (не даем уйти в крайности)
            lambda_home = max(min(float(lambda_home), 3.5), 0.5)
            lambda_away = max(min(float(lambda_away), 3.0), 0.3)

            return {
                'home_lambda': round(lambda_home, 2),
                'away_lambda': round(lambda_away, 2)
            }

        except Exception as e:
            # logger.error(f"Ошибка расчета лямбда Пуассона: {e}")
            # Всегда возвращаем словарь, никогда строку!
            return {
                'home_lambda': 1.2,
                'away_lambda': 1.0,
                'error': str(e)
            }

    def get_poisson_probabilities(self, max_goals=5):
        """
        Рассчитывает сетку вероятностей счета на основе лямбд.
        """
        lambdas = self.calculate_poisson_lambda()

        # Всегда проверяем, что это словарь и содержит нужные ключи
        if not isinstance(lambdas, dict) or 'home_lambda' not in lambdas or 'away_lambda' not in lambdas:
            return {}  # Возвращаем пустой словарь вместо строки

        l_home = lambdas['home_lambda']
        l_away = lambdas['away_lambda']

        def poisson_prob(l, x):
            if x > 10:  # Защита от больших факториалов
                return 0
            try:
                return (math.exp(-l) * (l ** x)) / math.factorial(x)
            except (OverflowError, ValueError):
                return 0

        prob_matrix = {}
        total_prob = 0

        for h in range(max_goals + 1):
            for a in range(max_goals + 1):
                prob = poisson_prob(l_home, h) * poisson_prob(l_away, a)
                prob_matrix[f"{h}:{a}"] = round(prob * 100, 2)
                total_prob += prob

        total_prob = sum(prob_matrix.values())
        if total_prob > 0:
            for score in prob_matrix:
                prob_matrix[score] = round((prob_matrix[score] / total_prob) * 100, 2)

        return prob_matrix


    def get_historical_pattern_report(self, window=4):
        """
        Метод 'Исторический шаблон' с выводом истории игр.
        """
        from django.db.models import Q

        def get_team_form_string(team, date, season):
            past_matches = Match.objects.filter(
                (Q(home_team=team) | Q(away_team=team)),
                date__lt=date,
                season=season,
                home_score_reg__isnull=False
            ).order_by('-date')[:window]

            if past_matches.count() < window:
                return None

            form = []
            for m in reversed(list(past_matches)):
                is_home = (m.home_team == team)
                h_score = m.home_score_reg
                a_score = m.away_score_reg
                if h_score == a_score:
                    form.append('D')
                elif (is_home and h_score > a_score) or (not is_home and a_score > h_score):
                    form.append('W')
                else:
                    form.append('L')
            return "".join(form)

        home_form = get_team_form_string(self.home_team, self.date, self.season)
        away_form = get_team_form_string(self.away_team, self.date, self.season)

        if not home_form or not away_form:
            return "Недостаточно данных (нужно минимум по 4 игры в сезоне)"

        # Поиск матчей в этой же лиге (история)
        all_historical_matches = Match.objects.filter(
            league=self.league,
            date__lt=self.date,
            home_score_reg__isnull=False
        ).select_related('home_team', 'away_team', 'season').order_by('-date')

        matches_found = []
        for h_match in all_historical_matches:
            h_h_form = get_team_form_string(h_match.home_team, h_match.date, h_match.season)
            h_a_form = get_team_form_string(h_match.away_team, h_match.date, h_match.season)

            if h_h_form == home_form and h_a_form == away_form:
                matches_found.append(h_match)

        if not matches_found:
            return f"Шаблон [{home_form} vs {away_form}] не встречался."

        total = len(matches_found)
        h_wins = sum(1 for m in matches_found if m.home_score_reg > m.away_score_reg)
        draws = sum(1 for m in matches_found if m.home_score_reg == m.away_score_reg)
        a_wins = sum(1 for m in matches_found if m.home_score_reg < m.away_score_reg)

        return {
            'pattern': f"{home_form} vs {away_form}",
            'matches_count': total,
            'outcomes': {
                'P1': round(h_wins/total*100, 1),
                'X': round(draws/total*100, 1),
                'P2': round(a_wins/total*100, 1),
            },
            'avg_goals': round(sum(m.home_score_reg + m.away_score_reg for m in matches_found) / total, 2),
            'history': [f"{m.date.strftime('%d.%m.%Y')}: {m.home_team.name} {m.home_score_reg}:{m.away_score_reg} {m.away_team.name}"
                        for m in matches_found]
        }

    def get_vector_synthesis(self):
        """
        Синтез всех методов: Пуассон, Близнецы, Шаблоны, H2H.
        """
        score_poisson = self.get_poisson_probabilities()
        twins = self.get_twins()
        pattern = self.get_historical_pattern_report()

        signals = []

        # 1. Анализ Пуассона - проверяем что это словарь и не пустой
        if isinstance(score_poisson, dict) and score_poisson:
            top_score = max(score_poisson, key=score_poisson.get)
            if top_score:
                signals.append(f"Пуассон: {top_score} ({score_poisson[top_score]}%)")

        # 2. Анализ Близнецов
        if twins.exists():
            t_count = twins.count()
            h_wins = twins.filter(home_score_reg__gt=F('away_score_reg')).count()
            draws = twins.filter(home_score_reg=F('away_score_reg')).count()
            a_wins = twins.filter(home_score_reg__lt=F('away_score_reg')).count()

            if (h_wins / t_count) > 0.6:
                signals.append(f"Близнецы: П1 {round(h_wins / t_count * 100)}%")
            elif (a_wins / t_count) > 0.6:
                signals.append(f"Близнецы: П2 {round(a_wins / t_count * 100)}%")
            elif (draws / t_count) > 0.4:
                signals.append(f"Близнецы: X {round(draws / t_count * 100)}%")

        # 3. Анализ Шаблона
        if isinstance(pattern, dict):
            if pattern.get('outcomes', {}).get('P1', 0) >= 70:
                signals.append(f"Шаблон: П1 {pattern['outcomes']['P1']}%")
            elif pattern.get('outcomes', {}).get('P2', 0) >= 70:
                signals.append(f"Шаблон: П2 {pattern['outcomes']['P2']}%")

        # Итоговый вердикт
        if not signals:
            return "Недостаточно данных для уверенного прогноза."

        return " | ".join(signals)

    def get_historical_total_insight(self):
        """
        ИСТОРИЧЕСКИЙ АНАЛИЗ ТОТАЛА С БАЙЕСОВСКОЙ ВЕРОЯТНОСТЬЮ.
        """
        result = {
            'bayesian': None,
            'h2h': None,
            'twins_context': None,
            'trend': None,
            'synthetic': None
        }

        # --- ТОЛЬКИ САМЫЕ НЕОБХОДИМЫЕ ПРОВЕРКИ ---
        if not self.league_id or not self.season_id:
            return result
        if not self.home_team_id or not self.away_team_id:
            return result

        try:
            # --- АПРИОРНАЯ ВЕРОЯТНОСТЬ: ВСЯ ИСТОРИЯ ЛИГИ ---
            all_matches = Match.objects.filter(
                league_id=self.league_id,
                home_score_reg__isnull=False,
                away_score_reg__isnull=False
            )

            # Фильтр по HISTORICAL_YEARS через Season
            if self.season and self.season.start_date:
                start_year = self.season.start_date.year
                cutoff_year = start_year - AnalysisConstants.HISTORICAL_YEARS

                seasons_history = Season.objects.filter(
                    start_date__year__gte=cutoff_year,
                    start_date__year__lte=start_year
                ).values_list('id', flat=True)

                if seasons_history:
                    all_matches = all_matches.filter(season_id__in=list(seasons_history))

            total_matches = all_matches.count()
            if total_matches < AnalysisConstants.HISTORICAL_MIN_MATCHES:
                return result

            over_25_total = 0
            goals_sum = 0

            for match in all_matches.iterator(chunk_size=1000):
                total = match.home_score_reg + match.away_score_reg
                goals_sum += total
                if total > AnalysisConstants.TOTAL_THRESHOLD:
                    over_25_total += 1

            prior_prob = over_25_total / total_matches
            league_avg = goals_sum / total_matches

            # --- 1. БАЙЕСОВСКИЙ АНАЛИЗ ---
            recent_base = Match.objects.filter(
                league_id=self.league_id,
                home_score_reg__isnull=False,
                away_score_reg__isnull=False
            )

            if seasons_history:
                recent_base = recent_base.filter(season_id__in=list(seasons_history))

            recent_matches = list(recent_base.order_by('-date')[:AnalysisConstants.HISTORICAL_MAX_SAMPLE])

            weighted_over = 0.0
            weighted_total = 0.0

            for match in recent_matches:
                similarity = 0

                # ТЕ ЖЕ КОМАНДЫ (50 баллов)
                if match.home_team_id == self.home_team_id and match.away_team_id == self.away_team_id:
                    similarity += 50
                elif match.home_team_id == self.away_team_id and match.away_team_id == self.home_team_id:
                    similarity += 45

                # ПОХОЖИЕ КОЭФФИЦИЕНТЫ (30 баллов)
                if self.odds_home and match.odds_home:
                    try:
                        diff_home = abs(float(match.odds_home) - float(self.odds_home))
                        if diff_home < 0.1:
                            similarity += 15
                        elif diff_home < 0.2:
                            similarity += 10
                        elif diff_home < 0.3:
                            similarity += 5
                    except (TypeError, ValueError):
                        pass

                if self.odds_away and match.odds_away:
                    try:
                        diff_away = abs(float(match.odds_away) - float(self.odds_away))
                        if diff_away < 0.1:
                            similarity += 15
                        elif diff_away < 0.2:
                            similarity += 10
                        elif diff_away < 0.3:
                            similarity += 5
                    except (TypeError, ValueError):
                        pass

                # ТОТ ЖЕ ХОЗЯИН (10 баллов)
                if match.home_team_id == self.home_team_id:
                    similarity += 10

                # ТОТ ЖЕ ГОСТЬ (10 баллов)
                if match.away_team_id == self.away_team_id:
                    similarity += 10

                # ПОХОЖИЙ ТУР (10 баллов)
                if match.round_number and self.round_number:
                    round_diff = abs(match.round_number - self.round_number)
                    if round_diff <= 2:
                        similarity += 10
                    elif round_diff <= 5:
                        similarity += 5

                # СВЕЖЕСТЬ ДАННЫХ (10 баллов) - пропускаем если нет даты
                if self.date and match.date:
                    days_diff = (self.date - match.date).days
                    if days_diff < 365:
                        similarity += 10
                    elif days_diff < 730:
                        similarity += 5
                    elif days_diff < 1095:
                        similarity += 2

                if similarity >= AnalysisConstants.HISTORICAL_SIMILARITY_THRESHOLD:
                    weight = similarity / 100.0
                    weighted_total += weight
                    match_total = match.home_score_reg + match.away_score_reg
                    if match_total > AnalysisConstants.TOTAL_THRESHOLD:
                        weighted_over += weight

            if weighted_total >= 1.0:
                empirical_prob = weighted_over / weighted_total
                strength = min(weighted_total / AnalysisConstants.HISTORICAL_WEIGHT_CAP, 1.0)
                bayesian_prob = prior_prob * (1 - strength) + empirical_prob * strength

                result['bayesian'] = {
                    'over_25': round(bayesian_prob * 100, 1),
                    'under_25': round((1 - bayesian_prob) * 100, 1),
                    'weight': round(weighted_total, 1),
                    'analogs': sum(1 for m in recent_matches
                                   if m.home_team_id == self.home_team_id and m.away_team_id == self.away_team_id),
                    'method': f'Байес (история {AnalysisConstants.HISTORICAL_YEARS} лет)'
                }

            # --- 2. ЛИЧНЫЕ ВСТРЕЧИ (H2H) ---
            try:
                h2h = self.get_h2h(limit=10)
                if h2h and h2h.exists():
                    count = h2h.count()
                    if count >= 2:
                        over_25 = 0
                        total_goals = 0

                        for match in h2h:
                            match_total = match.home_score_reg + match.away_score_reg
                            total_goals += match_total
                            if match_total > AnalysisConstants.TOTAL_THRESHOLD:
                                over_25 += 1

                        result['h2h'] = {
                            'over_25': round(over_25 / count * 100, 1),
                            'under_25': round((count - over_25) / count * 100, 1),
                            'avg_goals': round(total_goals / count, 2),
                            'count': count,
                            'method': 'Личные встречи (H2H)'
                        }
            except Exception:
                pass

            # --- 3. БЛИЗНЕЦЫ В КОНТЕКСТЕ ---
            try:
                twins = self.get_twins(tolerance=Decimal('0.10'))
                if twins and twins.exists():
                    count = twins.count()
                    if count >= 3:
                        over_25 = 0
                        total_goals = 0

                        for match in twins:
                            match_total = match.home_score_reg + match.away_score_reg
                            total_goals += match_total
                            if match_total > AnalysisConstants.TOTAL_THRESHOLD:
                                over_25 += 1

                        result['twins_context'] = {
                            'over_25': round(over_25 / count * 100, 1),
                            'under_25': round((count - over_25) / count * 100, 1),
                            'avg_goals': round(total_goals / count, 2),
                            'count': count,
                            'method': 'Близнецы (похожие коэффициенты)'
                        }
            except Exception:
                pass

            # --- 4. ТРЕНДЫ ФОРМЫ ---
            try:
                home_recent = Match.objects.filter(
                    league_id=self.league_id,
                    home_team_id=self.home_team_id,
                    home_score_reg__isnull=False,
                    date__lt=self.date
                ).order_by('-date')[:5] if self.date else Match.objects.none()

                away_recent = Match.objects.filter(
                    league_id=self.league_id,
                    away_team_id=self.away_team_id,
                    away_score_reg__isnull=False,
                    date__lt=self.date
                ).order_by('-date')[:5] if self.date else Match.objects.none()

                if home_recent.count() >= 3 and away_recent.count() >= 3:
                    home_over = 0
                    home_total_goals = 0
                    for m in home_recent:
                        mt = m.home_score_reg + m.away_score_reg
                        home_total_goals += mt
                        if mt > AnalysisConstants.TOTAL_THRESHOLD:
                            home_over += 1

                    away_over = 0
                    away_total_goals = 0
                    for m in away_recent:
                        mt = m.home_score_reg + m.away_score_reg
                        away_total_goals += mt
                        if mt > AnalysisConstants.TOTAL_THRESHOLD:
                            away_over += 1

                    result['trend'] = {
                        'home_over_25': round(home_over / home_recent.count() * 100, 1),
                        'home_avg_goals': round(home_total_goals / home_recent.count(), 2),
                        'away_over_25': round(away_over / away_recent.count() * 100, 1),
                        'away_avg_goals': round(away_total_goals / away_recent.count(), 2),
                        'method': 'Тренды формы (последние 5 матчей)'
                    }
            except Exception:
                pass

            # --- 5. СИНТЕЗ ---
            probs = []
            weights = []

            if result.get('bayesian'):
                probs.append(result['bayesian']['over_25'])
                weights.append(0.45)

            if result.get('h2h'):
                probs.append(result['h2h']['over_25'])
                weights.append(0.25)

            if result.get('twins_context'):
                probs.append(result['twins_context']['over_25'])
                weights.append(0.20)

            if result.get('trend'):
                trend_avg = (result['trend']['home_over_25'] + result['trend']['away_over_25']) / 2
                probs.append(trend_avg)
                weights.append(0.10)

            if probs:
                final_prob = sum(p * w for p, w in zip(probs, weights)) / sum(weights)

                # ИСПРАВЛЕННАЯ ЛОГИКА С КОНСТАНТАМИ
                if final_prob <= 35:
                    confidence = AnalysisConstants.CONFIDENCE_HIGH
                    prediction = AnalysisConstants.PREDICTION_UNDER
                elif final_prob <= 40:
                    confidence = AnalysisConstants.CONFIDENCE_MEDIUM
                    prediction = AnalysisConstants.PREDICTION_UNDER
                elif final_prob <= 45:
                    confidence = AnalysisConstants.CONFIDENCE_LOW
                    prediction = AnalysisConstants.PREDICTION_UNDER
                elif final_prob <= 55:
                    confidence = AnalysisConstants.CONFIDENCE_RANDOM
                    prediction = AnalysisConstants.PREDICTION_FIFTY
                elif final_prob <= 60:
                    confidence = AnalysisConstants.CONFIDENCE_LOW
                    prediction = AnalysisConstants.PREDICTION_OVER
                elif final_prob <= 65:
                    confidence = AnalysisConstants.CONFIDENCE_MEDIUM
                    prediction = AnalysisConstants.PREDICTION_OVER
                else:
                    confidence = AnalysisConstants.CONFIDENCE_HIGH
                    prediction = AnalysisConstants.PREDICTION_OVER

                result['synthetic'] = {
                    'over_25': round(final_prob, 1),
                    'under_25': round(100 - final_prob, 1),
                    'prediction': f"{prediction} {AnalysisConstants.TOTAL_THRESHOLD}" if prediction != AnalysisConstants.PREDICTION_FIFTY else prediction,
                    'confidence': confidence,
                    'methods': len(probs)
                }

        except Exception as e:
            print(f"Error in get_historical_total_insight: {e}")

        return result

    def calculate_poisson_lambda_last_n(self, n=10):
        try:
            league_stats = self.league.get_season_averages(self.season)
            if not league_stats or league_stats['total_matches'] == 0:
                return {'home_lambda': 1.2, 'away_lambda': 1.0, 'error': 'Нет статистики лиги'}

            l_avg_home_goals = Decimal(str(league_stats['avg_home_goals'] or 1.2))
            l_avg_away_goals = Decimal(str(league_stats['avg_away_goals'] or 1.0))
            l_avg_home_conceded = l_avg_away_goals
            l_avg_away_conceded = l_avg_home_goals

            # Последние N домашних матчей хозяев в сезоне (независимо от даты, но они уже сыграны)
            home_matches = Match.objects.filter(
                league=self.league,
                season=self.season,
                home_team=self.home_team,
                home_score_reg__isnull=False,
                away_score_reg__isnull=False
            ).order_by('-date')[:n]

            home_count = home_matches.count()
            if home_count < 3:
                home_attack = Decimal('1.0')
                home_defense = Decimal('1.0')
            else:
                h_agg = home_matches.aggregate(s=Sum('home_score_reg'), c=Sum('away_score_reg'))
                h_avg_scored = Decimal(str(h_agg['s'] or 0)) / home_count
                h_avg_conceded = Decimal(str(h_agg['c'] or 0)) / home_count
                h_avg_scored = max(h_avg_scored, Decimal('0.5'))
                h_avg_conceded = max(h_avg_conceded, Decimal('0.5'))
                home_attack = h_avg_scored / l_avg_home_goals
                home_defense = h_avg_conceded / l_avg_home_conceded

            # Последние N гостевых матчей гостей в сезоне
            away_matches = Match.objects.filter(
                league=self.league,
                season=self.season,
                away_team=self.away_team,
                home_score_reg__isnull=False,
                away_score_reg__isnull=False
            ).order_by('-date')[:n]

            away_count = away_matches.count()
            if away_count < 3:
                away_attack = Decimal('1.0')
                away_defense = Decimal('1.0')
            else:
                a_agg = away_matches.aggregate(s=Sum('away_score_reg'), c=Sum('home_score_reg'))
                a_avg_scored = Decimal(str(a_agg['s'] or 0)) / away_count
                a_avg_conceded = Decimal(str(a_agg['c'] or 0)) / away_count
                a_avg_scored = max(a_avg_scored, Decimal('0.3'))
                a_avg_conceded = max(a_avg_conceded, Decimal('0.5'))
                away_attack = a_avg_scored / l_avg_away_goals
                away_defense = a_avg_conceded / l_avg_away_conceded

            lambda_home = home_attack * away_defense * l_avg_home_goals
            lambda_away = away_attack * home_defense * l_avg_away_goals

            lambda_home = max(min(float(lambda_home), 3.5), 0.5)
            lambda_away = max(min(float(lambda_away), 3.0), 0.3)

            return {'home_lambda': round(lambda_home, 2), 'away_lambda': round(lambda_away, 2)}

        except Exception as e:
            print(f"Ошибка в calculate_poisson_lambda_last_n: {e}")
            return {'home_lambda': 1.2, 'away_lambda': 1.0, 'error': str(e)}


class Bank(models.Model):
    balance = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('1000.00'))
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Банк"
        verbose_name_plural = "Банк"

    @classmethod
    def get_instance(cls):
        obj, created = cls.objects.get_or_create(pk=1, defaults={'balance': Decimal('1000.00')})
        return obj

    @classmethod
    def get_balance(cls):
        return cls.get_instance().balance

    @classmethod
    def update_balance(cls, amount, transaction_type='CORRECTION', description='', bet=None):
        """
        Обновляет баланс и создает запись в истории.
        amount может быть положительным (пополнение) или отрицательным (снятие)
        """
        from .models import BankTransaction

        obj = cls.get_instance()
        balance_before = obj.balance
        obj.balance += Decimal(str(amount))
        obj.save()

        # Создаем запись в истории
        BankTransaction.objects.create(
            amount=abs(Decimal(str(amount))),
            transaction_type=transaction_type,
            balance_before=balance_before,
            balance_after=obj.balance,
            description=description,
            bet=bet
        )

        return obj.balance

    def __str__(self):
        return f"Банк: {self.balance}"


class BankTransaction(models.Model):
    class TransactionType(models.TextChoices):
        DEPOSIT = 'DEPOSIT', 'Пополнение'
        WITHDRAWAL = 'WITHDRAWAL', 'Снятие'
        CORRECTION = 'CORRECTION', 'Корректировка'

    amount = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Сумма")
    transaction_type = models.CharField(max_length=10, choices=TransactionType.choices, verbose_name="Тип операции")
    balance_before = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Баланс до")
    balance_after = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Баланс после")
    description = models.TextField(blank=True, verbose_name="Описание")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Дата операции")
    bet = models.ForeignKey('Bet', on_delete=models.SET_NULL, null=True, blank=True, verbose_name="Связанная ставка")

    class Meta:
        ordering = ['-created_at']
        verbose_name = "Транзакция банка"
        verbose_name_plural = "Транзакции банка"

    def __str__(self):
        return f"{self.get_transaction_type_display()} {self.amount} ({self.created_at.strftime('%d.%m.%Y')})"

    def delete(self, *args, **kwargs):
        """При удалении транзакции откатываем изменения банка"""
        from .models import Bank

        # Получаем текущий банк
        bank = Bank.get_instance()

        # Вычисляем, как транзакция повлияла на баланс
        # Сравниваем balance_after и balance_before
        effect = self.balance_after - self.balance_before

        # Откатываем эффект
        bank.balance -= effect
        bank.save()

        # Удаляем транзакцию
        super().delete(*args, **kwargs)

class Bet(models.Model):
    class ResultChoices(models.TextChoices):
        WIN = 'WIN', 'Выигрыш'
        LOSS = 'LOSS', 'Проигрыш'
        REFUND = 'REFUND', 'Возврат'

    class TargetChoices(models.TextChoices):
        OVER = 'over', 'ТБ 2.5'
        UNDER = 'under', 'ТМ 2.5'

    match_time = models.CharField(max_length=5, verbose_name="Время матча")
    home_team = models.ForeignKey('Team', on_delete=models.PROTECT, related_name='bets_home', verbose_name="Хозяева")
    away_team = models.ForeignKey('Team', on_delete=models.PROTECT, related_name='bets_away', verbose_name="Гости")
    league = models.ForeignKey('League', on_delete=models.PROTECT, verbose_name="Лига")

    odds_over = models.DecimalField(max_digits=6, decimal_places=2, verbose_name="Коэф. ТБ 2.5")
    odds_under = models.DecimalField(max_digits=6, decimal_places=2, verbose_name="Коэф. ТМ 2.5")

    recommended_target = models.CharField(max_length=5, choices=TargetChoices.choices, verbose_name="Исход")
    recommended_odds = models.DecimalField(max_digits=6, decimal_places=2, verbose_name="Кэф")

    poisson_prob = models.FloatField(verbose_name="По Пуассону, %")
    actual_prob = models.FloatField(verbose_name="Факт. вероятность, %")
    ev = models.FloatField(verbose_name="Доходность (EV), %")
    n_last_matches = models.PositiveSmallIntegerField(verbose_name="Использовано последних матчей (n)")
    interval = models.CharField(max_length=10, verbose_name="Интервал, %")

    stake = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Сумма ставки")
    bank_before = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Банк до ставки")
    bank_after = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="Банк после ставки")

    result = models.CharField(
        max_length=6,
        choices=ResultChoices.choices,
        default=ResultChoices.WIN,  # или LOSS, или REFUND - любой существующий
        verbose_name="Результат"
    )
    profit = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="Прибыль")
    settled_at = models.DateTimeField(blank=True, null=True, verbose_name="Дата")

    date_placed = models.DateTimeField(auto_now_add=True, verbose_name="Дата ставки")
    notes = models.TextField(blank=True, verbose_name="Заметки")

    fractional_kelly = models.FloatField(blank=True, null=True, verbose_name="Доля Келли")

    class Meta:
        ordering = ['-date_placed']
        indexes = [
            models.Index(fields=['result']),
            models.Index(fields=['date_placed']),
        ]
        verbose_name = "Ставка"
        verbose_name_plural = "Ставки"

    def __str__(self):
        return f"{self.home_team.name} - {self.away_team.name} ({self.date_placed.strftime('%d.%m.%Y %H:%M')})"

    def calculate_profit(self):
        """Рассчитывает прибыль на основе результата."""
        if not self.result or self.stake is None or self.recommended_odds is None:
            return None

        if self.result == self.ResultChoices.WIN:
            return self.stake * (self.recommended_odds - 1)
        elif self.result == self.ResultChoices.LOSS:
            return -self.stake
        elif self.result == self.ResultChoices.REFUND:
            return 0
        return None

    def save(self, *args, **kwargs):
        """Полный контроль сохранения с обработкой изменений."""
        from .models import Bank

        # Получаем старую версию, если это обновление
        old_profit = None
        if self.pk:
            try:
                old = Bet.objects.get(pk=self.pk)
                old_profit = old.profit
                old_result = old.result
            except Bet.DoesNotExist:
                old_profit = None
                old_result = None
        else:
            old_profit = None
            old_result = None

        # Устанавливаем bank_before для новой ставки
        if not self.bank_before and not self.pk:
            self.bank_before = Bank.get_balance()

        # Рассчитываем profit и bank_after
        if self.result:
            self.profit = self.calculate_profit()
            if self.result == self.ResultChoices.REFUND:
                self.bank_after = self.bank_before
            elif self.profit is not None:
                self.bank_after = self.bank_before + self.profit

        # Сохраняем
        super().save(*args, **kwargs)

        # ОБРАБОТКА БАНКА
        # 1. Новая ставка
        if not old_profit and self.profit and self.result != self.ResultChoices.REFUND:
            Bank.update_balance(self.profit)

        # 2. Изменение существующей ставки
        elif old_profit is not None:
            # Откатываем старую прибыль
            if old_profit != 0 and old_result != self.ResultChoices.REFUND:
                Bank.update_balance(-old_profit)

            # Добавляем новую прибыль
            if self.profit and self.result != self.ResultChoices.REFUND:
                Bank.update_balance(self.profit)

    def delete(self, *args, **kwargs):
        """Контролируемое удаление с откатом банка."""
        from .models import Bank

        if self.profit and self.result != self.ResultChoices.REFUND:
            Bank.update_balance(-self.profit)

        super().delete(*args, **kwargs)