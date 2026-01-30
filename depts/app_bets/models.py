import math
from django.db import models
from django.core.validators import MinValueValidator
from django.core.exceptions import ValidationError
from django.db.models import Q, Avg, Sum, Count, F
from decimal import Decimal


# --- СПРАВОЧНИКИ И ГЕОГРАФИЯ ---

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
        verbose_name_plural = "Страны"

    def __str__(self):
        return self.name


class League(models.Model):
    """Лиги и чемпионаты (бывшие Tournament)."""
    name = models.CharField(max_length=150, verbose_name="Название лиги")
    sport = models.ForeignKey(Sport, on_delete=models.CASCADE, verbose_name="Вид спорта", related_name="sport_leagues")
    country = models.ForeignKey(Country, on_delete=models.CASCADE, verbose_name="Страна", related_name="country_leagues")

    class Meta:
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


# --- СИСТЕМА ИМЕН И СИНОНИМОВ ---

class Team(models.Model):
    """Каноническая запись команды или игрока (Мастер-запись)."""
    name = models.CharField(max_length=100, verbose_name="Каноническое название")
    sport = models.ForeignKey(Sport, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, on_delete=models.CASCADE)

    class Meta:
        unique_together = ('name', 'sport', 'country')
        verbose_name = "Команда / Игрок"
        verbose_name_plural = "Команды и Игроки"

    def __str__(self):
        return f"{self.name} ({self.sport.name})"


class TeamAlias(models.Model):
    """
    Синонимы названий для парсинга (напр. 'Man City' и 'Манчестер Сити' -> ID одной команды).
    """
    name = models.CharField(max_length=150, unique=True, verbose_name="Вариант из источника")
    team = models.ForeignKey(Team, on_delete=models.CASCADE, related_name="aliases")

    class Meta:
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

# --- ГЛАВНАЯ МОДЕЛЬ МАТЧА ---

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
        verbose_name = "Матч"
        verbose_name_plural = "Матчи"
        ordering = ['-date']

    # --- МАКСИМАЛЬНАЯ ВАЛИДАЦИЯ ---

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
        self.full_clean()  # Принудительная валидация при любом способе сохранения
        super().save(*args, **kwargs)

    # --- МЕТОДЫ ТВОЕГО АЛГОРИТМА ---

    def get_twins(self, tolerance=Decimal('0.05')):
        """
        Поиск матчей-'близнецов' в той же стране и спорте.
        Учитывает кэфы П1 и П2 с погрешностью +- 0.05.
        """
        qs = Match.objects.filter(
            league__country=self.league.country,
            league__sport=self.league.sport,
            odds_home__range=(self.odds_home - tolerance, self.odds_home + tolerance),
            odds_away__range=(self.odds_away - tolerance, self.odds_away + tolerance)
        ).exclude(id=self.id)

        if self.league.sport.has_draw and self.odds_draw:
            qs = qs.filter(odds_draw__range=(self.odds_draw - tolerance, self.odds_draw + tolerance))

        return qs

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
        Условие: расчет только если у каждой команды >= 5 игр в сезоне.
        """
        # 1. Получаем данные лиги за сезон
        league_stats = self.league.get_season_averages(self.season)
        if not league_stats or league_stats['total_matches'] == 0:
            return None

        # Средние показатели лиги (B4, B5, B6, B7 из твоего примера)
        # Защита от деления на 0 если голов в лиге еще нет
        l_avg_home_goals = Decimal(str(league_stats['avg_home_goals'] or 1))
        l_avg_away_goals = Decimal(str(league_stats['avg_away_goals'] or 1))
        l_avg_home_conceded = l_avg_away_goals  # B6
        l_avg_away_conceded = l_avg_home_goals  # B7

        # 2. Собираем статистику Хозяев (только их домашние игры в этом сезоне)
        home_team_matches = Match.objects.filter(
            league=self.league, season=self.season,
            home_team=self.home_team, home_score_reg__isnull=False
        )

        # 3. Собираем статистику Гостей (только их гостевые игры в этом сезоне)
        away_team_matches = Match.objects.filter(
            league=self.league, season=self.season,
            away_team=self.away_team, away_score_reg__isnull=False
        )

        # Проверка на твоё условие: не менее 5 игр
        if home_team_matches.count() < 5 or away_team_matches.count() < 5:
            return "Недостаточно данных (нужно >= 5 игр)"

        # Агрегация с защитой от None (or 0)
        h_agg = home_team_matches.aggregate(s=Sum('home_score_reg'), c=Sum('away_score_reg'))
        a_agg = away_team_matches.aggregate(s=Sum('away_score_reg'), c=Sum('home_score_reg'))

        # Статистика Хозяев дома (B8, B9, B20)
        h_avg_scored = Decimal(str(h_agg['s'] or 0)) / home_team_matches.count()
        h_avg_conceded = Decimal(str(h_agg['c'] or 0)) / home_team_matches.count()

        # Статистика Гостей в гостях (B11, B17, B12)
        a_avg_scored = Decimal(str(a_agg['s'] or 0)) / away_team_matches.count()
        a_avg_conceded = Decimal(str(a_agg['c'] or 0)) / away_team_matches.count()

        # 4. РАСЧЕТ СИЛЫ (АТАКА / ОБОРОНА)
        h_attack_strength = h_avg_scored / l_avg_home_goals  # B14
        a_defense_strength = a_avg_conceded / l_avg_away_conceded  # B15
        a_attack_strength = a_avg_scored / l_avg_away_goals  # B19
        h_defense_strength = h_avg_conceded / l_avg_home_conceded  # B22

        # 5. ИТОГОВАЯ ВЕРОЯТНОСТЬ ГОЛОВ (LYAMBDA)
        lambda_home = h_attack_strength * a_defense_strength * l_avg_home_goals  # B16
        lambda_away = a_attack_strength * h_defense_strength * l_avg_away_goals  # B23

        return {
            'home_lambda': float(round(lambda_home, 2)),
            'away_lambda': float(round(lambda_away, 2))
        }

    def get_poisson_probabilities(self, max_goals=5):
        """
        Рассчитывает сетку вероятностей счета на основе лямбд.
        """
        lambdas = self.calculate_poisson_lambda()
        if isinstance(lambdas, str) or not lambdas: return lambdas

        l_home = lambdas['home_lambda']
        l_away = lambdas['away_lambda']

        # Функция распределения Пуассона: P(x; λ) = (e^-λ * λ^x) / x!
        def poisson_prob(l, x):
            return (math.exp(-l) * (l ** x)) / math.factorial(x)

        prob_matrix = {}
        for h in range(max_goals + 1):
            for a in range(max_goals + 1):
                prob = poisson_prob(l_home, h) * poisson_prob(l_away, a)
                prob_matrix[f"{h}:{a}"] = round(prob * 100, 2)

        return prob_matrix

    def __str__(self):
        res = f"{self.home_score_final}:{self.away_score_final}" if self.home_score_final is not None else "VS"
        return f"{self.date.strftime('%d.%m')} {self.home_team.name} {res} {self.away_team.name}"