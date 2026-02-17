from django.contrib import admin
from django.utils.html import format_html
from .models import Sport, Country, League, Team, TeamAlias, Season, Match, Bank, Bet

# --- ИНЛАЙНЫ ---

class TeamAliasInline(admin.TabularInline):
    model = TeamAlias
    extra = 1
    autocomplete_fields = ['team']  # для выбора команды при добавлении алиаса

# --- НАСТРОЙКИ МОДЕЛЕЙ ---

@admin.register(TeamAlias)
class TeamAliasAdmin(admin.ModelAdmin):
    list_display = ('name', 'team')
    search_fields = ('name', 'team__name')
    list_filter = ('team__sport', 'team__country')
    autocomplete_fields = ['team']  # поиск по основному имени команды

@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    list_display = ('name', 'sport', 'country')
    list_filter = ('sport', 'country')
    search_fields = ('name',)
    autocomplete_fields = ['sport', 'country']
    inlines = [TeamAliasInline]

@admin.register(League)
class LeagueAdmin(admin.ModelAdmin):
    list_display = ('name', 'sport', 'country', 'external_id', 'display_draw_freq')
    list_filter = ('sport', 'country')
    search_fields = ('name', 'external_id')
    autocomplete_fields = ['sport', 'country']

    def display_draw_freq(self, obj):
        return f"{obj.get_draw_frequency()}%"
    display_draw_freq.short_description = "% Ничьих"

@admin.register(Match)
class MatchAdmin(admin.ModelAdmin):
    list_display = (
        'date_format', 'league', 'match_label',
        'score_display', 'poisson_prediction', 'twins_count'
    )
    list_filter = ('league__sport', 'league__country', 'season', 'is_anomaly')
    search_fields = ('home_team__name', 'away_team__name', 'league__name')
    ordering = ('-date',)
    autocomplete_fields = ['home_team', 'away_team', 'league', 'season']

    fieldsets = (
        ('Основная информация', {
            'fields': (('date', 'season', 'league', 'round_number'), ('home_team', 'away_team'))
        }),
        ('Результаты и коэффициенты', {
            'fields': (('home_score_reg', 'away_score_reg'), ('home_score_final', 'away_score_final'), 'finish_type',
                       ('odds_home', 'odds_draw', 'odds_away'))
        }),
        ('Аналитика и Составы', {
            'classes': ('collapse',),
            'fields': ('is_anomaly', 'home_lineup', 'away_lineup'),
        }),
    )

    def date_format(self, obj):
        return obj.date.strftime('%d.%m %H:%M')
    date_format.short_description = 'Дата'

    def match_label(self, obj):
        return f"{obj.home_team.name} vs {obj.away_team.name}"
    match_label.short_description = 'Матч'

    def score_display(self, obj):
        if obj.home_score_reg is not None:
            return format_html("<b>{}:{}</b> ({}:{})",
                               obj.home_score_reg, obj.away_score_reg,
                               obj.home_score_final, obj.away_score_final)
        return "—"
    score_display.short_description = 'Счет (Осн/Итог)'

    def poisson_prediction(self, obj):
        res = obj.calculate_poisson_lambda()
        if isinstance(res, dict):
            h, a = res['home_lambda'], res['away_lambda']
            color = "green" if h != a else "black"
            return format_html("<span style='color: {};'>λ {} : {}</span>", color, h, a)
        return format_html("<span style='color: gray;'>{}</span>", res)
    poisson_prediction.short_description = 'Пуассон (H:A)'

    def twins_count(self, obj):
        count = obj.get_twins().count()
        if count > 0:
            return format_html("<b style='color: #d4a017;'>{} шт.</b>", count)
        return "0"
    twins_count.short_description = 'Близнецы'

# --- ПРОСТЫЕ РЕГИСТРАЦИИ ---
@admin.register(Sport)
class SportAdmin(admin.ModelAdmin):
    list_display = ('name', 'has_draw')
    search_fields = ('name',)

@admin.register(Country)
class CountryAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)

@admin.register(Season)
class SeasonAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_current', 'start_date', 'end_date')
    list_filter = ('is_current',)
    search_fields = ('name',)

# --- БАНК И СТАВКИ ---
@admin.register(Bank)
class BankAdmin(admin.ModelAdmin):
    list_display = ['balance', 'updated_at']

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return not Bank.objects.exists()

@admin.register(Bet)
class BetAdmin(admin.ModelAdmin):
    list_display = [
        'date_placed', 'match_time', 'home_team', 'away_team', 'league',
        'recommended_target', 'recommended_odds', 'stake', 'ev', 'result_colored', 'profit_colored'
    ]
    list_display_links = ['home_team', 'away_team']
    list_filter = [
        'league', 'recommended_target', 'result', 'n_last_matches', 'interval',
        ('date_placed', admin.DateFieldListFilter),
    ]
    search_fields = ['home_team__name', 'away_team__name', 'league__name', 'notes']
    ordering = ['-date_placed']
    autocomplete_fields = ['home_team', 'away_team', 'league']  # автодополнение по основному имени
    readonly_fields = ['date_placed', 'profit', 'bank_after']

    fieldsets = (
        ('Матч', {
            'fields': ('match_time', 'home_team', 'away_team', 'league')
        }),
        ('Коэффициенты', {
            'fields': ('odds_over', 'odds_under', 'recommended_target', 'recommended_odds')
        }),
        ('Прогноз и калибровка', {
            'fields': ('poisson_prob', 'actual_prob', 'ev', 'n_last_matches', 'interval')
        }),
        ('Параметры ставки', {
            'fields': ('stake', 'bank_before', 'bank_after', 'profit', 'fractional_kelly')
        }),
        ('Результат', {
            'fields': ('result', 'settled_at')
        }),
        ('Метаданные', {
            'fields': ('notes', 'date_placed')
        }),
    )

    def result_colored(self, obj):
        if obj.result == Bet.ResultChoices.WIN:
            color = 'green'
        elif obj.result == Bet.ResultChoices.LOSS:
            color = 'red'
        elif obj.result == Bet.ResultChoices.REFUND:
            color = 'orange'
        else:
            color = 'gray'
        return format_html('<span style="color: {}; font-weight: bold;">{}</span>', color, obj.get_result_display())
    result_colored.short_description = 'Результат'
    result_colored.admin_order_field = 'result'

    def profit_colored(self, obj):
        if obj.profit is None:
            return '-'
        color = 'green' if obj.profit > 0 else 'red' if obj.profit < 0 else 'black'
        return format_html('<span style="color: {}; font-weight: bold;">{}</span>', color, obj.profit)
    profit_colored.short_description = 'Прибыль'
    profit_colored.admin_order_field = 'profit'

    actions = ['mark_as_win', 'mark_as_loss', 'mark_as_refund']

    def mark_as_win(self, request, queryset):
        for bet in queryset:
            bet.result = Bet.ResultChoices.WIN
            bet.save()
        self.message_user(request, "Выбранные ставки отмечены как выигрыш.")
    mark_as_win.short_description = "Отметить как выигрыш"

    def mark_as_loss(self, request, queryset):
        for bet in queryset:
            bet.result = Bet.ResultChoices.LOSS
            bet.save()
        self.message_user(request, "Выбранные ставки отмечены как проигрыш.")
    mark_as_loss.short_description = "Отметить как проигрыш"

    def mark_as_refund(self, request, queryset):
        for bet in queryset:
            bet.result = Bet.ResultChoices.REFUND
            bet.save()
        self.message_user(request, "Выбранные ставки отмечены как возврат.")
    mark_as_refund.short_description = "Отметить как возврат"

    def save_model(self, request, obj, form, change):
        if not obj.bank_before:
            obj.bank_before = Bank.get_balance()
        super().save_model(request, obj, form, change)
        if obj.result and obj.profit is not None:
            Bank.update_balance(obj.profit)