from django.contrib import admin
from django.utils.html import format_html
from .models import Sport, Country, League, Team, TeamAlias, Season, Match

# --- ИНЛАЙНЫ ---

class TeamAliasInline(admin.TabularInline):
    model = TeamAlias
    extra = 1

# --- НАСТРОЙКИ МОДЕЛЕЙ ---

@admin.register(TeamAlias)
class TeamAliasAdmin(admin.ModelAdmin):
    list_display = ('name', 'team')
    search_fields = ('name', 'team__name')
    list_filter = ('team__sport', 'team__country')
    # ВОТ ЭТА СТРОЧКА: теперь в псевдонимах появится удобный поиск по буквам
    autocomplete_fields = ['team']

@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    list_display = ('name', 'sport', 'country')
    list_filter = ('sport', 'country')
    # Это нужно, чтобы autocomplete заработал в других местах
    search_fields = ('name',)
    inlines = [TeamAliasInline]

@admin.register(League)
class LeagueAdmin(admin.ModelAdmin):
    list_display = ('name', 'sport', 'country', 'external_id', 'display_draw_freq')
    list_filter = ('sport', 'country')

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
    search_fields = ('home_team__name', 'away_team__name')
    ordering = ('-date',)

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
admin.site.register(Sport)
admin.site.register(Country)
admin.site.register(Season)