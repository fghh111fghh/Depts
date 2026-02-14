# export_matches.py
import csv
import json
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'depts.settings')
django.setup()

from app_bets.models import Match

# CSV экспорт
with open('matches_export.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    # Заголовки
    writer.writerow([
        'id', 'league_id', 'date', 'home_team_id', 'away_team_id',
        'home_score_reg', 'away_score_reg', 'odds_home', 'odds_draw', 'odds_away', 'season_id'
    ])

    # Данные
    for match in Match.objects.filter(
            home_score_reg__isnull=False,
            away_score_reg__isnull=False
    ).order_by('date'):
        writer.writerow([
            match.id,
            match.league_id,
            match.date.isoformat(),
            match.home_team_id,
            match.away_team_id,
            match.home_score_reg,
            match.away_score_reg,
            float(match.odds_home) if match.odds_home else '',
            float(match.odds_draw) if match.odds_draw else '',
            float(match.odds_away) if match.odds_away else '',
            match.season_id
        ])

print("CSV файл создан: matches_export.csv")

# JSON экспорт (если нужен)
matches_data = []
for match in Match.objects.filter(
        home_score_reg__isnull=False,
        away_score_reg__isnull=False
).order_by('date'):  # ограничьте для JSON
    matches_data.append({
        'id': match.id,
        'league_id': match.league_id,
        'date': match.date.isoformat(),
        'home_team_id': match.home_team_id,
        'away_team_id': match.away_team_id,
        'home_score_reg': match.home_score_reg,
        'away_score_reg': match.away_score_reg,
        'odds_home': float(match.odds_home) if match.odds_home else None,
        'odds_draw': float(match.odds_draw) if match.odds_draw else None,
        'odds_away': float(match.odds_away) if match.odds_away else None,
        'season_id': match.season_id
    })

with open('matches_export.json', 'w', encoding='utf-8') as f:
    json.dump(matches_data, f, ensure_ascii=False, indent=2)

print("JSON файл создан: matches_export.json")