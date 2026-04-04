import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests
from dotenv import load_dotenv
from ingestion.utils.bigquery_client import load_json_to_bigquery
from google.cloud import bigquery

load_dotenv()

API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY
}

LEAGUES = {
    "liga_portugal": 94,
    "premier_league": 39,
    "la_liga": 140,
    "champions_league": 2
}

SCHEMA = [
    bigquery.SchemaField("fixture_id", "INTEGER"),
    bigquery.SchemaField("league_id", "INTEGER"),
    bigquery.SchemaField("league_name", "STRING"),
    bigquery.SchemaField("league_country", "STRING"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("round", "STRING"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("home_team_id", "INTEGER"),
    bigquery.SchemaField("home_team_name", "STRING"),
    bigquery.SchemaField("away_team_id", "INTEGER"),
    bigquery.SchemaField("away_team_name", "STRING"),
    bigquery.SchemaField("home_goals", "INTEGER"),
    bigquery.SchemaField("away_goals", "INTEGER"),
    bigquery.SchemaField("source_league_name", "STRING"),
]

def get_fixtures(league_id: int, season: int) -> list:
    url = f"{BASE_URL}/fixtures"
    params = {
        "league": league_id,
        "season": season
    }
    response = requests.get(url, headers=HEADERS, params=params)
    data = response.json()
    print(f"  API response: {data.get('results', 0)} resultados | errors: {data.get('errors', [])}")
    return data.get("response", [])

def parse_fixture(fixture: dict, league_name: str) -> dict:
    return {
        "fixture_id": fixture["fixture"]["id"],
        "league_id": fixture["league"]["id"],
        "league_name": fixture["league"]["name"],
        "league_country": fixture["league"]["country"],
        "season": fixture["league"]["season"],
        "round": fixture["league"]["round"],
        "date": fixture["fixture"]["date"],
        "status": fixture["fixture"]["status"]["short"],
        "home_team_id": fixture["teams"]["home"]["id"],
        "home_team_name": fixture["teams"]["home"]["name"],
        "away_team_id": fixture["teams"]["away"]["id"],
        "away_team_name": fixture["teams"]["away"]["name"],
        "home_goals": fixture["goals"]["home"],
        "away_goals": fixture["goals"]["away"],
        "source_league_name": league_name,
    }

def main():
    season = 2025
    all_fixtures = []

    for league_name, league_id in LEAGUES.items():
        print(f"A extrair fixtures: {league_name}...")
        fixtures = get_fixtures(league_id, season)
        for fixture in fixtures:
            parsed = parse_fixture(fixture, league_name)
            all_fixtures.append(parsed)
        print(f"  → {len(fixtures)} jogos encontrados")

    print(f"\nTotal: {len(all_fixtures)} fixtures extraídos")
    print("A carregar no BigQuery...")
    load_json_to_bigquery(all_fixtures, "raw", "fixtures", SCHEMA)

if __name__ == "__main__":
    main()