import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import requests
from dotenv import load_dotenv
from ingestion.utils.bigquery_client import load_json_to_bigquery
from google.cloud import bigquery

load_dotenv()

API_KEY = os.getenv("FOOTBALL_DATA_KEY")
BASE_URL = "http://api.football-data.org/v4"

HEADERS = {
    "X-Auth-Token": API_KEY
}

LEAGUES = {
    "premier_league": "PL",
    "la_liga": "PD",
    "champions_league": "CL",
    "liga_portugal": "PPL"
}

SCHEMA = [
    bigquery.SchemaField("fixture_id", "INTEGER"),
    bigquery.SchemaField("league_code", "STRING"),
    bigquery.SchemaField("league_name", "STRING"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("matchday", "INTEGER"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("home_team_id", "INTEGER"),
    bigquery.SchemaField("home_team_name", "STRING"),
    bigquery.SchemaField("away_team_id", "INTEGER"),
    bigquery.SchemaField("away_team_name", "STRING"),
    bigquery.SchemaField("home_goals", "INTEGER"),
    bigquery.SchemaField("away_goals", "INTEGER"),
]

def get_fixtures(league_code: str) -> list:
    url = f"{BASE_URL}/competitions/{league_code}/matches"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    return data.get("matches", [])

def parse_fixture(match: dict, league_code: str) -> dict:
    return {
        "fixture_id": match["id"],
        "league_code": league_code,
        "league_name": match["competition"]["name"],
        "season": match["season"]["startDate"][:4],
        "matchday": match.get("matchday"),
        "date": match["utcDate"],
        "status": match["status"],
        "home_team_id": match["homeTeam"]["id"],
        "home_team_name": match["homeTeam"]["name"],
        "away_team_id": match["awayTeam"]["id"],
        "away_team_name": match["awayTeam"]["name"],
        "home_goals": match["score"]["fullTime"]["home"],
        "away_goals": match["score"]["fullTime"]["away"],
    }

def main():
    all_fixtures = []

    for league_name, league_code in LEAGUES.items():
        print(f"Extracting fixtures: {league_name}...")
        fixtures = get_fixtures(league_code)
        for match in fixtures:
            parsed = parse_fixture(match, league_code)
            all_fixtures.append(parsed)
        print(f"  → {len(fixtures)} fixtures found")

    print(f"\nTotal: {len(all_fixtures)} extracted fixtures")
    print("Loading to BigQuery...")
    load_json_to_bigquery(all_fixtures, "raw", "fixtures", SCHEMA)

if __name__ == "__main__":
    main()