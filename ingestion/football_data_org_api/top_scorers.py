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
    "liga_portugal": "PPL",
    "champions_league": "CL"
}

SCHEMA = [
    bigquery.SchemaField("league_code", "STRING"),
    bigquery.SchemaField("league_name", "STRING"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("player_id", "INTEGER"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("team_id", "INTEGER"),
    bigquery.SchemaField("team_name", "STRING"),
    bigquery.SchemaField("goals", "INTEGER"),
    bigquery.SchemaField("assists", "INTEGER"),
    bigquery.SchemaField("penalties", "INTEGER"),
]

def get_top_scorers(league_code: str) -> tuple:
    url = f"{BASE_URL}/competitions/{league_code}/scorers"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    season = data.get("season", {}).get("startDate", "")[:4]
    league_name = data.get("competition", {}).get("name", "")
    return data.get("scorers", []), season, league_name

def parse_scorer(scorer: dict, league_code: str, league_name: str, season: str) -> dict:
    return {
        "league_code": league_code,
        "league_name": league_name,
        "season": int(season),
        "player_id": scorer["player"]["id"],
        "player_name": scorer["player"]["name"],
        "team_id": scorer["team"]["id"],
        "team_name": scorer["team"]["name"],
        "goals": scorer.get("goals", 0),
        "assists": scorer.get("assists", 0),
        "penalties": scorer.get("penalties", 0),
    }

def main():
    all_scorers = []

    for league_name, league_code in LEAGUES.items():
        print(f"Extracting top scorers: {league_name}...")
        scorers, season, league_name_api = get_top_scorers(league_code)
        for scorer in scorers:
            parsed = parse_scorer(scorer, league_code, league_name_api, season)
            all_scorers.append(parsed)
        print(f"  -> {len(scorers)} scorers found")

    print(f"\nTotal: {len(all_scorers)} rows extracted")
    print("Loading to BigQuery...")
    load_json_to_bigquery(all_scorers, "raw", "top_scorers", SCHEMA)

if __name__ == "__main__":
    main()