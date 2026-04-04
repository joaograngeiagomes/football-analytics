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
    "liga_portugal": "PPL"
}

SCHEMA = [
    bigquery.SchemaField("league_code", "STRING"),
    bigquery.SchemaField("league_name", "STRING"),
    bigquery.SchemaField("season", "INTEGER"),
    bigquery.SchemaField("position", "INTEGER"),
    bigquery.SchemaField("team_id", "INTEGER"),
    bigquery.SchemaField("team_name", "STRING"),
    bigquery.SchemaField("played_games", "INTEGER"),
    bigquery.SchemaField("won", "INTEGER"),
    bigquery.SchemaField("draw", "INTEGER"),
    bigquery.SchemaField("lost", "INTEGER"),
    bigquery.SchemaField("points", "INTEGER"),
    bigquery.SchemaField("goals_for", "INTEGER"),
    bigquery.SchemaField("goals_against", "INTEGER"),
    bigquery.SchemaField("goal_difference", "INTEGER"),
]

def get_standings(league_code: str) -> list:
    url = f"{BASE_URL}/competitions/{league_code}/standings"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    standings = data.get("standings", [])
    season = data.get("season", {}).get("startDate", "")[:4]
    for s in standings:
        if s.get("type") == "TOTAL":
            return s.get("table", []), season
    return [], season

def parse_standing(row: dict, league_code: str, league_name: str, season: str) -> dict:
    return {
        "league_code": league_code,
        "league_name": league_name,
        "season": int(season),
        "position": row["position"],
        "team_id": row["team"]["id"],
        "team_name": row["team"]["name"],
        "played_games": row["playedGames"],
        "won": row["won"],
        "draw": row["draw"],
        "lost": row["lost"],
        "points": row["points"],
        "goals_for": row["goalsFor"],
        "goals_against": row["goalsAgainst"],
        "goal_difference": row["goalDifference"],
    }

def main():
    all_standings = []

    for league_name, league_code in LEAGUES.items():
        print(f"Extracting standings: {league_name}...")
        table, season = get_standings(league_code)
        for row in table:
            parsed = parse_standing(row, league_code, league_name, season)
            all_standings.append(parsed)
        print(f"  → {len(table)} teams found")

    print(f"\nTotal: {len(all_standings)} extracted rows")
    print("Loading to BigQuery...")
    load_json_to_bigquery(all_standings, "raw", "standings", SCHEMA)

if __name__ == "__main__":
    main()