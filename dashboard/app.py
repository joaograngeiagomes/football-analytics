import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Page configuration
st.set_page_config(
    page_title="Football Analytics",
    page_icon="⚽",
    layout="wide"
)

@st.cache_data
def load_data(query: str) -> pd.DataFrame:
    client = bigquery.Client(project="football-analytics-492122")
    return client.query(query).to_dataframe()

# Sidebar navigation
st.sidebar.title("⚽ Football Analytics")
page = st.sidebar.selectbox(
    "Navigation",
    ["🏠 Team Search", "📊 Standings", "⭐ Players & Fantasy"]
)

# Pages
if page == "🏠 Team Search":
    st.title("Team Search")

    df_teams = load_data("SELECT DISTINCT team_name FROM `football-analytics-492122.marts.mart_team_performance` ORDER BY team_name")

    team = st.selectbox("Search for a team", df_teams["team_name"])

    df = load_data(f"""
                SELECT position, team_name, matches_played, total_wins,
                total_draws, total_losses, points,
                total_goals_scored, total_goals_conceded    
                FROM `football-analytics-492122.marts.mart_team_performance`
                WHERE team_name = '{team}'
                ORDER BY position
                """)
    st.dataframe(df, use_container_width=True, hide_index=True)

elif page == "📊 Standings":
    st.title("📊 Standings")

    league = st.selectbox(
        "Select League",
        ["Premier League", "La Liga", "Primeira Liga"]
    )

    league_map = {
        "Premier League": "PL",
        "La Liga": "PD",
        "Primeira Liga": "PPL"
    }

    df = load_data(f"""
                SELECT position, team_name, matches_played, total_wins,
                total_draws, total_losses, points,
                total_goals_scored, total_goals_conceded    
                FROM `football-analytics-492122.marts.mart_team_performance`
                WHERE league_code = '{league_map[league]}'
                ORDER BY position
                """)

    st.dataframe(df, use_container_width=True, hide_index=True)

elif page == "⭐ Players & Fantasy":
    st.title("⭐ Players & Fantasy")

    league = st.selectbox(
        "Select League",
        ["Premier League", "La Liga", "Primeira Liga"]
    )

    league_map = {
        "Premier League": "PL",
        "La Liga": "PD",
        "Primeira Liga": "PPL"
    }

    df = load_data(f"""
                   SELECT player_name, team_name, total_player_goals
                   , total_player_assists, total_player_penalties, percentage_player_contribution
                   ,goals_per_match, assists_per_match, goal_contributions_per_match
                   ,penalty_dependency_ratio
                   FROM `football-analytics-492122.marts.mart_player_performance`
                   WHERE league_code = '{league_map[league]}'
                   ORDER BY total_player_goals DESC
                   """)
    

    st.dataframe(df, use_container_width=True, hide_index=True)