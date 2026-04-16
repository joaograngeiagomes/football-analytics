import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv
import plotly.graph_objects as go

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

    df_teams = load_data("SELECT DISTINCT team_name, team_crest_url FROM `football-analytics-492122.marts.mart_team_performance` ORDER BY team_name")

    team = st.selectbox("Search for a team", df_teams["team_name"])

    team_crest = df_teams[df_teams["team_name"] == team]["team_crest_url"].values[0]
    st.image(team_crest, width=80)
   

    df = load_data(f"""
                SELECT position, team_name, matches_played, total_wins,
                total_draws, total_losses, points,
                total_goals_scored, total_goals_conceded, team_form, home_win_rate, away_win_rate,
                goals_scored_per_match, goals_conceded_per_match, avg_total_goals_per_match, 
                total_home_goals_scored, total_away_goals_scored, total_home_goals_conceded, total_away_goals_conceded
                FROM `football-analytics-492122.marts.mart_team_performance`
                WHERE team_name = '{team}'
                ORDER BY position
                """)
    df2 = load_data(f"""
                    SELECT fixture_date, league_name, matchday,
                    home_team_name, home_team_position, home_team_points, home_team_form_home,
                    away_team_name, away_team_position, away_team_points, away_team_form_away
                    FROM `football-analytics-492122.marts.mart_fixtures`
                    WHERE home_team_name = '{team}' OR away_team_name = '{team}'
                    ORDER BY fixture_date
                    """)
    ##st.dataframe(df, use_container_width=True, hide_index=True)
     
    ## Cards ## 
    col1, col2, col3, col4, col5, col6, col7= st.columns(7)
    col1.metric("Position", df["position"].values[0])
    col2.metric("Points", df["points"].values[0])
    col3.metric("Wins", df["total_wins"].values[0])
    col4.metric("Form", df["team_form"].values[0])
    col5.metric("Home Win Rate", df["home_win_rate"].values[0])
    col6.metric("Away Win Rate",df["away_win_rate"].values[0])
    col7.metric("Avg Goals Per Match", df["avg_total_goals_per_match"].values[0])

    ## Bar Chart ##
    fig = go.Figure()
    fig.update_layout(barmode='group', title='Goals Scored vs Conceded — Home & Away',title_font_size=18)

    fig.add_trace(go.Bar(name='Goals Scored', x=['Home', 'Away'], y=[df["total_home_goals_scored"].values[0]
                                                                    , df["total_away_goals_scored"].values[0]]
                                                                    ,marker_color='#2ecc71'))
    fig.add_trace(go.Bar(name='Goals Conceded', x=['Home', 'Away'], y=[df["total_home_goals_conceded"].values[0]
                                                                    , df["total_away_goals_conceded"].values[0]]
                                                                    ,marker_color='#e74c3c'))
   

    ## Pie Chart ##
    fig2 = go.Figure()
    fig2.update_layout(title='Results Distribution', title_font_size=18)

    fig2.add_trace(go.Pie(labels=['Wins', 'Draws', 'Losses']
                          , values=[df["total_wins"].values[0],df["total_draws"].values[0], df["total_losses"].values[0]]
                          , marker=dict(colors=['#2ecc71', '#f39c12', '#e74c3c'])))
    
    
    ## Radar Chart ##
    fig3 = go.Figure()

    attack = min(df["goals_scored_per_match"].values[0] / 3 * 100, 100)
    defence = min((1 / df["goals_conceded_per_match"].values[0]) * 100, 100)
    home_strength = df["home_win_rate"].values[0]
    away_strength = df["away_win_rate"].values[0]
    form = df["team_form"].values[0].count("W") * 20

    fig3.add_trace(go.Scatterpolar(
    r=[attack,defence,home_strength,away_strength,form],
    theta=['Attack', 'Defence', 'Home Strength', 'Away Strength', 'Form'],
    fill='toself',
    name=team
    ))

    fig3.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
    title='Team Profile'
    )

    st.subheader("Upcoming Fixtures")
    st.dataframe(df2, use_container_width=True, hide_index=True)

    st.plotly_chart(fig3, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.plotly_chart(fig2, use_container_width=True)

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