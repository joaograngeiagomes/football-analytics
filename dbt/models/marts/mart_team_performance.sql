
with last_5_matches as (
    select team_id
    , STRING_AGG(match_result, '' ORDER BY fixture_date DESC) as team_form
    from {{ref ("int_team_last_5_matches")}} 
    group by team_id
)

select s.team_id
, s.team_name
, s.league_name
, s.season
, s.points
, s.position
, s.matches_played
, h.home_wins
, h.home_draws
, h.home_losses
, a.away_wins
, a.away_draws
, a.away_losses
, h.home_goals_scored as total_home_goals_scored
, h.home_goals_conceded as total_home_goals_conceded
, a.away_goals_scored as total_away_goals_scored
, a.away_goals_conceded as total_away_goals_conceded
, s.goals_for as total_goals_scored
, s.goals_against as total_goals_conceded
, ROUND(s.goals_for / matches_played,2) as goals_scored_per_match
, ROUND(s.goals_against / matches_played,2) as goals_conceded_per_match
, s.matches_won as total_wins
, s.matches_draw as total_draws
, s.matches_lost as total_losses
, ROUND((s.goals_for + s.goals_against) / s.matches_played,2) as avg_total_goals_per_match
, ROUND(h.home_wins / NULLIF(h.home_wins + h.home_draws + h.home_losses, 0) * 100,2) as home_win_rate
, ROUND(a.away_wins / NULLIF(a.away_wins + a.away_draws + a.away_losses, 0) * 100,2) as away_win_rate
, l.team_form
from {{ref ("stg_standings")}} s
join {{ref ("int_team_home_stats")}} h on s.team_id = h.home_team_id and s.league_code = h.league_code
join {{ref ("int_team_away_stats")}} a on s.team_id = a.away_team_id and s.league_code = a.league_code
join last_5_matches l on s.team_id = l.team_id