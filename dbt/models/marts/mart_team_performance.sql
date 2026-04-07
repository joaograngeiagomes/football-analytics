
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
, l.team_form
from {{ref ("stg_standings")}} s
join {{ref ("int_team_home_stats")}} h on s.team_id = h.home_team_id and s.league_code = h.league_code
join {{ref ("int_team_away_stats")}} a on s.team_id = a.away_team_id and s.league_code = a.league_code
join last_5_matches l on s.team_id = l.team_id