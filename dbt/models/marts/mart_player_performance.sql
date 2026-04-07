select t.league_code
, t.league_name
, t.season
, t.player_name
, t.team_name
, t.total_goals as total_player_goals
, t.total_assists as total_player_assists
, t.total_penalties as total_player_penalties
, s.matches_played
, s.matches_won
, s.matches_draw as matches_drawn
, s.matches_lost
, s.points as team_points
, s.goals_for as team_goals_scored
, s.goals_against as team_goals_conceded
, s.goal_difference as team_goal_difference
, ROUND(((t.total_goals + t.total_assists) / s.goals_for),2) * 100 as percentage_player_contribution
, t.total_goals - t.total_penalties as open_play_goals
, ROUND(t.total_goals / s.matches_played,2) as goals_per_match
, ROUND(t.total_assists / s.matches_played,2) as assists_per_match
, ROUND((t.total_goals + t.total_assists) / s.matches_played,2) as goal_contributions_per_match 
, ROUND(t.total_penalties / NULLIF(t.total_goals, 0) * 100,2) as penalty_dependency_ratio
from {{ref('stg_top_scorers')}} t
join {{ref('stg_standings')}} s on t.team_id = s.team_id and s.league_code = t.league_code