select league_code
, league_name
, away_team_id
, away_team_name
, season
, COUNTIF(home_goals_fulltime < away_goals_fulltime) as away_wins
, COUNTIF(home_goals_fulltime = away_goals_fulltime) as away_draws
, COUNTIF(home_goals_fulltime > away_goals_fulltime) as away_losses
, SUM(away_goals_fulltime) as away_goals_scored
, SUM(home_goals_fulltime) as away_goals_conceded
from {{ ref("stg_fixtures")}} 
where fixture_status = 'FINISHED'
group by 1,2,3,4,5