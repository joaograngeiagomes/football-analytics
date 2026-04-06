select league_code
, league_name
, home_team_id
, home_team_name
, season
, COUNTIF(home_goals_fulltime > away_goals_fulltime) as home_wins
, COUNTIF(home_goals_fulltime = away_goals_fulltime) as home_draws
, COUNTIF(home_goals_fulltime < away_goals_fulltime) as home_losses
, SUM(home_goals_fulltime) as home_goals_scored
, SUM(away_goals_fulltime) as home_goals_conceded
from {{ ref("stg_fixtures")}} 
where fixture_status = 'FINISHED'
group by 1,2,3,4,5