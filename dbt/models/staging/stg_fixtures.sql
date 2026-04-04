select fixture_id
, league_code
, league_name
, season
, matchday
, cast(datetime(timestamp(date)) as date) as fixture_date
, status as fixture_status
, home_team_id
, home_team_name
, away_team_id
, away_team_name
, home_goals as home_goals_fulltime
, away_goals as away_goals_fulltime
from {{ source('raw', 'fixtures') }}