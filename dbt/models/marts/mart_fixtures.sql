select f.fixture_id
, f.fixture_date
, f.season
, f.league_name
, f.matchday
, f.fixture_status
, tph.team_id as home_team_id
, tph.team_name as home_team_name
, tph.position as home_team_position
, tph.points as home_team_points
, tph.team_form_home as home_team_form_home
, tpa.team_id as away_team_id
, tpa.team_name as away_team_name
, tpa.position as away_team_position
, tpa.points as away_team_points
, tpa.team_form_away as away_team_form_away
from {{ref("stg_fixtures")}} f
join {{ref("mart_team_performance")}} tph on f.home_team_id = tph.team_id
join {{ref("mart_team_performance")}} tpa on f.away_team_id = tpa.team_id
where f.fixture_status in ('TIMED', 'SCHEDULED')