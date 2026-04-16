select league_code
, league_name
, cast(season as int) as season
, position
, team_id
, team_name
, team_crest_url
, played_games as matches_played
, won as matches_won
, draw as matches_draw
, lost as matches_lost
, points 
, goals_for
, goals_against
, goal_difference
from {{ source('raw', 'standings') }}