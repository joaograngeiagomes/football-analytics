select league_code
, league_name
, cast(season as int) as season
, player_id
, player_name
, team_id
, team_name
, goals as total_goals
, assists as total_assists
, penalties as total_penalties
from {{source('raw', 'top_scorers') }}