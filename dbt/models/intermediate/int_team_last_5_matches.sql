
with home_matches as (
    select home_team_id
    , fixture_date
    , CASE WHEN home_goals_fulltime > away_goals_fulltime THEN 'W'
        WHEN home_goals_fulltime < away_goals_fulltime THEN 'L'
        ELSE 'D' END AS match_result
    from {{ref('stg_fixtures')}}
    where fixture_status = 'FINISHED'
),

away_matches as (
    select away_team_id
    , fixture_date
    , CASE WHEN away_goals_fulltime > home_goals_fulltime THEN 'W'
        WHEN away_goals_fulltime < home_goals_fulltime THEN 'L'
        ELSE 'D' END AS match_result
    from {{ref('stg_fixtures')}}
    where fixture_status = 'FINISHED'
),

all_matches as (
    select home_team_id as team_id
    , fixture_date
    , match_result
    , 1 as is_home
    from home_matches

    union all

    select away_team_id as team_id
    , fixture_date
    , match_result
    , 0 as is_home
    from away_matches
),

last_5_matches as (
    select team_id
    , fixture_date
    , match_result
    , is_home
    , row_number() over(partition by team_id, is_home order by fixture_date desc) as rn
    from all_matches
)

select team_id
, fixture_date
, is_home
, match_result
from last_5_matches
where rn <= 5