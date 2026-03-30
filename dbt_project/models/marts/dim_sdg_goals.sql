-- dim_sdg_goals
-- SDG goal dimension with pillar grouping

select
    goal,
    goal_title,
    goal_description,
    sdg_pillar,
    target_year
from {{ ref('stg_sdg_goal_metadata') }}
