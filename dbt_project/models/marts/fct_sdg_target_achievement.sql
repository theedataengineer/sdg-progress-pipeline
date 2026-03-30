-- fct_sdg_target_achievement
-- One row per country per indicator showing 2030 target status
-- Powers the "on track / off track" dashboard view

select
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    goal,
    goal_title,
    sdg_pillar,
    is_african_country,
    latest_year,
    latest_value,
    baseline_value,
    baseline_year,
    target_value,
    direction,
    gap_to_target,
    progress_since_2015,
    target_status,
    years_to_2030
from {{ ref('int_sdg_target_gaps') }}
