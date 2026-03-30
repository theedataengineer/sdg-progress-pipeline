-- mart_africa_2030_tracker
-- Main dashboard table: Africa SDG progress towards 2030
-- Answers analytics questions 1, 2, 3, 5, 6, 9, 10

select
    s.country_code,
    s.country_name,
    s.goal,
    s.goal_title,
    s.sdg_pillar,
    s.indicators_tracked,
    s.avg_goal_score,
    s.indicators_achieved,
    s.indicators_on_track,
    s.indicators_off_track,
    s.data_as_of,

    -- country context
    c.region,
    c.income_group,
    c.income_tier,
    c.overall_sdg_score,

    -- progress classification
    case
        when s.avg_goal_score >= 75  then 'On track'
        when s.avg_goal_score >= 50  then 'Moderate progress'
        when s.avg_goal_score >= 25  then 'Limited progress'
        else                              'Off track'
    end                              as progress_category,

    -- years to 2030 from latest data
    2030 - s.data_as_of              as years_remaining

from {{ ref('int_africa_sdg_scores') }} s
left join {{ ref('dim_countries') }} c
    on s.country_code = c.country_code
