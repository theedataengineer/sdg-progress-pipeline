-- mart_sdg_goal_summary
-- Global summary of progress per SDG goal
-- Answers analytics questions 2, 3, 7

select
    goal,
    goal_title,
    sdg_pillar,

    -- across all African countries tracked
    count(distinct country_code)             as countries_tracked,
    round(avg(avg_goal_score)::numeric, 1)   as africa_avg_score,
    round(min(avg_goal_score)::numeric, 1)   as lowest_country_score,
    round(max(avg_goal_score)::numeric, 1)   as highest_country_score,

    -- achievement counts
    sum(indicators_achieved)                 as total_achieved,
    sum(indicators_on_track)                 as total_on_track,
    sum(indicators_off_track)                as total_off_track,

    -- overall goal status
    case
        when avg(avg_goal_score) >= 75  then 'On track'
        when avg(avg_goal_score) >= 50  then 'Moderate progress'
        when avg(avg_goal_score) >= 25  then 'Limited progress'
        else                                 'Off track'
    end                                      as goal_status

from {{ ref('int_africa_sdg_scores') }}
group by goal, goal_title, sdg_pillar
order by goal
