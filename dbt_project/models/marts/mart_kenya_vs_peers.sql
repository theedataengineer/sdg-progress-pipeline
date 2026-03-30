-- mart_kenya_vs_peers
-- Kenya compared to East African peers
-- Answers analytics question 4

with east_africa as (
    select
        s.*,
        c.region,
        c.income_group
    from {{ ref('int_africa_sdg_scores') }} s
    join {{ ref('dim_countries') }} c
        on s.country_code = c.country_code
    where s.country_code in ('KEN','TZA','UGA','RWA','ETH','SSD')
)

select
    country_code,
    country_name,
    goal,
    goal_title,
    sdg_pillar,
    avg_goal_score,
    indicators_achieved,
    indicators_on_track,
    indicators_off_track,
    income_group,
    data_as_of,

    -- rank within East Africa for each goal
    rank() over (
        partition by goal
        order by avg_goal_score desc
    )                                        as ea_rank,

    -- vs East African average
    round((
        avg_goal_score - avg(avg_goal_score) over (partition by goal)
    )::numeric, 1)                           as vs_ea_average

from east_africa
