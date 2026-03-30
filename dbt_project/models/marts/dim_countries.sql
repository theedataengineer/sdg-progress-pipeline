-- dim_countries
-- Country dimension with region, income group and SDG scores

with country_meta as (
    select * from {{ ref('stg_wb_country_metadata') }}
),

-- aggregate SDG scores per country across all goals
country_scores as (
    select
        country_code,
        round(avg(avg_goal_score)::numeric, 1)   as overall_sdg_score,
        sum(indicators_achieved)                  as total_indicators_achieved,
        sum(indicators_on_track)                  as total_indicators_on_track,
        sum(indicators_off_track)                 as total_indicators_off_track,
        max(data_as_of)                           as data_as_of
    from {{ ref('int_africa_sdg_scores') }}
    group by country_code
)

select
    m.country_code,
    m.country_name,
    m.region,
    m.income_group,
    m.income_tier,
    m.continent,
    m.is_african,
    s.overall_sdg_score,
    s.total_indicators_achieved,
    s.total_indicators_on_track,
    s.total_indicators_off_track,
    s.data_as_of
from country_meta m
left join country_scores s
    on m.country_code = s.country_code
