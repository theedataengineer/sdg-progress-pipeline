-- int_africa_sdg_scores
-- Calculates composite SDG progress scores for African countries
-- Score 0-100 where 100 = target achieved, 0 = no progress

with gaps as (
    select * from {{ ref('int_sdg_target_gaps') }}
    where is_african_country = true
    and   target_value is not null
),

scored as (
    select
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        goal,
        goal_title,
        sdg_pillar,
        latest_year,
        latest_value,
        baseline_value,
        target_value,
        target_status,
        direction,

        -- progress score 0-100
        case
            when target_status = 'achieved' then 100.0
            when baseline_value is null      then null
            when direction = 'lower' then
                case
                    when baseline_value <= target_value then 100.0
                    else greatest(0, least(100,
                        round((
                            (baseline_value - latest_value)
                            / nullif(baseline_value - target_value, 0)
                            * 100
                        )::numeric, 1)
                    ))
                end
            when direction = 'higher' then
                case
                    when baseline_value >= target_value then 100.0
                    else greatest(0, least(100,
                        round((
                            (latest_value - baseline_value)
                            / nullif(target_value - baseline_value, 0)
                            * 100
                        )::numeric, 1)
                    ))
                end
            else null
        end                                  as progress_score

    from gaps
)

select
    country_code,
    country_name,
    goal,
    goal_title,
    sdg_pillar,
    count(indicator_code)                    as indicators_tracked,
    round(avg(progress_score)::numeric, 1)   as avg_goal_score,
    sum(case when target_status = 'achieved'  then 1 else 0 end)
                                             as indicators_achieved,
    sum(case when target_status = 'on track'  then 1 else 0 end)
                                             as indicators_on_track,
    sum(case when target_status = 'off track' then 1 else 0 end)
                                             as indicators_off_track,
    max(latest_year)                         as data_as_of

from scored
where progress_score is not null
group by
    country_code, country_name,
    goal, goal_title, sdg_pillar
