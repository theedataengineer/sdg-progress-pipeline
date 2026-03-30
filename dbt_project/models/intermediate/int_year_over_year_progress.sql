-- int_year_over_year_progress
-- Calculates annual change for each indicator per country
-- Used for trend analysis and progress rate dashboards

with base as (
    select
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        goal,
        goal_title,
        year,
        value,
        is_african_country
    from {{ ref('int_sdg_indicators_combined') }}
    where value is not null
),

with_lag as (
    select
        *,
        lag(value) over (
            partition by country_code, indicator_code
            order by year
        )                                as previous_year_value,

        lag(year) over (
            partition by country_code, indicator_code
            order by year
        )                                as previous_year
    from base
),

calculated as (
    select
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        goal,
        goal_title,
        year,
        value,
        previous_year_value,
        previous_year,
        is_african_country,

        -- absolute change from previous year
        round((value - previous_year_value)::numeric, 4)
                                         as absolute_change,

        -- percentage change from previous year
        case
            when previous_year_value is null then null
            when previous_year_value = 0     then null
            else round(
                ((value - previous_year_value) / abs(previous_year_value) * 100)::numeric,
                2
            )
        end                              as pct_change,

        -- years between measurements (not always 1 — data gaps exist)
        year - previous_year             as years_elapsed,

        -- annualised change rate accounting for gaps
        case
            when previous_year_value is null then null
            when previous_year_value = 0     then null
            when (year - previous_year) = 0  then null
            else round(
                ((value - previous_year_value) / abs(previous_year_value) * 100
                 / (year - previous_year))::numeric,
                2
            )
        end                              as annualised_pct_change

    from with_lag
)

select * from calculated
where previous_year_value is not null
