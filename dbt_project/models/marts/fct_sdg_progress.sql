-- fct_sdg_progress
-- Core fact table: one row per country, indicator, and year
-- This is the primary table powering the SDG dashboard

select
    -- natural key
    country_code,
    indicator_code,
    year,

    -- descriptive
    country_name,
    indicator_name,
    goal,
    goal_title,
    sdg_pillar,

    -- measurement
    value,
    unit,
    data_source,
    is_african_country,
    sdg_milestone,
    extracted_at

from {{ ref('int_sdg_indicators_combined') }}
