-- stg_wb_country_metadata
-- Country reference data with region and income group
-- Source: raw_wb_country_metadata (seeded manually)

with source as (
    select * from {{ source('raw', 'raw_wb_country_metadata') }}
)

select
    country_code,
    trim(country_name)                   as country_name,
    trim(region)                         as region,
    trim(income_group)                   as income_group,
    continent,
    is_african,

    -- simplified income tier for dashboard grouping
    case income_group
        when 'Low income'          then 1
        when 'Lower middle income' then 2
        when 'Upper middle income' then 3
        when 'High income'         then 4
        else 0
    end                                  as income_tier

from source
