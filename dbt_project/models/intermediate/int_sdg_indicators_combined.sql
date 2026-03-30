-- int_sdg_indicators_combined
-- Merges World Bank batch + Kafka streaming data into one clean table
-- Deduplicates on country + indicator + year, preferring batch data

with batch as (
    select
        indicator_code,
        country_code,
        country_name,
        goal,
        indicator_name,
        year,
        value,
        unit,
        obs_status,
        source,
        extracted_at,
        is_african_country,
        sdg_milestone,
        'batch'                          as data_source
    from {{ ref('stg_wb_sdg_indicators') }}
),

streaming as (
    select
        indicator_code,
        country_code,
        country_name,
        null::integer                    as goal,
        indicator_name,
        year,
        value,
        unit,
        obs_status,
        source,
        extracted_at,
        true                             as is_african_country,
        case
            when year = 2015 then 'baseline'
            when year = 2020 then 'midpoint'
            else 'intermediate'
        end                              as sdg_milestone,
        'streaming'                      as data_source
    from {{ ref('stg_kafka_sdg_stream') }}
),

-- union batch and streaming, batch takes priority on conflict
combined as (
    select * from batch
    union all
    -- only include streaming records not already in batch
    select s.* from streaming s
    where not exists (
        select 1 from batch b
        where b.country_code   = s.country_code
        and   b.indicator_code = s.indicator_code
        and   b.year           = s.year
    )
),

-- join with goal metadata to fill any missing goal numbers
enriched as (
    select
        c.indicator_code,
        c.country_code,
        c.country_name,
        coalesce(c.goal, g.goal)         as goal,
        g.goal_title,
        g.sdg_pillar,
        c.indicator_name,
        c.year,
        c.value,
        c.unit,
        c.obs_status,
        c.source,
        c.extracted_at,
        c.is_african_country,
        c.sdg_milestone,
        c.data_source
    from combined c
    left join {{ ref('stg_sdg_goal_metadata') }} g
        on c.goal = g.goal
)

select * from enriched
