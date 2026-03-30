-- stg_kafka_sdg_stream
-- Cleans streaming records that arrived via Kafka
-- Source: raw_kafka_sdg_stream (loaded by kafka_consumer_dag)

with source as (
    select * from {{ source('raw', 'raw_kafka_sdg_stream') }}
),

cleaned as (
    select
        record_id,
        goal,
        indicator_code,
        trim(indicator_name)             as indicator_name,
        upper(trim(country_code))        as country_code,
        trim(country_name)               as country_name,
        year::integer                    as year,
        round(value::numeric, 4)         as value,
        coalesce(unit, '')               as unit,
        coalesce(obs_status, '')         as obs_status,
        source,
        extracted_at,
        published_at,
        batch_file,
        created_at,

        -- how long did it take from extraction to landing in postgres?
        extract(epoch from (created_at - extracted_at::timestamptz))
            / 60.0                       as pipeline_latency_minutes

    from source
    where
        record_id is not null
        and country_code is not null
        and year between 2000 and 2030
        and value is not null
)

select * from cleaned
