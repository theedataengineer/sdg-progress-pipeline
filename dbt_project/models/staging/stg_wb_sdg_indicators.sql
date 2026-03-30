-- stg_wb_sdg_indicators
-- Cleans and standardises World Bank batch indicator data
-- Source: raw_wb_sdg_indicators (loaded by batch_ingestion_dag)

with source as (
    select * from {{ source('raw', 'raw_wb_sdg_indicators') }}
),

cleaned as (
    select
        -- identifiers
        indicator_code,
        upper(trim(country_code))                    as country_code,
        trim(country_name)                           as country_name,

        -- SDG classification
        goal,
        trim(indicator_name)                         as indicator_name,

        -- time
        year::integer                                as year,

        -- value — cast and handle edge cases
        case
            when value is null then null
            when value < 0 and indicator_code != 'NY.GDP.MKTP.KD.ZG'
                then null   -- negative values invalid except GDP growth
            else round(value::numeric, 4)
        end                                          as value,

        -- metadata
        coalesce(unit, '')                           as unit,
        coalesce(obs_status, '')                     as obs_status,
        source,
        extracted_at,
        created_at,

        -- derived flags
        case
            when country_code in (
                'DZA','AGO','BEN','BWA','BFA','BDI','CPV','CMR','CAF','TCD',
                'COM','COD','COG','CIV','DJI','EGY','GNQ','ERI','SWZ','ETH',
                'GAB','GMB','GHA','GIN','GNB','KEN','LSO','LBR','LBY','MDG',
                'MWI','MLI','MRT','MUS','MAR','MOZ','NAM','NER','NGA','RWA',
                'STP','SEN','SLE','SOM','ZAF','SSD','SDN','TZA','TGO','TUN',
                'UGA','ZMB','ZWE','SHN'
            ) then true
            else false
        end                                          as is_african_country,

        -- SDG milestone years
        case
            when year = 2015 then 'baseline'
            when year = 2020 then 'midpoint'
            when year = 2030 then 'target'
            else 'intermediate'
        end                                          as sdg_milestone

    from source
    where
        country_code is not null
        and country_code != ''
        and year between 2000 and 2030
        and value is not null
)

select * from cleaned
