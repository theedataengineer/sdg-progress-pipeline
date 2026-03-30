-- int_sdg_target_gaps
-- Calculates how far each country is from 2030 SDG targets
-- Uses 2015 baseline and current trajectory to project 2030 outcome

with latest_values as (
    -- get the most recent value for each country + indicator
    select distinct on (country_code, indicator_code)
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        goal,
        goal_title,
        sdg_pillar,
        year                             as latest_year,
        value                            as latest_value,
        is_african_country
    from {{ ref('int_sdg_indicators_combined') }}
    where value is not null
    order by country_code, indicator_code, year desc
),

baseline_2015 as (
    -- get the 2015 baseline value for each country + indicator
    select
        country_code,
        indicator_code,
        value                            as baseline_value,
        year                             as baseline_year
    from {{ ref('int_sdg_indicators_combined') }}
    where year = 2015
    and   value is not null
),

-- 2030 target values per indicator
-- these are the official UN SDG targets
target_values as (
    select * from (values
        ('SI.POV.DDAY',     0.0,   'lower'),  -- zero poverty
        ('SH.DYN.MORT',    25.0,   'lower'),  -- under-5 mortality < 25 per 1000
        ('SH.MMR.RISK',     0.0,   'lower'),  -- reduce maternal mortality
        ('SE.PRM.ENRR',   100.0,   'higher'), -- 100% primary enrollment
        ('SE.ADT.LITR.ZS', 100.0,  'higher'), -- 100% literacy
        ('SG.GEN.PARL.ZS',  50.0,  'higher'), -- 50% women in parliament
        ('EG.ELC.ACCS.ZS', 100.0,  'higher'), -- universal electricity access
        ('SL.UEM.TOTL.ZS',  4.0,   'lower'),  -- < 4% unemployment
        ('EN.ATM.CO2E.PC',  2.0,   'lower'),  -- reduce CO2 per capita
        ('SH.H2O.BASW.ZS', 100.0,  'higher'), -- universal water access
        ('SH.STA.BASS.ZS', 100.0,  'higher')  -- universal sanitation
    ) as t(indicator_code, target_value, direction)
),

combined as (
    select
        l.country_code,
        l.country_name,
        l.indicator_code,
        l.indicator_name,
        l.goal,
        l.goal_title,
        l.sdg_pillar,
        l.latest_year,
        l.latest_value,
        l.is_african_country,
        b.baseline_value,
        b.baseline_year,
        t.target_value,
        t.direction,

        -- gap between current value and 2030 target
        case
            when t.target_value is null then null
            else round((l.latest_value - t.target_value)::numeric, 4)
        end                              as gap_to_target,

        -- progress made since 2015 baseline
        case
            when b.baseline_value is null then null
            else round((l.latest_value - b.baseline_value)::numeric, 4)
        end                              as progress_since_2015,

        -- on track? (simplified — positive progress in right direction)
        case
            when t.target_value is null      then 'no target defined'
            when t.direction = 'lower'
                and l.latest_value <= t.target_value then 'achieved'
            when t.direction = 'higher'
                and l.latest_value >= t.target_value then 'achieved'
            when t.direction = 'lower'
                and l.latest_value < b.baseline_value then 'on track'
            when t.direction = 'higher'
                and l.latest_value > b.baseline_value then 'on track'
            else 'off track'
        end                              as target_status,

        -- years remaining to 2030
        2030 - l.latest_year             as years_to_2030

    from latest_values l
    left join baseline_2015 b
        on l.country_code   = b.country_code
        and l.indicator_code = b.indicator_code
    left join target_values t
        on l.indicator_code = t.indicator_code
)

select * from combined
