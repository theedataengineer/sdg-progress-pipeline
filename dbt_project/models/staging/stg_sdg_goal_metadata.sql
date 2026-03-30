-- stg_sdg_goal_metadata
-- SDG goal reference data — 17 goals with titles and descriptions
-- Source: raw_sdg_goal_metadata (seeded manually)

with source as (
    select * from {{ source('raw', 'raw_sdg_goal_metadata') }}
)

select
    goal,
    trim(goal_title)                     as goal_title,
    trim(goal_description)               as goal_description,
    target_year,

    -- group goals into thematic pillars
    case
        when goal in (1, 2, 3, 4, 5)    then 'People'
        when goal in (6, 7, 8, 9, 10)   then 'Prosperity'
        when goal in (11, 12, 13, 14, 15) then 'Planet'
        when goal in (16, 17)            then 'Peace & Partnership'
        else 'Unknown'
    end                                  as sdg_pillar

from source
order by goal
