{{ config(materialized='table') }}

-- Grain: one row per station_id.
-- Coordinates are medians because the source reports the bike's GPS position
-- rather than the dock's, so values vary by a few metres between trips.

with observations as (

    select * from {{ ref('int_station_observations') }}

),

aggregated as (

    select
        station_id,
        mode(station_name)          as station_name,
        median(lat)                 as latitude,
        median(lng)                 as longitude,
        count(*)                    as observation_count
    from observations
    where lat is not null
      and lng is not null
    group by station_id

)

select
    station_id,
    station_name,
    latitude,
    longitude,
    observation_count,
    -- System and Jersey City stations use non-numeric ids; worth keeping
    -- visible because they behave differently from the numbered NYC docks.
    case
        when station_id like 'SYS%' then 'system'
        when station_id like 'JC%'  then 'jersey_city'
        else 'nyc'
    end as station_group
from aggregated

union all

-- Sentinel member for trips with no station recorded. Lets the fact table keep
-- every row while its station foreign keys stay strictly valid.
select
    'UNKNOWN'   as station_id,
    'Unknown'   as station_name,
    null        as latitude,
    null        as longitude,
    0           as observation_count,
    'unknown'   as station_group
