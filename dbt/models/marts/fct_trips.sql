{{ config(materialized='table') }}

-- Grain: one row per ride_id.
-- Every trip is kept. Quality problems are exposed as an `is_valid_duration`
-- flag rather than deleted, so row counts reconcile back to the source and the
-- defect rate stays measurable over time.

with trips as (

    select * from {{ ref('stg_trips') }}

)

select
    ride_id,

    -- Unknown stations get a sentinel key so joins to dim_station stay inner
    -- and fact row counts never silently shrink.
    coalesce(start_station_id, 'UNKNOWN')   as start_station_key,
    coalesce(end_station_id, 'UNKNOWN')     as end_station_key,

    cast(started_at as date)                as trip_date,
    started_at,
    ended_at,
    duration_seconds,
    round(duration_seconds / 60.0, 2)       as duration_minutes,

    bike_type,
    rider_type,
    extract(hour from started_at)           as start_hour,
    dayname(started_at)                     as day_name,
    extract(isodow from started_at) >= 6    as is_weekend,

    start_station_id is not null
        and end_station_id is not null
        and start_station_id = end_station_id as is_round_trip,

    not is_implausible_duration
        and not is_nonpositive_duration      as is_valid_duration,

    is_missing_start_station,
    is_missing_end_station,
    is_implausible_duration

from trips
