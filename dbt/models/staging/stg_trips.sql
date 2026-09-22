{{ config(materialized='view') }}

-- Typing and light cleaning only. No rows are dropped here; defects are
-- flagged so downstream models can decide what to exclude, and so the
-- quality tests below have something to assert against.

with source as (

    select * from {{ source('landing', 'trips') }}

),

renamed as (

    select
        ride_id,
        rideable_type                                    as bike_type,
        member_casual                                    as rider_type,
        partition_month,

        started_at,
        ended_at,
        date_diff('second', started_at, ended_at)        as duration_seconds,

        nullif(trim(start_station_name), '')             as start_station_name,
        nullif(trim(start_station_id), '')                as start_station_id,
        nullif(trim(end_station_name), '')               as end_station_name,
        nullif(trim(end_station_id), '')                  as end_station_id,

        start_lat,
        start_lng,
        end_lat,
        end_lng

    from source

)

select
    *,
    start_station_id is null                             as is_missing_start_station,
    end_station_id is null                               as is_missing_end_station,
    end_lat is null or end_lng is null                   as is_missing_end_coords,
    duration_seconds > 86400                             as is_implausible_duration,
    duration_seconds <= 0                                as is_nonpositive_duration
from renamed
