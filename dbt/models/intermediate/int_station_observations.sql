{{ config(materialized='view') }}

-- Stations are not published as a separate feed, so they are derived from both
-- endpoints of every trip. Coordinates drift slightly between trips for the
-- same station, so the dimension takes a median per station downstream.

with starts as (

    select
        start_station_id   as station_id,
        start_station_name as station_name,
        start_lat          as lat,
        start_lng          as lng
    from {{ ref('stg_trips') }}
    where start_station_id is not null

),

ends as (

    select
        end_station_id   as station_id,
        end_station_name as station_name,
        end_lat          as lat,
        end_lng          as lng
    from {{ ref('stg_trips') }}
    where end_station_id is not null

)

select * from starts
union all
select * from ends
