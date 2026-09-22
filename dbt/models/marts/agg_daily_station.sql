{{ config(materialized='table') }}

-- Grain: one row per station per day. The table most dashboards actually hit,
-- pre-aggregated so the BI layer never scans the full fact.

with departures as (

    select
        start_station_key as station_key,
        trip_date,
        count(*)                                                  as trips_started,
        avg(duration_minutes) filter (where is_valid_duration)     as avg_duration_minutes,
        count(*) filter (where rider_type = 'member')              as member_trips,
        count(*) filter (where rider_type = 'casual')              as casual_trips,
        count(*) filter (where bike_type = 'electric_bike')        as electric_trips
    from {{ ref('fct_trips') }}
    group by 1, 2

),

arrivals as (

    select
        end_station_key as station_key,
        trip_date,
        count(*)        as trips_ended
    from {{ ref('fct_trips') }}
    group by 1, 2

)

select
    coalesce(d.station_key, a.station_key)          as station_key,
    coalesce(d.trip_date, a.trip_date)              as trip_date,
    s.station_name,
    s.station_group,
    coalesce(d.trips_started, 0)                    as trips_started,
    coalesce(a.trips_ended, 0)                      as trips_ended,
    coalesce(a.trips_ended, 0)
        - coalesce(d.trips_started, 0)              as net_flow,
    round(d.avg_duration_minutes, 2)                as avg_duration_minutes,
    coalesce(d.member_trips, 0)                     as member_trips,
    coalesce(d.casual_trips, 0)                     as casual_trips,
    coalesce(d.electric_trips, 0)                   as electric_trips
from departures d
full outer join arrivals a
    on d.station_key = a.station_key
   and d.trip_date   = a.trip_date
left join {{ ref('dim_station') }} s
    on coalesce(d.station_key, a.station_key) = s.station_id
