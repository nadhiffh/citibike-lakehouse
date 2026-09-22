{{ config(materialized='table') }}

-- Grain: one row per calendar date spanned by the loaded partitions.
-- Generated from the fact's own date range rather than a fixed window, so it
-- always covers exactly what has been ingested and never more.

with bounds as (

    select
        min(trip_date) as min_date,
        max(trip_date) as max_date
    from {{ ref('fct_trips') }}

),

spine as (

    select unnest(
        generate_series(
            (select min_date from bounds),
            (select max_date from bounds),
            interval 1 day
        )
    )::date as date_day

)

select
    date_day,
    extract(year    from date_day)        as year,
    extract(month   from date_day)        as month,
    extract(day     from date_day)        as day_of_month,
    extract(isodow  from date_day)        as iso_day_of_week,
    dayname(date_day)                     as day_name,
    monthname(date_day)                   as month_name,
    extract(week    from date_day)        as iso_week,
    extract(isodow  from date_day) >= 6   as is_weekend,
    strftime(date_day, '%Y-%m')           as year_month
from spine
