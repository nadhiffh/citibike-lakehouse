-- Every trip date must exist in dim_date, and the spine must be contiguous.
-- A gap here silently drops days from any time-series report built on it.

with missing_dates as (

    select distinct f.trip_date as offending_date, 'missing from dim_date' as reason
    from {{ ref('fct_trips') }} f
    left join {{ ref('dim_date') }} d on f.trip_date = d.date_day
    where d.date_day is null

),

gaps as (

    select date_day as offending_date, 'gap in spine' as reason
    from (
        select
            date_day,
            lead(date_day) over (order by date_day) as next_day
        from {{ ref('dim_date') }}
    )
    where next_day is not null
      and date_diff('day', date_day, next_day) != 1

)

select * from missing_dates
union all
select * from gaps
