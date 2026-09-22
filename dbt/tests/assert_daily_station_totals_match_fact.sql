-- Departures in the daily aggregate must sum back to the fact row count.
-- Catches aggregation bugs in the full outer join between departures/arrivals.

with agg as (
    select sum(trips_started) as n from {{ ref('agg_daily_station') }}
),

fact as (
    select count(*) as n from {{ ref('fct_trips') }}
)

select
    a.n as agg_trips_started,
    f.n as fact_rows,
    a.n - f.n as difference
from agg a
cross join fact f
where a.n != f.n
