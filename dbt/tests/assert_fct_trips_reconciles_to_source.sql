-- Fails if the fact table and the landed source disagree on row count.
-- Guards against the most damaging silent failure in a batch pipeline: a join
-- fanning out or a filter quietly dropping trips. Returns rows only on drift.

with source_count as (
    select count(*) as n from {{ source('landing', 'trips') }}
),

fact_count as (
    select count(*) as n from {{ ref('fct_trips') }}
)

select
    s.n as source_rows,
    f.n as fact_rows,
    f.n - s.n as difference
from source_count s
cross join fact_count f
where s.n != f.n
