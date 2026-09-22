-- The defect flags are a contract: the marts and the README both rely on them.
-- This asserts each flag actually agrees with the condition it claims to
-- describe, so a refactor of stg_trips cannot silently invert one.

with checks as (

    select 'start flag disagrees with station id' as reason, count(*) as n
    from {{ ref('stg_trips') }}
    where is_missing_start_station != (start_station_id is null)

    union all
    select 'end flag disagrees with station id', count(*)
    from {{ ref('stg_trips') }}
    where is_missing_end_station != (end_station_id is null)

    union all
    select 'implausible flag disagrees with 24h rule', count(*)
    from {{ ref('stg_trips') }}
    where is_implausible_duration != (duration_seconds > 86400)

    union all
    select 'nonpositive flag disagrees with duration', count(*)
    from {{ ref('stg_trips') }}
    where is_nonpositive_duration != (duration_seconds <= 0)

    -- fct_trips.is_valid_duration must be the negation of both defect flags.
    union all
    select 'fct is_valid_duration disagrees with stg flags', count(*)
    from {{ ref('fct_trips') }}
    where is_valid_duration != (
        not is_implausible_duration and duration_seconds > 0
    )

)

select * from checks where n > 0
