# Citi Bike Lakehouse

Batch analytics pipeline over NYC Citi Bike trip data. Dagster orchestrates
monthly ingestion into a DuckDB lakehouse; dbt builds a tested dimensional
model on top.

Verified on 2024-01 and 2024-02: 4,008,943 trips, 45 dbt tests passing.

![ci](https://github.com/nadhifhafiz/citibike-lakehouse/actions/workflows/ci.yml/badge.svg)

## Architecture

```
S3 (tripdata.zip)
      |
      v
[Dagster] landed_trips          monthly-partitioned asset
      |                         data/landing/month=YYYYMM/trips.parquet
      v
[dbt]  stg_trips                typing + defect flags       (view)
      |
      +--> int_station_observations                          (view)
      |         |
      |         v
      |    dim_station                                       (table)
      |
      +--> fct_trips ----+--> dim_date                       (table)
                          |
                          +--> agg_daily_station             (table)
```

Everything runs locally against a single DuckDB file. No cloud account needed.

## Why these choices

**DuckDB as the warehouse.** Single file, no server, reads Parquet directly.
Swapping to Snowflake or BigQuery means editing `dbt/profiles.yml` and nothing
else, because no model contains engine-specific DDL.

**Parquet landing zone.** Decouples extraction from transformation, so the
model layer can be rebuilt without re-downloading ~370MB per month.

**Monthly partitions.** The published files are per calendar month, so the
partition grain matches the source grain. Reruns overwrite one month in place.

**Flag defects, don't delete them.** Bad rows carry boolean flags rather than
being filtered out, so the fact table reconciles 1:1 with the source and the
defect rate stays measurable over time. Consumers opt out via
`where is_valid_duration`.

## Data quality findings

Profiled from the real source, not assumed. Measured on 2024-01
(1,888,085 raw rows):

| Issue | Volume | Handling |
|---|---|---|
| Rows belonging to a neighbouring month | 410 in Jan, 233 in Feb | Filtered at ingest on the partition boundary |
| Null start station and coordinates | 1,160 | Flagged, routed to `UNKNOWN` station key |
| Null end station and coordinates | 5,459 | Flagged, routed to `UNKNOWN` station key |
| Trips longer than 24h | 571 | `is_valid_duration = false`, excluded from duration averages |
| Non-numeric station IDs (`SYS033`, `JC009`, `6173.08_Pillar`) | n/a | Pinned to VARCHAR; DuckDB's type inference samples numeric-looking rows and then fails on the rest |
| Station coordinates drift between trips | all stations | Median per station, since the feed reports the bike's GPS, not the dock's |

Two findings that shaped the model:

- Station IDs have no name collisions, so `dim_station` can key on ID safely.
- There are no zero or negative durations in this data, but the flag is kept
  because it costs nothing and the source makes no guarantee.

## Tests

45 tests run as part of `dbt build`, so a failure halts the graph before bad
data reaches the marts. Beyond the usual uniqueness, not-null, accepted-values
and referential checks, four are worth calling out:

- `assert_fct_trips_reconciles_to_source` — fact row count must equal the landed
  source count. Catches the most damaging silent failure in a batch pipeline: a
  join fanning out or a filter quietly dropping rows.
- `assert_daily_station_totals_match_fact` — departures in the daily aggregate
  must sum back to the fact row count, guarding the full outer join.
- `assert_dim_date_has_no_gaps` — the date spine must be contiguous and cover
  every trip date, so no day silently vanishes from a time series.
- `assert_defect_flags_are_consistent` — each defect flag must agree with the
  condition it claims to describe, so refactoring `stg_trips` cannot silently
  invert one and quietly change what the marts mean.

The reconciliation test was verified against a deliberately truncated fact
table to confirm it actually fails on drift rather than passing vacuously.

On top of the dbt tests, a **blocking Dagster asset check** validates each
landed partition (non-empty, no off-month rows) before dbt reads it, so a
corrupt partition halts the run instead of producing marts nobody can trust.

CI runs the whole thing — ingest, build, all 45 tests, plus a reconciliation
assertion — on every push and pull request.

## Scheduling

`monthly_refresh_schedule` runs at 06:00 America/New_York on the 5th of each
month. Not the 1st: Citi Bike publishes a month's file during the first week of
the following month, so a run on the 1st would reliably 404 on a file that does
not exist yet.

It ships with `default_status=STOPPED` so cloning the repo doesn't start doing
work unasked. Enable it in the Dagster UI, or flip it to `RUNNING`.

## Setup

```bash
make setup
```

`dbt-core` builds a dependency that downloads over HTTPS, so it needs a Python
with TLS root certificates. A `uv`-managed interpreter works. The python.org
macOS build ships without a certificate bundle and fails with
`CERTIFICATE_VERIFY_FAILED` during install.

## Run

```bash
make run MONTH=2024-01     # ingest + build + test, one month, via Dagster
make run MONTH=2024-02     # add another month
make ui                    # Dagster UI at localhost:3000
make build                 # rebuild models only
make clean                 # drop warehouse and artifacts
```

`make run` is idempotent: rerunning a month leaves row counts unchanged.

## Implementation notes

Two environment-specific details that cost real debugging time:

- `pipeline/definitions.py` deliberately omits `from __future__ import
  annotations`. Dagster inspects the `context` parameter's annotation at
  runtime, and postponed evaluation turns it into a string, which fails its
  type check with a misleading "must be annotated with AssetExecutionContext"
  error.
- The venv's `bin` is prepended to `PATH` at import time. Dagster resolves
  `dbt` from `PATH`, which misses the venv when launched via an absolute
  interpreter path, and `DbtProject.prepare_if_dev()` constructs its own
  `DbtCliResource` that cannot be configured directly.

dbt reads the landing zone through `CITIBIKE_ROOT` because `project_root` is
not exposed in dbt's Jinja context and macros do not render inside source
`meta`. The Makefile and Dagster module both set it.

## Extending this

- More months: `make run MONTH=YYYY-MM`. The partition design handles it.
- Incremental `fct_trips` once the data outgrows a full refresh.
- Weather join (NOAA GHCN) to explain demand variance.
- Snapshots on `dim_station` for SCD Type 2 history as stations move.
