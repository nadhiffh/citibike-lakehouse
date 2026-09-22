"""Dagster wiring: monthly-partitioned ingest feeding the dbt model graph.

Note: no `from __future__ import annotations` here. Dagster inspects the
`context` parameter's annotation at runtime, and postponed evaluation turns it
into a string, which fails its type check.
"""

import os
import subprocess
import sys
from pathlib import Path

from typing import Any, Mapping

import duckdb
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MonthlyPartitionsDefinition,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from pipeline.ingest import LANDING_DIR
from pipeline.ingest import run as run_ingest

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt"

# dbt resolves the landing zone through this variable, so it must be set before
# any dbt invocation regardless of the working directory Dagster runs from.
os.environ.setdefault("CITIBIKE_ROOT", str(ROOT))

# Dagster resolves `dbt` from PATH, which misses the venv when the process is
# launched via an absolute interpreter path (as `dagster dev` does). Prepending
# the interpreter's own bin directory fixes every dbt invocation, including the
# DbtCliResource that DbtProject.prepare_if_dev() constructs internally.
_BIN = str(Path(sys.executable).parent)
if _BIN not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = _BIN + os.pathsep + os.environ.get("PATH", "")

DBT_EXECUTABLE = str(Path(_BIN) / "dbt")

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)

# prepare_if_dev() only regenerates the manifest under `dagster dev`, so the
# plain CLI fails on a fresh clone or after `make clean`. Parse unconditionally
# when the manifest is absent so any entry point works from scratch.
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():
    subprocess.run(
        [DBT_EXECUTABLE, "parse", "--quiet"],
        cwd=DBT_DIR,
        env={**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
        check=True,
    )

# Citi Bike publishes per-calendar-month files.
monthly = MonthlyPartitionsDefinition(start_date="2024-01-01", fmt="%Y-%m-%d")


@asset(
    partitions_def=monthly,
    compute_kind="python",
    description="Download one month of trips and land it as Parquet.",
)
def landed_trips(context: AssetExecutionContext) -> None:
    month = context.partition_key[:7]  # YYYY-MM-DD -> YYYY-MM
    out = run_ingest(month)
    context.add_output_metadata(
        {
            "path": str(out),
            "month": month,
            "size_mb": round(out.stat().st_size / 1_048_576, 1),
        }
    )


@asset_check(asset=landed_trips, blocking=True)
def landed_partition_is_readable(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Fail fast if a landed partition is missing, empty, or off-month.

    Blocking, so a corrupt partition stops the dbt graph from running against
    it rather than producing marts nobody can trust.
    """
    month = context.partition_key[:7].replace("-", "")
    path = LANDING_DIR / f"month={month}" / "trips.parquet"

    if not path.exists():
        return AssetCheckResult(
            passed=False, metadata={"reason": f"missing partition: {path}"}
        )

    con = duckdb.connect()
    try:
        n_rows, off_month = con.execute(
            f"""
            select
                count(*),
                count(*) filter (where strftime(started_at, '%Y%m') != '{month}')
            from read_parquet('{path}')
            """
        ).fetchone()
    finally:
        con.close()

    return AssetCheckResult(
        passed=n_rows > 0 and off_month == 0,
        metadata={"rows": n_rows, "rows_outside_month": off_month},
    )


class CitibikeDbtTranslator(DagsterDbtTranslator):
    """Map the dbt source `landing.trips` onto the `landed_trips` asset.

    Without this the source becomes its own standalone asset with no edge to
    the ingest step, so Dagster considers them independent and runs dbt
    concurrently with (or before) ingestion. That passes locally whenever a
    previous run left Parquet behind, and fails on a clean checkout with
    "No files found that match ...". Pointing the source at the ingest asset
    gives the graph a real dependency and a deterministic order.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["source_name"] == "landing"
            and dbt_resource_props["name"] == "trips"
        ):
            return landed_trips.key
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CitibikeDbtTranslator(),
    partitions_def=monthly,
)
def citibike_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Staging, intermediate and mart models, plus their tests.

    `dbt build` interleaves tests with models so a failing test halts the
    downstream graph instead of publishing bad data to the marts.
    """
    yield from dbt.cli(["build"], context=context).stream()


monthly_refresh_job = define_asset_job(
    name="monthly_refresh",
    selection=AssetSelection.all(),
    partitions_def=monthly,
)

# Citi Bike publishes a month's file during the first week of the following
# month, so running on the 1st would reliably 404. The 5th gives the upstream
# publisher room, and Dagster's default partition for a monthly schedule is the
# month that just ended, which is exactly the one now available.
monthly_refresh_schedule = ScheduleDefinition(
    name="monthly_refresh_schedule",
    job=monthly_refresh_job,
    cron_schedule="0 6 5 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[landed_trips, citibike_dbt_assets],
    asset_checks=[landed_partition_is_readable],
    jobs=[monthly_refresh_job],
    schedules=[monthly_refresh_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_DIR),
            profiles_dir=str(DBT_DIR),
            dbt_executable=DBT_EXECUTABLE,
        ),
    },
)
