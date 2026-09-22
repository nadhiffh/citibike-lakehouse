"""Download a month of Citi Bike trips and land it as Parquet.

Idempotent: re-running a month overwrites exactly that month's partition.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

import duckdb

SOURCE_URL = "https://s3.amazonaws.com/tripdata/{yyyymm}-citibike-tripdata.zip"

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
LANDING_DIR = ROOT / "data" / "landing"

# start_station_id holds values like 'SYS033', 'JC009', '6173.08_Pillar'.
# Auto-detection samples only numeric-looking rows and then fails on the rest,
# so these two columns must be pinned to VARCHAR.
COLUMN_TYPES = {
    "start_station_id": "VARCHAR",
    "end_station_id": "VARCHAR",
}


def download(yyyymm: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dest = RAW_DIR / f"{yyyymm}-citibike-tripdata.zip"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  cached: {dest.name}")
        return dest

    url = SOURCE_URL.format(yyyymm=yyyymm)
    print(f"  downloading {url}")
    tmp = dest.with_suffix(".zip.part")
    with urllib.request.urlopen(url, timeout=600) as resp, tmp.open("wb") as fh:
        shutil.copyfileobj(resp, fh)
    tmp.rename(dest)
    return dest


def extract(archive: Path) -> list[Path]:
    """Unzip to a per-month folder and return the CSV members."""
    target = RAW_DIR / archive.stem
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zf:
        members = [
            m
            for m in zf.namelist()
            if m.lower().endswith(".csv") and not Path(m).name.startswith("._")
        ]
        for m in members:
            out = target / Path(m).name
            if not out.exists():
                with zf.open(m) as src, out.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
    csvs = sorted(target.glob("*.csv"))
    if not csvs:
        raise RuntimeError(f"no CSV members found in {archive}")
    print(f"  extracted {len(csvs)} csv file(s)")
    return csvs


def land(yyyymm: str, csv_dir: Path) -> Path:
    """Convert CSVs to a single Parquet partition, trimming stray months.

    The published file for a month contains a few hundred rows that belong to
    the neighbouring month. Filtering on the partition boundary keeps the
    partition self-consistent so reruns stay deterministic.
    """
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    out = LANDING_DIR / f"month={yyyymm}" / "trips.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    year, month = int(yyyymm[:4]), int(yyyymm[4:])
    period_start = f"{year:04d}-{month:02d}-01"
    nxt_y, nxt_m = (year + 1, 1) if month == 12 else (year, month + 1)
    period_end = f"{nxt_y:04d}-{nxt_m:02d}-01"

    con = duckdb.connect()
    types = ", ".join(f"'{k}': '{v}'" for k, v in COLUMN_TYPES.items())
    src = (
        f"read_csv('{csv_dir}/*.csv', types={{{types}}}, "
        f"timestampformat='%Y-%m-%d %H:%M:%S.%f')"
    )

    total, kept = con.execute(
        f"""
        select
            count(*),
            count(*) filter (
                where started_at >= timestamp '{period_start}'
                  and started_at <  timestamp '{period_end}'
            )
        from {src}
        """
    ).fetchone()

    con.execute(
        f"""
        copy (
            select *, '{yyyymm}' as partition_month
            from {src}
            where started_at >= timestamp '{period_start}'
              and started_at <  timestamp '{period_end}'
        ) to '{out}' (format parquet, compression zstd)
        """
    )
    con.close()

    print(f"  read {total:,} rows, landed {kept:,} ({total - kept:,} outside month)")
    print(f"  -> {out.relative_to(ROOT)}")
    return out


def run(month: str) -> Path:
    yyyymm = month.replace("-", "")
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise SystemExit(f"month must look like 2024-01, got {month!r}")
    print(f"[ingest] {month}")
    archive = download(yyyymm)
    csvs = extract(archive)
    return land(yyyymm, csvs[0].parent)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--month", required=True, help="month to ingest, e.g. 2024-01")
    args = ap.parse_args(argv)
    run(args.month)
    return 0


if __name__ == "__main__":
    sys.exit(main())
