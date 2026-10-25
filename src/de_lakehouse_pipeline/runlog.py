"""Append-only ingest log: one Parquet file per run, one row per (run, table)."""

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Literal

import polars as pl

from de_lakehouse_pipeline.io import atomic_write_parquet

_UTC_TS = pl.Datetime("us", "UTC")
INGEST_LOG_SCHEMA: Final[dict[str, pl.DataType | type[pl.DataType]]] = {
    "run_id": pl.String,
    "data_source": pl.String,
    "table_name": pl.String,
    "mode": pl.String,
    "status": pl.String,
    "started_at": _UTC_TS,
    "finished_at": _UTC_TS,
    "rows_in": pl.Int64,
    "rows_out": pl.Int64,
    "rows_rejected": pl.Int64,
    "watermark_column": pl.String,
    "watermark_from": pl.Datetime("us"),
    "watermark_to": pl.Datetime("us"),
    "landing_file": pl.String,
    "error": pl.String,
}


@dataclass(slots=True)
class TableRun:
    """Outcome of ingesting one table; invariant ``rows_in == rows_out + rows_rejected``."""

    run_id: str
    data_source: str
    table_name: str
    mode: Literal["full", "incremental"]
    status: Literal["success", "failed"]
    started_at: datetime
    finished_at: datetime
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    watermark_column: str | None = None
    watermark_from: datetime | None = None
    watermark_to: datetime | None = None
    landing_file: str | None = None
    error: str | None = None


def write_run_log(records: list[TableRun], log_dir: Path, run_id: str) -> Path:
    """Write the run's rows to ``run-<run_id>.parquet`` atomically."""
    path = log_dir / f"run-{run_id}.parquet"
    df = pl.DataFrame([asdict(r) for r in records], schema=INGEST_LOG_SCHEMA)
    atomic_write_parquet(df, path)
    return path


def read_run_log(log_dir: Path) -> pl.DataFrame:
    """Read every run file, ordered by ``run_id`` then table; empty frame if none exist."""
    files = sorted(log_dir.glob("run-*.parquet"))
    if not files:
        return pl.DataFrame(schema=INGEST_LOG_SCHEMA)
    return pl.concat([pl.read_parquet(f) for f in files]).sort("run_id", "table_name")
