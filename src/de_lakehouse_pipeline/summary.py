"""Human-readable run summary: last ingest log rows plus mart row counts."""

import duckdb
import polars as pl

from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.runlog import last_watermark, read_run_log
from de_lakehouse_pipeline.settings import Settings


def _mart_counts(settings: Settings) -> list[tuple[str, int]]:
    path = settings.warehouse_path
    if not path.exists():
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        names = [
            row[0]
            for row in con.execute(
                "select table_name from information_schema.tables"
                " where table_schema = 'marts' order by table_name"
            ).fetchall()
        ]
        return [
            (name, int(con.execute(f'select count(*) from marts."{name}"').fetchone()[0]))  # type: ignore[index]
            for name in names
        ]
    finally:
        con.close()


def summarize(settings: Settings, *, watermarks: bool = False) -> str:
    """Summarize the latest ingest run and, if dbt has built them, the marts.

    Args:
        settings: Pipeline settings.
        watermarks: Also list the current watermark of every incremental table.

    Returns:
        Multi-line text.
    """
    log = read_run_log(settings.ingest_log_dir)
    lines: list[str] = []
    if log.is_empty():
        lines.append("No ingest runs found.")
    else:
        last = log.filter(pl.col("run_id") == log["run_id"].max())
        lines.append(f"Last ingest run: {last['run_id'][0]} ({last['data_source'][0]})")
        for row in last.iter_rows(named=True):
            lines.append(
                f"  {row['table_name']:<22} {row['mode']:<11} {row['status']:<8} in={row['rows_in']:>7}"
                f" out={row['rows_out']:>7} rejected={row['rows_rejected']:>5}"
            )
    if watermarks:
        lines.append("Watermarks:")
        lines.extend(
            f"  {c.name:<22} {c.incremental_column} = "
            f"{last_watermark(settings.ingest_log_dir, c.name) or 'none'}"
            for c in CONTRACTS.values()
            if c.incremental_column
        )
    marts = _mart_counts(settings)
    if marts:
        lines.append("Marts:")
        lines.extend(f"  {name:<22} {count:>7} rows" for name, count in marts)
    else:
        lines.append("Marts: not built yet (run dbt).")
    return "\n".join(lines)
