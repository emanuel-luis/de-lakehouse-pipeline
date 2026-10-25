"""End to end: source -> full load -> CDC -> incremental loads, with dbt run in process."""

from pathlib import Path
from typing import Any

import duckdb
import pytest

from de_lakehouse_pipeline.cdc import simulate
from de_lakehouse_pipeline.ingest import ingest
from de_lakehouse_pipeline.runlog import last_watermark
from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"

pytestmark = [
    pytest.mark.slow,
    pytest.mark.timeout(900),
    pytest.mark.skipif(
        not (DBT_DIR / "dbt_packages").is_dir(),
        reason="dbt packages not installed: run `make dbt-deps` first",
    ),
]


def _query(settings: Settings, sql: str) -> list[tuple[Any, ...]]:
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _marts_digest(settings: Settings) -> dict[str, str]:
    """One order-independent hash per mart."""
    names = [
        row[0]
        for row in _query(
            settings,
            "select table_name from information_schema.tables where table_schema = 'marts'",
        )
    ]
    return {
        name: _query(
            settings,
            f"select md5(coalesce(string_agg(h, ',' order by h), ''))"
            f' from (select md5(t::varchar) as h from marts."{name}" t)',
        )[0][0]
        for name in sorted(names)
    }


def test_cdc_round_trip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from dbt.cli.main import dbtRunner

    data_dir = (tmp_path / "data").resolve()
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_PATH", str(data_dir / "warehouse" / "lakehouse.duckdb"))
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    settings = Settings(synthetic_orders=400)
    runner = dbtRunner()

    def build(*extra: str) -> None:
        result = runner.invoke(
            [
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
                "--target-path",
                str(tmp_path / "target"),
                "--log-path",
                str(tmp_path / "logs"),
                *extra,
            ]
        )
        assert result.success, result.exception
        # dbt-duckdb keeps its DuckDB instance open for the life of the process, which would
        # block the read-only connections below; close it like a separate dbt process would.
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager as manager

        with manager._LOCK:
            if manager._ENV is not None:
                manager._ENV.close()
                manager._ENV = None

    def fct_orders() -> dict[str, str]:
        return dict(_query(settings, "select order_id, order_status from marts.fct_orders"))

    prepare_source(settings)
    assert all(r.status == "success" for r in ingest(settings))
    build()
    before = fct_orders()
    first_watermark = last_watermark(settings.ingest_log_dir, "orders")

    result = simulate(settings.source_dir, changes=10, new_orders=5, seed=1)
    assert result.changed
    runs = {r.table_name: r for r in ingest(settings, mode="incremental")}
    assert runs["orders"].mode == "incremental"
    build()
    after = fct_orders()
    assert len(after) == len(before) + result.new_orders
    for order_id, status in result.changed.items():
        assert after[order_id] == status
    assert first_watermark is not None
    assert last_watermark(settings.ingest_log_dir, "orders") == result.clock > first_watermark

    digest = _marts_digest(settings)
    empty = next(r for r in ingest(settings, mode="incremental") if r.table_name == "orders")
    assert (empty.rows_in, empty.landing_file) == (0, None)
    build()
    assert _marts_digest(settings) == digest

    bronze_sql = "select * from bronze.bronze_orders order by order_id"
    incremental_bronze = _query(settings, bronze_sql)
    build("--full-refresh")
    assert _query(settings, bronze_sql) == incremental_bronze
