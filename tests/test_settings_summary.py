from pathlib import Path

import duckdb
import pytest

from de_lakehouse_pipeline.ingest import ingest
from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.summary import summarize


def test_settings_defaults_and_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("SYNTHETIC_ORDERS", "250")
    s = Settings()
    assert s.synthetic_orders == 250
    assert s.landing_dir == (tmp_path / "d").resolve() / "landing"
    assert s.warehouse_path.name == "lakehouse.duckdb"
    s.ensure_dirs()
    assert s.ingest_log_dir.is_dir()
    assert s.warehouse_path.parent.is_dir()
    assert not s.warehouse_path.exists()  # dbt creates the file, never Python
    assert Settings(duckdb_path=tmp_path / "w.duckdb").warehouse_path == tmp_path / "w.duckdb"


def test_summary_without_runs_or_warehouse(tmp_path: Path) -> None:
    text = summarize(Settings(data_dir=tmp_path))
    assert "No ingest runs" in text
    assert "not built" in text


def test_summary_with_run_and_marts(settings: Settings) -> None:
    ingest(settings, ["sellers"])
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    con.execute("create schema marts; create table marts.fct_orders as select 1 as x")
    con.close()
    text = summarize(settings)
    assert "sellers" in text
    assert "fct_orders" in text
