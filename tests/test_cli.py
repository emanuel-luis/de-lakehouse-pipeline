import sys
import types
from pathlib import Path

import pytest

from de_lakehouse_pipeline import __version__
from de_lakehouse_pipeline.cli import main


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_source_ingest_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    assert main(["source", "--orders", "300", "--seed", "3"]) == 0
    assert main(["ingest", "--tables", "orders", "customers"]) == 0
    assert (tmp_path / "landing" / "orders" / "full.parquet").exists()
    assert main(["summary"]) == 0
    assert "orders" in capsys.readouterr().out


def test_ingest_exit_1_when_a_table_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    assert main(["ingest", "--tables", "sellers"]) == 1  # no source files yet


def test_source_creates_warehouse_dir_but_not_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path / "fresh"))
    assert main(["source", "--orders", "200"]) == 0
    assert (tmp_path / "fresh" / "warehouse").is_dir()
    assert not list((tmp_path / "fresh" / "warehouse").iterdir())


def test_source_if_missing_skips_only_when_all_csvs_exist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    assert main(["source", "--orders", "200", "--if-missing"]) == 0
    assert "wrote 9 source files" in capsys.readouterr().out
    assert main(["source", "--if-missing"]) == 0
    assert "nothing to do" in capsys.readouterr().out
    (tmp_path / "source" / "olist_orders_dataset.csv").unlink()
    assert main(["source", "--orders", "200", "--if-missing"]) == 0
    assert "wrote 9 source files" in capsys.readouterr().out


def test_orders_below_minimum_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["source", "--orders", "100"])
    assert exc.value.code == 2
    assert "at least 200" in capsys.readouterr().err


def test_kaggle_without_token_exits_1_without_traceback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_SOURCE", "kaggle")
    monkeypatch.setitem(sys.modules, "kagglehub", types.ModuleType("kagglehub"))
    assert main(["source"]) == 1
    assert "KAGGLE_API_TOKEN" in capsys.readouterr().err


def test_invalid_env_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_SOURCE", "nope")
    assert main(["summary"]) == 1


def test_cdc_simulate_incremental_ingest_and_watermarks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    assert main(["source", "--orders", "300"]) == 0
    assert main(["ingest", "--tables", "orders"]) == 0
    assert main(["cdc", "simulate", "--changes", "5", "--new-orders", "2", "--seed", "1"]) == 0
    assert "new orders 2" in capsys.readouterr().out
    assert main(["ingest", "--tables", "orders", "--mode", "incremental"]) == 0
    assert list((tmp_path / "landing" / "orders").glob("batch-*.parquet"))
    assert main(["summary", "--watermarks"]) == 0
    out = capsys.readouterr().out
    assert "incremental" in out
    assert "order_updated_at = " in out


def test_cdc_simulate_without_source_exits_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("LAKEHOUSE_DATA_DIR", str(tmp_path))
    assert main(["cdc", "simulate"]) == 1


def test_cdc_negative_count_is_a_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["cdc", "simulate", "--changes", "-1"])
    assert exc.value.code == 2
