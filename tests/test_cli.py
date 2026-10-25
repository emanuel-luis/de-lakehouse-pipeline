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
