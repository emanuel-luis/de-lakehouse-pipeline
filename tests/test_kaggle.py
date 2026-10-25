import sys
import types
from pathlib import Path

import pytest

from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source
from de_lakehouse_pipeline.sources.kaggle import KaggleUnavailableError, fetch


def _fake_kagglehub(monkeypatch: pytest.MonkeyPatch, folder: Path) -> None:
    module = types.ModuleType("kagglehub")
    module.dataset_download = lambda slug: str(folder)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "kagglehub", module)


def test_missing_extra(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setitem(sys.modules, "kagglehub", None)  # makes the import fail
    with pytest.raises(KaggleUnavailableError, match="kagglehub"):
        fetch(tmp_path)


def test_missing_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _fake_kagglehub(monkeypatch, tmp_path)
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    with pytest.raises(KaggleUnavailableError, match="KAGGLE_API_TOKEN"):
        fetch(tmp_path)


def test_fetch_copies_nine_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    download = tmp_path / "dl"
    download.mkdir()
    for c in CONTRACTS.values():
        (download / c.source_file).write_text("a\n", encoding="utf-8")
    _fake_kagglehub(monkeypatch, download)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "secret-value")
    settings = Settings(data_dir=tmp_path / "data", data_source="kaggle")
    paths = prepare_source(settings)
    assert set(paths) == set(CONTRACTS)
    assert all(p.read_text(encoding="utf-8") == "a\n" for p in paths.values())

    (download / CONTRACTS["orders"].source_file).unlink()
    with pytest.raises(FileNotFoundError):
        fetch(tmp_path / "again")
