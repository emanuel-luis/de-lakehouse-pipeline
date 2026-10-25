from pathlib import Path

import pytest

from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source

_ENV_VARS = (
    "DATA_SOURCE",
    "LAKEHOUSE_DATA_DIR",
    "LAKEHOUSE_DUCKDB_PATH",
    "SYNTHETIC_ORDERS",
    "SYNTHETIC_SEED",
    "MAX_REJECT_RATIO",
    "KAGGLE_API_TOKEN",
)


@pytest.fixture(autouse=True)
def hermetic_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Hide the caller's environment and repo .env: tests set what they need explicitly."""
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings rooted in tmp_path with a small synthetic source already generated."""
    s = Settings(data_dir=tmp_path, synthetic_orders=300)
    prepare_source(s)
    return s
