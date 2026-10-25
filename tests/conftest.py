from pathlib import Path

import pytest

from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings rooted in tmp_path with a small synthetic source already generated."""
    s = Settings(data_dir=tmp_path, synthetic_orders=300)
    prepare_source(s)
    return s
