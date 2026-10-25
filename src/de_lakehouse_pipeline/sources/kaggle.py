"""Opt-in adapter that downloads the real Olist dataset from Kaggle."""

import os
import shutil
from collections.abc import Callable
from pathlib import Path

from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.io import atomic_write

DATASET = "olistbr/brazilian-ecommerce"


class KaggleUnavailableError(RuntimeError):
    """The ``kaggle`` extra is not installed or no token is configured."""


def _copier(src: Path) -> Callable[[Path], None]:
    def copy(tmp: Path) -> None:
        shutil.copyfile(src, tmp)

    return copy


def fetch(dest: Path) -> dict[str, Path]:
    """Download the Olist dataset and copy its nine CSVs into ``dest``.

    Requires the ``kaggle`` extra and ``KAGGLE_API_TOKEN`` in the environment; the token's
    value is never read here, only its presence.

    Args:
        dest: Source directory.

    Returns:
        Landing table name -> copied CSV path.

    Raises:
        KaggleUnavailableError: Extra missing or token unset.
        FileNotFoundError: The download lacks an expected CSV.
    """
    try:
        import kagglehub
    except ImportError as exc:
        raise KaggleUnavailableError("kagglehub missing: run `uv sync --extra kaggle`") from exc
    if not os.environ.get("KAGGLE_API_TOKEN"):
        raise KaggleUnavailableError("KAGGLE_API_TOKEN is not set")
    downloaded = Path(kagglehub.dataset_download(DATASET))
    paths: dict[str, Path] = {}
    for name, contract in CONTRACTS.items():
        src = downloaded / contract.source_file
        if not src.is_file():
            raise FileNotFoundError(f"{contract.source_file} not found in {DATASET}")
        target = dest / contract.source_file
        atomic_write(target, _copier(src))
        paths[name] = target
    return paths
