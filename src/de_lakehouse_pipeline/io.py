"""Atomic file writes: readers never see a partial file."""

import contextlib
import os
from collections.abc import Callable
from pathlib import Path

import polars as pl


def atomic_write(path: Path, write: Callable[[Path], None]) -> None:
    """Produce ``path`` through a temp file in the same directory and ``os.replace``.

    Args:
        path: Final location.
        write: Callback that writes the complete content to the path it is given.

    Raises:
        Exception: Whatever ``write`` raises; the temp file is removed and ``path`` is untouched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")  # never matches a `*.parquet` or `*.csv` glob
    try:
        write(tmp)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
        raise


def atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Write ``df`` to ``path`` atomically."""
    atomic_write(path, df.write_parquet)
