"""Source CSV -> typed landing Parquet: read as text, cast per contract, split rejects."""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from de_lakehouse_pipeline.contracts import METADATA_COLUMNS, TIMESTAMP_FORMAT, TableContract
from de_lakehouse_pipeline.io import atomic_write_parquet

logger = logging.getLogger(__name__)

REJECT_REASON = "_reject_reason"
_MONEY_PATTERN = r"^-?\d+(\.\d{1,2})?$"  # polars silently truncates extra decimals


class SchemaMismatchError(ValueError):
    """The source file lacks a column the contract requires."""


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of :func:`validate`; ``len(valid) + len(rejects)`` equals the input row count."""

    valid: pl.DataFrame
    rejects: pl.DataFrame


def read_source_csv(path: Path, contract: TableContract) -> pl.DataFrame:
    """Read a source CSV as text, in contract column order.

    Empty strings become null, a BOM is stripped from header names and columns outside the
    contract are dropped with a warning.

    Args:
        path: CSV file.
        contract: Contract of the table the file feeds.

    Returns:
        All-String frame holding exactly the contract columns.

    Raises:
        SchemaMismatchError: A contract column is missing from the header.
    """
    df = pl.read_csv(path, infer_schema=False)
    df = df.rename({c: c.lstrip("﻿") for c in df.columns})
    missing = [c for c in contract.columns if c not in df.columns]
    if missing:
        raise SchemaMismatchError(f"{path.name}: missing columns {missing}")
    extra = [c for c in df.columns if c not in contract.columns]
    if extra:
        logger.warning("%s: dropping extra columns %s", path.name, extra)
    return df.select(list(contract.columns)).with_columns(
        pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in contract.columns
    )


def _typed(column: str, dtype: pl.DataType | type[pl.DataType]) -> pl.Expr:
    src = pl.col(column)
    if dtype == pl.String:
        return src
    if isinstance(dtype, pl.Datetime):
        return src.str.to_datetime(TIMESTAMP_FORMAT, strict=False)
    if isinstance(dtype, pl.Decimal):
        return pl.when(src.str.contains(_MONEY_PATTERN)).then(src.cast(dtype, strict=False))
    return src.cast(dtype, strict=False)


def validate(raw: pl.DataFrame, contract: TableContract) -> ValidationResult:
    """Cast ``raw`` to the contract and split off rows that break a rule.

    A row is rejected when a required column is null or a non-empty value fails its cast; the
    reason names the first failing column. Duplicate keys are kept.

    Args:
        raw: Output of :func:`read_source_csv`.
        contract: Table contract.

    Returns:
        ``valid`` typed rows (plus derived columns) and ``rejects`` as the original text rows
        with a ``_reject_reason`` column.
    """
    checks: list[pl.Expr] = []
    typed: dict[str, pl.Expr] = {}
    for column, dtype in contract.columns.items():
        typed[column] = _typed(column, dtype)
        if column in contract.required:
            checks.append(pl.when(pl.col(column).is_null()).then(pl.lit(f"null_required:{column}")))
        if dtype != pl.String:
            checks.append(
                pl.when(pl.col(column).is_not_null() & typed[column].is_null()).then(
                    pl.lit(f"invalid_value:{column}")
                )
            )
    reason = pl.coalesce(*checks, pl.lit(None, dtype=pl.String)).alias(REJECT_REASON)
    checked = raw.with_columns(reason)
    rejects = checked.filter(pl.col(REJECT_REASON).is_not_null())
    valid = (
        checked.filter(pl.col(REJECT_REASON).is_null())
        .drop(REJECT_REASON)
        .with_columns(**typed)
        .with_columns(**contract.derived)
    )
    return ValidationResult(valid=valid, rejects=rejects)


def after_watermark(
    raw: pl.DataFrame, contract: TableContract, watermark: datetime
) -> pl.DataFrame:
    """Keep the raw rows whose incremental value is after ``watermark`` or cannot be computed.

    The value is computed best effort with the same casts as :func:`validate`, so a row that
    validation would reject still reaches it (and its reject file) unless it is provably old.

    Args:
        raw: Output of :func:`read_source_csv`.
        contract: Table contract with an ``incremental_column``.
        watermark: Latest committed value of the incremental column.

    Returns:
        The slice of ``raw``, unchanged text rows.
    """
    assert contract.incremental_column is not None
    value = (
        raw.with_columns(**{c: _typed(c, d) for c, d in contract.columns.items()})
        .with_columns(**contract.derived)
        .get_column(contract.incremental_column)
    )
    return raw.filter((value > watermark).fill_null(True))


def write_landing(
    df: pl.DataFrame,
    landing_dir: Path,
    contract: TableContract,
    batch_id: str,
    mode: Literal["full", "incremental"],
    *,
    extracted_at: datetime,
    source_file: str,
) -> Path:
    """Add metadata columns and write the landing file atomically.

    ``full`` replaces ``full.parquet`` and removes older ``batch-*.parquet`` files;
    ``incremental`` adds ``batch-<batch_id>.parquet``.

    Args:
        df: Valid typed rows from :func:`validate`.
        landing_dir: Landing root; the table gets its own subdirectory.
        contract: Table contract (column order).
        batch_id: Run id, stored as ``_batch_id``.
        mode: Write mode.
        extracted_at: Extraction time (timezone-aware UTC), stored as ``_extracted_at``.
        source_file: Name of the source file, stored as ``_source_file``.

    Returns:
        The written file.
    """
    table_dir = landing_dir / contract.name
    path = table_dir / ("full.parquet" if mode == "full" else f"batch-{batch_id}.parquet")
    out = df.with_columns(
        pl.lit(batch_id).alias("_batch_id"),
        pl.lit(extracted_at).cast(METADATA_COLUMNS["_extracted_at"]).alias("_extracted_at"),
        pl.lit(source_file).alias("_source_file"),
    ).select([*contract.columns, *contract.derived, *METADATA_COLUMNS])
    atomic_write_parquet(out, path)
    if mode == "full":
        for stale in table_dir.glob("batch-*.parquet"):
            stale.unlink()
    return path


def write_rejects(
    rejects: pl.DataFrame, rejects_dir: Path, contract: TableContract, batch_id: str
) -> Path | None:
    """Write rejected rows, all as text, with ``_reject_reason`` and ``_batch_id``.

    Returns:
        The written file, or ``None`` when there is nothing to reject.
    """
    if rejects.is_empty():
        return None
    path = rejects_dir / contract.name / f"batch-{batch_id}.parquet"
    atomic_write_parquet(
        rejects.with_columns(pl.lit(batch_id).alias("_batch_id")).select(
            [*contract.columns, REJECT_REASON, "_batch_id"]
        ),
        path,
    )
    return path
