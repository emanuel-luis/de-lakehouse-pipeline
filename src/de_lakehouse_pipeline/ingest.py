"""Orchestrate source CSV -> landing for every table and record the run."""

import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Literal

from de_lakehouse_pipeline.contracts import CONTRACTS, TableContract
from de_lakehouse_pipeline.landing import (
    read_source_csv,
    validate,
    write_landing,
    write_rejects,
)
from de_lakehouse_pipeline.runlog import TableRun, write_run_log
from de_lakehouse_pipeline.settings import Settings

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """Current time, timezone-aware UTC (the default clock)."""
    return datetime.now(UTC)


def new_run_id(now: datetime) -> str:
    """Return ``YYYYMMDDTHHMMSSffffffZ`` for a UTC instant; sorts lexically."""
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def _ingest_table(
    settings: Settings,
    contract: TableContract,
    run_id: str,
    mode: Literal["full", "incremental"],
    started_at: datetime,
    log: logging.LoggerAdapter[logging.Logger],
) -> TableRun:
    raw = read_source_csv(settings.source_dir / contract.source_file, contract)
    result = validate(raw, contract)
    rows_in, rejected = raw.height, result.rejects.height
    record = TableRun(
        run_id=run_id,
        data_source=settings.data_source,
        table_name=contract.name,
        mode=mode,
        status="success",
        started_at=started_at,
        finished_at=started_at,
        rows_in=rows_in,
        rows_rejected=rejected,
        watermark_column=contract.incremental_column,
    )
    # Rejects are evidence even when the table fails, so they are always written.
    write_rejects(result.rejects, settings.rejects_dir, contract, run_id)
    if rows_in and rejected / rows_in > settings.max_reject_ratio:
        raise ValueError(
            f"{rejected}/{rows_in} rows rejected exceeds max_reject_ratio "
            f"{settings.max_reject_ratio}"
        )
    path = write_landing(
        result.valid,
        settings.landing_dir,
        contract,
        run_id,
        mode,
        extracted_at=started_at,
        source_file=contract.source_file,
    )
    record.rows_out = result.valid.height
    record.landing_file = str(path)
    if contract.incremental_column is not None:
        high = result.valid.get_column(contract.incremental_column).max()
        record.watermark_to = high if isinstance(high, datetime) else None
    log.info("%s: in=%d out=%d rejected=%d", contract.name, rows_in, record.rows_out, rejected)
    return record


def ingest(
    settings: Settings,
    tables: Sequence[str] | None = None,
    *,
    mode: Literal["full", "incremental"] = "full",
    clock: Callable[[], datetime] = utc_now,
) -> list[TableRun]:
    """Ingest source CSVs into typed landing files and write the run log.

    A table that fails (missing file, schema mismatch, too many rejects) is recorded as
    ``failed`` and does not stop the others. The log is written even if the run is interrupted.

    Args:
        settings: Pipeline settings.
        tables: Landing table names to ingest; all nine by default.
        mode: ``full`` replaces the table's landing file; ``incremental`` adds a batch file.
        clock: Source of the current UTC time (injectable for tests).

    Returns:
        One record per attempted table.

    Raises:
        KeyError: A requested table is not in the contract registry.
    """
    contracts = [CONTRACTS[name] for name in (tables or CONTRACTS)]
    run_id = new_run_id(clock())
    log = logging.LoggerAdapter(logger, {"run_id": run_id})
    settings.ensure_dirs()
    records: list[TableRun] = []
    try:
        for contract in contracts:
            started_at = clock()
            try:
                record = _ingest_table(settings, contract, run_id, mode, started_at, log)
            except Exception as exc:  # noqa: BLE001 - one table's failure is logged, not fatal
                log.error("%s failed: %s", contract.name, exc)
                record = TableRun(
                    run_id=run_id,
                    data_source=settings.data_source,
                    table_name=contract.name,
                    mode=mode,
                    status="failed",
                    started_at=started_at,
                    finished_at=started_at,
                    error=f"{type(exc).__name__}: {exc}",
                )
            record.finished_at = clock()
            records.append(record)
    finally:
        write_run_log(records, settings.ingest_log_dir, run_id)
    return records
