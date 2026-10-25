from datetime import UTC, datetime
from pathlib import Path

from de_lakehouse_pipeline.runlog import INGEST_LOG_SCHEMA, TableRun, read_run_log, write_run_log


def _run(run_id: str, table: str, *, failed: bool = False) -> TableRun:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return TableRun(
        run_id=run_id,
        data_source="synthetic",
        table_name=table,
        mode="full",
        status="failed" if failed else "success",
        started_at=now,
        finished_at=now,
        rows_in=3,
        rows_out=2,
        rows_rejected=1,
        error="boom" if failed else None,
    )


def test_round_trip(tmp_path: Path) -> None:
    assert read_run_log(tmp_path).is_empty()
    write_run_log([_run("r2", "b"), _run("r2", "a", failed=True)], tmp_path, "r2")
    write_run_log([_run("r1", "a")], tmp_path, "r1")
    df = read_run_log(tmp_path)
    assert df.schema == INGEST_LOG_SCHEMA
    assert df["run_id"].to_list() == ["r1", "r2", "r2"]
    assert df["table_name"].to_list() == ["a", "a", "b"]
    assert df["error"].to_list() == [None, "boom", None]
