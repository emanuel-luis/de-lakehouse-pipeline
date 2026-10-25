from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import count
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from de_lakehouse_pipeline.cdc import simulate
from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.ingest import ingest, new_run_id
from de_lakehouse_pipeline.runlog import last_watermark, read_run_log
from de_lakehouse_pipeline.settings import MIN_ORDERS, Settings
from de_lakehouse_pipeline.sources import prepare_source


def _clock() -> Callable[[], datetime]:
    ticks = count()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return lambda: base + timedelta(seconds=next(ticks))


def _data(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    return df.select([c for c in df.columns if c not in ("_batch_id", "_extracted_at")])


def test_new_run_id_format() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
    assert new_run_id(now) == "20260102T030405000006Z"


def test_ingest_counts_log_and_idempotent_rerun(settings: Settings) -> None:
    clock = _clock()
    first = ingest(settings, clock=clock)
    assert {r.table_name for r in first} == set(CONTRACTS)
    for r in first:
        assert r.status == "success"
        assert r.rows_in == r.rows_out + r.rows_rejected
    by_table = {r.table_name: r for r in first}
    assert by_table["orders"].rows_rejected > 0
    assert by_table["orders"].watermark_to is not None

    log = read_run_log(settings.ingest_log_dir)
    assert log.height == 9
    assert log["rows_out"].sum() == sum(r.rows_out for r in first)

    before = {t: _data(settings.landing_dir / t / "full.parquet") for t in CONTRACTS}
    ingest(settings, clock=clock)
    for table, df in before.items():
        assert _data(settings.landing_dir / table / "full.parquet").equals(df)
    assert read_run_log(settings.ingest_log_dir).height == 18
    assert len(list((settings.landing_dir / "orders").iterdir())) == 1


def test_landing_schema_of_orders(settings: Settings) -> None:
    ingest(settings, ["orders"])
    schema = pl.read_parquet(settings.landing_dir / "orders" / "full.parquet").schema
    assert schema["order_purchase_timestamp"] == pl.Datetime("us")
    assert schema["order_updated_at"] == pl.Datetime("us")
    assert schema["_extracted_at"] == pl.Datetime("us", "UTC")


def test_reject_ratio_fails_table_without_landing_file(settings: Settings) -> None:
    strict = settings.model_copy(update={"max_reject_ratio": 0.0})
    runs = ingest(strict, ["orders", "sellers"])
    status = {r.table_name: r.status for r in runs}
    assert status == {"orders": "failed", "sellers": "success"}
    assert not (settings.landing_dir / "orders").exists()
    assert list((settings.rejects_dir / "orders").glob("batch-*.parquet"))
    failed = next(r for r in runs if r.table_name == "orders")
    assert failed.error is not None
    assert "max_reject_ratio" in failed.error


def test_missing_source_file_is_recorded_and_log_written(settings: Settings) -> None:
    (settings.source_dir / CONTRACTS["sellers"].source_file).unlink()
    runs = ingest(settings, ["sellers", "customers"])
    assert [r.status for r in runs] == ["failed", "success"]
    assert read_run_log(settings.ingest_log_dir).height == 2


def test_every_itemless_order_is_unfulfilled(settings: Settings) -> None:
    ingest(settings, ["orders", "order_items"])
    orders = pl.read_parquet(settings.landing_dir / "orders" / "full.parquet")
    items = pl.read_parquet(settings.landing_dir / "order_items" / "full.parquet")
    itemless = orders.filter(~pl.col("order_id").is_in(items["order_id"].implode()))
    assert itemless.height > 0
    assert set(itemless["order_status"]) <= {"canceled", "unavailable", "created"}


def test_smallest_allowed_scale_ingests_under_default_ratio(tmp_path: Path) -> None:
    small = Settings(data_dir=tmp_path, synthetic_orders=MIN_ORDERS)
    prepare_source(small)
    assert all(r.status == "success" for r in ingest(small))


def test_below_minimum_scale_is_invalid(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings(data_dir=tmp_path, synthetic_orders=MIN_ORDERS - 1)


def test_unknown_table_raises(settings: Settings) -> None:
    with pytest.raises(KeyError):
        ingest(settings, ["nope"])


def test_incremental_without_watermark_falls_back_to_full(settings: Settings) -> None:
    runs = ingest(settings, ["orders", "sellers"], mode="incremental")
    assert {r.table_name: r.mode for r in runs} == {"orders": "full", "sellers": "full"}
    assert (settings.landing_dir / "orders" / "full.parquet").exists()


def test_incremental_lands_only_rows_after_the_watermark(settings: Settings) -> None:
    clock = _clock()
    full = next(r for r in ingest(settings, clock=clock) if r.table_name == "orders")
    result = simulate(settings.source_dir, changes=12, new_orders=4, seed=1)
    runs = {r.table_name: r for r in ingest(settings, mode="incremental", clock=clock)}
    inc = runs["orders"]
    assert inc.mode == "incremental"
    assert {r.mode for name, r in runs.items() if name != "orders"} == {"full"}
    assert all(r.status == "success" for r in runs.values())
    assert inc.watermark_from == full.watermark_to
    assert inc.watermark_to == result.clock
    assert inc.rows_in == inc.rows_out + inc.rows_rejected == 16
    batch = pl.read_parquet(inc.landing_file or "")
    assert (batch["order_updated_at"] > full.watermark_to).all()
    assert Path(inc.landing_file or "").name == f"batch-{inc.run_id}.parquet"
    assert last_watermark(settings.ingest_log_dir, "orders") == result.clock

    again = ingest(settings, ["orders"], mode="incremental", clock=clock)[0]
    assert (again.status, again.rows_in, again.rows_out, again.landing_file) == (
        "success",
        0,
        0,
        None,
    )
    assert again.watermark_from == again.watermark_to == result.clock
    assert len(list((settings.landing_dir / "orders").glob("batch-*.parquet"))) == 1


def test_incremental_slice_keeps_rows_it_cannot_date(settings: Settings) -> None:
    ingest(settings, ["orders"])
    path = settings.source_dir / CONTRACTS["orders"].source_file
    lines = path.read_text(encoding="utf-8").splitlines()
    fields = lines[1].split(",")
    fields[0], fields[3:7] = "f" * 32, ["not-a-date", "", "", ""]
    path.write_text("\n".join([*lines, ",".join(fields)]) + "\n", encoding="utf-8")
    lenient = settings.model_copy(update={"max_reject_ratio": 1.0})
    run = ingest(lenient, ["orders"], mode="incremental")[0]
    assert (run.status, run.rows_in, run.rows_out, run.rows_rejected) == ("success", 1, 0, 1)
