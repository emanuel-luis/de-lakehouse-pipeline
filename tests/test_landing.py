from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.io import atomic_write_parquet
from de_lakehouse_pipeline.landing import (
    SchemaMismatchError,
    read_source_csv,
    validate,
    write_landing,
    write_rejects,
)

ITEMS = CONTRACTS["order_items"]
ORDERS = CONTRACTS["orders"]
GOOD_ITEM = {
    "order_id": "o1",
    "order_item_id": "1",
    "product_id": "p",
    "seller_id": "s",
    "shipping_limit_date": "2018-01-02 03:04:05",
    "price": "10.50",
    "freight_value": "2.00",
}


def _items(**overrides: str | None) -> pl.DataFrame:
    return pl.DataFrame(
        [{**GOOD_ITEM, **overrides}], schema=dict.fromkeys(ITEMS.columns, pl.String)
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"order_id": None}, "null_required:order_id"),
        ({"price": None}, "null_required:price"),
        ({"price": "abc"}, "invalid_value:price"),
        ({"price": "1.234"}, "invalid_value:price"),
        ({"order_item_id": "1.0"}, "invalid_value:order_item_id"),
        ({"shipping_limit_date": "2018-13-45 00:00:00"}, "invalid_value:shipping_limit_date"),
    ],
)
def test_reject_reasons(overrides: dict[str, str | None], reason: str) -> None:
    result = validate(_items(**overrides), ITEMS)
    assert result.valid.is_empty()
    assert result.rejects["_reject_reason"].to_list() == [reason]


def test_valid_row_is_typed() -> None:
    result = validate(_items(shipping_limit_date=None), ITEMS)
    assert result.rejects.is_empty()
    assert result.valid.schema["price"] == pl.Decimal(12, 2)
    assert result.valid.schema["order_item_id"] == pl.Int64
    assert result.valid["shipping_limit_date"][0] is None


def test_orders_updated_at_is_latest_lifecycle_timestamp() -> None:
    row = dict.fromkeys(ORDERS.columns)
    row.update(
        order_id="o",
        customer_id="c",
        order_status="shipped",
        order_purchase_timestamp="2018-01-01 00:00:00",
        order_approved_at="2018-01-02 00:00:00",
        order_estimated_delivery_date="2018-03-01 00:00:00",
    )
    raw = pl.DataFrame([row], schema=dict.fromkeys(ORDERS.columns, pl.String))
    valid = validate(raw, ORDERS).valid
    assert valid["order_updated_at"][0] == datetime(2018, 1, 2)


def test_read_source_csv_strips_bom_nulls_blanks_and_drops_extras(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    contract = CONTRACTS["category_translation"]
    path = tmp_path / "t.csv"
    path.write_bytes(
        b"\xef\xbb\xbfproduct_category_name,product_category_name_english,extra\na,,x\n"
    )
    df = read_source_csv(path, contract)
    assert df.columns == list(contract.columns)
    assert df.row(0) == ("a", None)
    assert "extra" in caplog.text


def test_missing_column_raises(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("product_category_name\na\n", encoding="utf-8")
    with pytest.raises(SchemaMismatchError, match="product_category_name_english"):
        read_source_csv(path, CONTRACTS["category_translation"])


def test_failed_atomic_write_keeps_old_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "x.parquet"
    atomic_write_parquet(pl.DataFrame({"a": [1]}), target)

    def boom(self: pl.DataFrame, file: Path) -> None:
        file.write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(OSError, match="disk full"):
        atomic_write_parquet(pl.DataFrame({"a": [2]}), target)
    monkeypatch.undo()
    assert pl.read_parquet(target)["a"].to_list() == [1]
    assert [p.name for p in tmp_path.iterdir()] == ["x.parquet"]


def test_write_landing_modes_and_rejects(tmp_path: Path) -> None:
    valid = validate(_items(), ITEMS).valid
    now = datetime(2026, 1, 1, tzinfo=UTC)
    inc = write_landing(
        valid, tmp_path, ITEMS, "b1", "incremental", extracted_at=now, source_file="f.csv"
    )
    assert inc.name == "batch-b1.parquet"
    full = write_landing(
        valid, tmp_path, ITEMS, "b2", "full", extracted_at=now, source_file="f.csv"
    )
    assert full.name == "full.parquet"
    assert not inc.exists()
    df = pl.read_parquet(full)
    assert df.columns[-3:] == ["_batch_id", "_extracted_at", "_source_file"]
    assert df.schema["_extracted_at"] == pl.Datetime("us", "UTC")

    assert write_rejects(valid.clear(), tmp_path, ITEMS, "b2") is None
    bad = validate(_items(price="x"), ITEMS).rejects
    path = write_rejects(bad, tmp_path, ITEMS, "b2")
    assert path is not None
    assert pl.read_parquet(path).columns == [*ITEMS.columns, "_reject_reason", "_batch_id"]


_cell = st.one_of(st.none(), st.text(alphabet="0123456789.-ab :", max_size=6))


@given(rows=st.lists(st.tuples(*[_cell] * len(ITEMS.columns)), max_size=30))
def test_validate_conserves_row_count(rows: list[tuple[str | None, ...]]) -> None:
    raw = pl.DataFrame(rows, schema=dict.fromkeys(ITEMS.columns, pl.String), orient="row")
    result = validate(raw, ITEMS)
    assert result.valid.height + result.rejects.height == raw.height
