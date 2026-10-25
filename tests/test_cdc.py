import csv
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from de_lakehouse_pipeline.cdc import simulate
from de_lakehouse_pipeline.contracts import CONTRACTS, LIFECYCLE_COLUMNS
from de_lakehouse_pipeline.landing import read_source_csv, validate
from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source

_SET = {
    "approved": "order_approved_at",
    "shipped": "order_delivered_carrier_date",
    "delivered": "order_delivered_customer_date",
}


def _rows(source: Path, table: str) -> list[dict[str, str]]:
    with (source / CONTRACTS[table].source_file).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _digest(source: Path) -> dict[str, str]:
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source.iterdir())}


def _max_lifecycle(source: Path) -> datetime:
    return max(
        datetime.fromisoformat(row[c])
        for row in _rows(source, "orders")
        for c in LIFECYCLE_COLUMNS
        if row[c]
    )


def test_deterministic_per_seed_and_source(tmp_path: Path, settings: Settings) -> None:
    other = Settings(data_dir=tmp_path / "other", synthetic_orders=300)
    prepare_source(other)
    a = simulate(settings.source_dir, changes=10, new_orders=5, seed=1)
    b = simulate(other.source_dir, changes=10, new_orders=5, seed=1)
    assert (a.clock, a.transitions, a.changed) == (b.clock, b.transitions, b.changed)
    assert _digest(settings.source_dir) == _digest(other.source_dir)

    third = Settings(data_dir=tmp_path / "third", synthetic_orders=300)
    prepare_source(third)
    simulate(third.source_dir, changes=10, new_orders=5, seed=2)
    assert _digest(third.source_dir) != _digest(settings.source_dir)


def test_clock_comes_from_data_and_moves(settings: Settings) -> None:
    before = _max_lifecycle(settings.source_dir)
    first = simulate(settings.source_dir, changes=5, new_orders=2, seed=1)
    assert first.clock == before + timedelta(hours=1)
    second = simulate(settings.source_dir, changes=5, new_orders=2, seed=1)
    assert second.clock == first.clock + timedelta(hours=1)
    assert second.changed != first.changed


def test_each_transition_sets_exactly_its_timestamp(settings: Settings) -> None:
    old = {r["order_id"]: r for r in _rows(settings.source_dir, "orders")}
    result = simulate(settings.source_dir, changes=10, new_orders=0, seed=7)
    assert sum(result.transitions.values()) == len(result.changed) == 10
    new = {r["order_id"]: r for r in _rows(settings.source_dir, "orders")}
    stamp = result.clock.strftime("%Y-%m-%d %H:%M:%S")
    for order_id, row in new.items():
        if order_id not in result.changed:
            assert row == old[order_id]
            continue
        status = result.changed[order_id]
        assert row["order_status"] == status
        diff = {k for k in row if row[k] != old[order_id][k]}
        assert diff == {"order_status", _SET[status]}
        assert row[_SET[status]] == stamp


def test_fewer_eligible_orders_mean_fewer_changes(settings: Settings) -> None:
    result = simulate(settings.source_dir, changes=10_000, new_orders=0, seed=1)
    assert 0 < len(result.changed) < 10_000
    assert simulate(settings.source_dir, changes=0, new_orders=0, seed=1).paths == ()


def test_new_orders_keep_referential_integrity(settings: Settings) -> None:
    src = settings.source_dir
    products = {r["product_id"] for r in _rows(src, "products")}
    sellers = {r["seller_id"] for r in _rows(src, "sellers")}
    n_orders = len(_rows(src, "orders"))
    result = simulate(src, changes=0, new_orders=15, seed=3)
    assert len(result.paths) == 4
    orders = _rows(src, "orders")[n_orders:]
    assert len(orders) == 15
    customers = {r["customer_id"] for r in _rows(src, "customers")}
    items = [
        r for r in _rows(src, "order_items") if r["order_id"] in {o["order_id"] for o in orders}
    ]
    payments = {r["order_id"]: r for r in _rows(src, "payments")}
    for order in orders:
        assert order["order_status"] == "created"
        assert order["customer_id"] in customers
        lines = [i for i in items if i["order_id"] == order["order_id"]]
        assert 1 <= len(lines) <= 3
        cents = 0
        for line in lines:
            assert line["product_id"] in products
            assert line["seller_id"] in sellers
            for money in ("price", "freight_value"):
                assert len(line[money].split(".")[1]) == 2
                cents += round(float(line[money]) * 100)
        pay = payments[order["order_id"]]
        assert pay["payment_type"] == "credit_card"
        assert round(float(pay["payment_value"]) * 100) == cents


@pytest.mark.parametrize("table", ["orders", "customers", "order_items", "payments"])
def test_rewritten_csvs_add_no_rejects(settings: Settings, table: str) -> None:
    contract = CONTRACTS[table]
    path = settings.source_dir / contract.source_file
    before = validate(read_source_csv(path, contract), contract).rejects.height
    simulate(settings.source_dir, changes=30, new_orders=10, seed=5)
    after = validate(read_source_csv(path, contract), contract)
    assert after.rejects.height == before


def test_no_parseable_timestamp_raises(settings: Settings) -> None:
    path = settings.source_dir / CONTRACTS["orders"].source_file
    path.write_text(path.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no parseable"):
        simulate(settings.source_dir, changes=1, new_orders=1, seed=1)
