import csv
import hashlib
from pathlib import Path

import pytest

from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.sources.synthetic import generate


def _digests(paths: dict[str, Path]) -> dict[str, str]:
    return {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in paths.items()}


def _rows(paths: dict[str, Path], table: str) -> list[dict[str, str]]:
    with paths[table].open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def paths(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    return generate(tmp_path_factory.mktemp("src"), orders=400, seed=1)


def test_same_seed_is_byte_identical_and_other_seed_differs(
    tmp_path: Path, paths: dict[str, Path]
) -> None:
    again = generate(tmp_path / "a", orders=400, seed=1)
    other = generate(tmp_path / "b", orders=400, seed=2)
    assert _digests(again) == _digests(paths)
    assert _digests(other) != _digests(paths)


def test_headers_match_contract_order(paths: dict[str, Path]) -> None:
    assert set(paths) == set(CONTRACTS)
    for name, path in paths.items():
        assert path.name == CONTRACTS[name].source_file
        header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
        assert header == list(CONTRACTS[name].columns)


def test_referential_integrity(paths: dict[str, Path]) -> None:
    orders = {r["order_id"] for r in _rows(paths, "orders") if r["order_id"]}
    customers = {r["customer_id"] for r in _rows(paths, "customers") if r["customer_id"]}
    products = {r["product_id"] for r in _rows(paths, "products")}
    sellers = {r["seller_id"] for r in _rows(paths, "sellers")}
    assert {r["customer_id"] for r in _rows(paths, "orders")} - {""} <= customers
    for r in _rows(paths, "order_items"):
        assert r["order_id"] in orders
        assert r["product_id"] in products
        assert r["seller_id"] in sellers
    for table in ("payments", "reviews"):
        assert {r["order_id"] for r in _rows(paths, table)} <= orders


def test_dirt_is_present(paths: dict[str, Path]) -> None:
    geo = [tuple(r.values()) for r in _rows(paths, "geolocation")]
    assert len(geo) != len(set(geo))  # exact duplicates
    zips = [r[0] for r in geo]
    assert len(zips) != len(set(zips))  # several rows per zip

    orders = _rows(paths, "orders")
    with_items = {r["order_id"] for r in _rows(paths, "order_items")}
    itemless = [r for r in orders if r["order_id"] and r["order_id"] not in with_items]
    assert itemless
    assert {r["order_status"] for r in itemless} <= {"canceled", "unavailable", "created"}

    assert any(not r["order_id"] for r in orders)
    customers = _rows(paths, "customers")
    assert any(not r["customer_id"] for r in customers)
    assert any(not r["price"].replace(".", "").isdigit() for r in _rows(paths, "order_items"))

    reviews = [r["review_id"] for r in _rows(paths, "reviews")]
    assert len(reviews) != len(set(reviews))

    products = _rows(paths, "products")
    assert any(not r["product_category_name"] for r in products)
    translated = {r["product_category_name"] for r in _rows(paths, "category_translation")}
    assert any(r["product_category_name"] not in translated | {""} for r in products)

    cities = [r["customer_city"] for r in customers]
    assert any(c != c.strip() for c in cities)
    assert any(c != c.lower() and c.strip() for c in cities)
    assert any(len(r["customer_zip_code_prefix"]) < 5 for r in customers)
