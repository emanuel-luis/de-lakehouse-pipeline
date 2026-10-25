import polars as pl

from de_lakehouse_pipeline.contracts import CONTRACTS


def test_nine_expected_source_files() -> None:
    assert {c.source_file for c in CONTRACTS.values()} == {
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_geolocation_dataset.csv",
        "product_category_name_translation.csv",
    }


def test_key_subset_required_subset_columns() -> None:
    for name, contract in CONTRACTS.items():
        assert contract.name == name
        assert set(contract.primary_key) <= contract.required <= set(contract.columns)


def test_incremental_column_is_derived_for_orders() -> None:
    orders = CONTRACTS["orders"]
    assert orders.incremental_column in orders.derived
    assert all(isinstance(e, pl.Expr) for e in orders.derived.values())
