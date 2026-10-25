"""Typed landing contract for every source table."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final

import polars as pl

STR: Final = pl.String
INT: Final = pl.Int64
TS: Final = pl.Datetime("us")  # naive source local time
MONEY: Final = pl.Decimal(12, 2)

#: Columns added to every landing file.
METADATA_COLUMNS: Final[Mapping[str, pl.DataType | type[pl.DataType]]] = MappingProxyType(
    {"_batch_id": STR, "_extracted_at": pl.Datetime("us", "UTC"), "_source_file": STR}
)

TIMESTAMP_FORMAT: Final = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True, slots=True)
class TableContract:
    """Schema, keys and validation rules of one landing table.

    Attributes:
        name: Landing table name.
        source_file: CSV file name in the source directory.
        columns: Source column -> target dtype, in source file order.
        primary_key: Natural key (may be empty); informational, duplicates are not rejected.
        required: Columns that must be non-null; a null rejects the row.
        derived: Extra landing columns computed from the typed columns.
        incremental_column: Derived or typed column used as the CDC watermark.
    """

    name: str
    source_file: str
    columns: Mapping[str, pl.DataType | type[pl.DataType]]
    primary_key: tuple[str, ...]
    required: frozenset[str]
    derived: Mapping[str, pl.Expr] = field(default_factory=dict)
    incremental_column: str | None = None


def _cols(
    **dtypes: pl.DataType | type[pl.DataType],
) -> Mapping[str, pl.DataType | type[pl.DataType]]:
    return MappingProxyType(dtypes)


_LIFECYCLE = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
)


def _contract(
    name: str,
    source_file: str,
    columns: Mapping[str, pl.DataType | type[pl.DataType]],
    primary_key: tuple[str, ...],
    *required: str,
    derived: Mapping[str, pl.Expr] | None = None,
    incremental_column: str | None = None,
) -> TableContract:
    return TableContract(
        name=name,
        source_file=source_file,
        columns=columns,
        primary_key=primary_key,
        required=frozenset(primary_key) | frozenset(required),
        derived=MappingProxyType(derived or {}),
        incremental_column=incremental_column,
    )


_CONTRACT_LIST: Final = (
    _contract(
        "orders",
        "olist_orders_dataset.csv",
        _cols(
            order_id=STR,
            customer_id=STR,
            order_status=STR,
            order_purchase_timestamp=TS,
            order_approved_at=TS,
            order_delivered_carrier_date=TS,
            order_delivered_customer_date=TS,
            order_estimated_delivery_date=TS,
        ),
        ("order_id",),
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        # Olist has no update column: the latest lifecycle timestamp stands in for it.
        derived={"order_updated_at": pl.max_horizontal(*_LIFECYCLE).cast(TS)},
        incremental_column="order_updated_at",
    ),
    _contract(
        "order_items",
        "olist_order_items_dataset.csv",
        _cols(
            order_id=STR,
            order_item_id=INT,
            product_id=STR,
            seller_id=STR,
            shipping_limit_date=TS,
            price=MONEY,
            freight_value=MONEY,
        ),
        ("order_id", "order_item_id"),
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ),
    _contract(
        "customers",
        "olist_customers_dataset.csv",
        _cols(
            customer_id=STR,
            customer_unique_id=STR,
            customer_zip_code_prefix=STR,
            customer_city=STR,
            customer_state=STR,
        ),
        ("customer_id",),
        "customer_unique_id",
    ),
    _contract(
        "products",
        "olist_products_dataset.csv",
        _cols(
            product_id=STR,
            product_category_name=STR,
            product_name_lenght=INT,  # sic: the typo is part of the Olist header
            product_description_lenght=INT,  # sic
            product_photos_qty=INT,
            product_weight_g=INT,
            product_length_cm=INT,
            product_height_cm=INT,
            product_width_cm=INT,
        ),
        ("product_id",),
    ),
    _contract(
        "sellers",
        "olist_sellers_dataset.csv",
        _cols(
            seller_id=STR,
            seller_zip_code_prefix=STR,
            seller_city=STR,
            seller_state=STR,
        ),
        ("seller_id",),
    ),
    _contract(
        "reviews",
        "olist_order_reviews_dataset.csv",
        _cols(
            review_id=STR,
            order_id=STR,
            review_score=INT,
            review_comment_title=STR,
            review_comment_message=STR,
            review_creation_date=TS,
            review_answer_timestamp=TS,
        ),
        ("review_id", "order_id"),
        "review_score",
    ),
    _contract(
        "payments",
        "olist_order_payments_dataset.csv",
        _cols(
            order_id=STR,
            payment_sequential=INT,
            payment_type=STR,
            payment_installments=INT,
            payment_value=MONEY,
        ),
        ("order_id", "payment_sequential"),
        "payment_value",
    ),
    _contract(
        "geolocation",
        "olist_geolocation_dataset.csv",
        _cols(
            geolocation_zip_code_prefix=STR,
            geolocation_lat=pl.Float64,
            geolocation_lng=pl.Float64,
            geolocation_city=STR,
            geolocation_state=STR,
        ),
        (),  # no natural key: bronze collapses to one row per zip prefix
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
    ),
    _contract(
        "category_translation",
        "product_category_name_translation.csv",
        _cols(product_category_name=STR, product_category_name_english=STR),
        ("product_category_name",),
        "product_category_name_english",
    ),
)

CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {c.name: c for c in _CONTRACT_LIST}
)
