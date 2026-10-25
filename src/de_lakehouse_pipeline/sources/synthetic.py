"""Deterministic synthetic Olist-shaped data.

Only a seeded ``random.Random`` is used and the wall clock is never read, so the same
``(orders, seed)`` yields byte-identical files. Every value is built as a string, exactly as it
would appear in a CSV. The generator plants the dirt the contracts are meant to catch.
"""

import csv
import random
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

from de_lakehouse_pipeline.contracts import CONTRACTS, TIMESTAMP_FORMAT
from de_lakehouse_pipeline.io import atomic_write
from de_lakehouse_pipeline.sources import _vocab as v

_START = datetime(2017, 1, 1)  # noqa: DTZ001 - naive on purpose, like the source system
_WINDOW = int((datetime(2018, 8, 31) - _START).total_seconds())  # noqa: DTZ001

Row = list[str]
Rows = dict[str, list[Row]]


def new_id(rng: random.Random) -> str:
    return f"{rng.getrandbits(128):032x}"


def format_ts(moment: datetime) -> str:
    return moment.strftime(TIMESTAMP_FORMAT)


def _dirty_city(rng: random.Random, city: str, i: int) -> str:
    """Every 7th city gets mixed case or stray spaces."""
    if i % 7:
        return city
    return rng.choice((city.upper(), city.title(), city + "  ", " " + city.title() + " "))


def _geolocation(rng: random.Random, zips: Sequence[str]) -> tuple[list[Row], dict[str, Row]]:
    rows: list[Row] = []
    place: dict[str, Row] = {}
    cities = [(city, uf) for city, uf, _ in v.CITIES]
    weights = [w for _, _, w in v.CITIES]
    for i, zip_prefix in enumerate(zips):
        city, uf = rng.choices(cities, weights)[0]
        lat, lng = rng.uniform(-33.0, 4.0), rng.uniform(-73.0, -35.0)
        place[zip_prefix] = [city, uf]
        base = [zip_prefix, f"{lat:.15f}", f"{lng:.15f}", city, uf]
        rows.append(base)
        if i % 3 == 0:  # exact duplicate
            rows.append(list(base))
        elif i % 3 == 1:  # jittered duplicate with a dirty city
            jitter = (rng.uniform(-0.01, 0.01), rng.uniform(-0.01, 0.01))
            dirty = _dirty_city(rng, city, 0)
            rows.append(
                [zip_prefix, f"{lat + jitter[0]:.15f}", f"{lng + jitter[1]:.15f}", dirty, uf]
            )
    return rows, place


def _products(rng: random.Random, count: int) -> list[Row]:
    rows: list[Row] = []
    for i in range(count):
        if i == 0:
            category = "pc_gamer"  # used by products, absent from the translation table
        elif i == 1 or rng.random() < 0.02:
            category = ""  # null category; Olist then also lacks name, description and photos
        else:
            category = rng.choice(v.CATEGORIES)[0]
        described = (
            [str(rng.randint(5, 60)), str(rng.randint(0, 3000)), str(rng.randint(1, 8))]
            if category
            else ["", "", ""]
        )
        measures = [str(rng.randint(50, 20000))] + [str(rng.randint(5, 100)) for _ in range(3)]
        rows.append([new_id(rng), category, *described, *measures])
    return rows


def _generate_rows(orders: int, seed: int) -> Rows:
    rng = random.Random(seed)
    zips = [str(z) for z in rng.sample(range(1000, 99999), max(10, orders // 4))]  # not padded
    geo, place = _geolocation(rng, zips)

    uniques = [new_id(rng) for _ in range(max(1, int(orders * 0.8)))]
    customer_ids = [new_id(rng) for _ in range(orders)]
    customers: list[Row] = []
    for i, customer_id in enumerate(customer_ids):
        zip_prefix = rng.choice(zips)
        city, uf = place[zip_prefix]
        customers.append(
            [customer_id, rng.choice(uniques), zip_prefix, _dirty_city(rng, city, i), uf]
        )

    sellers: list[Row] = []
    for i in range(max(5, orders // 30)):
        zip_prefix = rng.choice(zips)
        city, uf = place[zip_prefix]
        sellers.append([new_id(rng), zip_prefix, _dirty_city(rng, city, i), uf])

    products = _products(rng, max(30, orders // 3))
    unfulfilled = set(rng.sample(range(orders), max(1, orders // 100)))  # orders without items
    status_names = [s for s, _ in v.STATUSES]
    status_weights = [w for _, w in v.STATUSES]
    pay_names = [p for p, _ in v.PAYMENT_TYPES]
    pay_weights = [w for _, w in v.PAYMENT_TYPES]

    orders_rows: list[Row] = []
    items: list[Row] = []
    payments: list[Row] = []
    reviews: list[Row] = []
    for i, customer_id in enumerate(customer_ids):
        order_id = new_id(rng)
        status = (
            rng.choice(v.UNFULFILLED)
            if i in unfulfilled
            else rng.choices(status_names, status_weights)[0]
        )
        purchase = _START + timedelta(seconds=rng.randrange(_WINDOW))
        approved = purchase + timedelta(minutes=rng.randint(5, 3000))
        carrier = approved + timedelta(hours=rng.randint(4, 120))
        delivered = carrier + timedelta(days=rng.randint(2, 25), seconds=rng.randrange(86400))
        estimated = purchase + timedelta(days=rng.randint(12, 45))
        approved_known = status != "created" and not (status == "canceled" and i % 2)
        orders_rows.append(
            [
                order_id,
                customer_id,
                status,
                format_ts(purchase),
                format_ts(approved) if approved_known else "",
                format_ts(carrier) if status in ("shipped", "delivered") else "",
                format_ts(delivered) if status == "delivered" else "",
                format_ts(estimated),
            ]
        )

        if i in unfulfilled:
            payments.append([order_id, "1", "not_defined", "1", "0.00"])
            continue
        total = 0.0
        for item_no in range(1, rng.choices((1, 2, 3, 4), (80, 13, 5, 2))[0] + 1):
            price, freight = rng.uniform(5, 600), rng.uniform(0, 70)
            total += price + freight
            items.append(
                [
                    order_id,
                    str(item_no),
                    rng.choice(products)[0],
                    rng.choice(sellers)[0],
                    format_ts(purchase + timedelta(days=rng.randint(3, 8))),
                    f"{price:.2f}",
                    f"{freight:.2f}",
                ]
            )
        pay_types = [rng.choices(pay_names, pay_weights)[0]]
        if rng.random() < 0.1:
            pay_types.append("voucher")
        for seq, pay_type in enumerate(pay_types, start=1):
            share = total if len(pay_types) == 1 else total * (0.7 if seq == 1 else 0.3)
            installments = rng.randint(1, 10) if pay_type == "credit_card" else 1
            payments.append([order_id, str(seq), pay_type, str(installments), f"{share:.2f}"])

        if rng.random() < 0.9:
            created = delivered if status == "delivered" else purchase + timedelta(days=10)
            commented = rng.random() < 0.4
            reviews.append(
                [
                    new_id(rng),
                    order_id,
                    str(rng.choices((1, 2, 3, 4, 5), (10, 4, 8, 20, 58))[0]),
                    "Avaliacao" if commented and rng.random() < 0.3 else "",
                    rng.choice(v.REVIEW_MESSAGES) if commented else "",
                    format_ts(created),
                    format_ts(created + timedelta(hours=rng.randint(1, 72))),
                ]
            )

    _plant_dirt(rng, orders_rows, customers, items, reviews)
    return {
        "orders": orders_rows,
        "order_items": items,
        "customers": customers,
        "products": products,
        "sellers": sellers,
        "reviews": reviews,
        "payments": payments,
        "geolocation": geo,
        "category_translation": [[pt, en] for pt, en in v.CATEGORIES if en is not None],
    }


def _plant_dirt(
    rng: random.Random,
    orders: list[Row],
    customers: list[Row],
    items: list[Row],
    reviews: list[Row],
) -> None:
    """Add rows and values the contracts must reject; nothing valid ends up orphaned."""
    scale = len(orders)
    for k in range(0, len(reviews) - 1, max(2, len(reviews) // 100)):  # review_id reused
        reviews[k + 1][0] = reviews[k][0]
    last_item = {row[0]: int(row[1]) for row in items}  # items are ordered per order
    for _ in range(max(1, scale // 1000)):  # junk items: extra line with a non-numeric price
        junk_item = list(rng.choice(items))
        last_item[junk_item[0]] += 1
        junk_item[1] = str(last_item[junk_item[0]])
        junk_item[5] = rng.choice(("abc", "12,50", "N/A"))
        items.append(junk_item)
    for _ in range(max(1, scale // 500)):  # junk rows: null primary key, no children
        junk_order = list(rng.choice(orders))
        junk_order[0] = ""
        orders.append(junk_order)
        junk_customer = list(rng.choice(customers))
        junk_customer[0] = ""
        customers.append(junk_customer)


def write_csv(path: Path, header: Sequence[str], rows: Sequence[Row]) -> None:
    def write(tmp: Path) -> None:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)

    atomic_write(path, write)


def generate(dest: Path, *, orders: int, seed: int) -> dict[str, Path]:
    """Write the nine Olist-named CSVs into ``dest``.

    Args:
        dest: Output directory (created if missing).
        orders: Number of orders; other tables scale from it.
        seed: Seed of the random generator.

    Returns:
        Landing table name -> CSV path.
    """
    paths: dict[str, Path] = {}
    for name, rows in _generate_rows(orders, seed).items():
        contract = CONTRACTS[name]
        paths[name] = dest / contract.source_file
        write_csv(paths[name], list(contract.columns), rows)
    return paths
