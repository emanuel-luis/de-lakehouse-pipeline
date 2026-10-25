"""Simulate an operational source changing between two extractions.

The simulator mutates the source CSVs in place, as the source system would: some orders move one
step along their lifecycle and a few new orders arrive. The clock is taken from the data (one hour
after the latest lifecycle timestamp), never from the wall clock, so the same source and seed give
byte-identical files and each call moves the clock forward.
"""

import csv
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from de_lakehouse_pipeline.contracts import _LIFECYCLE, CONTRACTS, TIMESTAMP_FORMAT
from de_lakehouse_pipeline.sources.synthetic import _id, _ts, _write_csv

#: from-status -> (to-status, the one timestamp column the transition sets)
_TRANSITIONS = {
    "created": ("approved", "order_approved_at"),
    "approved": ("shipped", "order_delivered_carrier_date"),
    "invoiced": ("shipped", "order_delivered_carrier_date"),
    "processing": ("shipped", "order_delivered_carrier_date"),
    "shipped": ("delivered", "order_delivered_customer_date"),
}


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Outcome of :func:`simulate`.

    Attributes:
        clock: Simulated instant written into every changed or new timestamp.
        transitions: New status -> number of orders moved to it.
        changed: Changed order id -> its new status.
        new_orders: Orders created.
        paths: CSV files rewritten.
    """

    clock: datetime
    transitions: dict[str, int]
    changed: dict[str, str]
    new_orders: int
    paths: tuple[Path, ...]


class _Table:
    """A source CSV held as text rows, with column positions by name."""

    def __init__(self, source_dir: Path, name: str) -> None:
        self.path = source_dir / CONTRACTS[name].source_file
        with self.path.open(encoding="utf-8", newline="") as handle:
            self.header, *self.rows = csv.reader(handle)
        self.col = {c.lstrip("﻿"): i for i, c in enumerate(self.header)}

    def values(self, column: str) -> list[str]:
        return [row[self.col[column]] for row in self.rows]

    def write(self) -> Path:
        _write_csv(self.path, self.header, self.rows)
        return self.path


def _clock(orders: _Table) -> datetime:
    moments = []
    for column in _LIFECYCLE:
        for value in orders.values(column):
            try:
                moments.append(datetime.strptime(value, TIMESTAMP_FORMAT))  # noqa: DTZ007 - naive source time
            except ValueError:
                continue
    if not moments:
        raise ValueError(f"{orders.path.name}: no parseable lifecycle timestamp")
    return max(moments) + timedelta(hours=1)


def _cents(value: int) -> str:
    return f"{value // 100}.{value % 100:02d}"


def simulate(source_dir: Path, *, changes: int, new_orders: int, seed: int) -> SimulationResult:
    """Apply lifecycle changes and new orders to the source CSVs.

    Each changed order takes one step (created -> approved, approved/invoiced/processing ->
    shipped, shipped -> delivered) and gets exactly the matching timestamp set to the simulated
    clock. Only orders with at least one item line are eligible, so no item-less order leaves the
    unfulfilled statuses. New orders are ``created`` and come with a new customer, 1 to 3 item
    lines on existing products and sellers, and one credit card payment for the total.

    Args:
        source_dir: Directory holding the Olist-named CSVs.
        changes: Most orders to move one lifecycle step; fewer when fewer are eligible.
        new_orders: Orders to create.
        seed: Seed, combined with the simulated clock.

    Returns:
        What was changed and written.

    Raises:
        ValueError: The orders file has no parseable lifecycle timestamp.
    """
    orders = _Table(source_dir, "orders")
    customers = _Table(source_dir, "customers")
    items = _Table(source_dir, "order_items")
    payments = _Table(source_dir, "payments")
    clock = _clock(orders)
    rng = random.Random(f"{seed}:{clock.isoformat()}")
    now = _ts(clock)

    with_items = set(items.values("order_id"))
    position = {row[orders.col["order_id"]]: i for i, row in enumerate(orders.rows)}
    eligible = sorted(
        order_id
        for order_id, i in position.items()
        if order_id
        and order_id in with_items
        and orders.rows[i][orders.col["order_status"]] in _TRANSITIONS
    )
    changed: dict[str, str] = {}
    for order_id in rng.sample(eligible, min(changes, len(eligible))):
        row = orders.rows[position[order_id]]
        status, column = _TRANSITIONS[row[orders.col["order_status"]]]
        row[orders.col["order_status"]] = status
        row[orders.col[column]] = now
        changed[order_id] = status

    valid_customers = [
        row
        for row in customers.rows
        if row[customers.col["customer_id"]] and row[customers.col["customer_unique_id"]]
    ]
    products = sorted(set(items.values("product_id")))
    sellers = sorted(set(items.values("seller_id")))
    for _ in range(new_orders):
        order_id, customer_id = _id(rng), _id(rng)
        place = rng.choice(valid_customers)[2:]  # zip prefix, city, state
        customers.rows.append([customer_id, _id(rng), *place])
        estimated = clock + timedelta(days=rng.randint(12, 30))
        orders.rows.append([order_id, customer_id, "created", now, "", "", "", _ts(estimated)])
        total = 0
        for item_no in range(1, rng.randint(1, 3) + 1):
            price, freight = rng.randint(500, 60000), rng.randint(0, 7000)
            total += price + freight
            items.rows.append(
                [
                    order_id,
                    str(item_no),
                    rng.choice(products),
                    rng.choice(sellers),
                    _ts(clock + timedelta(days=6)),
                    _cents(price),
                    _cents(freight),
                ]
            )
        payments.rows.append([order_id, "1", "credit_card", str(rng.randint(1, 10)), _cents(total)])

    touched = [orders] if changed or new_orders else []
    if new_orders:
        touched += [customers, items, payments]
    return SimulationResult(
        clock=clock,
        transitions=dict(Counter(changed.values())),
        changed=changed,
        new_orders=new_orders,
        paths=tuple(table.write() for table in touched),
    )
