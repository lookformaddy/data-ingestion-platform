"""Deterministic synthetic generators with controlled corruption.

Three generic domains (retail orders, customers, payments) with a seeded
``random.Random`` so every demo and test reproduces byte-for-byte. Each
generator can inject a controlled share of corruption:

* ``bad_type`` — a field with the wrong type (e.g. amount as a string)
* ``missing_field`` — a required field dropped
* ``duplicate`` — an exact re-emission of an earlier record (same dedup key)
* ``late`` — an event time far behind the watermark
* ``unknown_field`` — a field the registered schema has never seen (drift)

Corruption is applied deterministically (every Nth record), never at
random, so demo counts are exact and repeatable.
"""

from __future__ import annotations

import copy
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from .connectors import ChangeEvent

BASE_TIME = datetime(2026, 11, 18, 9, 0, 0, tzinfo=timezone.utc)

CITIES = ["Springfield", "Riverton", "Lakeside", "Fairview", "Brookfield"]
STATUSES = ["placed", "paid", "shipped", "delivered"]
CURRENCIES = ["USD", "EUR", "GBP"]


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def _order(rng: random.Random, i: int, event_time: datetime) -> dict[str, Any]:
    return {
        "order_id": f"ORD-{100000 + i}",
        "customer_id": f"CUST-{rng.randint(1, 800):05d}",
        "amount": round(rng.uniform(12.0, 950.0), 2),
        "currency": rng.choice(CURRENCIES),
        "status": rng.choice(STATUSES),
        "event_time": _iso(event_time),
        "shipping": {
            "city": rng.choice(CITIES),
            "zip": f"{rng.randint(10000, 99999)}",
        },
        "items": [
            {"sku": f"SKU-{rng.randint(100, 999)}", "qty": rng.randint(1, 4),
             "price": round(rng.uniform(5.0, 200.0), 2)}
            for _ in range(rng.randint(1, 3))
        ],
    }


def _customer(rng: random.Random, i: int, event_time: datetime) -> dict[str, Any]:
    return {
        "customer_id": f"CUST-{i + 1:05d}",
        "email": f"customer{i + 1}@example.test",
        "name": f"Customer {i + 1}",
        "segment": rng.choice(["standard", "premium", "enterprise"]),
        "country": rng.choice(["US", "GB", "DE", "FR", "CA"]),
        "created_at": _iso(event_time - timedelta(days=rng.randint(30, 900))),
        "event_time": _iso(event_time),
    }


def _payment(rng: random.Random, i: int, event_time: datetime) -> dict[str, Any]:
    return {
        "payment_id": f"PAY-{200000 + i}",
        "order_id": f"ORD-{100000 + rng.randint(0, 1999)}",
        "amount": round(rng.uniform(12.0, 950.0), 2),
        "method": rng.choice(["card", "bank_transfer", "wallet"]),
        "status": rng.choice(["authorized", "captured", "refunded"]),
        "event_time": _iso(event_time),
    }


def _corrupt(
    record: dict[str, Any], kind: str, rng: random.Random
) -> dict[str, Any]:
    """Apply one deterministic corruption to a copy of ``record``."""
    bad = copy.deepcopy(record)
    if kind == "bad_type":
        # amount becomes a non-numeric string: a type_mismatch error.
        if "amount" in bad:
            bad["amount"] = "not-a-number"
        elif "segment" in bad:
            bad["segment"] = 42
    elif kind == "missing_field":
        bad.pop("customer_id", None)
        bad.pop("order_id", None)
    elif kind == "unknown_field":
        bad["loyalty_tier_v2"] = "gold"
    elif kind == "late":
        old = BASE_TIME - timedelta(hours=6)
        bad["event_time"] = _iso(old)
    return bad


def generate_orders(
    n: int,
    *,
    seed: int = 42,
    duplicate_every: int = 50,
    bad_type_every: int = 40,
    missing_every: int = 60,
    late_every: int = 45,
    unknown_every: int = 0,
) -> list[dict[str, Any]]:
    """Generate ``n`` order records with deterministic corruption.

    Corruption cadence is by index, so the count of each defect class is
    exact: duplicates = n // duplicate_every (minus overlaps), etc.
    """
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []
    for i in range(n):
        event_time = BASE_TIME + timedelta(seconds=i * 7)
        rec = _order(rng, i, event_time)
        if unknown_every and i % unknown_every == 0 and i > 0:
            rec = _corrupt(rec, "unknown_field", rng)
        elif bad_type_every and i % bad_type_every == 0 and i > 0:
            rec = _corrupt(rec, "bad_type", rng)
        elif missing_every and i % missing_every == 0 and i > 0:
            rec = _corrupt(rec, "missing_field", rng)
        elif late_every and i % late_every == 0 and i > 0:
            rec = _corrupt(rec, "late", rng)
        records.append(rec)
        if duplicate_every and i % duplicate_every == 0 and i > 0:
            records.append(copy.deepcopy(records[-1]))
    return records


def generate_customers(
    n: int,
    *,
    seed: int = 7,
    duplicate_every: int = 55,
    bad_type_every: int = 48,
    missing_every: int = 70,
    late_every: int = 0,
    unknown_every: int = 0,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []
    for i in range(n):
        event_time = BASE_TIME + timedelta(seconds=i * 9)
        rec = _customer(rng, i, event_time)
        if unknown_every and i % unknown_every == 0 and i > 0:
            rec = _corrupt(rec, "unknown_field", rng)
        elif bad_type_every and i % bad_type_every == 0 and i > 0:
            rec = _corrupt(rec, "bad_type", rng)
        elif missing_every and i % missing_every == 0 and i > 0:
            rec = _corrupt(rec, "missing_field", rng)
        elif late_every and i % late_every == 0 and i > 0:
            rec = _corrupt(rec, "late", rng)
        records.append(rec)
        if duplicate_every and i % duplicate_every == 0 and i > 0:
            records.append(copy.deepcopy(records[-1]))
    return records


def generate_payments_cdc(
    n: int,
    *,
    seed: int = 99,
    duplicate_every: int = 60,
    bad_type_every: int = 52,
    missing_every: int = 0,
    late_every: int = 0,
    unknown_every: int = 0,
) -> list[ChangeEvent]:
    """Generate ``n`` CDC change events (insert/update/delete by LSN)."""
    rng = random.Random(seed)
    events: list[ChangeEvent] = []
    lsn = 1000
    for i in range(n):
        event_time = BASE_TIME + timedelta(seconds=i * 11)
        rec = _payment(rng, i, event_time)
        op = "insert"
        if i > 0 and i % 9 == 0:
            op = "update"
        if i > 0 and i % 23 == 0:
            op = "delete"
        if unknown_every and i % unknown_every == 0 and i > 0:
            rec = _corrupt(rec, "unknown_field", rng)
        elif bad_type_every and i % bad_type_every == 0 and i > 0:
            rec = _corrupt(rec, "bad_type", rng)
        elif missing_every and i % missing_every == 0 and i > 0:
            rec = _corrupt(rec, "missing_field", rng)
        events.append(ChangeEvent(lsn=lsn, op=op, record=rec))
        lsn += 1
        if duplicate_every and i % duplicate_every == 0 and i > 0:
            # Re-emit the same event (same LSN): the CDC connector must
            # dedup it via the checkpoint, not the dedup key.
            events.append(ChangeEvent(lsn=events[-1].lsn, op=op, record=copy.deepcopy(rec)))
    return events


def build_demo_dataset() -> dict[str, Any]:
    """The demo dataset: 5,000 records across three sources.

    Orders (file, 2,000 + duplicates), customers (HTTP API, 2,000 +
    duplicates), payments (CDC feed, 1,000 events + duplicate re-emissions).
    """
    orders = generate_orders(2000)
    customers = generate_customers(2000)
    payments = generate_payments_cdc(1000)
    return {
        "orders": orders,
        "customers": customers,
        "payments": payments,
        "total_records": len(orders) + len(customers) + len(payments),
    }
