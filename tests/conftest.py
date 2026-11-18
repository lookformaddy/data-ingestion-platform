"""Pytest configuration: put the src layout on the import path so plain
``pytest`` passes from the project root, with or without an install.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json

import pytest

from ingest.connectors import ConnectorRegistry, FileConnector
from ingest.engine import Engine, SourceConfig
from ingest.store import IngestStore


@pytest.fixture()
def state_dir(tmp_path):
    return tmp_path / "state"


@pytest.fixture()
def store(state_dir):
    return IngestStore(state_dir)


@pytest.fixture()
def engine(store):
    return Engine(store, ConnectorRegistry())


@pytest.fixture()
def orders_schema_dict():
    return {
        "name": "orders",
        "version": 1,
        "dedup_key": ["order_id"],
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "float", "minimum": 0},
            {"name": "currency", "type": "string",
             "enum": ["USD", "EUR", "GBP"]},
            {"name": "status", "type": "string",
             "enum": ["placed", "paid", "shipped", "delivered"]},
            {"name": "event_time", "type": "timestamp"},
            {"name": "shipping", "type": "object", "children": [
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "string"},
            ]},
            {"name": "items", "type": "array", "item_type": "float", "required": False},
        ],
    }


@pytest.fixture()
def orders_schema(orders_schema_dict):
    from ingest.schema import SchemaVersion

    return SchemaVersion.from_dict(orders_schema_dict)


def _valid_order(i=1, **overrides):
    rec = {
        "order_id": f"ORD-{100000 + i}",
        "customer_id": f"CUST-{i:05d}",
        "amount": 42.5,
        "currency": "USD",
        "status": "paid",
        "event_time": "2026-11-18T09:00:00Z",
        "shipping": {"city": "Springfield", "zip": "12345"},
    }
    rec.update(overrides)
    return rec


@pytest.fixture()
def valid_order():
    return _valid_order()


@pytest.fixture()
def orders_file(tmp_path, valid_order):
    """A small JSONL orders file: 6 valid, 1 bad type, 1 missing field,
    1 duplicate of the first row, 1 late row."""
    rows = [_valid_order(i) for i in range(1, 7)]
    bad = _valid_order(7, amount="not-a-number")
    missing = _valid_order(8)
    del missing["customer_id"]
    late = _valid_order(9, event_time="2026-11-17T09:00:00Z")
    rows += [bad, missing, _valid_order(1), late]
    path = tmp_path / "orders.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path


@pytest.fixture()
def file_registry(orders_file):
    registry = ConnectorRegistry()
    registry.register(FileConnector("orders-file", orders_file))
    return registry


@pytest.fixture()
def orders_config():
    return SourceConfig(
        name="orders-file",
        connector="orders-file",
        table="orders",
        schema_name="orders",
        dedup_key=["order_id"],
        batch_size=3,
        allowed_lateness_seconds=300,
    )


@pytest.fixture()
def engine_with_file(store, file_registry):
    return Engine(store, file_registry)
