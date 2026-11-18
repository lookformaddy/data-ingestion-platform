"""FastAPI service: sources, runs, DLQ, metrics, audit.

Skipped automatically when FastAPI/httpx are not installed (system
python); the full suite runs in the venv / CI.
"""

import json

import pytest

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient

from ingest.service import build_app


@pytest.fixture()
def client(tmp_path, orders_file):
    from ingest.connectors import ConnectorRegistry, FileConnector

    registry = ConnectorRegistry()
    registry.register(FileConnector("orders-file", orders_file))
    app = build_app(state_dir=tmp_path / "state", registry=registry)
    return TestClient(app)


def test_health_and_audit_chain(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["audit_chain"]["valid"] is True


def test_sources_and_run_flow(client):
    resp = client.get("/sources")
    assert resp.status_code == 200
    assert resp.json()["connectors"][0]["name"] == "orders-file"

    resp = client.post("/sources", json={
        "name": "orders-file", "connector": "orders-file", "table": "orders",
        "schema_name": "orders", "dedup_key": ["order_id"], "batch_size": 3,
    })
    assert resp.status_code == 201

    resp = client.post("/runs", json={"source": "orders-file", "run_id": "api-r1"})
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "completed"
    assert body["metrics"]["records_in"] == 10
    assert body["metrics"]["valid"] == 7
    assert body["metrics"]["quarantined"] == 2

    resp = client.get("/runs/api-r1")
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"

    resp = client.get("/landing/orders")
    assert resp.json()["count"] == 7


def test_run_unknown_source_404(client):
    resp = client.post("/runs", json={"source": "nope"})
    assert resp.status_code == 404


def test_dlq_and_redrive(client):
    client.post("/sources", json={
        "name": "orders-file", "connector": "orders-file", "table": "orders",
        "schema_name": "orders", "dedup_key": ["order_id"], "batch_size": 3,
    })
    client.post("/runs", json={"source": "orders-file", "run_id": "api-r2"})
    resp = client.get("/dlq")
    assert resp.json()["count"] == 2
    entry_id = resp.json()["entries"][0]["id"]
    resp = client.post(f"/dlq/{entry_id}/redrive")
    assert resp.status_code == 200
    assert resp.json()["outcome"] == "still_invalid"  # record still bad


def test_health_metrics_rollup(client):
    client.post("/sources", json={
        "name": "orders-file", "connector": "orders-file", "table": "orders",
        "schema_name": "orders", "dedup_key": ["order_id"], "batch_size": 3,
    })
    client.post("/runs", json={"source": "orders-file", "run_id": "api-r3"})
    resp = client.get("/metrics/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["runs"] == 1
    assert body["records_in"] == 10
    assert body["status"] == "DEGRADED"  # quarantine rate 0.2 > 0.05
