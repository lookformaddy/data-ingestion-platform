"""Connectors: file, simulated HTTP API, CDC feed."""

import json

import pytest

from ingest.connectors import (
    CdcConnector,
    ChangeEvent,
    FileConnector,
    HttpApiConnector,
    SimulatedApi,
)
from ingest.errors import ConnectorError, RateLimitError


def _read_batches(connector, checkpoint=None, batch_size=2):
    return list(connector.iter_batches(checkpoint, batch_size))


def test_file_connector_jsonl_batches_and_resume(tmp_path):
    rows = [{"id": i, "v": f"r{i}"} for i in range(5)]
    path = tmp_path / "data.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    conn = FileConnector("f", path)
    batches = _read_batches(conn)
    assert [len(b.records) for b in batches] == [2, 2, 1]
    assert batches[-1].checkpoint == {"line": 5}
    # Resume from the checkpoint after batch 1: no rows replayed.
    resumed = _read_batches(conn, checkpoint=batches[0].checkpoint)
    assert [r["id"] for b in resumed for r in b.records] == [2, 3, 4]


def test_file_connector_csv(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,name\n1,alpha\n2,beta\n", encoding="utf-8")
    conn = FileConnector("f", path)
    batches = _read_batches(conn, batch_size=10)
    assert len(batches) == 1
    assert batches[0].records[0]["name"] == "alpha"


def test_file_connector_missing_file(tmp_path):
    with pytest.raises(ConnectorError):
        FileConnector("f", tmp_path / "nope.jsonl")


def test_http_api_pagination_and_retry():
    pages = [[{"id": i} for i in range(3)], [{"id": 3}, {"id": 4}]]
    api = SimulatedApi(pages, failures={1: 2})
    conn = HttpApiConnector("api", api, backoff_base=0.0)
    batches = _read_batches(conn, batch_size=2)
    assert [r["id"] for b in batches for r in b.records] == [0, 1, 2, 3, 4]
    assert conn.retries == 2
    assert api.calls == 4  # page 0 once, page 1 three times (2 failures + 1 ok)


def test_http_api_checkpoint_mid_page():
    pages = [[{"id": i} for i in range(5)]]
    api = SimulatedApi(pages)
    conn = HttpApiConnector("api", api)
    batches = _read_batches(conn, batch_size=2)
    assert batches[0].checkpoint == {"page": 0, "offset": 2}
    # Resume mid-page: records 2..4 only, no loss, no replay.
    resumed = _read_batches(conn, checkpoint=batches[0].checkpoint, batch_size=2)
    assert [r["id"] for b in resumed for r in b.records] == [2, 3, 4]


def test_http_api_rate_limit_exhausted_raises():
    api = SimulatedApi([[{"id": 1}]], failures={0: 10})
    conn = HttpApiConnector("api", api, max_retries=2, backoff_base=0.0)
    with pytest.raises(RateLimitError):
        list(conn.iter_batches(None, 10))


def test_cdc_connector_ordering_and_lsn_resume():
    events = [
        ChangeEvent(lsn=3, op="update", record={"payment_id": "P3"}),
        ChangeEvent(lsn=1, op="insert", record={"payment_id": "P1"}),
        ChangeEvent(lsn=2, op="insert", record={"payment_id": "P2"}),
        ChangeEvent(lsn=4, op="delete", record={"payment_id": "P1"}),
    ]
    conn = CdcConnector("cdc", events)
    batches = _read_batches(conn, batch_size=2)
    rows = [r for b in batches for r in b.records]
    assert [r["_lsn"] for r in rows] == [1, 2, 3, 4]
    assert rows[-1]["_deleted"] is True
    assert rows[-1]["_op"] == "delete"
    resumed = _read_batches(conn, checkpoint=batches[0].checkpoint, batch_size=2)
    assert [r["_lsn"] for b in resumed for r in b.records] == [3, 4]


def test_cdc_duplicate_lsn_replayed_once():
    events = [
        ChangeEvent(lsn=1, op="insert", record={"payment_id": "P1"}),
        ChangeEvent(lsn=1, op="insert", record={"payment_id": "P1"}),
    ]
    conn = CdcConnector("cdc", events)
    batches = _read_batches(conn, batch_size=10)
    assert len(batches[0].records) == 2  # both yielded; dedup key drops one
    resumed = _read_batches(conn, checkpoint=batches[0].checkpoint, batch_size=10)
    assert resumed == []
