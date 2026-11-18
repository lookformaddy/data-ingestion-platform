"""Audit log (hash chain + tamper detection) and DLQ (quarantine/redrive)."""

import json

from ingest.audit import AuditLog
from ingest.dlq import DlqStore
from ingest.schema import FieldError, SchemaVersion


def test_audit_chain_verifies_and_detects_tamper(tmp_path):
    log = AuditLog(tmp_path / "audit.jsonl")
    log.append("run_started", "r1", {"source": "s"})
    log.append("run_completed", "r1", {"ok": True})
    ok, detail = log.verify()
    assert ok is True
    assert "2 entries" in detail
    # Tamper with the payload of entry 1 on disk.
    lines = (tmp_path / "audit.jsonl").read_text().splitlines()
    entry = json.loads(lines[0])
    entry["payload"] = {"source": "forged"}
    lines[0] = json.dumps(entry)
    (tmp_path / "audit.jsonl").write_text("\n".join(lines) + "\n")
    tampered = AuditLog(tmp_path / "audit.jsonl")
    ok, detail = tampered.verify()
    assert ok is False
    assert "tampered" in detail or "mismatch" in detail


def test_audit_entries_filter_by_run(tmp_path):
    log = AuditLog(tmp_path / "audit.jsonl")
    log.append("run_started", "r1", {})
    log.append("run_started", "r2", {})
    assert len(log.entries("r1")) == 1
    assert len(log.entries()) == 2


def test_dlq_quarantine_and_redrive(tmp_path, orders_schema, valid_order):
    dlq = DlqStore(tmp_path / "dlq.jsonl")
    bad = dict(valid_order, amount="not-a-number")
    entry = dlq.quarantine(
        source="orders-file", run_id="r1", table="orders", record=bad,
        errors=[FieldError("amount", "type_mismatch", "expected float")],
        reason="validation",
    )
    assert dlq.list(status="quarantined")[0].id == entry.id
    # Still invalid against the current schema.
    outcome, _coerced, errors = dlq.redrive(entry.id, orders_schema)
    assert outcome == "still_invalid"
    assert len(entry.attempts) == 1
    # Fix the record in place (as an operator would) and redrive again.
    entry.record["amount"] = 42.5
    outcome, coerced, errors = dlq.redrive(entry.id, orders_schema)
    assert outcome == "redriven"
    assert errors == []
    assert coerced["amount"] == 42.5
    assert dlq.list(status="redriven")[0].id == entry.id


def test_dlq_unknown_entry_raises(tmp_path, orders_schema):
    from ingest.errors import DlqError

    dlq = DlqStore(tmp_path / "dlq.jsonl")
    import pytest

    with pytest.raises(DlqError):
        dlq.redrive("nope", orders_schema)
