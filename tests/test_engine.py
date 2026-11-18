"""Engine: full runs, exactly-once, resume, drift policies, outbox."""

import json

from ingest.engine import DriftPolicy, Engine, RunStatus, SourceConfig
from ingest.generators import generate_orders
from ingest.schema import infer_schema
from ingest.watermark import LatePolicy


def test_full_run_counts(engine_with_file, orders_config, store):
    result = engine_with_file.run(orders_config, run_id="r1")
    m = result.metrics
    assert result.status is RunStatus.COMPLETED
    assert m.records_in == 10
    assert m.valid == 7  # 6 valid + 1 late (accepted with flag)
    assert m.quarantined == 2  # bad type + missing field
    assert m.duplicates_dropped == 1
    assert m.late_accepted == 1
    assert m.batches_committed == 4
    assert len(store.landing_rows("orders")) == 7
    assert "orders-file-" in store.landing_rows("orders")[0]["_batch_id"]
    assert len(engine_with_file.dlq.list()) == 2
    assert result.schema_version == 1


def test_late_policy_quarantine(engine_with_file, orders_config, store):
    orders_config.late_policy = LatePolicy.QUARANTINE
    result = engine_with_file.run(orders_config, run_id="r-late")
    assert result.metrics.valid == 6
    assert result.metrics.quarantined == 3  # bad + missing + late
    assert result.metrics.late_quarantined == 1
    assert len(store.landing_rows("orders")) == 6


def test_resume_after_interruption_no_dup_no_loss(engine_with_file, orders_config, store):
    crashed = engine_with_file.run(orders_config, run_id="r-resume", fault_after_batches=1)
    assert crashed.status is RunStatus.INTERRUPTED
    assert crashed.metrics.batches_committed == 1
    assert len(store.landing_rows("orders")) == 3
    resumed = engine_with_file.run(orders_config, run_id="r-resume")
    assert resumed.status is RunStatus.COMPLETED
    # The resumed run's metrics are cumulative for the run: 3 records from
    # the crashed attempt + 7 replayed after the checkpoint. The genuine
    # duplicate (a copy of row 1, replayed in batch 3) still drops once.
    assert resumed.metrics.records_in == 10
    assert resumed.metrics.valid == 7
    assert resumed.metrics.duplicates_dropped == 1
    assert len(store.landing_rows("orders")) == 7
    # Every landed row is unique by dedup key.
    keys = [r["order_id"] for r in store.landing_rows("orders")]
    assert len(keys) == len(set(keys))


def test_idempotent_rerun_lands_zero_new_rows(engine_with_file, orders_config, store):
    first = engine_with_file.run(orders_config, run_id="r-first")
    assert first.status is RunStatus.COMPLETED
    before = len(store.landing_rows("orders"))
    rerun = engine_with_file.run(orders_config, run_id="r-second")
    assert rerun.status is RunStatus.COMPLETED
    assert rerun.metrics.valid == 0
    # 6 valid + 1 late, replayed — plus the duplicate copy of row 1,
    # which is itself a valid record whose key is already indexed.
    assert rerun.metrics.duplicates_dropped == 8
    assert len(store.landing_rows("orders")) == before


def test_outbox_commit_is_idempotent(store):
    rows = [{"order_id": "ORD-1", "_batch_id": "b1"}]
    store.stage_batch("b1", rows)
    m1 = store.commit_batch("b1", "r1", "t", rows, ["ORD-1"])
    m2 = store.commit_batch("b1", "r1", "t", rows, ["ORD-1"])
    assert m1.checksum == m2.checksum
    assert len(store.landing_rows("t")) == 1


def _drift_file(tmp_path, n_valid=3):
    rows = []
    for i in range(1, n_valid + 1):
        rows.append({
            "order_id": f"ORD-{100000 + i}", "customer_id": f"CUST-{i:05d}",
            "amount": 10.0, "currency": "USD", "status": "paid",
            "event_time": "2026-11-18T09:00:00Z",
            "shipping": {"city": "Springfield", "zip": "12345"},
            "items": [],
        })
    drifted = dict(rows[0], order_id="ORD-100099", loyalty_tier="gold")
    rows.append(drifted)
    path = tmp_path / "drift.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path


def test_drift_quarantine_policy(tmp_path, store):
    from ingest.connectors import ConnectorRegistry, FileConnector

    path = _drift_file(tmp_path)
    registry = ConnectorRegistry()
    registry.register(FileConnector("drift-file", path))
    engine = Engine(store, registry)
    config = SourceConfig(
        name="drift-file", connector="drift-file", table="orders",
        schema_name="orders", dedup_key=["order_id"], batch_size=3,
    )
    result = engine.run(config, run_id="r-drift")
    assert result.status is RunStatus.COMPLETED
    assert len(result.drift_events) == 1
    assert result.drift_events[0]["added"] == ["loyalty_tier"]
    assert result.metrics.quarantined == 1
    assert result.metrics.valid == 3
    assert len(store.landing_rows("orders")) == 3
    drift_entries = [e for e in engine.dlq.list() if e.reason == "drift"]
    assert len(drift_entries) == 1
    assert drift_entries[0].errors[0]["code"] == "drift_added_field"


def test_drift_evolve_policy_registers_v2(tmp_path, store):
    from ingest.connectors import ConnectorRegistry, FileConnector

    path = _drift_file(tmp_path)
    registry = ConnectorRegistry()
    registry.register(FileConnector("drift-file", path))
    engine = Engine(store, registry)
    # Pre-register v1 (no loyalty_tier) so the drift is real.
    v1 = infer_schema(generate_orders(20, seed=5), "orders_evolve", version=1,
                      dedup_key=["order_id"])
    store.save_schema(v1.to_dict())
    config = SourceConfig(
        name="drift-file", connector="drift-file", table="orders_evolve",
        schema_name="orders_evolve", dedup_key=["order_id"], batch_size=3,
        drift_policy=DriftPolicy.EVOLVE,
    )
    result = engine.run(config, run_id="r-evolve")
    assert result.status is RunStatus.COMPLETED
    assert result.schema_version == 2
    assert result.metrics.valid == 4
    assert result.metrics.quarantined == 0
    assert len(store.landing_rows("orders_evolve")) == 4
    assert store.load_schema("orders_evolve", 2) is not None


def test_run_record_persisted_and_listed(engine_with_file, orders_config, store):
    engine_with_file.run(orders_config, run_id="r-list")
    record = store.load_run("r-list")
    assert record["status"] == "completed"
    assert record["metrics"]["records_in"] == 10
    assert len(store.list_runs()) == 1
