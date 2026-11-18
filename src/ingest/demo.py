"""End-to-end demo: 5,000 records across three sources, plus the
exactly-once proofs.

Phase A — ingest the full demo dataset (file orders, paginated HTTP
customers with simulated 429s, CDC payments) and report exact counts.
Phase B — resume after interruption: crash the orders run after two
committed batches, resume the same run id, prove no duplicates/loss.
Phase C — idempotent re-run: ingest the orders source again under a new
run id; every record must drop as a duplicate, zero new rows landed.
Phase D — schema drift: a file with a brand-new field, first under
QUARANTINE (rows quarantined), then under EVOLVE (schema v2 registered).

Run with ``python -m ingest.demo`` or ``ingest demo``.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from .connectors import (
    CdcConnector,
    ConnectorRegistry,
    FileConnector,
    HttpApiConnector,
    SimulatedApi,
)
from .engine import DriftPolicy, Engine, RunStatus, SourceConfig
from .generators import build_demo_dataset, generate_orders
from .metrics import rollup
from .store import IngestStore
from .watermark import LatePolicy


def _chunk(records: list[dict], size: int) -> list[list[dict]]:
    return [records[i:i + size] for i in range(0, len(records), size)]


def build_registry_for_config(config_data: dict[str, Any]) -> ConnectorRegistry:
    """Build connectors from a JSON config (used by the CLI ``run``)."""
    registry = ConnectorRegistry()
    conn = config_data.get("connector", {})
    name = config_data["source"]["connector"]
    kind = conn.get("kind", "file")
    if kind == "file":
        registry.register(FileConnector(name, conn["path"], fmt=conn.get("fmt", "jsonl")))
    elif kind == "http_api":
        records = conn.get("records", [])
        pages = _chunk(records, conn.get("page_size", 250))
        api = SimulatedApi(pages, failures=conn.get("failures", {}),
                           rate_limit_per_sec=conn.get("rate_limit_per_sec", 100.0))
        registry.register(HttpApiConnector(name, api))
    elif kind == "cdc":
        from .connectors import ChangeEvent

        events = [ChangeEvent(lsn=e["lsn"], op=e["op"], record=e["record"])
                  for e in conn.get("events", [])]
        registry.register(CdcConnector(name, events))
    else:
        raise ValueError(f"unknown connector kind: {kind}")
    return registry


def _orders_config() -> SourceConfig:
    return SourceConfig(
        name="orders-file",
        connector="orders-file",
        table="orders",
        schema_name="orders",
        dedup_key=["order_id"],
        batch_size=500,
        allowed_lateness_seconds=300,
        late_policy=LatePolicy.ACCEPT_WITH_FLAG,
    )


def _customers_config() -> SourceConfig:
    return SourceConfig(
        name="customers-api",
        connector="customers-api",
        table="customers",
        schema_name="customers",
        dedup_key=["customer_id"],
        batch_size=500,
        allowed_lateness_seconds=300,
        late_policy=LatePolicy.ACCEPT_WITH_FLAG,
    )


def _payments_config() -> SourceConfig:
    return SourceConfig(
        name="payments-cdc",
        connector="payments-cdc",
        table="payments",
        schema_name="payments",
        dedup_key=["payment_id"],
        batch_size=500,
        allowed_lateness_seconds=300,
        late_policy=LatePolicy.QUARANTINE,
    )


def run_demo(state_dir: str | Path | None = None) -> dict[str, Any]:
    state_dir = Path(state_dir or tempfile.mkdtemp(prefix="ingest-demo-"))
    dataset = build_demo_dataset()

    # -- wire up the three simulated sources ------------------------------
    orders_path = state_dir / "sources" / "orders.jsonl"
    orders_path.parent.mkdir(parents=True, exist_ok=True)
    orders_path.write_text(
        "".join(json.dumps(r) + "\n" for r in dataset["orders"]), encoding="utf-8"
    )
    api = SimulatedApi(
        _chunk(dataset["customers"], 250),
        failures={2: 2},  # page 2 returns 429 twice, then succeeds (retry proof)
        rate_limit_per_sec=100.0,
    )
    registry = ConnectorRegistry()
    registry.register(FileConnector("orders-file", orders_path))
    http_connector = HttpApiConnector("customers-api", api)
    registry.register(http_connector)
    registry.register(CdcConnector("payments-cdc", dataset["payments"]))

    store = IngestStore(state_dir)
    engine = Engine(store, registry)

    # -- Phase A: full ingestion ------------------------------------------
    results: dict[str, Any] = {"state_dir": str(state_dir), "phases": {}}
    phase_a: dict[str, Any] = {}
    for config in (_orders_config(), _customers_config(), _payments_config()):
        result = engine.run(config)
        phase_a[config.name] = result.to_dict()
    total_in = sum(r["metrics"]["records_in"] for r in phase_a.values())
    total_valid = sum(r["metrics"]["valid"] for r in phase_a.values())
    total_quar = sum(r["metrics"]["quarantined"] for r in phase_a.values())
    total_dup = sum(r["metrics"]["duplicates_dropped"] for r in phase_a.values())
    total_late = sum(
        r["metrics"]["late_accepted"] + r["metrics"]["late_quarantined"]
        for r in phase_a.values()
    )
    phase_a["totals"] = {
        "records_in": total_in,
        "valid": total_valid,
        "quarantined": total_quar,
        "duplicates_dropped": total_dup,
        "late": total_late,
        "api_retries": http_connector.retries,
        "api_calls": api.calls,
    }
    results["phases"]["A_full_ingestion"] = phase_a

    # -- Phase B: crash after 2 batches, resume the same run id ----------
    resume_config = SourceConfig(
        name="orders-file", connector="orders-file", table="orders_resume",
        schema_name="orders", dedup_key=["order_id"], batch_size=500,
        allowed_lateness_seconds=300, late_policy=LatePolicy.ACCEPT_WITH_FLAG,
    )
    crashed = engine.run(resume_config, run_id="demo-resume-run", fault_after_batches=2)
    resumed = engine.run(resume_config, run_id="demo-resume-run")
    landed = len(store.landing_rows("orders_resume"))
    phase_b = {
        "crashed_status": crashed.status.value,
        "crashed_committed_batches": crashed.metrics.batches_committed,
        "resumed_status": resumed.status.value,
        "resumed_valid": resumed.metrics.valid,
        "resumed_duplicates": resumed.metrics.duplicates_dropped,
        "total_landed_rows": landed,
        "expected_rows": len({r["order_id"] for r in dataset["orders"]
                              if isinstance(r.get("amount"), (int, float))
                              and "customer_id" in r}),
        "no_duplicates_or_loss": (
            resumed.status is RunStatus.COMPLETED
            and landed == len({r["order_id"] for r in dataset["orders"]
                               if isinstance(r.get("amount"), (int, float))
                               and "customer_id" in r})
        ),
    }
    results["phases"]["B_resume_after_interruption"] = phase_b

    # -- Phase C: idempotent re-run of the orders source ------------------
    before = len(store.landing_rows("orders"))
    rerun = engine.run(_orders_config(), run_id="demo-rerun")
    after = len(store.landing_rows("orders"))
    results["phases"]["C_idempotent_rerun"] = {
        "status": rerun.status.value,
        "records_in": rerun.metrics.records_in,
        "new_valid": rerun.metrics.valid,
        "duplicates_dropped": rerun.metrics.duplicates_dropped,
        "landing_rows_before": before,
        "landing_rows_after": after,
        "zero_new_rows": after == before and rerun.metrics.valid == 0,
    }

    # -- Phase D: schema drift, quarantine then evolve --------------------
    drift_rows = generate_orders(50, seed=1234, unknown_every=10)
    drift_path = state_dir / "sources" / "orders_drift.jsonl"
    drift_path.write_text(
        "".join(json.dumps(r) + "\n" for r in drift_rows), encoding="utf-8"
    )
    registry.register(FileConnector("orders-drift-file", drift_path))
    drift_config = SourceConfig(
        name="orders-drift-file", connector="orders-drift-file",
        table="orders_drift", schema_name="orders", dedup_key=["order_id"],
        batch_size=500, allowed_lateness_seconds=300,
        late_policy=LatePolicy.ACCEPT_WITH_FLAG,
    )
    drifted = engine.run(drift_config)
    dlq_drift = [e for e in engine.dlq.list() if e.reason == "drift"]
    evolve_config = SourceConfig(
        name="orders-drift-file", connector="orders-drift-file",
        table="orders_evolve", schema_name="orders_evolve",
        dedup_key=["order_id"], batch_size=500,
        allowed_lateness_seconds=300, late_policy=LatePolicy.ACCEPT_WITH_FLAG,
        drift_policy=DriftPolicy.EVOLVE,
    )
    # Pre-register v1 without the new field, then evolve on the drifted file.
    from .schema import infer_schema

    v1 = infer_schema(generate_orders(50, seed=5), "orders_evolve", version=1,
                      dedup_key=["order_id"])
    store.save_schema(v1.to_dict())
    evolved = engine.run(evolve_config)
    results["phases"]["D_schema_drift"] = {
        "quarantine_run": {
            "status": drifted.status.value,
            "drift_events": drifted.drift_events,
            "quarantined": drifted.metrics.quarantined,
            "dlq_drift_entries": len(dlq_drift),
        },
        "evolve_run": {
            "status": evolved.status.value,
            "schema_version_after": evolved.schema_version,
            "valid": evolved.metrics.valid,
        },
    }

    # -- health rollup -----------------------------------------------------
    runs = store.list_runs()
    metrics_list = [engine.store.load_run(r["run_id"]) for r in runs]
    from .metrics import RunMetrics as _RM

    results["health"] = rollup(
        [_RM.from_dict(r.get("metrics", {})) for r in metrics_list],
        failed_runs=sum(1 for r in runs if r.get("status") == "failed"),
    ).to_dict()
    results["audit"] = engine.audit.verify()[1]
    return results


def main() -> None:
    summary = run_demo()
    print("=== Data Ingestion Platform — demo ===")
    phase_a = summary["phases"]["A_full_ingestion"]["totals"]
    print(f"Phase A — full ingestion: {phase_a['records_in']} records in, "
          f"{phase_a['valid']} valid, {phase_a['quarantined']} quarantined, "
          f"{phase_a['duplicates_dropped']} duplicates dropped, "
          f"{phase_a['late']} late, {phase_a['api_retries']} API retries "
          f"({phase_a['api_calls']} calls)")
    phase_b = summary["phases"]["B_resume_after_interruption"]
    print(f"Phase B — resume after crash: crashed after "
          f"{phase_b['crashed_committed_batches']} batches, resumed to "
          f"{phase_b['resumed_status']}, landed {phase_b['total_landed_rows']} rows "
          f"(expected {phase_b['expected_rows']}), "
          f"no duplicates/loss: {phase_b['no_duplicates_or_loss']}")
    phase_c = summary["phases"]["C_idempotent_rerun"]
    print(f"Phase C — idempotent re-run: {phase_c['records_in']} records replayed, "
          f"{phase_c['duplicates_dropped']} dropped as duplicates, "
          f"{phase_c['new_valid']} new rows, zero new rows: {phase_c['zero_new_rows']}")
    phase_d = summary["phases"]["D_schema_drift"]
    print(f"Phase D — drift: quarantine run quarantined "
          f"{phase_d['quarantine_run']['quarantined']} drifted rows; evolve run "
          f"registered schema v{phase_d['evolve_run']['schema_version_after']}, "
          f"{phase_d['evolve_run']['valid']} valid")
    print(f"Health: {summary['health']['status']} "
          f"(quarantine rate {summary['health']['quarantine_rate']}, "
          f"success rate {summary['health']['success_rate']})")
    print(f"Audit: {summary['audit']}")
    print(f"State dir: {summary['state_dir']}")


if __name__ == "__main__":
    main()
