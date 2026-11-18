"""FastAPI service: sources, runs, DLQ, metrics, audit.

The service is a thin HTTP shell over :class:`~ingest.engine.Engine`.
State lives in the state directory (``INGEST_STATE_DIR``, default
``.ingest-state``), so the API, the CLI and the demo all see the same
runs, landing tables, DLQ and audit log.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .audit import AuditLog
from .connectors import ConnectorRegistry
from .dlq import DlqStore
from .engine import Engine, RunStatus, SourceConfig
from .metrics import RunMetrics, rollup
from .schema import SchemaVersion
from .store import IngestStore


class SourceCreate(BaseModel):
    name: str
    connector: str
    table: str
    schema_name: str
    dedup_key: list[str] = []
    event_time_field: str = "event_time"
    batch_size: int = 500
    rate_limit_per_sec: float = 0.0
    allowed_lateness_seconds: float = 300.0
    late_policy: str = "accept_with_flag"
    drift_policy: str = "quarantine"
    max_buffer_batches: int = 4


class RunCreate(BaseModel):
    source: str
    run_id: str | None = None


def build_app(
    state_dir: str | Path | None = None,
    registry: ConnectorRegistry | None = None,
) -> FastAPI:
    state_dir = Path(state_dir or os.environ.get("INGEST_STATE_DIR", ".ingest-state"))
    store = IngestStore(state_dir)
    registry = registry or ConnectorRegistry()
    audit = AuditLog(state_dir / "audit.jsonl")
    dlq = DlqStore(state_dir / "dlq.jsonl")
    engine = Engine(store, registry, audit=audit, dlq=dlq)
    configs: dict[str, SourceConfig] = {}

    app = FastAPI(title="Data Ingestion Platform", version="1.0.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        ok, detail = audit.verify()
        return {"status": "ok", "audit_chain": {"valid": ok, "detail": detail}}

    @app.get("/sources")
    def list_sources() -> dict[str, Any]:
        return {
            "connectors": [c.to_dict() for c in registry.list()],
            "configured_sources": [c.to_dict() for c in configs.values()],
        }

    @app.post("/sources", status_code=201)
    def create_source(body: SourceCreate) -> dict[str, Any]:
        config = SourceConfig.from_dict(body.model_dump())
        configs[config.name] = config
        return {"source": config.to_dict()}

    @app.post("/runs", status_code=201)
    def start_run(body: RunCreate) -> dict[str, Any]:
        config = configs.get(body.source)
        if config is None:
            raise HTTPException(status_code=404, detail=f"unknown source: {body.source}")
        result = engine.run(config, run_id=body.run_id)
        return result.to_dict()

    @app.get("/runs")
    def list_runs() -> dict[str, Any]:
        return {"runs": store.list_runs()}

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        record = store.load_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"unknown run: {run_id}")
        return record

    @app.get("/runs/{run_id}/metrics")
    def run_metrics(run_id: str) -> dict[str, Any]:
        record = store.load_run(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"unknown run: {run_id}")
        return record.get("metrics", {})

    @app.get("/dlq")
    def list_dlq(status: str | None = None, source: str | None = None) -> dict[str, Any]:
        entries = dlq.list(status=status, source=source)
        return {"count": len(entries), "entries": [e.to_dict() for e in entries]}

    @app.post("/dlq/{entry_id}/redrive")
    def redrive(entry_id: str) -> dict[str, Any]:
        try:
            entry = dlq.get(entry_id)
        except Exception:
            raise HTTPException(status_code=404, detail=f"unknown DLQ entry: {entry_id}") from None
        stored = store.load_schema(entry.table)
        if stored is None:
            raise HTTPException(status_code=409, detail=f"no schema registered for {entry.table}")
        schema = SchemaVersion.from_dict(stored)
        outcome, coerced, errors = dlq.redrive(entry_id, schema)
        if outcome == "redriven" and coerced is not None:
            # Land the rescued row through the normal commit path.
            config = configs.get(entry.source)
            table = entry.table
            batch_id = f"redrive-{entry_id}"
            coerced["_run_id"] = entry.run_id
            coerced["_batch_id"] = batch_id
            store.stage_batch(batch_id, [coerced])
            store.commit_batch(batch_id, entry.run_id, table, [coerced], [])
        return {
            "outcome": outcome,
            "errors": [e.to_dict() for e in errors],
            "record": coerced,
        }

    @app.get("/metrics/health")
    def health_metrics() -> dict[str, Any]:
        runs = store.list_runs()
        metrics_list = [RunMetrics.from_dict(r.get("metrics", {})) for r in runs]
        failed = sum(1 for r in runs if r.get("status") == RunStatus.FAILED.value)
        return rollup(metrics_list, failed_runs=failed).to_dict()

    @app.get("/audit")
    def audit_log(run_id: str | None = None) -> dict[str, Any]:
        ok, detail = audit.verify()
        return {
            "valid": ok,
            "detail": detail,
            "entries": [e.to_dict() for e in audit.entries(run_id)],
        }

    @app.get("/landing/{table}")
    def landing(table: str, limit: int = 10) -> dict[str, Any]:
        rows = store.landing_rows(table)
        return {"table": table, "count": len(rows), "rows": rows[:limit]}

    return app


app = build_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
