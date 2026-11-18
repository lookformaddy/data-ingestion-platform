"""Run orchestration: exactly-once ingestion with resumable runs.

The engine wires the pieces together:

1. Load (or infer and register) the versioned schema for the landing table.
2. Producer thread pulls batches from the source connector into a bounded
   buffer (backpressure + pause/resume live here).
3. Consumer validates each record against the schema, detects schema
   drift, drops duplicates by dedup key, applies the watermark / late
   policy, then commits the batch through the store's outbox protocol.
4. Every lifecycle event is hash-chained into the audit log; per-run
   metrics and the checkpoint are persisted after each committed batch,
   so an interrupted run resumes without duplicates or loss.

A run interrupted mid-batch (crash, or the injected fault used by the
demo/tests) resumes from the last *committed* batch: the connector
replays from the saved checkpoint and the dedup index + outbox markers
make the replay a no-op for already-landed rows.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from .audit import AuditLog
from .connectors import ConnectorRegistry, SourceConnector
from .dlq import DlqStore
from .errors import ConnectorError, IngestionError
from .flow import Batch, BoundedBuffer
from .metrics import RunMetrics
from .schema import (
    FieldError,
    SchemaVersion,
    detect_drift,
    infer_schema,
    validate_record,
)
from .store import IngestStore
from .watermark import LatePolicy, WatermarkTracker, parse_event_time


class DriftPolicy(str, Enum):
    QUARANTINE = "quarantine"
    EVOLVE = "evolve"


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass
class SourceConfig:
    """Declarative config for one source → one landing table."""

    name: str
    connector: str  # connector name in the registry
    table: str
    schema_name: str
    dedup_key: list[str] = field(default_factory=list)
    event_time_field: str = "event_time"
    batch_size: int = 500
    rate_limit_per_sec: float = 0.0  # 0 = no engine-side limit
    allowed_lateness_seconds: float = 300.0
    late_policy: LatePolicy = LatePolicy.ACCEPT_WITH_FLAG
    drift_policy: DriftPolicy = DriftPolicy.QUARANTINE
    max_buffer_batches: int = 4

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "connector": self.connector,
            "table": self.table,
            "schema_name": self.schema_name,
            "dedup_key": self.dedup_key,
            "event_time_field": self.event_time_field,
            "batch_size": self.batch_size,
            "rate_limit_per_sec": self.rate_limit_per_sec,
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
            "late_policy": self.late_policy.value,
            "drift_policy": self.drift_policy.value,
            "max_buffer_batches": self.max_buffer_batches,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SourceConfig":
        return cls(
            name=d["name"],
            connector=d["connector"],
            table=d["table"],
            schema_name=d["schema_name"],
            dedup_key=d.get("dedup_key", []),
            event_time_field=d.get("event_time_field", "event_time"),
            batch_size=d.get("batch_size", 500),
            rate_limit_per_sec=d.get("rate_limit_per_sec", 0.0),
            allowed_lateness_seconds=d.get("allowed_lateness_seconds", 300.0),
            late_policy=LatePolicy(d.get("late_policy", "accept_with_flag")),
            drift_policy=DriftPolicy(d.get("drift_policy", "quarantine")),
            max_buffer_batches=d.get("max_buffer_batches", 4),
        )


@dataclass
class RunResult:
    run_id: str
    source: str
    table: str
    status: RunStatus
    metrics: RunMetrics
    schema_version: int
    watermark: dict[str, Any]
    drift_events: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "table": self.table,
            "status": self.status.value,
            "metrics": self.metrics.to_dict(),
            "schema_version": self.schema_version,
            "watermark": self.watermark,
            "drift_events": self.drift_events,
        }


class _SimulatedInterruption(Exception):
    """Raised by the fault-injection hook to simulate a mid-run crash."""


class Engine:
    """Runs ingestion for one source config against a state directory."""

    def __init__(
        self,
        store: IngestStore,
        registry: ConnectorRegistry,
        *,
        audit: AuditLog | None = None,
        dlq: DlqStore | None = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.audit = audit or AuditLog(store.root / "audit.jsonl")
        self.dlq = dlq or DlqStore(store.root / "dlq.jsonl")
        self.buffer: BoundedBuffer | None = None

    # -- pause / resume ---------------------------------------------------
    def pause(self) -> None:
        if self.buffer is not None:
            self.buffer.pause()

    def resume(self) -> None:
        if self.buffer is not None:
            self.buffer.resume()

    # -- schema -----------------------------------------------------------
    def _load_or_infer_schema(
        self, config: SourceConfig, sample: list[dict[str, Any]]
    ) -> SchemaVersion:
        stored = self.store.load_schema(config.schema_name)
        if stored is not None:
            return SchemaVersion.from_dict(stored)
        schema = infer_schema(
            sample, config.schema_name, version=1, dedup_key=config.dedup_key
        )
        self.store.save_schema(schema.to_dict())
        return schema

    def _dedup_key_value(self, record: dict[str, Any], config: SourceConfig) -> str | None:
        if not config.dedup_key:
            return None
        parts = []
        for key in config.dedup_key:
            if key not in record or record[key] is None:
                return None
            parts.append(str(record[key]))
        return "|".join(parts)

    # -- main entry point -------------------------------------------------
    def run(
        self,
        config: SourceConfig,
        *,
        run_id: str | None = None,
        fault_after_batches: int | None = None,
    ) -> RunResult:
        run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
        connector = self.registry.get(config.connector)
        checkpoint = self.store.load_checkpoint(run_id)
        saved_run = self.store.load_run(run_id)
        metrics = RunMetrics.from_dict(saved_run["metrics"]) if saved_run else RunMetrics()
        watermark = WatermarkTracker(config.allowed_lateness_seconds)
        if saved_run and saved_run.get("watermark", {}).get("max_event_time"):
            watermark.max_event_time = datetime.fromisoformat(
                saved_run["watermark"]["max_event_time"].replace("Z", "+00:00")
            )
        schema: SchemaVersion | None = None
        drift_events: list[dict[str, Any]] = []
        status = RunStatus.RUNNING
        started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.audit.append("run_started", run_id, {"source": config.name, "table": config.table})
        self.store.save_run(run_id, {
            "run_id": run_id, "source": config.name, "table": config.table,
            "status": status.value, "started_at": started,
            "metrics": metrics.to_dict(), "config": config.to_dict(),
            "watermark": watermark.to_dict(),
        })

        buffer = BoundedBuffer(max_batches=config.max_buffer_batches)
        self.buffer = buffer
        producer_error: list[BaseException] = []

        def _produce() -> None:
            try:
                for batch in connector.iter_batches(checkpoint, config.batch_size):
                    if not buffer.put(batch):
                        break
            except BaseException as exc:  # noqa: BLE001 — re-raised on the consumer side
                producer_error.append(exc)
            finally:
                buffer.close()

        producer = threading.Thread(target=_produce, name=f"producer-{run_id}", daemon=True)
        producer.start()
        committed_batches = 0
        try:
            while True:
                batch = buffer.get()
                if batch is None:
                    break
                # Batch ids are deterministic per source (e.g. a file line
                # offset), so namespace them by run: the outbox marker
                # check must skip replays *within this run* (crash between
                # commit and checkpoint save), not a legitimate re-run of
                # the same source under a new run id.
                batch.batch_id = f"{run_id}:{batch.batch_id}"
                metrics.backpressure_events += buffer.backpressure_events
                buffer.backpressure_events = 0
                if schema is None:
                    schema = self._load_or_infer_schema(config, batch.records)
                t0 = time.perf_counter()
                schema = self._process_batch(
                    config, run_id, batch, schema, watermark, metrics, drift_events
                )
                batch_ms = (time.perf_counter() - t0) * 1000.0
                committed_batches += 1
                if fault_after_batches is not None and committed_batches >= fault_after_batches:
                    raise _SimulatedInterruption(
                        f"simulated crash after {committed_batches} committed batches"
                    )
                # Persist checkpoint + run record after every committed batch.
                self.store.save_checkpoint(run_id, batch.checkpoint)
                self.store.save_run(run_id, {
                    "run_id": run_id, "source": config.name, "table": config.table,
                    "status": RunStatus.RUNNING.value, "started_at": started,
                    "metrics": metrics.to_dict(), "config": config.to_dict(),
                    "watermark": watermark.to_dict(),
                    "schema_version": schema.version,
                })
        except _SimulatedInterruption:
            status = RunStatus.INTERRUPTED
            self.audit.append("run_interrupted", run_id, {
                "source": config.name, "committed_batches": committed_batches,
            })
        except Exception as exc:  # noqa: BLE001 — run failure is a recorded outcome
            status = RunStatus.FAILED
            self.audit.append("run_failed", run_id, {
                "source": config.name, "error": str(exc),
            })
        else:
            if producer_error:
                status = RunStatus.FAILED
                self.audit.append("run_failed", run_id, {
                    "source": config.name, "error": str(producer_error[0]),
                })
            else:
                status = RunStatus.COMPLETED
                self.audit.append("run_completed", run_id, {
                    "source": config.name, "metrics": metrics.to_dict(),
                })
        finally:
            # Close first so a producer blocked on a full buffer (consumer
            # stopped early, e.g. simulated crash) unblocks and exits.
            buffer.close()
            producer.join(timeout=10)

        finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.store.save_run(run_id, {
            "run_id": run_id, "source": config.name, "table": config.table,
            "status": status.value, "started_at": started, "finished_at": finished,
            "metrics": metrics.to_dict(), "config": config.to_dict(),
            "watermark": watermark.to_dict(),
            "schema_version": schema.version if schema else 0,
            "drift_events": drift_events,
        })
        return RunResult(
            run_id=run_id,
            source=config.name,
            table=config.table,
            status=status,
            metrics=metrics,
            schema_version=schema.version if schema else 0,
            watermark=watermark.to_dict(),
            drift_events=drift_events,
        )

    # -- batch processing -------------------------------------------------
    def _process_batch(
        self,
        config: SourceConfig,
        run_id: str,
        batch: Batch,
        schema: SchemaVersion,
        watermark: WatermarkTracker,
        metrics: RunMetrics,
        drift_events: list[dict[str, Any]],
    ) -> SchemaVersion:
        """Process one batch; returns the schema to use for the next batch
        (a new version when the drift policy evolved it)."""
        if self.store.marker_for(batch.batch_id) is not None:
            # Already committed by a previous attempt of this run: skip.
            return schema
        schema, records = self._apply_drift_policy(
            config, run_id, batch, schema, metrics, drift_events
        )
        staged_rows: list[dict[str, Any]] = []
        dedup_keys: list[str] = []
        metrics.records_in += len(batch.records)
        metrics.bytes_in += sum(len(json.dumps(r, default=str)) for r in batch.records)
        # Keys staged in this batch: the durable dedup index only updates
        # at commit time, so without this set a duplicate *within* the
        # same batch would land twice.
        seen_this_batch: set[str] = set()
        for record in records:
            t0 = time.perf_counter()
            coerced, errors = validate_record(record, schema)
            metrics.record_latency((time.perf_counter() - t0) * 1000.0)
            if errors:
                metrics.quarantined += 1
                self.dlq.quarantine(
                    source=config.name, run_id=run_id, table=config.table,
                    record=record, errors=errors, reason="validation",
                )
                continue
            key = self._dedup_key_value(coerced, config)
            if key is not None and (
                key in seen_this_batch or self.store.is_duplicate(config.table, key)
            ):
                metrics.duplicates_dropped += 1
                continue
            # Watermark / late-data handling.
            is_late = False
            event_raw = coerced.get(config.event_time_field)
            if event_raw is not None:
                try:
                    event_time = parse_event_time(event_raw)
                    is_late = watermark.observe(event_time)
                except (ValueError, TypeError):
                    is_late = False
            if is_late:
                if config.late_policy is LatePolicy.QUARANTINE:
                    metrics.quarantined += 1
                    metrics.late_quarantined += 1
                    self.dlq.quarantine(
                        source=config.name, run_id=run_id, table=config.table,
                        record=record,
                        errors=[FieldError(config.event_time_field, "late_arrival",
                                           f"event time {event_raw} is behind the watermark")],
                        reason="late",
                    )
                    continue
                metrics.late_accepted += 1
                coerced["_late"] = True
            coerced["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            coerced["_run_id"] = run_id
            coerced["_batch_id"] = batch.batch_id
            staged_rows.append(coerced)
            if key is not None:
                dedup_keys.append(key)
                seen_this_batch.add(key)
        metrics.valid += len(staged_rows)
        self.store.stage_batch(batch.batch_id, staged_rows)
        self.store.commit_batch(
            batch.batch_id, run_id, config.table, staged_rows, dedup_keys
        )
        metrics.batches_committed += 1
        return schema

    def _apply_drift_policy(
        self,
        config: SourceConfig,
        run_id: str,
        batch: Batch,
        schema: SchemaVersion,
        metrics: RunMetrics,
        drift_events: list[dict[str, Any]],
    ) -> tuple[SchemaVersion, list[dict[str, Any]]]:
        """Detect drift in this batch and apply the configured policy.

        Returns (schema, clean_records). Under QUARANTINE, rows carrying
        the drifted fields are quarantined with reason "drift" and removed
        from the batch. Under EVOLVE with additive-only drift, a new
        schema version is registered and all rows proceed.
        """
        inferred = infer_schema(batch.records[:500], config.schema_name, version=schema.version)
        drift = detect_drift(inferred, schema)
        if not drift.has_drift:
            return schema, list(batch.records)
        event = {
            "batch_id": batch.batch_id,
            "added": drift.added,
            "removed": drift.removed,
            "changed": [{"field": f, "from": o, "to": n} for f, o, n in drift.changed],
            "policy": config.drift_policy.value,
        }
        drift_events.append(event)
        self.audit.append("schema_drift", run_id, event)
        if config.drift_policy is DriftPolicy.EVOLVE and drift.is_additive_only:
            new_version = SchemaVersion(
                name=schema.name,
                version=schema.version + 1,
                fields=schema.fields + [
                    f for f in inferred.fields if f.name in drift.added
                ],
                dedup_key=schema.dedup_key,
                created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            self.store.save_schema(new_version.to_dict())
            self.audit.append("schema_evolved", run_id, {
                "schema": schema.name, "from": schema.version, "to": new_version.version,
                "added": drift.added,
            })
            return new_version, list(batch.records)
        # QUARANTINE (or non-additive drift under EVOLVE): quarantine the
        # rows that actually carry the drift, let the rest proceed.
        clean: list[dict[str, Any]] = []
        for record in batch.records:
            reasons: list[FieldError] = []
            for added_field in drift.added:
                if added_field in record:
                    reasons.append(FieldError(
                        added_field, "drift_added_field",
                        f"field '{added_field}' is not in schema v{schema.version}"))
            for removed_field in drift.removed:
                if removed_field not in record:
                    reasons.append(FieldError(
                        removed_field, "drift_removed_field",
                        f"field '{removed_field}' was removed from the source"))
            for changed_field, _old, _new in drift.changed:
                if changed_field in record:
                    reasons.append(FieldError(
                        changed_field, "drift_type_changed",
                        f"field '{changed_field}' changed type from {_old} to {_new}"))
            if reasons:
                metrics.quarantined += 1
                self.dlq.quarantine(
                    source=config.name, run_id=run_id, table=config.table,
                    record=record, errors=reasons, reason="drift",
                )
            else:
                clean.append(record)
        return schema, clean
