"""Durable state: landing tables, dedup index, outbox, checkpoints, runs.

Layout under the state root (default ``.ingest-state``)::

    landing/<table>.jsonl          committed rows (append-only)
    staging/<batch_id>.jsonl       rows of a batch not yet committed
    dedup/<table>.json             {dedup_key: batch_id} for committed rows
    outbox.jsonl                   commit markers, one per committed batch
    checkpoints/<run_id>.json      per-run resume offsets
    runs/<run_id>.json             run record + metrics
    dlq.jsonl                      quarantined records with failure history
    audit.jsonl                    hash-chained audit log
    schemas/<name>.json            registered schema versions

Exactly-once commit protocol
----------------------------
A batch is *committed* only when its marker exists in the outbox. The
consumer stages rows to ``staging/<batch_id>.jsonl``, then commits by
(1) appending the staged rows to the landing file, (2) appending the
outbox marker, (3) updating the dedup index, (4) deleting the staging
file. If the process dies between (1) and (2), the batch has no marker:
on the next open the store replays the outbox and drops any landing rows
whose batch has no marker, so a re-run never lands a row twice. The
dedup index is only updated for committed batches, so a replayed batch
is re-checked against it before writing.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


@dataclass
class CommitMarker:
    batch_id: str
    run_id: str
    table: str
    rows: int
    checksum: str
    committed_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "table": self.table,
            "rows": self.rows,
            "checksum": self.checksum,
            "committed_at": self.committed_at,
        }


class IngestStore:
    """File-backed durable state for the ingestion platform."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        for sub in ("landing", "staging", "dedup", "checkpoints", "runs", "schemas"):
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        self._recover()

    # -- recovery ---------------------------------------------------------
    def _recover(self) -> None:
        """Drop staged rows and unmarked landing rows from crashed runs."""
        committed = {m["batch_id"] for m in _read_jsonl(self.root / "outbox.jsonl")}
        # Staging files without a marker are uncommitted work: discard.
        for staging in (self.root / "staging").glob("*.jsonl"):
            if staging.stem not in committed:
                staging.unlink()
        # Landing rows carry _batch_id; strip rows from uncommitted batches.
        for landing in (self.root / "landing").glob("*.jsonl"):
            rows = _read_jsonl(landing)
            kept = [r for r in rows if r.get("_batch_id") in committed]
            if len(kept) != len(rows):
                landing.write_text(
                    "".join(json.dumps(r, sort_keys=True) + "\n" for r in kept),
                    encoding="utf-8",
                )

    # -- landing / commit protocol ---------------------------------------
    def stage_batch(self, batch_id: str, rows: list[dict[str, Any]]) -> Path:
        path = self.root / "staging" / f"{batch_id}.jsonl"
        path.write_text(
            "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows),
            encoding="utf-8",
        )
        return path

    def commit_batch(
        self,
        batch_id: str,
        run_id: str,
        table: str,
        rows: list[dict[str, Any]],
        dedup_keys: list[str],
    ) -> CommitMarker:
        """Commit a staged batch: land rows, write the outbox marker, update
        the dedup index. Idempotent: committing an already-committed batch
        is a no-op returning the existing marker."""
        existing = self.marker_for(batch_id)
        if existing is not None:
            return existing
        staged = self.root / "staging" / f"{batch_id}.jsonl"
        landing = self.root / "landing" / f"{table}.jsonl"
        payload = "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
        with landing.open("a", encoding="utf-8") as fh:
            fh.write(payload)
        checksum = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        marker = CommitMarker(
            batch_id=batch_id,
            run_id=run_id,
            table=table,
            rows=len(rows),
            checksum=checksum,
            committed_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        _append_jsonl(self.root / "outbox.jsonl", marker.to_dict())
        dedup = self._load_dedup(table)
        for key in dedup_keys:
            dedup[key] = batch_id
        _write_json(self.root / "dedup" / f"{table}.json", dedup)
        if staged.exists():
            staged.unlink()
        return marker

    def marker_for(self, batch_id: str) -> CommitMarker | None:
        for row in _read_jsonl(self.root / "outbox.jsonl"):
            if row["batch_id"] == batch_id:
                return CommitMarker(**row)
        return None

    def committed_batch_ids(self) -> set[str]:
        return {m["batch_id"] for m in _read_jsonl(self.root / "outbox.jsonl")}

    def landing_rows(self, table: str) -> list[dict[str, Any]]:
        return _read_jsonl(self.root / "landing" / f"{table}.jsonl")

    def _load_dedup(self, table: str) -> dict[str, str]:
        path = self.root / "dedup" / f"{table}.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def is_duplicate(self, table: str, dedup_key: str) -> bool:
        return dedup_key in self._load_dedup(table)

    def dedup_count(self, table: str) -> int:
        return len(self._load_dedup(table))

    # -- checkpoints ------------------------------------------------------
    def save_checkpoint(self, run_id: str, checkpoint: dict[str, Any]) -> None:
        _write_json(self.root / "checkpoints" / f"{run_id}.json", checkpoint)

    def load_checkpoint(self, run_id: str) -> dict[str, Any] | None:
        path = self.root / "checkpoints" / f"{run_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    # -- run records ------------------------------------------------------
    def save_run(self, run_id: str, record: dict[str, Any]) -> None:
        _write_json(self.root / "runs" / f"{run_id}.json", record)

    def load_run(self, run_id: str) -> dict[str, Any] | None:
        path = self.root / "runs" / f"{run_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def list_runs(self) -> list[dict[str, Any]]:
        runs = []
        for path in sorted((self.root / "runs").glob("*.json")):
            runs.append(json.loads(path.read_text(encoding="utf-8")))
        return runs

    # -- schemas ----------------------------------------------------------
    def save_schema(self, schema: dict[str, Any]) -> None:
        _write_json(
            self.root / "schemas" / f"{schema['name']}-v{schema['version']}.json", schema
        )

    def load_schema(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        matches = sorted((self.root / "schemas").glob(f"{name}-v*.json"))
        if not matches:
            return None
        if version is None:
            return json.loads(matches[-1].read_text(encoding="utf-8"))
        path = self.root / "schemas" / f"{name}-v{version}.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def list_schema_versions(self, name: str) -> list[int]:
        versions = []
        for path in (self.root / "schemas").glob(f"{name}-v*.json"):
            versions.append(int(path.stem.rsplit("-v", 1)[1]))
        return sorted(versions)
