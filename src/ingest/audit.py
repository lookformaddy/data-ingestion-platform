"""SHA-256 hash-chained audit log for run lifecycle events.

Each entry commits to the previous entry's hash, so any edit, deletion or
reordering of the log is detectable by :meth:`AuditLog.verify`. The chain
is append-only and lives in the state directory next to the landing data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _canonical(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


@dataclass
class AuditEntry:
    seq: int
    timestamp: str
    event: str
    run_id: str
    payload: dict[str, Any]
    prev_hash: str
    hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "timestamp": self.timestamp,
            "event": self.event,
            "run_id": self.run_id,
            "payload": self.payload,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }


class AuditLog:
    """Append-only, hash-chained audit log backed by a JSONL file."""

    GENESIS = "0" * 64

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: list[AuditEntry] = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    self._entries.append(AuditEntry(**json.loads(line)))

    @property
    def last_hash(self) -> str:
        return self._entries[-1].hash if self._entries else self.GENESIS

    def append(self, event: str, run_id: str, payload: dict[str, Any] | None = None) -> AuditEntry:
        entry_payload = payload or {}
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        prev_hash = self.last_hash
        body = {
            "seq": len(self._entries) + 1,
            "timestamp": timestamp,
            "event": event,
            "run_id": run_id,
            "payload": entry_payload,
            "prev_hash": prev_hash,
        }
        digest = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
        entry = AuditEntry(**body, hash=digest)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict(), sort_keys=True) + "\n")
        self._entries.append(entry)
        return entry

    def verify(self) -> tuple[bool, str]:
        """Recompute the chain. Returns (ok, detail)."""
        prev = self.GENESIS
        for entry in self._entries:
            body = {
                "seq": entry.seq,
                "timestamp": entry.timestamp,
                "event": entry.event,
                "run_id": entry.run_id,
                "payload": entry.payload,
                "prev_hash": entry.prev_hash,
            }
            expected = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
            if entry.prev_hash != prev:
                return False, f"entry {entry.seq}: prev_hash break in chain"
            if entry.hash != expected:
                return False, f"entry {entry.seq}: hash mismatch (tampered payload?)"
            prev = entry.hash
        return True, f"chain verified: {len(self._entries)} entries"

    def entries(self, run_id: str | None = None) -> list[AuditEntry]:
        if run_id is None:
            return list(self._entries)
        return [e for e in self._entries if e.run_id == run_id]
