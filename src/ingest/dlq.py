"""Dead-letter queue: quarantined records with full failure history.

A quarantined record keeps the raw payload, every validation/drift/late
error that sent it here, the source and run that produced it, and a
history of redrive attempts. Redrive re-validates against the *current*
schema version, so a fixed schema (or a fixed record) can rescue a row
without re-ingesting the source.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import DlqError
from .schema import FieldError, SchemaVersion, validate_record


@dataclass
class DlqEntry:
    id: str
    source: str
    run_id: str
    table: str
    record: dict[str, Any]
    errors: list[dict[str, str]]
    reason: str  # validation | drift | late
    quarantined_at: str
    attempts: list[dict[str, Any]] = field(default_factory=list)
    status: str = "quarantined"  # quarantined | redriven | dead

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "run_id": self.run_id,
            "table": self.table,
            "record": self.record,
            "errors": self.errors,
            "reason": self.reason,
            "quarantined_at": self.quarantined_at,
            "attempts": self.attempts,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DlqEntry":
        return cls(**d)


class DlqStore:
    """JSONL-backed DLQ. One file, append-only writes, full rewrite on update."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: list[DlqEntry] = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    self._entries.append(DlqEntry.from_dict(json.loads(line)))

    def _persist(self) -> None:
        self.path.write_text(
            "".join(json.dumps(e.to_dict(), sort_keys=True) + "\n" for e in self._entries),
            encoding="utf-8",
        )

    def quarantine(
        self,
        *,
        source: str,
        run_id: str,
        table: str,
        record: dict[str, Any],
        errors: list[FieldError],
        reason: str,
    ) -> DlqEntry:
        entry = DlqEntry(
            id=str(uuid.uuid4())[:8],
            source=source,
            run_id=run_id,
            table=table,
            record=record,
            errors=[e.to_dict() for e in errors],
            reason=reason,
            quarantined_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        self._entries.append(entry)
        self._persist()
        return entry

    def list(self, *, status: str | None = None, source: str | None = None) -> list[DlqEntry]:
        entries = self._entries
        if status is not None:
            entries = [e for e in entries if e.status == status]
        if source is not None:
            entries = [e for e in entries if e.source == source]
        return list(entries)

    def get(self, entry_id: str) -> DlqEntry:
        for entry in self._entries:
            if entry.id == entry_id:
                return entry
        raise DlqError(f"no DLQ entry with id {entry_id}")

    def redrive(
        self, entry_id: str, schema: SchemaVersion
    ) -> tuple[str, dict[str, Any] | None, list[FieldError]]:
        """Re-validate a quarantined record against the current schema.

        Returns (outcome, coerced_record, errors). ``outcome`` is
        ``redriven`` (now valid — the caller lands it) or ``still_invalid``
        (the attempt is appended to the failure history and the entry
        stays quarantined).
        """
        entry = self.get(entry_id)
        coerced, errors = validate_record(entry.record, schema)
        attempt = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "schema_version": schema.version,
            "errors": [e.to_dict() for e in errors],
        }
        entry.attempts.append(attempt)
        if not errors:
            entry.status = "redriven"
            self._persist()
            return "redriven", coerced, []
        self._persist()
        return "still_invalid", None, errors
