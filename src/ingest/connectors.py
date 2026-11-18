"""Pluggable source connectors behind one interface.

Every source — file, HTTP API, CDC change feed — implements
:class:`SourceConnector` and declares its :class:`Capability` flags, so
the engine never branches on source kind. Checkpoints are opaque dicts
the connector owns (line offset, page cursor, last LSN); the engine
only stores and hands them back.

The HTTP API and CDC feed are *simulated* in-process: they model the
behaviour that matters for ingestion (pagination, rate limits, transient
failures, ordered change events) without any network dependency, so the
whole platform runs offline and deterministically.
"""

from __future__ import annotations

import csv
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Flag, auto
from pathlib import Path
from typing import Any, Iterator

from .errors import ConnectorError, RateLimitError
from .flow import Batch, TokenBucket


class Capability(Flag):
    PAGINATED = auto()
    RATE_LIMITED = auto()
    RESUMABLE = auto()
    ORDERED = auto()
    CDC = auto()


@dataclass
class ConnectorInfo:
    name: str
    kind: str
    capabilities: Capability

    def to_dict(self) -> dict[str, Any]:
        caps = [c.name for c in Capability if c in self.capabilities]
        return {"name": self.name, "kind": self.kind, "capabilities": caps}


class SourceConnector(ABC):
    """One source of records. Yields batches; each batch carries the
    checkpoint to resume from if the run is interrupted after it."""

    def __init__(self, name: str, kind: str, capabilities: Capability) -> None:
        self.info = ConnectorInfo(name, kind, capabilities)

    @abstractmethod
    def iter_batches(
        self, checkpoint: dict[str, Any] | None, batch_size: int
    ) -> Iterator[Batch]:
        """Yield batches starting after ``checkpoint`` (None = from start)."""

    def close(self) -> None:
        pass


# ---------------------------------------------------------------- file ----
class FileConnector(SourceConnector):
    """CSV or JSONL/NDJSON files. Checkpoint is the line offset."""

    def __init__(
        self, name: str, path: str | Path, *, fmt: str | None = None
    ) -> None:
        super().__init__(name, "file", Capability.RESUMABLE | Capability.ORDERED)
        self.path = Path(path)
        if not self.path.exists():
            raise ConnectorError(f"file source {name}: {self.path} does not exist")
        self.fmt = fmt or ("csv" if self.path.suffix.lower() == ".csv" else "jsonl")

    def _iter_records(self) -> Iterator[dict[str, Any]]:
        if self.fmt == "csv":
            with self.path.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    yield {k: v for k, v in row.items() if k is not None}
        elif self.fmt in ("jsonl", "ndjson"):
            with self.path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
        else:
            raise ConnectorError(f"file source {self.info.name}: unknown format {self.fmt}")

    def iter_batches(
        self, checkpoint: dict[str, Any] | None, batch_size: int
    ) -> Iterator[Batch]:
        offset = int((checkpoint or {}).get("line", 0))
        batch: list[dict[str, Any]] = []
        line_no = 0
        for record in self._iter_records():
            line_no += 1
            if line_no <= offset:
                continue
            batch.append(record)
            if len(batch) >= batch_size:
                yield Batch(
                    batch_id=f"{self.info.name}-{line_no}",
                    records=batch,
                    checkpoint={"line": line_no},
                    source=self.info.name,
                )
                batch = []
        if batch:
            yield Batch(
                batch_id=f"{self.info.name}-{line_no}",
                records=batch,
                checkpoint={"line": line_no},
                source=self.info.name,
            )


# ------------------------------------------------------- simulated HTTP ----
class SimulatedApi:
    """A deterministic paginated API with rate limiting and transient failures.

    ``failures`` maps page numbers to the number of times that page must
    fail with a 429 before succeeding — this exercises the connector's
    retry logic deterministically.
    """

    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        *,
        failures: dict[int, int] | None = None,
        rate_limit_per_sec: float = 100.0,
    ) -> None:
        self.pages = pages
        self.failures = dict(failures or {})
        self._attempts: dict[int, int] = {}
        self.bucket = TokenBucket(rate_limit_per_sec, capacity=int(rate_limit_per_sec))
        self.calls = 0

    def fetch_page(self, page: int) -> tuple[list[dict[str, Any]], int | None]:
        """Returns (records, next_page). Raises RateLimitError on a
        simulated 429; raises ConnectorError past the last page."""
        self.calls += 1
        if page >= len(self.pages):
            raise ConnectorError(f"page {page} out of range")
        self._attempts[page] = self._attempts.get(page, 0) + 1
        if self._attempts[page] <= self.failures.get(page, 0):
            raise RateLimitError(f"simulated 429 on page {page}")
        self.bucket.try_acquire(1)  # model the server-side limit; never blocks here
        nxt = page + 1 if page + 1 < len(self.pages) else None
        return list(self.pages[page]), nxt


class HttpApiConnector(SourceConnector):
    """Paginated HTTP API source with retry + backoff. Checkpoint is the
    next page cursor."""

    def __init__(
        self,
        name: str,
        api: SimulatedApi,
        *,
        max_retries: int = 5,
        backoff_base: float = 0.05,
        sleep: Any = time.sleep,
    ) -> None:
        super().__init__(
            name, "http_api",
            Capability.PAGINATED | Capability.RATE_LIMITED | Capability.RESUMABLE,
        )
        self.api = api
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self._sleep = sleep
        self.retries = 0

    def _fetch_with_retry(self, page: int) -> tuple[list[dict[str, Any]], int | None]:
        attempt = 0
        while True:
            try:
                return self.api.fetch_page(page)
            except RateLimitError:
                attempt += 1
                self.retries += 1
                if attempt > self.max_retries:
                    raise
                self._sleep(self.backoff_base * (2 ** (attempt - 1)))

    def iter_batches(
        self, checkpoint: dict[str, Any] | None, batch_size: int
    ) -> Iterator[Batch]:
        page = int((checkpoint or {}).get("page", 0))
        offset = int((checkpoint or {}).get("offset", 0))
        while True:
            records, nxt = self._fetch_with_retry(page)
            for start in range(offset, len(records), batch_size):
                chunk = records[start:start + batch_size]
                consumed_page = start + batch_size >= len(records)
                if consumed_page:
                    next_cp: dict[str, Any] = {"page": nxt if nxt is not None else page + 1}
                else:
                    next_cp = {"page": page, "offset": start + batch_size}
                yield Batch(
                    batch_id=f"{self.info.name}-p{page}-{start}",
                    records=chunk,
                    checkpoint=next_cp,
                    source=self.info.name,
                )
            offset = 0
            if nxt is None:
                return
            page = nxt


# ------------------------------------------------------------- CDC feed ----
@dataclass
class ChangeEvent:
    lsn: int
    op: str  # insert | update | delete
    record: dict[str, Any]


class CdcConnector(SourceConnector):
    """CDC-style change feed: LSN-ordered insert/update/delete events.
    Checkpoint is the last applied LSN. Deletes are landed as tombstone
    rows (``_deleted: true``) so downstream consumers can reconcile."""

    def __init__(self, name: str, events: list[ChangeEvent]) -> None:
        super().__init__(name, "cdc", Capability.CDC | Capability.ORDERED | Capability.RESUMABLE)
        self.events = sorted(events, key=lambda e: e.lsn)

    def iter_batches(
        self, checkpoint: dict[str, Any] | None, batch_size: int
    ) -> Iterator[Batch]:
        last_lsn = int((checkpoint or {}).get("lsn", 0))
        batch: list[dict[str, Any]] = []
        for event in self.events:
            if event.lsn <= last_lsn:
                continue
            row = dict(event.record)
            row["_op"] = event.op
            row["_lsn"] = event.lsn
            if event.op == "delete":
                row["_deleted"] = True
            batch.append(row)
            if len(batch) >= batch_size:
                yield Batch(
                    batch_id=f"{self.info.name}-lsn{event.lsn}",
                    records=batch,
                    checkpoint={"lsn": event.lsn},
                    source=self.info.name,
                )
                batch = []
        if batch:
            yield Batch(
                batch_id=f"{self.info.name}-lsn{self.events[-1].lsn}",
                records=batch,
                checkpoint={"lsn": self.events[-1].lsn},
                source=self.info.name,
            )


# --------------------------------------------------------------- registry ----
class ConnectorRegistry:
    """Holds the connectors for a run; the engine looks them up by name."""

    def __init__(self) -> None:
        self._connectors: dict[str, SourceConnector] = {}

    def register(self, connector: SourceConnector) -> None:
        self._connectors[connector.info.name] = connector

    def get(self, name: str) -> SourceConnector:
        try:
            return self._connectors[name]
        except KeyError:
            raise ConnectorError(f"unknown source connector: {name}") from None

    def list(self) -> list[ConnectorInfo]:
        return [c.info for c in self._connectors.values()]
