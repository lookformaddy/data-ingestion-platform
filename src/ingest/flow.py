"""Backpressure and flow control.

Three primitives, kept deliberately small and testable:

* :class:`TokenBucket` — per-source rate limiting (token bucket with an
  injectable clock so tests are deterministic).
* :class:`BoundedBuffer` — a bounded in-memory batch buffer between the
  producer (connector) and the consumer (validator/writer). ``put`` blocks
  with a timeout and counts a backpressure event instead of growing
  memory without bound; the buffer can be paused and resumed.
* :class:`BatchSizer` — adaptive batch sizing (AIMD on observed batch
  processing latency) so a slow downstream shrinks batches instead of
  timing out.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Callable


class TokenBucket:
    """Token-bucket rate limiter.

    ``rate_per_sec`` tokens are added per second, up to ``capacity``.
    ``try_acquire`` never blocks; :meth:`wait_seconds` tells the caller
    how long to sleep before the requested tokens exist.
    """

    def __init__(
        self,
        rate_per_sec: float,
        capacity: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity
        self._clock = clock
        self._tokens = float(capacity)
        self._last = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
            self._last = now

    def try_acquire(self, n: int = 1) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    def wait_seconds(self, n: int = 1) -> float:
        with self._lock:
            self._refill()
            if self._tokens >= n:
                return 0.0
            return (n - self._tokens) / self.rate_per_sec

    def acquire(self, n: int = 1, *, sleep: Callable[[float], None] = time.sleep) -> None:
        """Block until ``n`` tokens are available (used by connectors)."""
        while True:
            if self.try_acquire(n):
                return
            sleep(self.wait_seconds(n))


@dataclass
class Batch:
    """One unit of work moving through the pipeline."""

    batch_id: str
    records: list[dict]
    checkpoint: dict
    source: str


class BoundedBuffer:
    """Bounded, pausable queue of batches between producer and consumer.

    The producer's ``put`` waits while the buffer is full, counting a
    backpressure event per timed-out attempt, instead of growing memory
    without bound. The consumer's ``get`` returns ``None`` when the
    stream is drained (producer called :meth:`close` and the queue is
    empty).
    """

    def __init__(self, max_batches: int = 4, *, put_timeout: float = 2.0) -> None:
        self._queue: queue.Queue[Batch | None] = queue.Queue(maxsize=max_batches)
        self.put_timeout = put_timeout
        self._paused = threading.Event()
        self._closed = threading.Event()
        self.backpressure_events = 0

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    @property
    def paused(self) -> bool:
        return self._paused.is_set()

    def put(self, batch: Batch) -> bool:
        """Enqueue a batch, waiting while the buffer is full.

        A full buffer is backpressure, not an error: the producer waits
        (counting an event per timed-out attempt) until the consumer
        makes room or the buffer is closed. Returns False only when the
        buffer was closed before the batch could be enqueued.
        """
        while True:
            if self._closed.is_set():
                return False
            try:
                self._queue.put(batch, timeout=self.put_timeout)
                return True
            except queue.Full:
                self.backpressure_events += 1
                if self._closed.is_set():
                    return False
                continue

    def get(self, timeout: float = 5.0) -> Batch | None:
        """Dequeue a batch, waiting while paused; None when drained.

        Waits for the producer until the buffer is closed and empty.
        Polls in short slices so a close is noticed promptly instead of
        after a full blocking ``queue.get`` timeout.
        """
        poll = min(timeout, 0.2)
        while True:
            if self._paused.is_set() and not self._closed.is_set():
                time.sleep(0.05)
                continue
            if self._closed.is_set() and self._queue.empty():
                return None
            try:
                return self._queue.get(timeout=poll)
            except queue.Empty:
                continue

    def close(self) -> None:
        """Signal no more batches will arrive (after the queue drains)."""
        self._closed.set()

    def qsize(self) -> int:
        return self._queue.qsize()


@dataclass
class BatchSizer:
    """AIMD batch sizing: grow when fast, shrink when slow."""

    current: int = 500
    minimum: int = 50
    maximum: int = 2000
    target_latency_ms: float = 250.0

    def observe(self, batch_latency_ms: float) -> int:
        if batch_latency_ms > self.target_latency_ms:
            self.current = max(self.minimum, self.current // 2)
        else:
            self.current = min(self.maximum, int(self.current * 1.25) + 1)
        return self.current
