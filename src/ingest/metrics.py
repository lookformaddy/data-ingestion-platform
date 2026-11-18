"""Per-run metrics, latency percentiles, and health / SLI rollups.

Metrics are computed from counters and a bounded reservoir of per-record
processing latencies (milliseconds). Percentiles use the nearest-rank
method on the reservoir — deterministic and dependency-free.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone


def percentile(samples: list[float], p: float) -> float | None:
    """Nearest-rank percentile; None when there are no samples."""
    if not samples:
        return None
    ordered = sorted(samples)
    rank = math.ceil(p / 100.0 * len(ordered))
    return ordered[max(0, min(rank - 1, len(ordered) - 1))]


@dataclass
class RunMetrics:
    """Counters and latency samples for one run."""

    records_in: int = 0
    valid: int = 0
    quarantined: int = 0
    duplicates_dropped: int = 0
    late_accepted: int = 0
    late_quarantined: int = 0
    bytes_in: int = 0
    batches_committed: int = 0
    batches_failed: int = 0
    backpressure_events: int = 0
    retries: int = 0
    _latencies_ms: list[float] = field(default_factory=list, repr=False)

    def record_latency(self, ms: float) -> None:
        # Bounded reservoir: keep at most 10k samples, evenly thinned.
        if len(self._latencies_ms) < 10_000:
            self._latencies_ms.append(ms)
        elif len(self._latencies_ms) % 2 == 0:
            self._latencies_ms.append(ms)

    @property
    def p50_latency_ms(self) -> float | None:
        return percentile(self._latencies_ms, 50)

    @property
    def p95_latency_ms(self) -> float | None:
        return percentile(self._latencies_ms, 95)

    @property
    def quarantine_rate(self) -> float:
        return self.quarantined / self.records_in if self.records_in else 0.0

    @property
    def duplicate_rate(self) -> float:
        return self.duplicates_dropped / self.records_in if self.records_in else 0.0

    def to_dict(self) -> dict:
        return {
            "records_in": self.records_in,
            "valid": self.valid,
            "quarantined": self.quarantined,
            "duplicates_dropped": self.duplicates_dropped,
            "late_accepted": self.late_accepted,
            "late_quarantined": self.late_quarantined,
            "bytes_in": self.bytes_in,
            "batches_committed": self.batches_committed,
            "batches_failed": self.batches_failed,
            "backpressure_events": self.backpressure_events,
            "retries": self.retries,
            "p50_latency_ms": self.p50_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "quarantine_rate": round(self.quarantine_rate, 4),
            "duplicate_rate": round(self.duplicate_rate, 4),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RunMetrics":
        m = cls()
        for key in (
            "records_in", "valid", "quarantined", "duplicates_dropped",
            "late_accepted", "late_quarantined", "bytes_in",
            "batches_committed", "batches_failed", "backpressure_events",
            "retries",
        ):
            setattr(m, key, int(d.get(key, 0)))
        return m


@dataclass
class HealthRollup:
    """SLI rollup across runs: the numbers an on-call reads first."""

    runs: int = 0
    records_in: int = 0
    quarantined: int = 0
    duplicates_dropped: int = 0
    late: int = 0
    failed_runs: int = 0
    p95_latency_ms: float | None = None

    @property
    def success_rate(self) -> float:
        return (self.runs - self.failed_runs) / self.runs if self.runs else 1.0

    @property
    def quarantine_rate(self) -> float:
        return self.quarantined / self.records_in if self.records_in else 0.0

    def status(self) -> str:
        """HEALTHY / DEGRADED / UNHEALTHY from simple SLO thresholds."""
        if self.failed_runs:
            return "UNHEALTHY"
        if self.quarantine_rate > 0.05:
            return "DEGRADED"
        return "HEALTHY"

    def to_dict(self) -> dict:
        return {
            "runs": self.runs,
            "records_in": self.records_in,
            "quarantined": self.quarantined,
            "duplicates_dropped": self.duplicates_dropped,
            "late": self.late,
            "failed_runs": self.failed_runs,
            "success_rate": round(self.success_rate, 4),
            "quarantine_rate": round(self.quarantine_rate, 4),
            "p95_latency_ms": self.p95_latency_ms,
            "status": self.status(),
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }


def rollup(metrics_list: list[RunMetrics], failed_runs: int = 0) -> HealthRollup:
    """Aggregate per-run metrics into one health rollup."""
    roll = HealthRollup(runs=len(metrics_list), failed_runs=failed_runs)
    all_latencies: list[float] = []
    for m in metrics_list:
        roll.records_in += m.records_in
        roll.quarantined += m.quarantined
        roll.duplicates_dropped += m.duplicates_dropped
        roll.late += m.late_accepted + m.late_quarantined
        all_latencies.extend(m._latencies_ms)
    roll.p95_latency_ms = percentile(all_latencies, 95)
    return roll
