"""Event-time watermarks and late-data policy.

The watermark is the highest event time seen so far minus the allowed
lateness. A record whose event time is at or below the watermark is late:
it arrived after the window it belongs to already closed. The platform
never silently drops late data — the run's late policy decides whether it
is accepted with a flag or quarantined.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class LatePolicy(str, Enum):
    ACCEPT_WITH_FLAG = "accept_with_flag"
    QUARANTINE = "quarantine"


def parse_event_time(value: str | int | float) -> datetime:
    """Parse an event-time value (ISO string or epoch seconds) to UTC."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise ValueError(f"cannot parse event time from {value!r}")


@dataclass
class WatermarkTracker:
    """Tracks the event-time watermark for one landing table."""

    allowed_lateness_seconds: float = 300.0
    max_event_time: datetime | None = None
    late_seen: int = 0
    on_time_seen: int = 0

    @property
    def watermark(self) -> datetime | None:
        if self.max_event_time is None:
            return None
        from datetime import timedelta

        return self.max_event_time - timedelta(seconds=self.allowed_lateness_seconds)

    def observe(self, event_time: datetime) -> bool:
        """Record an event time. Returns True when the record is late."""
        watermark = self.watermark
        is_late = watermark is not None and event_time <= watermark
        if is_late:
            self.late_seen += 1
        else:
            self.on_time_seen += 1
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time
        return is_late

    def lag_seconds(self, now: datetime) -> float | None:
        """Freshness: how far the watermark trails wall-clock time."""
        if self.watermark is None:
            return None
        return max(0.0, (now - self.watermark).total_seconds())

    def to_dict(self) -> dict:
        return {
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
            "max_event_time": self.max_event_time.isoformat() if self.max_event_time else None,
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "late_seen": self.late_seen,
            "on_time_seen": self.on_time_seen,
        }
