"""Watermarks and late-data handling."""

from datetime import datetime, timedelta, timezone

from ingest.watermark import LatePolicy, WatermarkTracker, parse_event_time


def _dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def test_parse_event_time_iso_and_epoch():
    assert parse_event_time("2026-11-18T09:00:00Z") == _dt("2026-11-18T09:00:00Z")
    assert parse_event_time(1763456400) == datetime.fromtimestamp(
        1763456400, tz=timezone.utc)


def test_watermark_advances_and_flags_late():
    tracker = WatermarkTracker(allowed_lateness_seconds=300)
    assert tracker.watermark is None
    assert tracker.observe(_dt("2026-11-18T09:00:00Z")) is False
    assert tracker.observe(_dt("2026-11-18T09:10:00Z")) is False
    # Watermark is now 09:05:00; 09:04:00 is late.
    assert tracker.observe(_dt("2026-11-18T09:04:00Z")) is True
    assert tracker.late_seen == 1
    assert tracker.on_time_seen == 2
    assert tracker.watermark == _dt("2026-11-18T09:05:00Z")


def test_watermark_lag_seconds():
    tracker = WatermarkTracker(allowed_lateness_seconds=60)
    tracker.observe(_dt("2026-11-18T09:00:00Z"))
    lag = tracker.lag_seconds(_dt("2026-11-18T09:05:00Z"))
    assert lag == 360.0  # now - watermark(09:00 - 60s) = 09:05 - 08:59


def test_late_policy_enum_values():
    assert LatePolicy.ACCEPT_WITH_FLAG.value == "accept_with_flag"
    assert LatePolicy.QUARANTINE.value == "quarantine"
