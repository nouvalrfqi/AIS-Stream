import time
from datetime import datetime, timedelta, timezone

from api.data_quality import _in_window, _parse_event_time


def test_parse_event_time_parses_utc_suffix():
    parsed = _parse_event_time("2026-09-10 12:00:00.000000 +0000 UTC")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.hour == 12


def test_parse_event_time_invalid_returns_none():
    assert _parse_event_time("nonsense") is None
    assert _parse_event_time(None) is None


def test_in_window_recent_partition_included():
    now = datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=24)
    assert _in_window("raw/dt=2026-09-10/HH=17/file.parquet", cutoff) is True


def test_in_window_old_partition_excluded():
    now = datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=24)
    assert _in_window("raw/dt=2026-09-09/HH=10/file.parquet", cutoff) is False


def test_in_window_ignores_missing_dt():
    now = datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=24)
    assert _in_window("raw/something-else/file.parquet", cutoff) is True