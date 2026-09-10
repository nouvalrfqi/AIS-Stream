import time
from datetime import datetime, timedelta, timezone

from ingestion.validator import classify_skew, parse_event_time

FUTURE_TOL = 60
STALE_TOL = 21600


def _utc_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S +0000 UTC")


def test_parse_event_time_aware_offset():
    parsed = parse_event_time("2026-09-10 18:00:01 +0000 UTC")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(0)


def test_parse_event_time_naive_defaults_utc():
    parsed = parse_event_time("2026-09-10 18:00:01")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(0)


def test_parse_event_time_invalid():
    assert parse_event_time("not-a-date") is None


def test_normal_skew_passes():
    now = time.time()
    ts = datetime.fromtimestamp(now, tz=timezone.utc) - timedelta(seconds=19)
    assert classify_skew(_utc_str(ts), now, FUTURE_TOL, STALE_TOL) is None


def test_future_skew_past_tolerance():
    now = time.time()
    ts = datetime.fromtimestamp(now, tz=timezone.utc) + timedelta(seconds=2940)
    assert classify_skew(_utc_str(ts), now, FUTURE_TOL, STALE_TOL) == "future_timestamp"


def test_future_skew_within_tolerance():
    now = time.time()
    ts = datetime.fromtimestamp(now, tz=timezone.utc) + timedelta(seconds=15)
    assert classify_skew(_utc_str(ts), now, FUTURE_TOL, STALE_TOL) is None


def test_stale_skew_past_tolerance():
    now = time.time()
    ts = datetime.fromtimestamp(now, tz=timezone.utc) - timedelta(seconds=90000)
    assert classify_skew(_utc_str(ts), now, FUTURE_TOL, STALE_TOL) == "stale_timestamp"


def test_stale_skew_within_tolerance():
    now = time.time()
    ts = datetime.fromtimestamp(now, tz=timezone.utc) - timedelta(seconds=7200)
    assert classify_skew(_utc_str(ts), now, FUTURE_TOL, STALE_TOL) is None


def test_unparseable_timestamp():
    assert classify_skew("garbage", time.time(), FUTURE_TOL, STALE_TOL) == "unparseable_timestamp"