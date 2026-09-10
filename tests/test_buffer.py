import time

from raw_sink.buffer import BatchBuffer, _partition_key


def _event(hour: str = "12", day: str = "2026-09-10"):
    return {
        "mmsi": "123456789",
        "event_time": f"{day} {hour}:00:00.000000 +0000 UTC",
        "event_id": f"{day}-{hour}",
    }


def test_partition_key_by_event_time():
    d, h = _partition_key(_event(hour="12"))
    assert d == "2026-09-10"
    assert h == "12"


def test_partition_key_fallback_when_unparseable():
    event = {"event_time": "garbage", "ingested_at": 1789061760.0, "mmsi": "1"}
    d, h = _partition_key(event)
    assert d == "2026-09-10"
    assert h == "17"


def test_partition_key_never_epoch_zero():
    event = {"event_time": "garbage"}
    d, _ = _partition_key(event)
    assert d != "1970-01-01"


def test_should_flush_on_max_rows():
    b = BatchBuffer(max_rows=3, max_seconds=300)
    for _ in range(3):
        b.add(_event())
    assert b.should_flush() is True


def test_should_flush_after_max_seconds_with_min_rows():
    b = BatchBuffer(max_rows=10, max_seconds=300, min_rows=2)
    b.add(_event())
    assert b.should_flush(now=time.time() + 400) is False  # min_rows not met
    b.add(_event())
    assert b.should_flush(now=time.time() + 400) is True


def test_should_flush_false_early():
    b = BatchBuffer(max_rows=10, max_seconds=300)
    b.add(_event())
    assert b.should_flush(now=time.time() + 10) is False


def test_drain_groups_by_partition():
    b = BatchBuffer(max_rows=100, max_seconds=300)
    b.add(_event(hour="12"))
    b.add(_event(hour="13"))
    b.add(_event(hour="12"))
    grouped = b.drain()
    assert grouped[("2026-09-10", "12")] == [
        _event(hour="12"),
        _event(hour="12"),
    ]
    assert len(grouped[("2026-09-10", "13")]) == 1
    assert len(b) == 0


def test_empty_buffer_never_flushes():
    b = BatchBuffer(max_rows=1, max_seconds=1)
    assert b.should_flush() is False