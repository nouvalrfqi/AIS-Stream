import time

from ingestion.validator import parse_event_time


def _partition_key(event: dict) -> tuple[str, str]:
    event_time = parse_event_time(event.get("event_time", ""))
    if event_time is None:
        ts = float(event.get("ingested_at") or time.time())
        event_time = time.gmtime(ts)
        return time.strftime("%Y-%m-%d", event_time), time.strftime("%H", event_time)
    return event_time.strftime("%Y-%m-%d"), event_time.strftime("%H")


class BatchBuffer:
    def __init__(self, max_rows: int, max_seconds: int, min_rows: int = 1):
        self.max_rows = max_rows
        self.max_seconds = max_seconds
        self.min_rows = min_rows
        self._events: list[dict] = []
        self._first_event_at: float | None = None

    def add(self, event: dict) -> None:
        if not self._events:
            self._first_event_at = time.time()
        self._events.append(event)

    def should_flush(self, now: float | None = None) -> bool:
        if not self._events:
            return False
        if len(self._events) >= self.max_rows:
            return True
        now = now or time.time()
        if self._first_event_at is None:
            return False
        time_triggered = now - self._first_event_at >= self.max_seconds
        if time_triggered:
            return len(self._events) >= self.min_rows
        return False

    def drain(self) -> dict[tuple[str, str], list[dict]]:
        grouped: dict[tuple[str, str], list[dict]] = {}
        for event in self._events:
            key = _partition_key(event)
            grouped.setdefault(key, []).append(event)
        self._events = []
        self._first_event_at = None
        return grouped

    def __len__(self) -> int:
        return len(self._events)