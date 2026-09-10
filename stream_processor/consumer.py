import json
import time

from kafka import KafkaConsumer

from common.dlq import send_to_dlq
from stream_processor import db

MAX_RETAIN_RETRIES = 3


class StreamProcessorConsumer:
    def __init__(self, config: dict, log):
        self.config = config
        self.log = log
        self.dlq_producer = config.get("dlq_producer")
        self.dlq_topic = config.get("kafka_topic_dlq", "ais.dlq")
        self.buffer: list[dict] = []
        self._first_event_at: float | None = None
        self.failures = 0
        self.conn = db.connect(config)
        self.consumer = KafkaConsumer(
            config["kafka_topic_raw"],
            bootstrap_servers=config["kafka_bootstrap_servers"],
            group_id=config["kafka_consumer_group_state"],
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _should_flush(self) -> bool:
        if not self.buffer:
            return False
        if len(self.buffer) >= self.config["state_flush_rows"]:
            return True
        if self._first_event_at is None:
            return False
        return time.time() - self._first_event_at >= self.config["state_flush_seconds"]

    def _ensure_conn(self) -> None:
        if self.conn.closed:
            try:
                self.conn = db.connect(self.config)
            except Exception as exc:
                self.log("error", "db reconnect failed", error=str(exc))

    def _apply_rows(self, state_rows: list[tuple], track_rows: list[tuple]) -> bool:
        try:
            db.apply_batch(self.conn, state_rows, track_rows)
            return True
        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            self._ensure_conn()
            self.log("error", "postgis batch failed", error=str(exc))
            return False

    def _db_healthy(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _flush(self) -> bool:
        if not self.buffer:
            return True
        rows = self.buffer
        self.buffer = []
        self._first_event_at = None
        try:
            state_rows, track_rows = db.build_rows(rows)
        except Exception as exc:
            self.buffer = rows + self.buffer
            self.failures += 1
            self.log("error", "row build failed, batch kept for retry", error=str(exc), kept=len(rows))
            return False
        if not (track_rows or state_rows):
            self.consumer.commit()
            return True
        if self._apply_rows(state_rows, track_rows):
            self.consumer.commit()
            self.failures = 0
            self.log(
                "info",
                "postgis batch committed",
                state_rows=len(state_rows),
                track_rows=len(track_rows),
                events=len(rows),
            )
            return True
        self.buffer = rows + self.buffer
        self._first_event_at = time.time()
        self.failures += 1
        self.log("error", "postgis batch failed, events kept in buffer for retry", kept=len(rows))
        return False

    def _isolate_poison(self) -> bool:
        """DB healthy but a batch keeps failing: push individual poison events to DLQ."""
        events = self.buffer
        self.buffer = []
        self._first_event_at = None
        poisoned = 0
        for event in events:
            try:
                state_rows, track_rows = db.build_rows([event])
            except Exception as exc:
                poisoned += 1
                self._send_poison(event, f"row_build_failed: {exc}")
                continue
            if self._apply_rows(state_rows, track_rows):
                continue
            if not self._db_healthy():
                self.log("warn", "db down during isolation, restoring batch for retry", kept=len(events))
                self.buffer = events + self.buffer
                return False
            poisoned += 1
            self._send_poison(event, "db_insert_failed")

        if events:
            self.consumer.commit()
            self.log("info", "postgis poison isolation finished", poisoned=poisoned, ok=len(events) - poisoned)
        return True

    def _send_poison(self, event: dict, reason: str) -> None:
        self.log(
            "error",
            "poison event sent to DLQ",
            reason=reason,
            mmsi=event.get("mmsi"),
            event_id=event.get("event_id"),
        )
        if self.dlq_producer:
            send_to_dlq(
                self.dlq_producer,
                self.dlq_topic,
                event,
                reason=reason,
                source="stream_processor",
            )

    def run(self, poll_timeout_ms: int = 1000, retry_sleep_seconds: float = 2.0) -> None:
        self.log("info", "stream processor started", group=self.config["kafka_consumer_group_state"])
        try:
            while True:
                if self._should_flush():
                    if not self._flush():
                        if self.failures >= MAX_RETAIN_RETRIES and self.buffer:
                            self.log(
                                "warn",
                                "max retries reached, isolating poison events",
                                failures=self.failures,
                            )
                            if self._isolate_poison():
                                self.failures = 0
                            else:
                                self.failures = 0
                                self.buffer, self._first_event_at = self.buffer, time.time()
                        time.sleep(retry_sleep_seconds)
                        continue

                records = self.consumer.poll(timeout_ms=poll_timeout_ms, max_records=500)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        event = msg.value
                        if event is None:
                            continue
                        if not self.buffer:
                            self._first_event_at = time.time()
                        self.buffer.append(event)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self._flush()
            except Exception:
                pass
            self.consumer.close()
            self.conn.close()
            self.log("info", "stream processor stopped")