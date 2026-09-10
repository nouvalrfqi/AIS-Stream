import json
import time

from kafka import KafkaConsumer

from stream_processor import db


class StreamProcessorConsumer:
    def __init__(self, config: dict, log):
        self.config = config
        self.log = log
        self.buffer: list[dict] = []
        self._first_event_at: float | None = None
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

    def _flush(self) -> bool:
        if not self.buffer:
            return True
        rows = self.buffer
        self.buffer = []
        self._first_event_at = None
        state_rows, track_rows = db.build_rows(rows)
        try:
            db.apply_batch(self.conn, state_rows, track_rows)
        except Exception as e:
            self.buffer = rows + self.buffer
            if self._first_event_at is None and rows:
                self._first_event_at = time.time()
            try:
                self.conn.rollback()
            except Exception:
                pass
            if self.conn.closed:
                try:
                    self.conn = db.connect(self.config)
                except Exception as reconnect_err:
                    self.log("error", "db reconnect failed", error=str(reconnect_err))
            self.log("error", "postgis batch failed, events kept in buffer for retry", error=str(e), kept=len(rows))
            return False
        if track_rows or state_rows:
            self.consumer.commit()
            self.log("info", "postgis batch committed", state_rows=len(state_rows), track_rows=len(track_rows), events=len(rows))
        return True

    def run(self, poll_timeout_ms: int = 1000, retry_sleep_seconds: float = 2.0) -> None:
        self.log("info", "stream processor started", group=self.config["kafka_consumer_group_state"])
        try:
            while True:
                if not self._should_flush() or self._flush():
                    records = self.consumer.poll(timeout_ms=poll_timeout_ms, max_records=500)
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            event = msg.value
                            if event is None:
                                continue
                            if not self.buffer:
                                self._first_event_at = time.time()
                            self.buffer.append(event)
                else:
                    time.sleep(retry_sleep_seconds)
        except KeyboardInterrupt:
            pass
        finally:
            self._flush()
            self.consumer.close()
            self.conn.close()
            self.log("info", "stream processor stopped")