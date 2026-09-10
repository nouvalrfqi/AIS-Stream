import hashlib
import json

from kafka import KafkaConsumer

from raw_sink.buffer import BatchBuffer
from raw_sink.parquet_writer import write_parquet_bytes


def _content_salt(events: list[dict]) -> str:
    ids = sorted(str(e.get("event_id")) for e in events)
    return hashlib.sha256(",".join(ids).encode("utf-8")).hexdigest()[:16]


class RawSinkConsumer:
    def __init__(self, config: dict, storage, log):
        self.config = config
        self.storage = storage
        self.log = log
        self.buffer = BatchBuffer(
            max_rows=config["parquet_max_rows"],
            max_seconds=config["parquet_max_seconds"],
        )
        self.consumer = KafkaConsumer(
            config["kafka_topic_raw"],
            bootstrap_servers=config["kafka_bootstrap_servers"],
            group_id=config["kafka_consumer_group"],
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _flush(self) -> None:
        if len(self.buffer) == 0:
            return

        grouped = self.buffer.drain()
        uploaded_keys = 0
        total_rows = 0

        try:
            for (event_date, event_hour), events in grouped.items():
                parquet_bytes = write_parquet_bytes(events)
                idempotency_id = self.storage.make_idempotency_id(
                    offset_start=event_date.replace("-", ""),
                    offset_end=event_hour,
                    group_id=self.config["kafka_consumer_group"],
                    content_salt=_content_salt(events),
                )
                key = self.storage.put_parquet(event_date, event_hour, parquet_bytes, idempotency_id)
                uploaded_keys += 1
                total_rows += len(events)
                self.log("info", "parquet uploaded", key=key, rows=len(events), bytes=len(parquet_bytes))
        except Exception as e:
            restored = 0
            for events in grouped.values():
                for event in events:
                    self.buffer.add(event)
                    restored += 1
            self.log("error", "parquet upload failed, events kept in buffer for retry", error=str(e), restored=restored)
            return

        if uploaded_keys > 0:
            self.consumer.commit()
            self.log("info", "kafka offsets committed", files=uploaded_keys, rows=total_rows)

    def run(self, poll_timeout_ms: int = 1000) -> None:
        self.log("info", "raw sink started", group=self.config["kafka_consumer_group"])
        try:
            while True:
                records = self.consumer.poll(timeout_ms=poll_timeout_ms, max_records=500)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        event = msg.value
                        if event is not None:
                            self.buffer.add(event)
                if self.buffer.should_flush():
                    self._flush()
        except KeyboardInterrupt:
            pass
        finally:
            self._flush()
            self.consumer.close()
            self.log("info", "raw sink stopped")