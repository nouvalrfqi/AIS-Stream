import hashlib
import json
import time

from kafka import KafkaConsumer

from common.dlq import send_to_dlq
from raw_sink.buffer import BatchBuffer
from raw_sink.parquet_writer import write_parquet_bytes

CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_MAX_BACKOFF = 60.0


def _content_salt(events: list[dict]) -> str:
    ids = sorted(str(e.get("event_id")) for e in events)
    return hashlib.sha256(",".join(ids).encode("utf-8")).hexdigest()[:16]


class RawSinkConsumer:
    def __init__(self, config: dict, storage, log):
        self.config = config
        self.storage = storage
        self.log = log
        self.dlq_producer = config.get("dlq_producer")
        self.dlq_topic = config.get("kafka_topic_dlq", "ais.dlq")
        self.buffer = BatchBuffer(
            max_rows=config["parquet_max_rows"],
            max_seconds=config["parquet_max_seconds"],
            min_rows=config.get("parquet_min_rows", 1),
        )
        self.consumer = self._connect_consumer()

    def _connect_consumer(self):
        return KafkaConsumer(
            self.config["kafka_topic_raw"],
            bootstrap_servers=self.config["kafka_bootstrap_servers"],
            group_id=self.config["kafka_consumer_group"],
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _reconnect(self, error=None) -> None:
        try:
            self.consumer.close()
        except Exception:
            pass
        self.consumer = self._connect_consumer()
        self.log(
            "warn",
            "consumer reconnecting",
            error=str(error) if error else None,
            group=self.config["kafka_consumer_group"],
        )

    def _flush_group(self, event_date: str, event_hour: str, events: list[dict]) -> bool:
        """Upload a single partition group.

        Returns True on success, False on infrastructure failure (S3 unreachable),
        or raises PoisonEventError if the data cannot be serialized.
        """
        try:
            parquet_bytes = write_parquet_bytes(events)
        except Exception as e:
            self.log("error", "parquet serialization failed for group", error=str(e), rows=len(events))
            raise PoisonEventError(str(e)) from e

        try:
            idempotency_id = self.storage.make_idempotency_id(
                offset_start=event_date.replace("-", ""),
                offset_end=event_hour,
                group_id=self.config["kafka_consumer_group"],
                content_salt=_content_salt(events),
            )
            key = self.storage.put_parquet(event_date, event_hour, parquet_bytes, idempotency_id)
        except Exception as e:
            self.log("error", "s3 upload failed", error=str(e), rows=len(events))
            return False

        self.log("info", "parquet uploaded", key=key, rows=len(events), bytes=len(parquet_bytes))
        return True

    def _handle_poison_group(self, event_date: str, event_hour: str, events: list[dict]) -> None:
        """Isolate poison events: serialize individually, DLQ the failures, upload the rest."""
        serializable: list[dict] = []
        for event in events:
            try:
                write_parquet_bytes([event])
                serializable.append(event)
            except Exception as e:
                self.log(
                    "error",
                    "poison event sent to DLQ",
                    error=str(e),
                    mmsi=event.get("mmsi"),
                    event_id=event.get("event_id"),
                )
                if self.dlq_producer:
                    send_to_dlq(
                        self.dlq_producer,
                        self.dlq_topic,
                        event,
                        reason="parquet_serialization_failed",
                        source="raw_sink",
                    )

        if serializable and serializable != events:
            try:
                self._flush_group(event_date, event_hour, serializable)
            except Exception:
                self.log("error", "re-upload of serializable subset failed", rows=len(serializable))

    def _flush(self) -> bool:
        if len(self.buffer) == 0:
            return True

        grouped = self.buffer.drain()
        uploaded_keys = 0
        total_rows = 0
        infra_failed = False

        for (event_date, event_hour), events in grouped.items():
            try:
                ok = self._flush_group(event_date, event_hour, events)
                if ok:
                    uploaded_keys += 1
                    total_rows += len(events)
                else:
                    infra_failed = True
            except PoisonEventError:
                self._handle_poison_group(event_date, event_hour, events)
                total_rows += len(events)
                uploaded_keys += 1

        if uploaded_keys > 0:
            self.consumer.commit()
            self.log("info", "kafka offsets committed", files=uploaded_keys, rows=total_rows)

        return not infra_failed

    def run(self, poll_timeout_ms: int = 1000) -> None:
        self.log("info", "raw sink started", group=self.config["kafka_consumer_group"])
        consecutive_failures = 0
        backoff = 1.0
        try:
            while True:
                if consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
                    self.log(
                        "warn",
                        "circuit breaker open, pausing consumption",
                        consecutive_failures=consecutive_failures,
                        backoff_s=round(backoff, 1),
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, CIRCUIT_BREAKER_MAX_BACKOFF)
                    continue

                try:
                    records = self.consumer.poll(timeout_ms=poll_timeout_ms, max_records=500)
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            event = msg.value
                            if event is not None:
                                self.buffer.add(event)

                    if self.buffer.should_flush():
                        if self._flush():
                            consecutive_failures = 0
                            backoff = 1.0
                        else:
                            consecutive_failures += 1
                            if consecutive_failures < CIRCUIT_BREAKER_THRESHOLD:
                                self.log(
                                    "warn",
                                    "flush failed, retrying next cycle",
                                    consecutive_failures=consecutive_failures,
                                )
                except Exception as exc:
                    self.log(
                        "error",
                        "consumer error, reconnecting",
                        error=str(exc),
                        consecutive_failures=consecutive_failures,
                    )
                    self._reconnect(error=exc)
                    time.sleep(min(backoff, CIRCUIT_BREAKER_MAX_BACKOFF))
                    consecutive_failures += 1
                    backoff = min(backoff * 2, CIRCUIT_BREAKER_MAX_BACKOFF)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self._flush()
            except Exception:
                pass
            self.consumer.close()
            self.log("info", "raw sink stopped")


class PoisonEventError(Exception):
    """Raised when a group of events cannot be serialized to Parquet."""