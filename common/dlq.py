import json
import logging
import time

from kafka import KafkaProducer

logger = logging.getLogger("common.dlq")

_dlq_producer: KafkaProducer | None = None


def init_dlq(bootstrap_servers: str, topic: str) -> KafkaProducer:
    global _dlq_producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    _dlq_producer = producer
    logger.info(json.dumps({"event": "dlq producer initialized", "topic": topic}))
    return producer


def send_to_dlq(
    producer: KafkaProducer,
    topic: str,
    event: dict,
    reason: str,
    source: str = "",
) -> None:
    payload = {
        "event": event,
        "_dlq_reason": reason,
        "_dlq_source": source,
        "_dlq_timestamp": time.time(),
    }
    try:
        producer.send(topic, value=payload)
        producer.flush(timeout=10)
        logger.warning(
            json.dumps({
                "event": "sent to dlq",
                "topic": topic,
                "reason": reason,
                "source": source,
                "mmsi": event.get("mmsi"),
            })
        )
    except Exception as exc:
        logger.error(
            json.dumps({
                "event": "dlq send failed",
                "error": str(exc),
                "reason": reason,
                "mmsi": event.get("mmsi"),
            })
        )


def close_dlq(producer: KafkaProducer) -> None:
    try:
        producer.flush(timeout=5)
        producer.close(timeout=5)
    except Exception:
        pass
