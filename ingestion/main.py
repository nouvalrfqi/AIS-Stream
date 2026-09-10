import asyncio
import json
import logging
import signal
import sys

from common.dlq import close_dlq, init_dlq, send_to_dlq
from .ais_client import consume
from .config import load_config
from .kafka_producer import AisKafkaProducer
from .normalizer import normalize
from .validator import parse_event_time, validate_message

logger = logging.getLogger("ingestion")


def _log(level: str, msg: str, **fields):
    record = {"event": msg}
    record.update(fields)
    getattr(logger, level)(json.dumps(record))


async def _handle_event(raw: dict, producer: AisKafkaProducer, dlq_producer, dlq_topic: str) -> None:
    message_type = raw.get("MessageType")
    if message_type != "PositionReport":
        _log("debug", "non-position-report message skipped", message_type=str(message_type))
        return

    valid, errors = validate_message(raw)
    if not valid:
        send_to_dlq(dlq_producer, dlq_topic, raw, reason="validation_failed", source="ingestion")
        _log("warn", "invalid event sent to DLQ", errors=errors, message_type=str(message_type))
        return

    canonical = normalize(raw, raw["_received_at"])

    event_time = parse_event_time(canonical["event_time"])
    event_epoch = event_time.timestamp() if event_time else 0.0
    latency_ms = int((canonical["ingested_at"] - event_epoch) * 1000)

    future = producer.send(canonical)
    try:
        await asyncio.to_thread(future.get, timeout=10)
    except Exception as exc:
        send_to_dlq(dlq_producer, dlq_topic, canonical, reason="kafka_publish_failed", source="ingestion")
        _log("error", "kafka publish failed, sent to DLQ", mmsi=canonical["mmsi"], error=str(exc))
        return

    _log(
        "info",
        "event published",
        mmsi=canonical["mmsi"],
        event_id=canonical["event_id"],
        ship_name=canonical["ship_name"],
        latitude=canonical["latitude"],
        longitude=canonical["longitude"],
        sog_knots=canonical["sog_knots"],
        latency_ms=latency_ms,
    )


async def _shutdown(sig, loop, producers):
    _log("info", "shutdown signal received", signal=str(sig))
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for p in producers:
        p.close()
    loop.stop()


async def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=config["log_level"],
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _log("info", "ingestion starting", **{k: v for k, v in config.items() if k != "api_key"})

    producer = AisKafkaProducer(config)
    _log("info", "kafka producer initialized", bootstrap=config["kafka_bootstrap_servers"])

    dlq_producer = init_dlq(config["kafka_bootstrap_servers"], config["kafka_topic_dlq"])

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(_shutdown(s, loop, [producer, dlq_producer])),
            )
        except NotImplementedError:
            pass

    async def handler(raw):
        await _handle_event(raw, producer, dlq_producer, config["kafka_topic_dlq"])

    await consume(config, on_event=handler, log=_log)
    producer.close()
    close_dlq(dlq_producer)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)