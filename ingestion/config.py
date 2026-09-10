import json
import os

from dotenv import load_dotenv

load_dotenv()

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"


def load_config() -> dict:
    return {
        "aisstream_url": os.getenv("AISSTREAM_URL", AISSTREAM_URL),
        "api_key": os.getenv("AISSTREAM_API_KEY", ""),
        "bounding_boxes": json.loads(os.getenv("AISSTREAM_BOUNDING_BOXES", "[[[-90, -180], [90, 180]]]")),
        "filter_message_types": json.loads(os.getenv("AISSTREAM_FILTER_MESSAGE_TYPES", '["PositionReport"]')),
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic_raw": os.getenv("KAFKA_TOPIC_RAW", "ais.raw"),
        "kafka_topic_dlq": os.getenv("KAFKA_TOPIC_DLQ", "ais.dlq"),
        "reconnect_base_seconds": float(os.getenv("INGEST_RECONNECT_BASE", "1")),
        "reconnect_max_seconds": float(os.getenv("INGEST_RECONNECT_MAX", "60")),
        "reconnect_jitter_seconds": float(os.getenv("INGEST_RECONNECT_JITTER", "0.3")),
        "future_skew_tolerance_seconds": float(os.getenv("INGEST_FUTURE_SKEW_TOLERANCE_SECONDS", "60")),
        "stale_skew_tolerance_seconds": float(os.getenv("INGEST_STALE_SKEW_TOLERANCE_SECONDS", "21600")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
    }