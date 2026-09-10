import os

from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic_raw": os.getenv("KAFKA_TOPIC_RAW", "ais.raw"),
        "kafka_consumer_group_state": os.getenv("KAFKA_CONSUMER_GROUP_STATE", "realtime-state"),
        "postgres_url": os.getenv("POSTGRES_URL", "postgresql://maritime:maritime@localhost:5432/maritime"),
        "state_flush_rows": int(os.getenv("STATE_FLUSH_ROWS", "500")),
        "state_flush_seconds": int(os.getenv("STATE_FLUSH_SECONDS", "5")),
        "state_stale_minutes": int(os.getenv("STATE_STALE_MINUTES", "45")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
    }