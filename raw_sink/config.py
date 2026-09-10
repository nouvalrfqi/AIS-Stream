import os

from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic_raw": os.getenv("KAFKA_TOPIC_RAW", "ais.raw"),
        "kafka_consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "raw-sink"),
        "s3_endpoint": os.getenv("S3_ENDPOINT", ""),
        "s3_access_key": os.getenv("S3_ACCESS_KEY", ""),
        "s3_secret_key": os.getenv("S3_SECRET_KEY", ""),
        "s3_bucket": os.getenv("S3_BUCKET", "maritime-raw"),
        "s3_prefix": os.getenv("S3_PREFIX", "raw"),
        "parquet_max_rows": int(os.getenv("PARQUET_MAX_ROWS", "1000")),
        "parquet_max_seconds": int(os.getenv("PARQUET_MAX_SECONDS", "300")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
    }