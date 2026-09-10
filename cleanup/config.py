import os

from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    return {
        "postgres_url": os.getenv("POSTGRES_URL", "postgresql://maritime:maritime@localhost:5433/maritime"),
        "track_retention_hours": int(os.getenv("TRACK_RETENTION_HOURS", "24")),
        "state_stale_minutes": int(os.getenv("STATE_STALE_MINUTES", "45")),
        "state_purge_after_hours": int(os.getenv("STATE_PURGE_AFTER_HOURS", "0")),
        "s3_endpoint": os.getenv("S3_ENDPOINT", ""),
        "s3_access_key": os.getenv("S3_ACCESS_KEY", ""),
        "s3_secret_key": os.getenv("S3_SECRET_KEY", ""),
        "s3_bucket": os.getenv("S3_BUCKET", "maritime-raw"),
        "s3_prefix": os.getenv("S3_PREFIX", "raw"),
        "s3_retention_days": int(os.getenv("S3_RETENTION_DAYS", "7")),
        "s3_lifecycle_on": os.getenv("S3_LIFECYCLE_ON", "false").lower() == "true",
        "cleanup_interval_seconds": int(os.getenv("CLEANUP_INTERVAL_SECONDS", "300")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
    }