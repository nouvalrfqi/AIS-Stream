import os

from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    return {
        "postgres_url": os.getenv("POSTGRES_URL", "postgresql://maritime:maritime@localhost:5433/maritime"),
        "track_window_hours": int(os.getenv("TRACK_WINDOW_HOURS", "12")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
    }