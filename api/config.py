import os

from dotenv import load_dotenv

load_dotenv()


def cors_origins() -> list[str]:
    raw = os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:4173",
    )
    return [o.strip() for o in raw.split(",") if o.strip()]


def load_config() -> dict:
    return {
        "postgres_url": os.getenv("POSTGRES_URL", "postgresql://maritime:maritime@localhost:5433/maritime"),
        "track_window_hours": int(os.getenv("TRACK_WINDOW_HOURS", "12")),
        "log_level": os.getenv("INGEST_LOG_LEVEL", "INFO"),
        "cors_origins": cors_origins(),
    }