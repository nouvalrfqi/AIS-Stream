import io
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("api.data_quality")

DEFAULT_WINDOW_HOURS = 24
CACHE_TTL_SECONDS = 60

BUCKET = os.getenv("S3_BUCKET", "maritime-raw")
PREFIX = os.getenv("S3_PREFIX", "raw")

_cache: dict = {"window_hours": None, "ts": 0.0, "result": None}


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def _parse_event_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace(" UTC", "").strip())
        return parsed.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None


def _in_window(key: str, cutoff: datetime) -> bool:
    parts = {}
    for token in key.split("/"):
        if token.startswith("dt="):
            parts["dt"] = token[3:]
        elif token.startswith("HH="):
            parts["hh"] = token[3:]
    if not parts.get("dt"):
        return True
    try:
        partition = datetime.strptime(parts["dt"], "%Y-%m-%d")
        if parts.get("hh"):
            partition = partition.replace(hour=int(parts["hh"]))
    except ValueError:
        return True
    return partition.replace(tzinfo=timezone.utc) >= cutoff.replace(minute=0, second=0, microsecond=0)


def _scan_parquet(window_hours: int) -> dict:
    client = _client()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    cutoff_epoch = cutoff.timestamp()

    keys = [
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}/").get("Contents", [])
        if _in_window(obj["Key"], cutoff)
    ]

    events_processed = 0
    deltas: list[float] = []
    negative = 0
    missing_position = 0
    missing_sog = 0
    missing_cog = 0
    invalid_coordinates = 0

    for key in keys:
        body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        cols = {name: col.to_pylist() for name, col in zip(table.column_names, table.columns)}

        event_times = [_parse_event_time(v) for v in cols["event_time"]]
        ingested = cols["ingested_at"]
        lat = cols["latitude"]
        lon = cols["longitude"]
        sog = cols["sog_knots"]
        cog = cols["cog_degrees"]

        rows = len(ingested)
        for i in range(rows):
            ing = ingested[i]
            if ing is None or ing < cutoff_epoch:
                continue
            events_processed += 1
            if lat[i] is None or lon[i] is None:
                missing_position += 1
            elif not (-90 <= lat[i] <= 90) or not (-180 <= lon[i] <= 180):
                invalid_coordinates += 1
            if sog[i] is None:
                missing_sog += 1
            if cog[i] is None:
                missing_cog += 1
            et = event_times[i]
            if et is None:
                continue
            delta = ing - et.timestamp()
            if delta < 0:
                negative += 1
            else:
                deltas.append(delta)

    deltas.sort()
    latency = {
        "avg_s": round(sum(deltas) / len(deltas), 1) if deltas else None,
        "p50_s": deltas[len(deltas) // 2] if deltas else None,
        "p95_s": deltas[int(len(deltas) * 0.95)] if deltas else None,
        "max_s": deltas[-1] if deltas else None,
        "events_negative_latency": negative,
    }

    return {
        "window_hours": window_hours,
        "events_processed": events_processed,
        "parquet_files": len(keys),
        "latency": latency,
        "missing_position": missing_position,
        "missing_sog": missing_sog,
        "missing_cog": missing_cog,
        "invalid_coordinates": invalid_coordinates,
    }


def get_data_quality(window_hours: int) -> dict:
    global _cache
    now = time.time()
    if _cache["result"] is not None and _cache["window_hours"] == window_hours:
        if now - _cache["ts"] < CACHE_TTL_SECONDS:
            return _cache["result"]
    result = _scan_parquet(window_hours)
    _cache = {"window_hours": window_hours, "ts": now, "result": result}
    return result