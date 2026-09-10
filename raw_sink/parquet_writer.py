import io

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("message_type", pa.string()),
        pa.field("mmsi", pa.string()),
        pa.field("ship_name", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("sog_knots", pa.float64()),
        pa.field("cog_degrees", pa.float64()),
        pa.field("true_heading", pa.float64()),
        pa.field("rate_of_turn", pa.float64()),
        pa.field("nav_status", pa.int32()),
        pa.field("position_accuracy", pa.bool_()),
        pa.field("raim", pa.bool_()),
        pa.field("event_time", pa.string()),
        pa.field("ingested_at", pa.float64()),
        pa.field("source", pa.string()),
        pa.field("raw", pa.string()),
    ]
)


def _denull_numeric(value):
    return None if value is None else float(value)


def _normalize_row(event: dict) -> dict:
    return {
        "event_id": event.get("event_id"),
        "message_type": event.get("message_type"),
        "mmsi": str(event.get("mmsi")),
        "ship_name": event.get("ship_name"),
        "latitude": _denull_numeric(event.get("latitude")),
        "longitude": _denull_numeric(event.get("longitude")),
        "sog_knots": _denull_numeric(event.get("sog_knots")),
        "cog_degrees": _denull_numeric(event.get("cog_degrees")),
        "true_heading": _denull_numeric(event.get("true_heading")),
        "rate_of_turn": _denull_numeric(event.get("rate_of_turn")),
        "nav_status": event.get("nav_status"),
        "position_accuracy": bool(event.get("position_accuracy")),
        "raim": bool(event.get("raim")),
        "event_time": event.get("event_time"),
        "ingested_at": _denull_numeric(event.get("ingested_at")),
        "source": event.get("source"),
        "raw": event.get("raw"),
    }


def write_parquet_bytes(events: list[dict]) -> bytes:
    rows = [_normalize_row(e) for e in events]
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()