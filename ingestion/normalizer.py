import hashlib
import json

from .validator import SENTINEL_HEADING, SENTINEL_RATE_OF_TURN, parse_event_time


def _to_heading(value):
    return None if value == SENTINEL_HEADING else float(value)


def _to_rate_of_turn(value):
    return None if value == SENTINEL_RATE_OF_TURN else float(value)


def _event_id(raw: dict) -> str:
    return hashlib.md5(json.dumps(raw, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def normalize(raw: dict, received_at: float) -> dict:
    meta = raw.get("MetaData") or {}
    position = (raw.get("Message") or {}).get("PositionReport") or {}

    return {
        "event_id": _event_id(raw),
        "message_type": raw.get("MessageType"),
        "mmsi": str(meta.get("MMSI")),
        "ship_name": meta.get("ShipName").strip() if isinstance(meta.get("ShipName"), str) else None,
        "latitude": position.get("Latitude"),
        "longitude": position.get("Longitude"),
        "sog_knots": position.get("Sog"),
        "cog_degrees": position.get("Cog"),
        "true_heading": _to_heading(position.get("TrueHeading")),
        "rate_of_turn": _to_rate_of_turn(position.get("RateOfTurn")),
        "nav_status": position.get("NavigationalStatus"),
        "position_accuracy": position.get("PositionAccuracy"),
        "raim": position.get("Raim"),
        "event_time": meta.get("time_utc"),
        "ingested_at": received_at,
        "source": "aisstream",
        "raw": json.dumps(raw),
    }