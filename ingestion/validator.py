from datetime import datetime

SENTINEL_HEADING = 511
SENTINEL_RATE_OF_TURN = -128

MESSAGE_TYPE = "PositionReport"

REQUIRED_KEYS = {
    "MessageType",
    "MetaData",
    "Message",
}

REQUIRED_METADATA_KEYS = {
    "MMSI",
    "time_utc",
}

REQUIRED_POSITION_KEYS = {
    "Latitude",
    "Longitude",
    "Sog",
    "Cog",
}

SUPPORTED_MESSAGE_TYPES = {"PositionReport", "SubscriptionConfirmation", "ShipStaticData", "VoyageData"}


def parse_event_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace(" UTC", "").strip())
    except ValueError:
        return None


def validate_message(raw: dict) -> tuple[bool, list[str]]:
    errors: list[str] = []

    if not isinstance(raw, dict):
        return False, ["payload is not a JSON object"]

    message_type = raw.get("MessageType")
    if message_type not in SUPPORTED_MESSAGE_TYPES:
        errors.append(f"unsupported MessageType: {message_type!r}")

    if message_type == "PositionReport":
        for key in REQUIRED_KEYS:
            if key not in raw:
                errors.append(f"missing top-level key: {key}")

        meta = raw.get("MetaData")
        if not isinstance(meta, dict):
            errors.append("MetaData is not an object")
        else:
            for key in REQUIRED_METADATA_KEYS:
                if key not in meta:
                    errors.append(f"missing MetaData key: {key}")
            time_utc = meta.get("time_utc")
            if time_utc is not None and parse_event_time(str(time_utc)) is None:
                errors.append(f"unparseable time_utc: {time_utc!r}")

        message = raw.get("Message")
        if not isinstance(message, dict) or not isinstance(message.get("PositionReport"), dict):
            errors.append("Message.PositionReport is not an object")
        else:
            pos = message["PositionReport"]
            for key in REQUIRED_POSITION_KEYS:
                if key not in pos:
                    errors.append(f"missing PositionReport key: {key}")

            latitude = pos.get("Latitude")
            longitude = pos.get("Longitude")
            if latitude is None or not (-90 <= latitude <= 90):
                errors.append(f"invalid Latitude: {latitude!r}")
            if longitude is None or not (-180 <= longitude <= 180):
                errors.append(f"invalid Longitude: {longitude!r}")

            sog = pos.get("Sog")
            if sog is None or sog < 0:
                errors.append(f"invalid Sog: {sog!r}")

    return len(errors) == 0, errors