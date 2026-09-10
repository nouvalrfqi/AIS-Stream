import pytest

from ingestion.validator import (
    SENTINEL_HEADING,
    SENTINEL_RATE_OF_TURN,
    parse_event_time,
    validate_message,
)


def _position_report(**overrides):
    raw = {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": "563070820",
            "ShipName": "MPA 2",
            "latitude": 1.26,
            "longitude": 103.84,
            "time_utc": "2026-09-10 12:00:00.000000 +0000 UTC",
        },
        "Message": {
            "PositionReport": {
                "MessageID": 1,
                "RepeatIndicator": 0,
                "UserID": 563070820,
                "Valid": True,
                "NavigationalStatus": 0,
                "RateOfTurn": 0,
                "Sog": 8.2,
                "PositionAccuracy": 1,
                "Longitude": 103.84,
                "Latitude": 1.26,
                "Cog": 120.0,
                "TrueHeading": 130,
                "Timestamp": 12,
                "SpecialManoeuvreIndicator": 0,
                "Spare": 0,
                "Raim": False,
                "CommunicationState": 0,
            }
        },
    }
    for k, v in overrides.items():
        parts = k.split(".")
        target = raw
        for p in parts[:-1]:
            target = target[p]
        target[parts[-1]] = v
    return raw


def test_valid_position_report():
    valid, errors = validate_message(_position_report())
    assert valid is True
    assert errors == []


def test_missing_top_level_key():
    raw = _position_report()
    del raw["MetaData"]
    valid, errors = validate_message(raw)
    assert valid is False
    assert any("missing top-level key" in e for e in errors)


def test_invalid_latitude_out_of_range():
    raw = _position_report(**{"Message.PositionReport.Latitude": 95.0})
    valid, errors = validate_message(raw)
    assert valid is False
    assert any("invalid Latitude" in e for e in errors)


def test_invalid_longitude_out_of_range():
    raw = _position_report(**{"Message.PositionReport.Longitude": -181.0})
    valid, errors = validate_message(raw)
    assert valid is False
    assert any("invalid Longitude" in e for e in errors)


def test_negative_sog_invalid():
    raw = _position_report(**{"Message.PositionReport.Sog": -1})
    valid, errors = validate_message(raw)
    assert valid is False
    assert any("invalid Sog" in e for e in errors)


def test_zero_sog_valid():
    raw = _position_report(**{"Message.PositionReport.Sog": 0})
    valid, _ = validate_message(raw)
    assert valid is True


def test_unparseable_time_utc_invalid():
    raw = _position_report(**{"MetaData.time_utc": "not-a-date"})
    valid, errors = validate_message(raw)
    assert valid is False
    assert any("unparseable time_utc" in e for e in errors)


def test_sentinel_constants():
    assert SENTINEL_HEADING == 511
    assert SENTINEL_RATE_OF_TURN == -128


def test_parse_event_time_parses_utc():
    parsed = parse_event_time("2026-09-10 12:00:00.000000 +0000 UTC")
    assert parsed is not None
    assert parsed.hour == 12


def test_parse_event_time_invalid():
    assert parse_event_time("garbage") is None


def test_non_position_report_supported_type():
    raw = {"MessageType": "SubscriptionConfirmation", "MetaData": {}, "Message": {}}
    valid, errors = validate_message(raw)
    assert valid is True
    assert errors == []