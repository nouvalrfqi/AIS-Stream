import json

from ingestion.normalizer import normalize, _to_heading, _to_rate_of_turn


def _raw(**position_overrides):
    pos = {
        "MessageID": 1,
        "RepeatIndicator": 0,
        "UserID": 563070820,
        "Valid": True,
        "NavigationalStatus": 0,
        "RateOfTurn": -128,
        "Sog": 8.2,
        "PositionAccuracy": 1,
        "Longitude": 103.84,
        "Latitude": 1.26,
        "Cog": 120.0,
        "TrueHeading": 511,
        "Timestamp": 12,
        "SpecialManoeuvreIndicator": 0,
        "Spare": 0,
        "Raim": False,
        "CommunicationState": 0,
    }
    pos.update(position_overrides)
    return {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": "563070820",
            "MMSI_String": "563070820",
            "ShipName": " MPA 2 ",
            "latitude": 1.26,
            "longitude": 103.84,
            "time_utc": "2026-09-10 12:00:00.000000 +0000 UTC",
        },
        "Message": {"PositionReport": pos},
    }


def test_normalize_strips_ship_name():
    out = normalize(_raw(), 1789061760.0)
    assert out["ship_name"] == "MPA 2"
    assert out["ship_name"].startswith(" ") is False


def test_normalize_sentinel_heading_to_none():
    out = normalize(_raw(), 1789061760.0)
    assert out["true_heading"] is None


def test_normalize_sentinel_rot_to_none():
    out = normalize(_raw(), 1789061760.0)
    assert out["rate_of_turn"] is None


def test_normalize_valid_heading_rot():
    out = normalize(
        _raw(**{"TrueHeading": 130, "RateOfTurn": 20}),
        1789061760.0,
    )
    assert out["true_heading"] == 130.0
    assert out["rate_of_turn"] == 20.0


def test_normalize_schema_version_present():
    out = normalize(_raw(), 1789061760.0)
    assert out["schema_version"] == "1.0"


def test_normalize_preserves_raw():
    out = normalize(_raw(), 1789061760.0)
    assert json.loads(out["raw"])["MessageType"] == "PositionReport"


def test_normalize_mmsi_is_string():
    out = normalize(_raw(), 1789061760.0)
    assert isinstance(out["mmsi"], str)
    assert out["mmsi"] == "563070820"


def test_normalize_ingested_at():
    out = normalize(_raw(), 123456.0)
    assert out["ingested_at"] == 123456.0


def test_to_heading_none_handling():
    assert _to_heading(None) is None
    assert _to_heading(511) is None
    assert _to_heading(120) == 120.0


def test_to_rate_of_turn_none_handling():
    assert _to_rate_of_turn(None) is None
    assert _to_rate_of_turn(-128) is None
    assert _to_rate_of_turn(30) == 30.0