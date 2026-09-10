from datetime import datetime, timezone

import psycopg2

from ingestion.validator import parse_event_time
from stream_processor.status import derive_status

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS vessel_current_state (
    mmsi            TEXT PRIMARY KEY,
    ship_name       TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    sog_knots       DOUBLE PRECISION,
    cog_degrees     DOUBLE PRECISION,
    true_heading    DOUBLE PRECISION,
    rate_of_turn    DOUBLE PRECISION,
    nav_status      INTEGER,
    position        GEOGRAPHY(Point, 4326),
    status          TEXT,
    last_seen_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vessel_track (
    id              BIGSERIAL PRIMARY KEY,
    mmsi            TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    sog_knots       DOUBLE PRECISION,
    event_time      TIMESTAMPTZ,
    position        GEOGRAPHY(Point, 4326),
    partition_hour  TIMESTAMP,
    UNIQUE (mmsi, event_id)
);

CREATE INDEX IF NOT EXISTS idx_vessel_track_mmsi_time
    ON vessel_track (mmsi, event_time);
"""

UPSERT_STATE_SQL = """
INSERT INTO vessel_current_state
    (mmsi, ship_name, latitude, longitude, sog_knots, cog_degrees,
     true_heading, rate_of_turn, nav_status, position, status, last_seen_at)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s,
     ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s, %s)
ON CONFLICT (mmsi) DO UPDATE SET
    ship_name    = EXCLUDED.ship_name,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    sog_knots    = EXCLUDED.sog_knots,
    cog_degrees  = EXCLUDED.cog_degrees,
    true_heading = EXCLUDED.true_heading,
    rate_of_turn = EXCLUDED.rate_of_turn,
    nav_status   = EXCLUDED.nav_status,
    position     = EXCLUDED.position,
    status       = EXCLUDED.status,
    last_seen_at = EXCLUDED.last_seen_at,
    updated_at   = now()
"""

INSERT_TRACK_SQL = """
INSERT INTO vessel_track
    (mmsi, event_id, latitude, longitude, sog_knots, event_time, position, partition_hour)
VALUES
    (%s, %s, %s, %s, %s, %s,
     ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, date_trunc('hour', %s))
ON CONFLICT (mmsi, event_id) DO NOTHING
"""


def connect(config: dict):
    return psycopg2.connect(config["postgres_url"])


def ensure_schema(conn) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)


def _event_time(event: dict) -> datetime:
    parsed = parse_event_time(str(event.get("event_time") or ""))
    if parsed is not None:
        return parsed
    return datetime.fromtimestamp(float(event.get("ingested_at") or 0), tz=timezone.utc)


def build_rows(events: list[dict]) -> tuple[list[tuple], list[tuple]]:
    state_rows: list[tuple] = []
    track_rows: list[tuple] = []
    for event in events:
        event_time = _event_time(event)
        status = derive_status(event.get("nav_status"), event.get("sog_knots"))
        latitude = event.get("latitude")
        longitude = event.get("longitude")
        if latitude is None or longitude is None:
            continue
        mmsi = str(event.get("mmsi") or "")
        if not mmsi:
            continue
        state_rows.append(
            (
                mmsi,
                event.get("ship_name"),
                latitude,
                longitude,
                event.get("sog_knots"),
                event.get("cog_degrees"),
                event.get("true_heading"),
                event.get("rate_of_turn"),
                event.get("nav_status"),
                longitude,
                latitude,
                status,
                event_time,
            )
        )
        track_rows.append(
            (
                mmsi,
                event.get("event_id"),
                latitude,
                longitude,
                event.get("sog_knots"),
                event_time,
                longitude,
                latitude,
                event_time,
            )
        )
    return state_rows, track_rows


def apply_batch(conn, state_rows: list[tuple], track_rows: list[tuple]) -> None:
    with conn:
        with conn.cursor() as cur:
            if track_rows:
                cur.executemany(INSERT_TRACK_SQL, track_rows)
            if state_rows:
                cur.executemany(UPSERT_STATE_SQL, state_rows)