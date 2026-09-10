-- Maritime platform schema — idempotent
-- Dijalankan otomatis oleh stream_processor.db.ensure_schema() saat start.
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

CREATE INDEX IF NOT EXISTS idx_vessel_state_position
    ON vessel_current_state USING gist (position);