import math

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from api import data_quality, db
from api.config import load_config
from api.schemas import (
    ConflictPair,
    ConflictVessel,
    DataQualityResult,
    FlowDistribution,
    Health,
    IntelligenceSummary,
    ManeuveringResult,
    NavDistribution,
    NearbyResult,
    SpeedDistribution,
    StatusCount,
    TrackPoint,
    VesselDetail,
    VesselLeader,
    VesselSummary,
)

config = load_config()

SELECT_STATE_COLS = (
    "mmsi, ship_name, latitude, longitude, sog_knots, cog_degrees, "
    "true_heading, nav_status, status, last_seen_at"
)

VALID_MMSI = "mmsi ~ '^[0-9]{9}$'"
STALE_MINUTES = 45

NAV_STATUS_LABELS = {
    0: "Underway",
    1: "At Anchor",
    2: "Not Under Command",
    3: "Restricted Manoeuverability",
    4: "Constrained by Draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in Fishing",
    8: "Underway Sailing",
    14: "AIS-SART",
    15: "Undefined",
}

FLOW_LABELS = [
    "N", "NNE", "NE", "ENE",
    "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW",
    "W", "WNW", "NW", "NNW",
]

FAST_ROT_DEG_MIN = 10.0


def _decode_rot(value: float | None) -> tuple[float | None, str | None, float]:
    if value is None:
        return None, None, 0.0
    magnitude = abs(value)
    if magnitude >= 127:
        return None, ("port" if value > 0 else "starboard"), FAST_ROT_DEG_MIN
    deg_min = round(4.733 * math.sqrt(magnitude), 1)
    direction = "starboard" if value > 0 else "port"
    return deg_min, direction, deg_min

app = FastAPI(title="Maritime Real-Time Platform API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health", response_model=Health)
def health() -> Health:
    db_ok = db.ping(config)
    return Health(status="ok" if db_ok else "degraded", database="ok" if db_ok else "unavailable")


@app.get("/vessels", response_model=list[VesselSummary])
def list_vessels() -> list[dict]:
    try:
        return db.fetch_all(
            config,
            f"SELECT {SELECT_STATE_COLS} FROM vessel_current_state ORDER BY mmsi",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.get("/vessels/nearby", response_model=list[NearbyResult])
def vessels_nearby(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_m: float = Query(..., gt=0),
) -> list[dict]:
    try:
        return db.fetch_all(
            config,
            "SELECT mmsi, ship_name, latitude, longitude, sog_knots, status, "
            "ST_Distance(position, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) AS distance_m "
            "FROM vessel_current_state "
            "WHERE ST_DWithin(position, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s) "
            "ORDER BY distance_m ASC LIMIT 100",
            (lon, lat, lon, lat, radius_m),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.get("/vessels/{mmsi}", response_model=VesselDetail)
def vessel_detail(mmsi: str) -> dict:
    try:
        rows = db.fetch_all(
            config,
            f"SELECT {SELECT_STATE_COLS}, rate_of_turn, updated_at "
            "FROM vessel_current_state WHERE mmsi = %s",
            (mmsi,),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    if not rows:
        raise HTTPException(status_code=404, detail=f"vessel {mmsi} not found")
    return rows[0]


@app.get("/vessels/{mmsi}/track", response_model=list[TrackPoint])
def vessel_track(mmsi: str) -> list[dict]:
    try:
        return db.fetch_all(
            config,
            "SELECT event_time, latitude, longitude, sog_knots FROM vessel_track "
            "WHERE mmsi = %s AND event_time > now() - make_interval(hours => %s) "
            "ORDER BY event_time ASC",
            (mmsi, config["track_window_hours"]),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.get("/intelligence/summary", response_model=IntelligenceSummary)
def intelligence_summary() -> dict:
    try:
        agg = db.fetch_all(
            config,
            f"SELECT "
            f"count(*) AS total, "
            f"count(*) FILTER (WHERE last_seen_at > now() - interval '{STALE_MINUTES} minutes') AS fresh, "
            f"count(*) FILTER (WHERE status = 'STALE') AS stale, "
            f"count(*) FILTER (WHERE COALESCE(sog_knots, 0) > 0.5) AS moving, "
            f"count(*) FILTER (WHERE COALESCE(sog_knots, 0) <= 0.5) AS idle, "
            f"count(*) FILTER (WHERE status = 'ANCHORED') AS anchored, "
            f"count(*) FILTER (WHERE status = 'MOORED') AS moored, "
            f"avg(sog_knots) AS avg_sog, max(sog_knots) AS max_sog "
            f"FROM vessel_current_state WHERE {VALID_MMSI}",
        )[0]
        events_window = db.fetch_all(
            config,
            "SELECT count(*) AS events_window FROM vessel_track "
            "WHERE event_time > now() - make_interval(hours => %s)",
            (config["track_window_hours"],),
        )[0]["events_window"]
        by_status = db.fetch_all(
            config,
            f"SELECT status, count(*) AS count FROM vessel_current_state "
            f"WHERE {VALID_MMSI} GROUP BY status ORDER BY count DESC",
        )
        fastest = db.fetch_all(
            config,
            f"SELECT mmsi, ship_name, sog_knots FROM vessel_current_state "
            f"WHERE {VALID_MMSI} ORDER BY sog_knots DESC NULLS LAST LIMIT 1",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    return {
        "total": agg["total"],
        "fresh": agg["fresh"],
        "stale": agg["stale"],
        "moving": agg["moving"],
        "idle": agg["idle"],
        "anchored": agg["anchored"],
        "moored": agg["moored"],
        "events_window": int(events_window),
        "avg_sog": agg["avg_sog"],
        "max_sog": agg["max_sog"],
        "fastest": fastest[0] if fastest else None,
        "by_status": by_status,
    }


@app.get("/intelligence/speed", response_model=SpeedDistribution)
def intelligence_speed() -> dict:
    try:
        buckets = db.fetch_all(
            config,
            f"SELECT width_bucket(sog_knots, 0, 20, 20) AS bucket, count(*) AS count "
            f"FROM vessel_current_state WHERE {VALID_MMSI} AND sog_knots IS NOT NULL "
            f"GROUP BY bucket ORDER BY bucket",
        )
        stats = db.fetch_all(
            config,
            f"SELECT avg(sog_knots) AS avg_sog, "
            f"percentile_cont(0.5) WITHIN GROUP (ORDER BY sog_knots) AS p50_sog, "
            f"max(sog_knots) AS max_sog, count(*) AS sample_n "
            f"FROM vessel_current_state WHERE {VALID_MMSI} AND sog_knots IS NOT NULL",
        )[0]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    return {
        "buckets": [
            {"bucket_min": b["bucket"] - 1,
             "bucket_max": None if b["bucket"] > 20 else float(b["bucket"]),
             "count": b["count"]}
            for b in buckets
        ],
        "avg_sog": stats["avg_sog"],
        "p50_sog": stats["p50_sog"],
        "max_sog": stats["max_sog"],
        "sample_n": stats["sample_n"],
    }


@app.get("/intelligence/navigation", response_model=NavDistribution)
def intelligence_navigation() -> dict:
    try:
        rows = db.fetch_all(
            config,
            f"SELECT nav_status, count(*) AS count FROM vessel_current_state "
            f"WHERE {VALID_MMSI} GROUP BY nav_status ORDER BY count DESC",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    grouped: dict[str, dict] = {}
    for r in rows:
        code = r["nav_status"]
        label = NAV_STATUS_LABELS.get(code, "Reserved" if code is not None else "Unknown")
        bucket = grouped.get(label)
        if bucket is None:
            grouped[label] = {"code": code, "label": label, "count": r["count"]}
        else:
            bucket["count"] += r["count"]
    buckets = sorted(grouped.values(), key=lambda b: b["count"], reverse=True)
    return {"buckets": buckets, "total": sum(b["count"] for b in buckets)}


@app.get("/intelligence/flow", response_model=FlowDistribution)
def intelligence_flow() -> dict:
    try:
        rows = db.fetch_all(
            config,
            f"SELECT mod(round(cog_degrees / 22.5)::int, 16) AS sector, "
            f"count(*) AS count, avg(sog_knots) AS avg_sog "
            f"FROM vessel_current_state "
            f"WHERE {VALID_MMSI} AND status IS DISTINCT FROM 'STALE' "
            f"AND cog_degrees IS NOT NULL AND cog_degrees >= 0 AND cog_degrees < 360 "
            f"GROUP BY sector ORDER BY sector",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    buckets = []
    for r in rows:
        sector = (r["sector"] % 16 + 16) % 16
        buckets.append({
            "sector_deg": float(sector * 22.5),
            "label": FLOW_LABELS[sector],
            "count": r["count"],
            "avg_sog": r["avg_sog"],
        })
    return {"buckets": buckets, "total": sum(b["count"] for b in buckets)}


@app.get("/intelligence/maneuvering", response_model=ManeuveringResult)
def intelligence_maneuvering(
    min_rot_deg_min: float = Query(5.0, ge=0, le=180),
) -> dict:
    try:
        rows = db.fetch_all(
            config,
            f"SELECT mmsi, ship_name, rate_of_turn, sog_knots, nav_status "
            f"FROM vessel_current_state WHERE {VALID_MMSI} AND rate_of_turn IS NOT NULL "
            f"ORDER BY abs(rate_of_turn) DESC LIMIT 100",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    vessels = []
    for r in rows:
        rot_deg_min, direction, effective = _decode_rot(r["rate_of_turn"])
        if effective < min_rot_deg_min:
            continue
        vessels.append({
            "mmsi": r["mmsi"],
            "ship_name": r["ship_name"],
            "rate_of_turn": r["rate_of_turn"],
            "rot_deg_min": rot_deg_min,
            "direction": direction,
            "sog_knots": r["sog_knots"],
            "nav_status": r["nav_status"],
        })
    return {"threshold_deg_min": min_rot_deg_min, "vessels": vessels}


@app.get("/intelligence/data-quality", response_model=DataQualityResult)
def intelligence_data_quality(
    window_hours: int = Query(data_quality.DEFAULT_WINDOW_HOURS, gt=0, le=168),
) -> dict:
    try:
        return data_quality.get_data_quality(window_hours)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"data-quality unavailable: {exc}") from exc


@app.get("/intelligence/conflicts", response_model=list[ConflictPair])
def intelligence_conflicts(
    radius_m: float = Query(500, gt=0, le=20000),
) -> list[dict]:
    try:
        margin_deg = radius_m / 111_000
        rows = db.fetch_all(
            config,
            "WITH s AS (SELECT mmsi, ship_name, latitude, longitude, sog_knots, status, "
            "position::geometry AS pos "
            f"FROM vessel_current_state WHERE {VALID_MMSI}) "
            "SELECT sa.mmsi AS ammsi, sa.ship_name AS aship, sa.latitude AS alat, sa.longitude AS alon, "
            "sa.sog_knots AS asog, sa.status AS astat, "
            "sb.mmsi AS bmmsi, sb.ship_name AS bship, sb.latitude AS blat, sb.longitude AS blon, "
            "sb.sog_knots AS bsog, sb.status AS bstat, "
            "ST_Distance(sa.pos::geography, sb.pos::geography) AS distance_m "
            "FROM s sa JOIN s sb ON sa.mmsi < sb.mmsi "
            "WHERE abs(sa.latitude - sb.latitude) < %s "
            "AND abs(sa.longitude - sb.longitude) < %s "
            "AND ST_DWithin(sa.pos, sb.pos, %s) "
            "ORDER BY distance_m ASC LIMIT 50",
            (margin_deg, margin_deg, radius_m / 111_000),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    def vessel(r: dict, side: str) -> dict:
        return {
            "mmsi": r[f"{side}mmsi"],
            "ship_name": r[f"{side}ship"],
            "latitude": r[f"{side}lat"],
            "longitude": r[f"{side}lon"],
            "sog_knots": r[f"{side}sog"],
            "status": r[f"{side}stat"],
        }

    return [
        {"a": vessel(r, "a"), "b": vessel(r, "b"), "distance_m": r["distance_m"]} for r in rows
    ]