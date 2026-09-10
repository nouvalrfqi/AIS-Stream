from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from api import db
from api.config import load_config
from api.schemas import (
    ConflictPair,
    ConflictVessel,
    Health,
    HotspotCell,
    IntelligenceSummary,
    NearbyResult,
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
            f"avg(sog_knots) AS avg_sog, max(sog_knots) AS max_sog "
            f"FROM vessel_current_state WHERE {VALID_MMSI}",
        )[0]
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
        "avg_sog": agg["avg_sog"],
        "max_sog": agg["max_sog"],
        "fastest": fastest[0] if fastest else None,
        "by_status": by_status,
    }


@app.get("/intelligence/conflicts", response_model=list[ConflictPair])
def intelligence_conflicts(
    radius_m: float = Query(500, gt=0, le=20000),
) -> list[dict]:
    try:
        rows = db.fetch_all(
            config,
            "WITH s AS (SELECT mmsi, ship_name, latitude, longitude, sog_knots, status, position "
            f"FROM vessel_current_state WHERE {VALID_MMSI}) "
            "SELECT sa.mmsi AS ammsi, sa.ship_name AS aship, sa.latitude AS alat, sa.longitude AS alon, "
            "sa.sog_knots AS asog, sa.status AS astat, "
            "sb.mmsi AS bmmsi, sb.ship_name AS bship, sb.latitude AS blat, sb.longitude AS blon, "
            "sb.sog_knots AS bsog, sb.status AS bstat, "
            "ST_Distance(sa.position, sb.position) AS distance_m "
            "FROM s sa JOIN s sb ON sa.mmsi < sb.mmsi "
            "WHERE ST_DWithin(sa.position, sb.position, %s) "
            "ORDER BY distance_m ASC LIMIT 50",
            (radius_m,),
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


@app.get("/intelligence/hotspots", response_model=list[HotspotCell])
def intelligence_hotspots(
    cell_deg: float = Query(0.05, gt=0, le=1),
) -> list[dict]:
    try:
        return db.fetch_all(
            config,
            "SELECT ROUND(latitude / %s) * %s AS latitude, "
            "ROUND(longitude / %s) * %s AS longitude, count(*) AS count "
            f"FROM vessel_current_state WHERE {VALID_MMSI} AND status IS DISTINCT FROM 'STALE' "
            "GROUP BY ROUND(latitude / %s) * %s, ROUND(longitude / %s) * %s "
            "ORDER BY count DESC LIMIT 200",
            (cell_deg, cell_deg, cell_deg, cell_deg, cell_deg, cell_deg, cell_deg, cell_deg),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc