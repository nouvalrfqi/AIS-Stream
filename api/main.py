from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from api import db
from api.config import load_config
from api.schemas import Health, NearbyResult, TrackPoint, VesselDetail, VesselSummary

config = load_config()

SELECT_STATE_COLS = (
    "mmsi, ship_name, latitude, longitude, sog_knots, cog_degrees, "
    "true_heading, nav_status, status, last_seen_at"
)

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