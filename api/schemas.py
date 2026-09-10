from datetime import datetime

from pydantic import BaseModel


class VesselSummary(BaseModel):
    mmsi: str
    ship_name: str | None
    latitude: float
    longitude: float
    sog_knots: float | None
    cog_degrees: float | None
    true_heading: float | None
    nav_status: int | None
    status: str | None
    last_seen_at: datetime | None


class VesselDetail(VesselSummary):
    rate_of_turn: float | None
    updated_at: datetime | None


class TrackPoint(BaseModel):
    latitude: float
    longitude: float
    event_time: datetime
    sog_knots: float | None


class NearbyResult(BaseModel):
    mmsi: str
    ship_name: str | None
    latitude: float
    longitude: float
    sog_knots: float | None
    status: str | None
    distance_m: float | None


class Health(BaseModel):
    status: str
    database: str