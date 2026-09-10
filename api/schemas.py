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


class StatusCount(BaseModel):
    status: str
    count: int


class VesselLeader(BaseModel):
    mmsi: str
    ship_name: str | None
    sog_knots: float | None


class IntelligenceSummary(BaseModel):
    total: int
    fresh: int
    stale: int
    moving: int
    idle: int
    avg_sog: float | None
    max_sog: float | None
    fastest: VesselLeader | None
    by_status: list[StatusCount]


class ConflictVessel(BaseModel):
    mmsi: str
    ship_name: str | None
    latitude: float
    longitude: float
    sog_knots: float | None
    status: str | None


class ConflictPair(BaseModel):
    a: ConflictVessel
    b: ConflictVessel
    distance_m: float


class HotspotCell(BaseModel):
    latitude: float
    longitude: float
    count: int