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
    anchored: int
    moored: int
    events_window: int
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


class SpeedBucket(BaseModel):
    bucket_min: float
    bucket_max: float | None
    count: int


class SpeedDistribution(BaseModel):
    buckets: list[SpeedBucket]
    avg_sog: float | None
    p50_sog: float | None
    max_sog: float | None
    sample_n: int


class NavBucket(BaseModel):
    code: int | None
    label: str
    count: int


class NavDistribution(BaseModel):
    buckets: list[NavBucket]
    total: int


class FlowBucket(BaseModel):
    sector_deg: float
    label: str
    count: int
    avg_sog: float | None


class FlowDistribution(BaseModel):
    buckets: list[FlowBucket]
    total: int


class ManeuverVessel(BaseModel):
    mmsi: str
    ship_name: str | None
    rate_of_turn: float | None
    rot_deg_min: float | None
    direction: str | None
    sog_knots: float | None
    nav_status: int | None


class ManeuveringResult(BaseModel):
    threshold_deg_min: float
    vessels: list[ManeuverVessel]


class LatencyStats(BaseModel):
    avg_s: float | None
    p50_s: float | None
    p95_s: float | None
    max_s: float | None
    events_negative_latency: int


class DataQualityResult(BaseModel):
    window_hours: int
    events_processed: int
    parquet_files: int
    latency: LatencyStats
    missing_position: int
    missing_sog: int
    missing_cog: int
    invalid_coordinates: int