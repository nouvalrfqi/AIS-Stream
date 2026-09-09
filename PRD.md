# PRD — Maritime Real-Time Data Platform (MVP)

> **Status:** Terkunci untuk implementasi MVP
> **Versi:** 1.0
> **Tanggal:** 2026-09-09
> **Sumber landasan:** `maritime_real_time_data_platform_product_knowledge.md` + hasil source profiling (`raw_sample.jsonl`, 800 events / ~33 menit)

---

## 1. Tujuan

Membangun platform **end-to-end maritime real-time data platform** berbasis live AIS (AISStream WebSocket) yang:

1. Menerima live AIS events dari AISStream.
2. Mengingest & mendistribusikan events secara andal melalui **Apache Kafka**.
3. Mempersistensikan raw historical data ke **AWS S3 sebagai Parquet**.
4. Memelihara **near-real-time vessel state** di **PostgreSQL/PostGIS**.
5. Mengekspos data current + historical track melalui **backend API**.
6. Menampilkan vessel + track di **peta (frontend map)**.

### Prinsip inti (dari product knowledge)
> **Build a reliable event-driven data platform first. Build maritime intelligence on top of it second.**

- Jangan over-engineer (no K8s, Flink, Spark Streaming, Schema Registry, Redis, Elasticsearch untuk MVP).
- Measured-first: keputusan kapasitas berdasar data profiling, bukan asumsi.
- Raw data dipertahankan (preserve source fields).
- Pisahkan `event_time` vs `ingested_at`.
- Perlakuan failure sebagai bagian normal dari arsitektur (retry, backoff, DLQ, replay).

---

## 2. Keputusan Produk (Terkunci)

Ini hasil kesepakatan sebelum pengembangan, jangan diubah tanpa persetujuan:

| # | Aspek | Keputusan | Alasan / catatan |
|---|---|---|---|
| P1 | **Area geografis MVP** | **Selat Malaka** (bukan seluruh Indonesia dulu) | Volume terukur & manageable; arsitektur sudah siap naik skala ke seluruh Indonesia tanpa ubah kode inti. Arsitektur didesain skala-nasional sejak awal. |
| P2 | **Track UX** | **Varian A — time window** | Klik kapal → tampilkan track **12 jam terakhir** (polyline dari mana → ke mana saat ini). Tidak ada history panjang di UI. |
| P3 | **Track storage** | **PostGIS `vessel_track`** | Query online harus cepat (S3/Parquet lambat untuk real-time). History pendek, DB tetap kecil. |
| P4 | **Retention track window** | **12 jam** di UI | Data 12 jam terakhir selalu lengkap. |
| P5 | **Retention `vessel_track` (DB)** | **24 jam** | Margin di atas window 12 jam agar track selalu utuh; baris >24 jam dihapus scheduled job. |
| P6 | **Retention S3/Parquet** | **7 hari** (default, konfigurasi) | Aman di bawah 5GB free tier; revisi setelah ukur ukuran Parquet asli. |
| P7 | **Storage historical** | **AWS S3 asli + S3 Lifecycle** | Pakai free tier; Lifecycle auto-expire >7 hari agar tetap ≤5GB. |
| P8 | **State storage** | **PostGIS `vessel_current_state`** | 1 baris per MMSI, posisi terakhir. |
| P9 | **Kafka partition** | **`ais.raw` = 8 partisi** | Moderasi: cukup utk volume Selat Malaka & headroom; key = MMSI (ordering per vessel). |
| P10 | **MVP MessageType** | **`PositionReport`** | Cukup utk live tracking + track. (StaticData dgn MMSI+ShipName sudah ada di `MetaData`.) |
| P11 | **Stack backend** | **FastAPI (Python)** | Python-native, cocok data engineering, async, dokumentasi OpenAPI auto. |
| P12 | **Stack frontend** | **React + MapLibre GL JS** | Open-source, gratis, render kapal live + polyline track. |
| P13 | **Orkestrasi batch** | **Airflow (belum utk MVP live path)** | Hanya utk cleanup/reporting batch bila perlu. Live path = WebSocket → Kafka. |
| P14 | **Bounding box** | Koordinat final Selat Malaka (lihat §6) | Di `.env`. |

---

## 3. Arsitektur

```
AISStream WebSocket
        │
        ▼
[Python Ingestion Service]
        │  validate, normalize, add ingested_at
        ▼
      Apache Kafka  (topic: ais.raw, 8 partitions, key=MMSI)
        │
        ├───────────────────► [Raw Sink Consumer] ──► S3 / Parquet (retention 7 hari)
        │                           buffer batch → tulis Parquet → upload S3
        │
        └───────────────────► [Stream Processor Consumer] ──► PostgreSQL / PostGIS
                                           │
                            ┌───────────────┼────────────────┐
                            ▼               ▼                ▼
                 vessel_current_state  vessel_track     (future analytics)
                   (1 baris/kapal,       (history 24 jam,)
                    posisi terakhir)     scheduled cleanup
                            │
                            ▼
                     [Backend API — FastAPI]
                            │
                            ▼
                     [Frontend — React + MapLibre]
```

### Aliran data lifecycle

```
AISStream → WebSocket → Python → Kafka
                                    ├── S3/Parquet (historical lake)
                                    └── PostGIS (current state + track pendek)
                                          └── API ──► Map UI
```

---

## 4. Model Data

### 4.1 Canonical event (internal, basis schema)

Dihasilkan Ingestion dari `PositionReport` + `MetaData`.

| Field | Tipe | Sumber | Catatan |
|---|---|---|---|
| `event_id` | string | generated | hash/dev. unik |
| `schema_version` | string | const | `"1.0"` — utk forward compatibility migrasi schema |
| `message_type` | string | `MessageType` | `"PositionReport"` |
| `mmsi` | string | `MetaData.MMSI` | di-string-kan utk konsistensi utk partition key |
| `event_time` | string ISO8601 | `MetaData.time_utc` | waktu sumber (UTC) |
| `ingested_at` | float epoch | `_received_at` (service) | di-tambah saat terima |
| `latitude` | float | `Message.PositionReport.Latitude` | **presisi tinggi** (bukan `MetaData`) |
| `longitude` | float | `Message.PositionReport.Longitude` | **presisi tinggi** |
| `sog_knots` | float | `Message.PositionReport.Sog` | |
| `cog_degrees` | float | `Message.PositionReport.Cog` | |
| `heading_degrees` | float\|null | `Message.PositionReport.TrueHeading` | `null` jika sentinel 511 |
| `rate_of_turn` | float\|null | `Message.PositionReport.RateOfTurn` | `null` jika sentinel -128 |
| `nav_status` | int | `Message.PositionReport.NavigationalStatus` | |
| `position_accuracy` | bool | `PositionAccuracy` | |
| `raim` | bool | `Raim` | |
| `ship_name` | string\|null | `MetaData.ShipName` | **harus `.strip()`** (88% berpadding) |
| `source` | string | const | `"aisstream"` |
| `raw` | string | event asli | dipertahankan (preserve raw) — utk debug/replay |

> **Penting**: Simpan juga `raw` (JSON asli) untuk memenuhi prinsip "preserve raw data". Field `Message` turunan (MessageID, RepeatIndicator, dll.) tersimpan di `raw`.

### 4.2 tbl `vessel_current_state` (PostGIS)

```
mmsi            TEXT PRIMARY KEY
ship_name       TEXT
latitude        FLOAT
longitude       FLOAT
sog_knots       FLOAT
cog_degrees     FLOAT
heading_degrees FLOAT NULL
rate_of_turn    FLOAT NULL
nav_status      INT
position        GEOGRAPHY(Point, 4326)   -- PostGIS: ST_SetSRID(ST_MakePoint(lon,lat),4326)
status          TEXT                     -- UNDERWAY/ANCHORED/MOORED/STOPPED/STALE/UNKNOWN
last_seen_at    TIMESTAMPTZ              -- = event_time
updated_at      TIMESTAMPTZ DEFAULT now()
```

- Satu baris per `mmsi`; setiap event baru → **UPSERT** (update posisi terakhir).

### 4.3 tbl `vessel_track` (PostGIS)

```
id            BIGSERIAL PRIMARY KEY
mmsi          TEXT
event_id      TEXT                     -- utk idempotent replay/dedup
latitude      FLOAT
longitude     FLOAT
sog_knots     FLOAT
event_time    TIMESTAMPTZ
position      GEOGRAPHY(Point, 4326)
partition_hour TIMESTAMP               -- utk cleanup batch

UNIQUE (mmsi, event_id)                -- mencegah duplikat saat reprocess/replay
```

- **Insert** satu baris per event (bukan UPSERT) — ini history pendek.
- **Idempotent replay**: unique constraint `(mmsi, event_id)` → `INSERT ... ON CONFLICT DO NOTHING` agar reprocess aman tanpa track ganda.
- **Retention**: delete baris dengan `event_time < now() - 24 jam` (scheduled job).
- Index: `(mmsi, event_time)` untuk query track.

### 4.4 S3 Layout (raw lake)

```
s3://{BUCKET}/
    raw/
        dt=YYYY-MM-DD/
            HH=HH/
                {run_id}.parquet
        dt=YYYY-MM-DD/
```

- Partitioning berdasar **event_time** (UTC).
- Hindari **small-file problem**: buffer dulu (lihat §7 Parquet strategy).

---

## 5. Schema asli AISStream (hasil profiling — referensi)

Dari `raw_sample.jsonl` (799 PositionReport):

```
{
  "MessageType": "PositionReport",
  "MetaData": { MMSI, MMSI_String, ShipName, latitude, longitude, time_utc },
  "Message": {
    "PositionReport": {
      MessageID, RepeatIndicator, UserID, Valid, NavigationalStatus,
      RateOfTurn, Sog, PositionAccuracy, Longitude, Latitude, Cog,
      TrueHeading, Timestamp, SpecialManoeuvreIndicator, Spare,
      Raim, CommunicationState
    }
  }
}
```

### Temuan profiling (input penting utk development):

| Temuan | Nilai | Implikasi |
|---|---|---|
| `TrueHeading = 511` | 140/799 (17.5%) | artinya "n/a" → harus jadi `null`, bukan error |
| `RateOfTurn = -128` | 136/799 (17%) | artinya "n/a" → jadi `null` |
| `Sog = 0` | 258/799 (32%) | kapal diam — valid, bukan duplikat |
| `ShipName` berpadding | 706/799 (88%) | wajib `.strip()` |
| Latency event→ingest | avg 1.31s, p95 1.52s, max 5.1s | target observability: p95 < 2s |
| Duplikat exact (MMSI+time_utc) | 0 | belum ada duplikat; tetap pakai `event_id` unik utk keamanan |
| `Valid=true` | 799/799 (100%) | siapkan handling `Valid=false` |
| NavStatus dist | 0:413, 1:205, 8:84, 5:72, 15:17, 3:6, 9:2 | 15/9 utk ditangani (undefined/reserved) |
| Volume | ~24 events/min, ~152 kapal unik/33mnt | utk skala Selat Malaka |

---

## 6. Konfigurasi (`.env`)

Semua nilai konfigurasi eksternal, tidak di-hard-code.

```
# AISStream
AISSTREAM_API_KEY=<server-side, never expose>
AISSTREAM_BOUNDING_BOXES=[[[ <minLat,minLon>, <maxLat,maxLon> ]]]
AISSTREAM_FILTER_MESSAGE_TYPES=["PositionReport"]

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092          # dari host; dari container gunakan kafka:29092
KAFKA_TOPIC_RAW=ais.raw
KAFKA_TOPIC_DLQ=ais.dlq                        # Dead Letter Queue (mandatory)
KAFKA_PARTITIONS=8
KAFKA_REPLICATION_FACTOR=1   # dev single node
KAFKA_RETENTION_MS=...       # replay window (mis. 24h)

# Consumer config (backpressure)
KAFKA_MAX_POLL_RECORDS=500                     # maks event per poll batch
KAFKA_MAX_POLL_INTERVAL_MS=300000              # 5 mnt, batas waktu proses per batch

# S3
S3_BUCKET=maritime-data
S3_REGION=...
S3_PREFIX=raw
S3_ENDPOINT=...              # kosong utk AWS; diisi utk MinIO/localstack bila dipakai dev
S3_ACCESS_KEY=...
S3_SECRET_KEY=...

# Parquet buffering
PARQUET_MAX_ROWS=1000
PARQUET_MAX_SECONDS=300      # 5 menit

# Postgres/PostGIS
POSTGRES_URL=postgresql://user:pass@localhost:5432/maritime

# Retention
TRACK_RETENTION_HOURS=24
S3_RETENTION_DAYS=7
STATE_STALE_MINUTES=45       # mark STALE jika tak ada update
S3_LIFECYCLE_ON=false        # toggle; utk AWS lifecyle rule

# Backend
API_HOST=0.0.0.0
API_PORT=8000
```

### Bounding box Selat Malaka (area MVP) — **PERLU VALIDASI AKHIR**
Profiling saat ini mencakup area sekitar Singapura (~lat 1.09–1.30, lon 103.69–104.01). "Selat Malaka" utuh lebih lebar ke barat. **Sebelum production, tentukan koordinat final** yang mewakili Selat Malaka (contoh rentang yang bisa dipertimbangkan, perlu diverifikasi):
```
[[[1.0, 98.5], [6.0, 104.0]]]
```
> Koordinat di atas **contoh sementara** — validasi & sesuaikan saat implementasi antar bounding box dengan tier AISStream. Pastikan tidak terlalu luas agar volume tetap terkelola.

---

## 7. Komponen & Tanggung Jawab

### 7.1 Ingestion Service (Python)
- Connect WebSocket AISStream (`wss://stream.aisstream.io/v0/stream`).
- Kirim subscription dari `.env` (APIKey, BoundingBoxes, FilterMessageTypes).
- Terima frame → decode UTF-8 JSON.
- **Validate** (transport + domain, §8).
- **Normalize** ke canonical event (§4.1, handling sentinel, strip ShipName).
- Tambah `event_id` + `ingested_at`.
- Publish ke Kafka `ais.raw`, **key = mmsi**.
- **Hidupkan `_received_at`** (ingested_at) saat frame diterima — ukur latency.
- Emit operational logs (§10).
- **Reconnect**: exponential backoff 1s→2s→4s→8s…+jitter, max ~60s; resubscribe otomatis.

**Lifecycle states**: DISCONNECTED → CONNECTING → SUBSCRIBING → CONNECTED/CONSUMING → (on err) backoff → reconnect.

### 7.2 Raw Sink Consumer (Python)
- Consume Kafka `ais.raw` (consumer group `raw-sink`).
- **Buffer** events dalam memory (append canonical + raw).
- **Flush** ke Parquet saat memenuhi `PARQUET_MAX_ROWS` (1000) **atau** `PARQUET_MAX_SECONDS` (300s).
- **Backpressure**: `max.poll.records=500`, `max.poll.interval.ms=300000`. Jika S3 unreachable berturut-turut (circuit breaker: 5x gagal), **pause consumer** sementara (exponential backoff), lalu resume.
- Tulis Parquet (kolom canonical) + simpan ke path S3 `raw/dt=.../HH=.../`.
- **Retry** upload (maks 3x per file); jika tetap gagal → log error + kirim metadata ke `ais.dlq`; hanya commit Kafka offset **setelah** file sukses ke S3 (offset strategy, §11).
- Nama file idempotent (hindari duplikat saat retry).

### 7.3 Stream Processor Consumer (Python)
- Consume Kafka `ais.raw` (consumer group `realtime-state`).
- **Upsert** `vessel_current_state` (per mmsi).
- **Insert** `vessel_track` (per event).
- Derive `status` (§9).
- **Commit offset setelah** commit DB sukses (CGL/at-least-once + idempotent UPSERT → aman).

### 7.4 Scheduler / Cleanup Job (batch)
- **`vessel_track` cleanup**: hapus baris `event_time < now() - TRACK_RETENTION_HOURS`.
- **`vessel_current_state` cleanup**: mark `STALE` jika `last_seen_at < now() - STATE_STALE_MINUTES`; opsional hapus kapal STALE yang keluar area / tak ada update lama.
- **S3 Lifecycle**: atur AWS rule expire object > S3_RETENTION_DAYS (bila S3_LIFECYCLE_ON).
- Untuk MVP cukup script terjadwal (cron/simple loop); Airflow bisa ditambahkan nanti utk reporting.

### 7.5 Backend API (FastAPI)
Endpoints:

| Method | Path | Fungsi | Sumber |
|---|---|---|---|
| GET | `/vessels` | Semua vessel current + posisi | `vessel_current_state` |
| GET | `/vessels/{mmsi}` | Detail vessel + status | `vessel_current_state` |
| GET | `/vessels/{mmsi}/track` | Track 12 jam terakhir (polyline) | `vessel_track` (filter event_time > now()-12h) |
| GET | `/vessels/nearby?lat&lon&radius` | Kapal dekat titik (PostGIS) | `vessel_current_state` |
| GET | `/health` | Health/status | — |

- Frontend **tidak** akses Kafka/S3/PostGIS langsung — selalu lewat API.
- Return JSON; `track` = list titik `[{lat, lon, event_time, sog}]` terurut.

### 7.6 Frontend (React + MapLibre)
- Peta live: marker semua vessel (warna bisa bedakan status).
- Klik kapal → detail (nama, mmsi, sog, heading, nav_status, last_seen) + **polyline track 12 jam**.
- Refresh live marker (polling `/vessels` berkala / interval).
- Tidak menampilkan history panjang — hanya track window.

---

## 8. Validasi Data

### 8.1 Transport/schema
- Frame valid, UTF-8, JSON valid.
- `MessageType` diketahui & ditangani (`PositionReport`).
- Field wajib ada (`MetaData` + `Message.PositionReport`).

### 8.2 Domain
- `latitude ∈ [-90,90]`, `longitude ∈ [-180,180]`
- `sog >= 0`
- `mmsi` valid (9 digit / pola, minimal non-negatif)
- `event_time` parseable, timezone UTC
- **Sentinel handling**: `TrueHeading=511`→null, `RateOfTurn=-128`→null (bukan error)
- `NavStatus` dalam set dikenal; 15/9 ditandai "undefined/reserved" tapi tetap diproses (jangan gagalkan)

### 8.3 Invalid handling
- Event invalid **tidak boleh crash** service.
- Valid → publish `ais.raw`.
- Invalid → log + **publish ke `ais.dlq`** (Dead Letter Queue, **mandatory**). DLQ menyimpan event gagal agar bisa di-inspect dan di-replay nanti.
- Consumer yang gagal memproses event setelah **3x retry** → kirim ke `ais.dlq`, lanjut proses event berikutnya (hindari poison pill blocking consumer).
- Jangan kill consumer karena 1 event buruk.

---

## 9. Status Derivation (`vessel_current_state.status`)

Gabungan (jangan infer dari missing saja):

| Prioritas | Kondisi | Status |
|---|---|---|
| 1 | `NavStatus == 1` | `ANCHORED` |
| 2 | `NavStatus == 5` | `MOORED` |
| 3 | `NavStatus == 0` dan `sog == 0` | `STOPPED` |
| 4 | `NavStatus == 0` atau `8` dan `sog > 0` | `UNDERWAY` |
| 5 | `last_seen_at < now() - STATE_STALE_MINUTES` | `STALE` |
| 6 | lainnya / `NavStatus==15/9` | `UNKNOWN` |

Order evaluasi: 1→2→3→4 lalu di akhir cek STALE (jika last_seen terlalu lama → override jadi STALE). Configurable.

---

## 10. Observability (struktur log + metrik)

### Ingestion
- connection status, reconnect count, received events, invalid events
- source latency (avg/p95), last received event time

### Kafka
- produced messages, producer errors, consumer lag, consumer errors

### Raw Sink (S3)
- records buffered, files written, write failures, last successful write time

### Stream Processor (PostGIS)
- events processed, updates succeeded, processing errors, last state update

MVP: **structured logs (JSON) + counters** cukup. Tidak perlu stack observasi kompleks.

---

## 11. Failure & Recovery (offset strategy penting)

- **Ingestion / WS putus** → backoff + reconnect + resubscribe.
- **Kafka producer fail** → retry; jangan lost event.
- **Raw sink S3 fail** → buffer + retry; **COMMIT offset hanya setelah file sukses upload** (at-least-once). Kalau gagal terus, offset tidak maju → replay dari Kafka setelah recovery. Nama file idempotent.
- **PostGIS fail / slow** → jangan commit offset sebelum DB commit sukses. Karena UPSERT idempotent + event granularity track (insert), **reprocess aman** (dup track → dedupe by event_id utk track bila perlu).
- **Dedup**: `event_id` unik per canonical; `vessel_track` tambahkan **unique constraint `(mmsi, event_id)`** (jika event_id disimpan) agar reprocess tidak bikin track ganda. (Simpler: cukup andalkan idempotent + replay dari boundary window.)
- **Duplicate events** (bila muncul): gunakan `event_id` file/identity utk skip. Konservatif (belum ada duplikat teramati).

**Offset commit recommendation**:
- Raw sink: `enable.auto.commit=false`; commit per batch setelah Parquet+upload S3 sukses.
- Stream processor: `enable.auto.commit=false`; commit per batch setelah DB txn sukses.

---

## 12. Security

- AISStream API key **hanya server-side** (`.env`), never di frontend.
- **Jangan commit** `.env`, key, `raw_sample.jsonl` (sudah di `.gitignore`: `.env`, `venv/`, `profiling/data/`).
- **`test-ais.py` berisi key hard-coded — HARUS DIHAPUS/arsip** sebelum git init (sudah digantikan `profiler.py`). Api key di-review & direvoke bila perlu.
- AWS kredensial via env / IAM least-privilege.
- Tidak ada credentials di source code.

---

## 13. Non-Goals (MVP — dikecualikan)

- Global/entire-Indonesia AIS (untuk MVP; desain siap naik skala).
- Voyage segmentation (Varian B) — enhancement nanti.
- Port/anchorage intelligence otomatis (point-in-polygon) — fase 6 enhancement.
- ML / anomaly detection / ETA prediction.
- Schema Registry, Flink, Spark Streaming, K8s.
- Frontend otentikasi/otorisasi production-grade.
- Advanced vessels classification / comprehensive port-call intelligence.

---

## 14. Rencana Implementasi (Fase)

| Fase | Lingkup | Deliverable |
|---|---|---|
| **0** | Source profiling (SELESAI) | `raw_sample.jsonl` + temuan §5 |
| **1** | Infra lokal: Kafka + Postgres/PostGIS (Docker) | docker-compose up; topik `ais.raw` (8 partisi), schema DB |
| **2** | Ingestion service | WS → validate → normalize → produce Kafka (Selat Malaka) |
| **3** | Raw sink | Kafka → buffer → Parquet → S3 (retry, offset safety) |
| **4** | Stream processor | Kafka → PostGIS (current_state upsert + track insert + status) |
| **5** | Cleanup jobs | track retention (24h), state STALE, S3 lifecycle |
| **6** | Backend API (FastAPI) | endpoints §7.5 |
| **7** | Frontend (React + MapLibre) | live map + detail + track 12 jam |
| **8** | Observability & hardening | structured logs, metrik, test failure scenarios |
| **9** | (Future) Skala ke seluruh Indonesia | re-measure volume, tambah bounding box / multi-connection |

### DoD per fase
Sesuai product knowledge section 49 — tiap fase selesai bila ada bukti (log, output file, query hasil).

---

## 15. Rencana Pengujian (verifikasi development)

- **Unit**: parser/normalization (sentinel, strip, validasi), status derivation.
- **Integration**: ingestion → Kafka (event muncul di topik); sink → Parquet → S3; processor → PostGIS.
- **Failure**: putus WS → reconnect; S3 gagal → retry & offset tidak maju; PostGIS down → konsum berhenti tanpa lost.
- **Latency**: ukur p95 < 2s sebagai baseline.
- **Retention**: verifikasi baris/track terhapus, S3 lifecycle expire.
- **Track API**: `/vessels/{mmsi}/track` balikin 12 jam polyline benar, terurut event_time.

---

## 16. Deployment (awal)

- **Lokal/Docker** untuk dev: Kafka (confluent/kafka atau bitnami), Postgres+PostGIS, semua service.
- S3: AWS asli (atau MinIO/localstack utk dev bila tak punya kredensial). Arsitektur S3-API-compatible → mudah balik.
- TIDAK pakai K8s untuk MVP.
- Semua service config via `.env` (bukan hard-code).

---

## 17. Open Items / Assumption (harus divalidasi saat dev)

1. **Bounding box final Selat Malaka** — koordinat pasti + coverage (belum terkunci).
2. **Ukuran Parquet asli per hari** — tentukan apakah retention S3 7 hari tetap aman ≤5GB; jika terlalu besar, turunkan hari.
3. **Tier AISStream** — apakah 1 koneksi cukup utk bounding box Selat Malaka / batas maks box.
4. **Duplikat pada volume besar** — belum teramati; pastikan `event_id` dedup aman.
5. **`NavStatus` mapping** utk nilai 15 & 9 (tampilkan "undefined/reserved").
6. **Kredensial AWS utk dev** — siapkan akses atau fallback MinIO utk pengujian lokal.

---

## 18. Glossary (ringkas)

- **AIS**: Automatic Identification System.
- **AISStream**: penyedia live AIS via WebSocket.
- **MMSI**: Maritime Mobile Service Identity (ID unik kapal).
- **PositionReport**: message jenis AIS berisi posisi/gerakan kapal.
- **Parquet**: format file kolom terkompresi utk analitik.
- **Raw sink**: consumer yang mempersistensikan raw events ke S3.
- **Stream processor**: consumer yang update PostGIS.
- **Canonical event**: format internal standar hasil normalisasi.
- **Sentinel**: nilai khusus penanda "n/a" (mis. 511/‑128).
