import { useEffect, useState } from 'react'
import {
  getIntelligenceConflicts,
  getIntelligenceDataQuality,
  getIntelligenceFlow,
  getIntelligenceManeuvering,
  getIntelligenceNavigation,
  getIntelligenceSpeed,
  getIntelligenceSummary,
} from '../api.js'
import { STATUS_COLORS, statusColor } from '../status.js'
import { FlowRose, ManeuveringTable, NavStatusDonut, SpeedHistogram } from './IntelCharts.jsx'

const POLL_MS = 15_000

const SECTIONS = {
  summary: getIntelligenceSummary,
  conflicts: getIntelligenceConflicts,
  speed: getIntelligenceSpeed,
  navigation: getIntelligenceNavigation,
  flow: getIntelligenceFlow,
  maneuvering: () => getIntelligenceManeuvering(5),
  dataQuality: () => getIntelligenceDataQuality(24),
}

function blankSections() {
  return Object.fromEntries(Object.keys(SECTIONS).map((k) => [k, null]))
}

function useIntel() {
  const [data, setData] = useState(blankSections)
  const [errors, setErrors] = useState({})
  useEffect(() => {
    let cancelled = false
    const tick = () => {
      Object.entries(SECTIONS).forEach(([key, load]) => {
        load()
          .then((value) => {
            if (cancelled) return
            setData((d) => ({ ...d, [key]: value }))
            setErrors((e) => {
              if (!e[key]) return e
              const next = { ...e }
              delete next[key]
              return next
            })
          })
          .catch(() => {
            if (!cancelled) setErrors((e) => ({ ...e, [key]: true }))
          })
      })
    }
    tick()
    const id = setInterval(tick, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])
  return { ...data, errors }
}

function fmt(v, suffix = '') {
  return v === null || v === undefined ? '—' : `${Number(v).toFixed ? Number(v).toFixed(1) : v}${suffix}`
}

function fmtM(m) {
  return m < 1000 ? `${Math.round(m)} m` : `${(m / 1000).toFixed(2)} km`
}

function PanelState({ loading, error, children }) {
  if (loading) return <p className="panel-state">Memuat…</p>
  if (error) return <p className="panel-state error">Gagal memuat — mencoba lagi…</p>
  return children
}

function StatCard({ label, value, sub, color }) {
  return (
    <div className="stat-card" style={color ? { '--accent': color } : undefined}>
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value}</span>
      {sub && <span className="stat-sub">{sub}</span>}
    </div>
  )
}

function ConflictRow({ pair }) {
  const d = pair.distance_m
  const level = d < 150 ? 'hi' : d < 350 ? 'mid' : 'low'
  const v = (x) => ({ name: x.ship_name || `Kapal ${x.mmsi}`, mmsi: x.mmsi, sog: x.sog_knots, status: x.status })
  const a = v(pair.a)
  const b = v(pair.b)
  return (
    <tr className={`conflict-${level}`}>
      <td className="dist">{fmtM(d)}</td>
      <td>
        <span className="badge" style={{ background: statusColor(a.status) }}>{a.status ?? '—'}</span>{' '}
        {a.name} <span className="muted">({a.mmsi})</span>
      </td>
      <td>{fmt(a.sog, ' kn')}</td>
      <td>
        <span className="badge" style={{ background: statusColor(b.status) }}>{b.status ?? '—'}</span>{' '}
        {b.name} <span className="muted">({b.mmsi})</span>
      </td>
      <td>{fmt(b.sog, ' kn')}</td>
    </tr>
  )
}

function StatusBars({ byStatus }) {
  const max = Math.max(...byStatus.map((s) => s.count), 1)
  return (
    <div className="status-bars">
      {byStatus.map((s) => (
        <div className="status-bar-row" key={s.status}>
          <span className="status-bar-label">{s.status}</span>
          <div className="status-bar-track">
            <div
              className="status-bar-fill"
              style={{
                width: `${(s.count / max) * 100}%`,
                background: STATUS_COLORS[s.status] ?? statusColor(null),
              }}
            />
          </div>
          <span className="status-bar-count">{s.count}</span>
        </div>
      ))}
    </div>
  )
}

function DataQualityCards({ dq }) {
  if (!dq) return null
  return (
    <div className="intel-stats dq-stats">
      <StatCard label="AIS events (24 jam)" value={dq.events_processed} color="#2563eb" />
      <StatCard label="File Parquet" value={dq.parquet_files} sub="di-scan" color="#64748b" />
      <StatCard label="Latensi sumber (p50)" value={fmt(dq.latency?.p50_s, ' dtk')} color="#16a34a" />
      <StatCard label="Latensi sumber (p95)" value={fmt(dq.latency?.p95_s, ' dtk')} color="#f59e0b" />
      <StatCard label="Missing position" value={dq.missing_position ?? '—'} color={dq.missing_position > 0 ? '#ef4444' : '#16a34a'} />
      <StatCard label="Missing SOG / COG" value={((dq.missing_sog ?? 0) + (dq.missing_cog ?? 0)) || '—'} color="#16a34a" />
      <StatCard label="Koordinat invalid" value={dq.invalid_coordinates ?? '—'} color={dq.invalid_coordinates > 0 ? '#ef4444' : '#16a34a'} />
      <StatCard label="Event latensi negatif" value={dq.latency?.events_negative_latency ?? '—'} sub="clock/skew" color="#64748b" />
    </div>
  )
}

export default function Intelligence() {
  const {
    summary,
    conflicts,
    speed,
    navigation,
    flow,
    maneuvering,
    dataQuality,
    errors,
  } = useIntel()

  if (errors.summary && !summary) {
    return <div className="intel-empty">API tidak tersedia — menunggu kembali…</div>
  }
  if (!summary) {
    return <div className="intel-empty">Memuat intelligence…</div>
  }

  return (
    <div className="intel">
      <section className="intel-stats">
        <StatCard label="Total kapal" value={summary.total} />
        <StatCard label="Kapal bergerak" value={summary.moving} />
        <StatCard label="Berlabuh (anchor)" value={summary.anchored} color="#f59e0b" />
        <StatCard label="Tertambat (moored)" value={summary.moored} color="#a855f7" />
        <StatCard label="AIS events (12 jam)" value={summary.events_window} color="#2563eb" />
        <StatCard label="Data segar (<45 mnt)" value={summary.fresh} color="#22c55e" />
        <StatCard label="STALE (tidak ada sinyal)" value={summary.stale} color={statusColor('STALE')} />
        <StatCard label="Rata-rata kecepatan" value={fmt(summary.avg_sog)} sub="knot" />
        <StatCard label="Kapal tercepat" value={summary.fastest?.ship_name || '—'} sub={summary.fastest ? fmt(summary.fastest.sog_knots, ' knot') : undefined} color="#f59e0b" />
      </section>

      <div className="intel-grid">
        <section className="panel">
          <h3>Status navigasi</h3>
          <PanelState loading={navigation === null} error={errors.navigation}>
            <NavStatusDonut data={navigation} />
          </PanelState>
        </section>

        <section className="panel">
          <h3>Distribusi kecepatan</h3>
          <PanelState loading={speed === null} error={errors.speed}>
            <SpeedHistogram data={speed} />
          </PanelState>
        </section>

        <section className="panel">
          <h3>Arah lalu lintas (COG)</h3>
          <PanelState loading={flow === null} error={errors.flow}>
            <FlowRose data={flow} />
          </PanelState>
        </section>

        <section className="panel">
          <h3>Komposisi status</h3>
          <StatusBars byStatus={summary.by_status} />
        </section>
      </div>

      <div className="intel-grid">
        <section className="panel">
          <h3>Maneuvering activity — kapal dengan rotasi signifikan</h3>
          <PanelState loading={maneuvering === null} error={errors.maneuvering}>
            <ManeuveringTable result={maneuvering} />
          </PanelState>
        </section>

        <section className="panel">
          <h3>Potensi konflik — pasangan kapal dalam jarak dekat</h3>
          <PanelState loading={conflicts === null} error={errors.conflicts}>
            {(conflicts ?? []).length === 0 ? (
              <p className="intel-empty">Tidak ada pasangan dalam radius terpantau.</p>
            ) : (
              <div className="conflicts-scroll">
                <table className="conflicts">
                  <thead>
                    <tr>
                      <th>Jarak</th>
                      <th>Kapal A</th>
                      <th>SOG</th>
                      <th>Kapal B</th>
                      <th>SOG</th>
                    </tr>
                  </thead>
                  <tbody>
                    {conflicts.map((p) => (
                      <ConflictRow key={`${p.a.mmsi}-${p.b.mmsi}`} pair={p} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </PanelState>
        </section>
      </div>

      <section className="panel panel-full">
        <h3>Kesehatan data &amp; sumber (window 24 jam)</h3>
        <PanelState loading={dataQuality === null} error={errors.dataQuality}>
          <DataQualityCards dq={dataQuality} />
        </PanelState>
      </section>
    </div>
  )
}