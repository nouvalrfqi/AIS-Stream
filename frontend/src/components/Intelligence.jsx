import { useEffect, useState } from 'react'
import {
  getIntelligenceConflicts,
  getIntelligenceHotspots,
  getIntelligenceSummary,
} from '../api.js'
import { STATUS_COLORS, statusColor } from '../status.js'

const POLL_MS = 15_000

function fmt(v, suffix = '') {
  return v === null || v === undefined ? '—' : `${Number(v).toFixed ? Number(v).toFixed(1) : v}${suffix}`
}

function fmtM(m) {
  return m < 1000 ? `${Math.round(m)} m` : `${(m / 1000).toFixed(2)} km`
}

function useIntel() {
  const [data, setData] = useState({ summary: null, conflicts: [], hotspots: [], error: false })
  useEffect(() => {
    let cancelled = false
    const tick = () => {
      Promise.all([getIntelligenceSummary(), getIntelligenceConflicts(), getIntelligenceHotspots()])
        .then(([summary, conflicts, hotspots]) => {
          if (cancelled) return
          setData({ summary, conflicts, hotspots, error: false })
        })
        .catch(() => {
          if (!cancelled) setData((d) => ({ ...d, error: true }))
        })
    }
    tick()
    const id = setInterval(tick, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])
  return data
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

export default function Intelligence() {
  const { summary, conflicts, hotspots, error } = useIntel()

  if (error && !summary) {
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
        <StatCard label="Diam / berlabuh" value={summary.idle} />
        <StatCard label="Data segar (<45 mnt)" value={summary.fresh} color="#22c55e" />
        <StatCard label="STALE (tidak ada sinyal)" value={summary.stale} color={statusColor('STALE')} />
        <StatCard label="Rata-rata kecepatan" value={fmt(summary.avg_sog)} sub="knot" />
        <StatCard label="Kapal tercepat" value={summary.fastest?.ship_name || '—'} sub={summary.fastest ? fmt(summary.fastest.sog_knots, ' knot') : undefined} color="#f59e0b" />
      </section>

      <div className="intel-grid">
        <section className="panel">
          <h3>Komposisi status</h3>
          <StatusBars byStatus={summary.by_status} />
        </section>

        <section className="panel">
          <h3>Potensi konflik — pasangan kapal dalam jarak dekat</h3>
          {conflicts.length === 0 ? (
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
        </section>

        <section className="panel">
          <h3>Zona terpadat (sel ± 0,05° ≈ 5 km)</h3>
          {hotspots.length === 0 ? (
            <p className="intel-empty">Belum ada zona terukur.</p>
          ) : (
            <table className="conflicts">
              <thead>
                <tr>
                  <th>Zona (lat, lon)</th>
                  <th>Kapal</th>
                </tr>
              </thead>
              <tbody>
                {hotspots.slice(0, 10).map((c) => (
                  <tr key={`${c.latitude}-${c.longitude}`}>
                    <td>{c.latitude.toFixed(2)}, {c.longitude.toFixed(2)}</td>
                    <td>{c.count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>
    </div>
  )
}