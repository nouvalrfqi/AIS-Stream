import { statusColor } from '../status.js'

function fmtTime(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

function fmtRelative(iso) {
  if (!iso) return '—'
  const minutes = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 60000))
  if (minutes < 1) return 'baru saja'
  if (minutes < 60) return `${minutes} menit lalu`
  return `${Math.floor(minutes / 60)} jam lalu`
}

function fmtNum(v, suffix = '') {
  return v === null || v === undefined ? '—' : `${v}${suffix}`
}

export default function DetailPanel({ vessel, track, onClose }) {
  return (
    <aside className="detail-panel">
      <header>
        <h2>{vessel.ship_name || `Kapal ${vessel.mmsi}`}</h2>
        <button type="button" className="close" onClick={onClose} aria-label="Tutup">
          ×
        </button>
      </header>
      <dl>
        <dt>MMSI</dt>
        <dd>{vessel.mmsi}</dd>
        <dt>Status</dt>
        <dd>
          <span className="badge" style={{ background: statusColor(vessel.status) }}>
            {vessel.status ?? 'UNKNOWN'}
          </span>
        </dd>
        <dt>Posisi</dt>
        <dd>
          {vessel.latitude.toFixed(5)}, {vessel.longitude.toFixed(5)}
        </dd>
        <dt>SOG</dt>
        <dd>{fmtNum(vessel.sog_knots, ' knot')}</dd>
        <dt>COG</dt>
        <dd>{fmtNum(vessel.cog_degrees, '°')}</dd>
        <dt>Heading</dt>
        <dd>{fmtNum(vessel.true_heading, '°')}</dd>
        <dt>Nav status</dt>
        <dd>{fmtNum(vessel.nav_status)}</dd>
        <dt>Last seen</dt>
        <dd>
          {fmtTime(vessel.last_seen_at)} <span className="muted">({fmtRelative(vessel.last_seen_at)})</span>
        </dd>
        <dt>Track points</dt>
        <dd>{track ? track.length : 0}</dd>
      </dl>
      <footer>{track && track.length > 0 ? 'Track 12 jam ditampilkan di peta' : 'Tidak ada track untuk ditampilkan'}</footer>
    </aside>
  )
}