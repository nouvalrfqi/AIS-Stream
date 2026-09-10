const ROSE_COLORS = ['#93c5fd', '#60a5fa', '#3b82f6', '#1d4ed8', '#1e3a8a']
const NAV_SHORT = {
  0: 'Underway',
  1: 'At Anchor',
  2: 'Not under command',
  3: 'Restricted',
  4: 'By draught',
  5: 'Moored',
  6: 'Aground',
  7: 'Fishing',
  8: 'Sailing',
  14: 'AIS-SART',
  15: 'Unknown',
}
const NAV_COLORS = ['#2563eb', '#f59e0b', '#a855f7', '#0ea5e9', '#ef4444', '#22c55e', '#64748b']

function fmt(v, suffix = '') {
  return v === null || v === undefined ? '—' : `${Number(v).toFixed ? Number(v).toFixed(1) : v}${suffix}`
}

function navLabel(code) {
  if (code === null || code === undefined) return 'Unknown'
  return NAV_SHORT[code] ?? (code === 15 ? 'Unknown' : 'Reserved')
}

export function SpeedHistogram({ data }) {
  if (!data) return null
  const max = Math.max(...data.buckets.map((b) => b.count), 1)
  return (
    <div className="intel-chart">
      <div className="chart-stats">
        <span>n={data.sample_n}</span>
        <span>avg {fmt(data.avg_sog, ' kn')}</span>
        <span>median {fmt(data.p50_sog, ' kn')}</span>
        <span>maks {fmt(data.max_sog, ' kn')}</span>
      </div>
      <div className="hist">
        {data.buckets.map((b) => (
          <div
            key={b.bucket_min}
            className="hist-bar"
            style={{ height: `${(b.count / max) * 100}%` }}
            title={`${b.bucket_min}–${b.bucket_max ?? '∞'} kn: ${b.count} kapal`}
          >
            <span className="hist-count">{b.count > 0 ? b.count : ''}</span>
          </div>
        ))}
      </div>
      <div className="hist-axis">
        <span>0</span>
        <span>5</span>
        <span>10</span>
        <span>15</span>
        <span>20+ kn</span>
      </div>
    </div>
  )
}

function donutPath(cx, cy, r, start, end) {
  const large = end - start > Math.PI ? 1 : 0
  const x1 = cx + r * Math.cos(start)
  const y1 = cy + r * Math.sin(start)
  const x2 = cx + r * Math.cos(end)
  const y2 = cy + r * Math.sin(end)
  return `M ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2}`
}

export function NavStatusDonut({ data }) {
  if (!data || data.buckets.length === 0) return <p className="intel-empty">Belum ada data nav status.</p>
  const total = data.total || data.buckets.reduce((s, b) => s + b.count, 0)
  const cx = 70
  const cy = 70
  const r = 56
  const w = 16
  const slices = data.buckets.reduce(
    (out, b) => {
      const start = (out.acc / total) * 2 * Math.PI - Math.PI / 2
      out.acc += b.count
      const end = (out.acc / total) * 2 * Math.PI - Math.PI / 2
      out.list.push({ b, start, end })
      return out
    },
    { acc: 0, list: [] },
  ).list
  return (
    <div className="donut-wrap">
      <svg className="donut" viewBox="0 0 140 140" width="150" height="150">
        {slices.map(({ b, start, end }, i) => (
          <path
            key={`${b.code}-${b.label}`}
            d={donutPath(cx, cy, r, start, end)}
            fill="none"
            stroke={NAV_COLORS[i % NAV_COLORS.length]}
            strokeWidth={w}
          />
        ))}
        <text x={cx} y={cy - 2} textAnchor="middle" className="donut-total">{total}</text>
        <text x={cx} y={cy + 14} textAnchor="middle" className="donut-cap">kapal</text>
      </svg>
      <div className="donut-legend">
        {slices.map(({ b }, i) => (
          <div className="donut-item" key={`${b.code}-${b.label}`}>
            <i style={{ background: NAV_COLORS[i % NAV_COLORS.length] }} />
            <span>{b.label}</span>
            <b>{b.count}</b>
          </div>
        ))}
      </div>
    </div>
  )
}

function rosePoint(cx, cy, radius, angleDeg) {
  const rad = ((angleDeg - 90) * Math.PI) / 180
  return [cx + radius * Math.cos(rad), cy + radius * Math.sin(rad)]
}

export function FlowRose({ data }) {
  if (!data) return null
  const max = Math.max(...data.buckets.map((b) => b.count), 1)
  const cx = 120
  const cy = 120
  const rMax = 88
  const rInner = 16
  const sectors = []
  for (let i = 0; i < 16; i += 1) {
    const bucket = data.buckets.find((b) => Math.round(b.sector_deg) === Math.round(i * 22.5)) ?? { label: '', count: 0 }
    const radius = rInner + (bucket.count / max) * (rMax - rInner)
    const color = ROSE_COLORS[Math.min(ROSE_COLORS.length - 1, Math.floor((bucket.count / max) * ROSE_COLORS.length))]
    const a0 = i * 22.5 - 11.25
    const a1 = i * 22.5 + 11.25
    const [x0, y0] = rosePoint(cx, cy, radius, a0)
    const [x1, y1] = rosePoint(cx, cy, radius, a1)
    const [xi0, yi0] = rosePoint(cx, cy, rInner, a0)
    const [xi1, yi1] = rosePoint(cx, cy, rInner, a1)
    sectors.push({
      label: bucket.label,
      count: bucket.count,
      color,
      d: `M ${x0} ${y0} A ${radius} ${radius} 0 0 1 ${x1} ${y1} L ${xi1} ${yi1} A ${rInner} ${rInner} 0 0 0 ${xi0} ${yi0} Z`,
    })
  }
  return (
    <div className="rose-wrap">
      <svg className="rose" viewBox="0 0 240 240" width="220" height="220">
        {[0.25, 0.5, 0.75, 1].map((f) => (
          <circle key={f} cx={cx} cy={cy} r={rInner + (rMax - rInner) * f} fill="none" stroke="#e5e7eb" strokeWidth="1" />
        ))}
        {sectors.map((s) => (
          <path key={s.label} d={s.d} fill={s.color} opacity={0.75} title={`${s.label}: ${s.count} kapal`} />
        ))}
        {['N', 'E', 'S', 'W'].map((cardinal, idx) => {
          const angle = idx * 90
          const [x, y] = rosePoint(cx, cy, rMax + 14, angle)
          return (
            <text key={cardinal} x={x} y={y} textAnchor="middle" className="rose-cardinal">
              {cardinal}
            </text>
          )
        })}
      </svg>
      <p className="rose-note">Total {data.total} kapal (20–360° COG valid, non-STALE)</p>
    </div>
  )
}

function navStatusBadge(code) {
  const idx = [0, 1, 5, 3, 8].indexOf(code)
  const color = idx >= 0 ? NAV_COLORS[idx] : '#64748b'
  return <span className="badge" style={{ background: color }}>{navLabel(code)}</span>
}

export function ManeuveringTable({ result }) {
  if (!result) return null
  if (result.vessels.length === 0) {
    return <p className="intel-empty">Tidak ada aktivitas manuver signifikan saat ini.</p>
  }
  return (
    <div className="conflicts-scroll">
      <table className="conflicts">
        <thead>
          <tr>
            <th>Kapal</th>
            <th>Arah putaran</th>
            <th>ROT</th>
            <th>SOG</th>
            <th>Status navigasi</th>
          </tr>
        </thead>
        <tbody>
          {result.vessels.slice(0, 20).map((v) => (
            <tr key={v.mmsi}>
              <td>
                {v.ship_name || `Kapal ${v.mmsi}`} <span className="muted">({v.mmsi})</span>
              </td>
              <td>{v.direction === 'starboard' ? 'Kanannya' : v.direction === 'port' ? 'Kirinya' : '—'}</td>
              <td>
                {v.rot_deg_min !== null && v.rot_deg_min !== undefined
                  ? `${v.rot_deg_min}°/mnt`
                  : <b className="fast">fast</b>}
              </td>
              <td>{fmt(v.sog_knots, ' kn')}</td>
              <td>{navStatusBadge(v.nav_status)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}