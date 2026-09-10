import { useCallback, useEffect, useState } from 'react'
import { getTrack, getVessel, getVessels } from './api.js'
import VesselMap from './components/VesselMap.jsx'
import DetailPanel from './components/DetailPanel.jsx'

const POLL_MS = 10_000

export default function App() {
  const [vessels, setVessels] = useState([])
  const [apiStatus, setApiStatus] = useState('loading')
  const [selected, setSelected] = useState(null)
  const [detail, setDetail] = useState(null)
  const [track, setTrack] = useState(null)
  const [showStale, setShowStale] = useState(true)

  useEffect(() => {
    let cancelled = false
    const tick = () => {
      getVessels()
        .then((data) => {
          if (cancelled) return
          setVessels(data)
          setApiStatus('ok')
        })
        .catch(() => {
          if (!cancelled) setApiStatus('error')
        })
    }
    tick()
    const id = setInterval(tick, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  const handleSelect = useCallback(async (v) => {
    if (!v) {
      setSelected(null)
      setDetail(null)
      setTrack(null)
      return
    }
    setSelected(v)
    try {
      const [d, t] = await Promise.all([getVessel(v.mmsi), getTrack(v.mmsi)])
      setDetail(d)
      setTrack(t)
    } catch {
      setDetail(null)
      setTrack(null)
    }
  }, [])

  return (
    <div className="app">
      <VesselMap
        vessels={vessels}
        showStale={showStale}
        track={track}
        selectedMmsi={selected?.mmsi}
        onSelect={handleSelect}
        onToggleStale={setShowStale}
      />
      {apiStatus === 'error' && (
        <div className="api-banner">API tidak tersedia — menunggu kembali…</div>
      )}
      {selected && <DetailPanel vessel={detail ?? selected} track={track} onClose={() => handleSelect(null)} />}
    </div>
  )
}