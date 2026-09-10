import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { statusColor } from '../status.js'

const STYLE_URL = 'https://tiles.openfreemap.org/styles/liberty'

const DEFAULT_CENTER = [103.8, 1.3]
const DEFAULT_ZOOM = 8

function buildVesselsData({ vessels, showStale, selectedMmsi }) {
  const features = vessels
    .filter((v) => showStale || v.status !== 'STALE')
    .map((v) => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [v.longitude, v.latitude],
      },
      properties: {
        ...v,
        selected: String(v.mmsi) === String(selectedMmsi),
      },
    }))
  return { type: 'FeatureCollection', features }
}

function buildTrackData(track) {
  if (!track || track.length === 0) {
    return { type: 'FeatureCollection', features: [] }
  }
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: track.map((p) => [p.longitude, p.latitude]),
        },
        properties: {},
      },
    ],
  }
}

function loadImageData(url, size = 96) {
  return new Promise((resolve, reject) => {
    const img = new Image()
    img.onload = () => {
      const canvas = document.createElement('canvas')
      canvas.width = size
      canvas.height = size
      const ctx = canvas.getContext('2d')
      ctx.drawImage(img, 0, 0, size, size)
      resolve(ctx.getImageData(0, 0, size, size))
    }
    img.onerror = () => reject(new Error(`Gagal memuat gambar: ${url}`))
    img.src = url
  })
}

export default function VesselMap({
  vessels,
  showStale,
  track,
  selectedMmsi,
  onSelect,
  onToggleStale,
}) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)
  const vesselsSourceRef = useRef(null)
  const trackSourceRef = useRef(null)
  const onSelectRef = useRef(onSelect)
  const dataRef = useRef({ vessels, showStale, selectedMmsi, track })

  useEffect(() => {
    onSelectRef.current = onSelect
  }, [onSelect])

  useEffect(() => {
    dataRef.current = { vessels, showStale, selectedMmsi, track }
  })

  useEffect(() => {
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: STYLE_URL,
      center: DEFAULT_CENTER,
      zoom: DEFAULT_ZOOM,
    })
    mapRef.current = map

    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-right')

    map.on('load', async () => {
      const vesselImageData = await loadImageData('/vessel.svg')
      if (!map.hasImage('vessel')) {
        map.addImage('vessel', vesselImageData, { sdf: true })
      }

      map.addSource('vessels', { type: 'geojson', data: buildVesselsData(dataRef.current) })
      map.addLayer({
        id: 'vessel-markers',
        type: 'symbol',
        source: 'vessels',
        layout: {
          'icon-image': 'vessel',
          'icon-size': [
            'case',
            ['==', ['get', 'selected'], true],
            0.4,
            ['==', ['get', 'status'], 'STALE'],
            0.2,
            0.3,
          ],
          'icon-rotation-alignment': 'map',
          'icon-rotate': ['coalesce', ['get', 'cog_degrees'], ['get', 'true_heading'], 0],
          'icon-allow-overlap': true,
          'icon-ignore-placement': true,
        },
        paint: {
          'icon-color': [
            'match',
            ['get', 'status'],
            'UNDERWAY',
            statusColor('UNDERWAY'),
            'ANCHORED',
            statusColor('ANCHORED'),
            'MOORED',
            statusColor('MOORED'),
            'STOPPED',
            statusColor('STOPPED'),
            'STALE',
            statusColor('STALE'),
            statusColor('UNKNOWN'),
          ],
          'icon-opacity': ['case', ['==', ['get', 'status'], 'STALE'], 0.5, 0.95],
          'icon-halo-color': '#ffffff',
          'icon-halo-width': ['case', ['==', ['get', 'selected'], true], 0.5, 0],
        },
      })

      map.addSource('track', { type: 'geojson', data: buildTrackData(null) })
      map.addLayer({
        id: 'track-line',
        type: 'line',
        source: 'track',
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#2563eb',
          'line-width': 3,
          'line-opacity': 0.85,
          'line-dasharray': [3, 3],
        },
      })

      vesselsSourceRef.current = map.getSource('vessels')
      trackSourceRef.current = map.getSource('track')
      trackSourceRef.current.setData(buildTrackData(dataRef.current.track))

      map.on('click', 'vessel-markers', (e) => {
        onSelectRef.current(e.features[0].properties)
      })

      map.on('mouseenter', 'vessel-markers', () => {
        map.getCanvas().style.cursor = 'pointer'
      })
      map.on('mouseleave', 'vessel-markers', () => {
        map.getCanvas().style.cursor = ''
      })

      map.on('click', (e) => {
        const hits = map.queryRenderedFeatures(e.point, { layers: ['vessel-markers'] })
        if (hits.length === 0) onSelectRef.current(null)
      })
    })

    return () => {
      map.remove()
      mapRef.current = null
      vesselsSourceRef.current = null
      trackSourceRef.current = null
    }
  }, [])

  useEffect(() => {
    const source = vesselsSourceRef.current
    if (source) source.setData(buildVesselsData({ vessels, showStale, selectedMmsi }))
  }, [vessels, showStale, selectedMmsi])

  useEffect(() => {
    const source = trackSourceRef.current
    if (source) source.setData(buildTrackData(track))
  }, [track])

  const visibleCount = vessels.filter((v) => showStale || v.status !== 'STALE').length

  return (
    <>
      <div ref={containerRef} className="map-container" />
      <div className="map-controls">
        <label>
          <input
            type="checkbox"
            checked={showStale}
            onChange={(e) => onToggleStale(e.target.checked)}
          />
          Tampilkan STALE
        </label>
        <span className="count">{visibleCount} kapal terlihat</span>
        <button type="button" onClick={() => mapRef.current?.flyTo({ center: DEFAULT_CENTER, zoom: DEFAULT_ZOOM })}>
          Reset view
        </button>
      </div>
    </>
  )
}