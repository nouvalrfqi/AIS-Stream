const API_BASE = import.meta.env.VITE_API_URL ?? 'http://127.0.0.1:8000'

async function request(path) {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText || 'error'}`)
  }
  return res.json()
}

export const getVessels = () => request('/vessels')
export const getVessel = (mmsi) => request(`/vessels/${mmsi}`)
export const getTrack = (mmsi) => request(`/vessels/${mmsi}/track`)
export const getIntelligenceSummary = () => request('/intelligence/summary')
export const getIntelligenceConflicts = (radiusM = 500) =>
  request(`/intelligence/conflicts?radius_m=${radiusM}`)
export const getIntelligenceSpeed = () => request('/intelligence/speed')
export const getIntelligenceNavigation = () => request('/intelligence/navigation')
export const getIntelligenceFlow = () => request('/intelligence/flow')
export const getIntelligenceManeuvering = (minRotDegMin = 5) =>
  request(`/intelligence/maneuvering?min_rot_deg_min=${minRotDegMin}`)
export const getIntelligenceDataQuality = (windowHours = 24) =>
  request(`/intelligence/data-quality?window_hours=${windowHours}`)