export const STATUS_COLORS = {
  UNDERWAY: '#22c55e',
  ANCHORED: '#f59e0b',
  MOORED: '#a855f7',
  STOPPED: '#ef4444',
  STALE: '#94a3b8',
  UNKNOWN: '#64748b',
}

export function statusColor(status) {
  return STATUS_COLORS[status] ?? STATUS_COLORS.UNKNOWN
}