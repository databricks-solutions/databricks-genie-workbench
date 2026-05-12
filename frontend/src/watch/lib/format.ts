export function formatUsd(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—'
  if (value < 0.01) return '<$0.01'
  if (value < 1) return `$${value.toFixed(2)}`
  if (value < 100) return `$${value.toFixed(2)}`
  if (value < 10_000) return `$${Math.round(value).toLocaleString()}`
  if (value < 1_000_000) return `$${(value / 1_000).toFixed(1)}k`
  return `$${(value / 1_000_000).toFixed(2)}M`
}

export function formatInt(value: number | null | undefined): string {
  if (value == null) return '—'
  return value.toLocaleString()
}

export function formatMs(value: number | null | undefined): string {
  if (value == null) return '—'
  if (value < 1000) return `${Math.round(value)} ms`
  return `${(value / 1000).toFixed(2)} s`
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleString()
}

export function formatDay(value: string | null | undefined): string {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

export function relativeTime(value: string | null | undefined): string {
  if (!value) return '—'
  const d = new Date(value).getTime()
  if (Number.isNaN(d)) return value as string
  const diff = Date.now() - d
  const minutes = Math.floor(diff / 60_000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  if (days < 30) return `${days}d ago`
  const months = Math.floor(days / 30)
  if (months < 12) return `${months}mo ago`
  return `${Math.floor(days / 365)}y ago`
}
