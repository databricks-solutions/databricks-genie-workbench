import { AlertTriangle, FilePlus, GitCommit, HelpCircle, RotateCcw, Sparkles, Upload } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import type { BadgeProps } from '@/components/ui/badge'
import type { Origin } from '@/types/version-control'

export type OriginMeta = { label: string; variant: BadgeProps['variant']; Icon: LucideIcon }

// Origin drives the color + icon of each version so "what changed this" reads at a glance.
export const ORIGIN_META: Record<Origin, OriginMeta> = {
  workbench: { label: 'Auto-captured', variant: 'info', Icon: GitCommit },
  external: { label: 'External', variant: 'warning', Icon: AlertTriangle },
  optimizer: { label: 'Optimizer', variant: 'default', Icon: Sparkles },
  restore: { label: 'Restore', variant: 'secondary', Icon: RotateCcw },
  promotion: { label: 'Promotion', variant: 'success', Icon: Upload },
  unknown: { label: 'Unknown', variant: 'secondary', Icon: HelpCircle },
  create: { label: 'Created', variant: 'success', Icon: FilePlus },
}

export function originMeta(origin: Origin): OriginMeta {
  return ORIGIN_META[origin] ?? ORIGIN_META.unknown
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

// The app service principal shows up as a raw GUID; render a friendly label but keep the
// raw identity available (in a title attribute) for audit/copy.
export function friendlyActor(observedBy: string): string {
  if (!observedBy) return 'Unknown'
  if (UUID_RE.test(observedBy)) return 'Workbench service principal'
  // A forwarded email (person@host) reads best as a name: title-case the local part with
  // dot/underscore/hyphen separators — "prashanth.subrahmanyam@databricks.com" -> "Prashanth
  // Subrahmanyam". The full email stays in the title attribute at the call sites for audit.
  const at = observedBy.indexOf('@')
  if (at > 0) {
    const name = observedBy
      .slice(0, at)
      .split(/[._-]+/)
      .filter(Boolean)
      .map(part => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ')
    if (name) return name
  }
  return observedBy
}

export function shortId(id: string): string {
  return id.length <= 12 ? id : `${id.slice(0, 8)}…`
}

// The app service principal is recorded as a raw GUID; a human capture/restore records an
// email. This lets the UI show a person vs. system icon next to the author.
export function isHumanActor(observedBy: string): boolean {
  return !!observedBy && !UUID_RE.test(observedBy)
}

// Day bucket label for a chronological timeline: "Today" / "Yesterday" / a short date.
export function dayGroupLabel(iso: string): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return 'Unknown date'
  const d = new Date(t)
  const startOfDay = (x: Date) => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime()
  const diffDays = Math.round((startOfDay(new Date()) - startOfDay(d)) / 86400000)
  if (diffDays <= 0) return 'Today'
  if (diffDays === 1) return 'Yesterday'
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
}

export function relativeTime(iso: string): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return iso
  const seconds = Math.max(0, Math.floor((Date.now() - t) / 1000))
  if (seconds < 45) return 'just now'
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  if (days < 30) return `${days}d ago`
  return new Date(t).toLocaleDateString()
}

export function absoluteTime(iso: string): string {
  const t = Date.parse(iso)
  return Number.isNaN(t) ? iso : new Date(t).toLocaleString()
}
