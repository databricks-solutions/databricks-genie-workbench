import { Lock, RefreshCw } from 'lucide-react'
import {
  CHECKING_ACCESS, SPACE_NO_ACCESS_FALLBACK, SPACE_NO_ACCESS_TITLE, SPACE_UNKNOWN_NOTICE, SPACE_VIEW_NOTICE,
  type SpaceAccessState,
} from '@/lib/space-access'

export function SpaceAccessNotice({ access, reason }: { access: SpaceAccessState; reason: string | null }) {
  const message = access === 'view' ? SPACE_VIEW_NOTICE : access === 'unknown' ? SPACE_UNKNOWN_NOTICE : null
  if (!message) return null
  return (
    <div role="status" className="mb-4 flex items-start gap-2 text-sm rounded-lg border border-default bg-surface-secondary text-secondary px-3 py-2">
      <Lock className="w-4 h-4 mt-0.5 shrink-0 text-muted" />
      <span>
        {message}
        {access === 'unknown' && reason && <span className="block text-xs text-muted mt-0.5">{reason}</span>}
      </span>
    </div>
  )
}

export function SpaceNoAccessState({ reason }: { reason: string | null }) {
  return (
    <div role="status" className="flex flex-col items-center justify-center rounded-xl border border-dashed border-default bg-surface p-10 text-center">
      <Lock className="w-8 h-8 mb-3 text-muted opacity-60" />
      <p className="text-secondary font-medium">{SPACE_NO_ACCESS_TITLE}</p>
      <p className="text-sm text-muted mt-1 max-w-md">{reason || SPACE_NO_ACCESS_FALLBACK}</p>
    </div>
  )
}

export function LockedSection({ title, message }: { title: string; message: string }) {
  return (
    <div role="status" className="rounded-xl border border-dashed border-default bg-surface px-5 py-4">
      <div className="flex items-center gap-2">
        <Lock className="w-4 h-4 text-muted" />
        <span className="text-sm font-semibold text-secondary uppercase tracking-wide">{title}</span>
      </div>
      <p className="text-sm text-muted mt-1">{message}</p>
    </div>
  )
}

export function CheckingAccess() {
  return (
    <div role="status" className="py-8 flex items-center justify-center gap-2 text-sm text-muted">
      <RefreshCw className="w-4 h-4 animate-spin" />
      {CHECKING_ACCESS}
    </div>
  )
}
