import type { ReactNode, Ref } from 'react'
import { Camera, GitBranch, Info, Lock, RefreshCw } from 'lucide-react'
import { Tooltip } from '@/components/ui/tooltip'
import {
  canEditVersions, captureExplainer, NO_ACCESS_FALLBACK, NO_ACCESS_TITLE, UNKNOWN_NOTICE, VIEW_NOTICE,
  type VcAccess,
} from './access-state'

interface HeaderProps {
  access: VcAccess
  capturing?: boolean
  syncing?: boolean
  onCapture?: () => void
}

export function VersionControlHeader({ access, capturing = false, syncing = false, onCapture }: HeaderProps) {
  const explainer = captureExplainer(access)
  const busy = capturing || syncing
  return (
    <div className="flex items-start justify-between gap-3">
      <div className="flex items-center gap-2 min-w-0">
        <span className="shrink-0 p-2 rounded-lg border border-default bg-surface-secondary text-muted">
          <GitBranch className="w-4 h-4" />
        </span>
        <h3 className="text-lg font-display font-semibold text-primary">Version Control</h3>
        <Tooltip content={<span className="block max-w-xs text-left">{explainer}</span>}>
          <span aria-label={explainer} className="text-muted hover:text-secondary transition-colors cursor-help">
            <Info className="w-4 h-4" />
          </span>
        </Tooltip>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        {(access === 'checking' || busy) && (
          <span role="status" className="inline-flex items-center gap-1.5 text-xs text-muted">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" />
            {access === 'checking' ? 'Checking access…' : capturing ? 'Capturing…' : 'Checking…'}
          </span>
        )}
        {canEditVersions(access) && onCapture && (
          <>
            {/* The compact icon and the labeled button run the same action — observe the
                live Genie space (source-check + record). */}
            <button
              onClick={onCapture}
              disabled={busy}
              className="p-2 rounded-lg border border-default text-muted hover:text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50"
              title="Check the live space for changes"
            >
              <RefreshCw className={`w-4 h-4 ${busy ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={onCapture}
              disabled={busy}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-default text-sm font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50"
            >
              <Camera className="w-4 h-4" />
              {capturing ? 'Capturing…' : 'Capture current state'}
            </button>
          </>
        )}
      </div>
    </div>
  )
}

export function AccessNotice({ access, reason = null }: { access: VcAccess; reason?: string | null }) {
  const message = access === 'view' ? VIEW_NOTICE : access === 'unknown' ? UNKNOWN_NOTICE : null
  if (!message) return null
  return (
    <div role="status" className="flex items-start gap-2 text-sm rounded-lg border border-default bg-surface-secondary text-secondary px-3 py-2">
      <Lock className="w-4 h-4 mt-0.5 shrink-0 text-muted" />
      <span>
        {message}
        {access === 'unknown' && reason && <span className="block text-xs text-muted mt-0.5">{reason}</span>}
      </span>
    </div>
  )
}

export function NoAccessState({ reason }: { reason: string | null }) {
  return (
    <div role="status" className="flex flex-col items-center justify-center rounded-xl border border-dashed border-default bg-surface p-10 text-center">
      <Lock className="w-8 h-8 mb-3 text-muted opacity-60" />
      <p className="text-secondary font-medium">{NO_ACCESS_TITLE}</p>
      <p className="text-sm text-muted mt-1 max-w-md">{reason || NO_ACCESS_FALLBACK}</p>
    </div>
  )
}

// Master–detail: narrow versions rail (left) + wide detail (right). Each pane scrolls on
// its own at lg+; below lg they stack and the page scrolls.
export function VersionPanes({ rail, detail, detailRef }: { rail: ReactNode; detail: ReactNode; detailRef?: Ref<HTMLDivElement> }) {
  return (
    <div className="grid gap-4 lg:grid-cols-[320px_minmax(0,1fr)] lg:items-start">
      <div className="rounded-xl border border-default bg-surface p-3 lg:h-[78vh] lg:overflow-auto">{rail}</div>
      <div ref={detailRef} className="min-w-0 lg:h-[78vh]">{detail}</div>
    </div>
  )
}

export function DetailPlaceholder({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-full items-center justify-center rounded-xl border border-dashed border-default bg-surface p-6 text-center text-sm text-muted">
      {children}
    </div>
  )
}
