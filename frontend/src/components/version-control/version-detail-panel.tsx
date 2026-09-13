import { useState } from 'react'
import { Bot, Copy, RotateCcw, Tag, Trash2, User, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { VersionDetail, VersionTag } from '@/types/version-control'
import { ConfigView } from './config-view'
import { absoluteTime, friendlyActor, isHumanActor, originMeta, relativeTime, shortId } from './version-format'

const CHIP = 'inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-0.5 text-xs text-muted'
const CURRENT_PILL = 'inline-flex items-center rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[11px] font-medium text-accent'

function Fingerprint({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-xs text-muted">{label}</span>
      <span className="font-mono text-xs text-secondary truncate" title={value}>{shortId(value)}</span>
    </div>
  )
}

interface VersionDetailPanelProps {
  detail: VersionDetail
  onClose: () => void
  onRestore?: () => void
  restoreEnabled?: boolean
  restoring?: boolean
  isCurrent?: boolean
  // Tag editor (Task 5): existing tag for this version + set/remove handlers. The whole
  // editor block is gated on `onSetTag` being provided.
  tag?: VersionTag
  onSetTag?: (label: string, note: string | null) => void
  onRemoveTag?: () => void
}

export function VersionDetailPanel({ detail, onClose, onRestore, restoreEnabled, restoring, isCurrent, tag, onSetTag, onRemoveTag }: VersionDetailPanelProps) {
  const meta = originMeta(detail.origin)
  const { Icon } = meta
  const ActorIcon = isHumanActor(detail.observed_by) ? User : Bot
  const snapshot = detail.snapshot
  // Seeded once per mount from the version's tag. The parent remounts this panel per
  // version_id (via a React `key`), so a version switch gives a fresh instance and the
  // input always reflects the CURRENT version's tag (empty for an untagged version).
  const [label, setLabel] = useState(tag?.label ?? '')
  return (
    <section aria-label="Version detail" className="flex h-full flex-col rounded-xl border border-default bg-surface">
      {/* Sticky metadata header — stays put while the configuration body scrolls below. */}
      <header className="shrink-0 space-y-4 border-b border-default p-4">
      <div className="flex items-center gap-2 flex-wrap">
        <Badge variant={meta.variant} className="gap-1">
          <Icon className="w-3 h-3" />
          {meta.label}
        </Badge>
        {isCurrent && <span className={CURRENT_PILL}>Current</span>}
        <span className="font-mono text-xs text-secondary" title={detail.version_id}>{shortId(detail.version_id)}</span>
        <button
          type="button"
          onClick={() => { void navigator.clipboard?.writeText(detail.version_id) }}
          title="Copy full version id"
          aria-label="Copy full version id"
          className="text-muted hover:text-secondary transition-colors"
        >
          <Copy className="w-3 h-3" />
        </button>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close version detail"
          className="ml-auto text-muted hover:text-secondary transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-xs">
        <div className="col-span-2 flex items-start justify-between gap-3">
          <span className="text-muted">Captured</span>
          <span className="text-right text-secondary">
            {absoluteTime(detail.observed_at)}
            <span className="text-muted"> · {relativeTime(detail.observed_at)}</span>
          </span>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-muted">By</span>
          <span className="flex items-center gap-1 text-secondary truncate" title={detail.observed_by}>
            <ActorIcon className="w-3 h-3 shrink-0" />
            {friendlyActor(detail.observed_by)}
          </span>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-muted">Canonicalizer</span>
          <span className="font-mono text-secondary">{detail.fingerprints.canonicalizer_version}</span>
        </div>
      </div>

      <div className="space-y-1.5">
        <p className="text-xs font-semibold text-secondary uppercase tracking-wide">Fingerprints</p>
        <Fingerprint label="Config" value={detail.fingerprints.config} />
        <Fingerprint label="Benchmark" value={detail.fingerprints.benchmark} />
        <Fingerprint label="Metadata" value={detail.fingerprints.metadata} />
      </div>

      {onSetTag && (
        <div className="space-y-1.5">
          <label htmlFor="vc-tag-label" className="flex items-center gap-1 text-xs font-semibold text-secondary uppercase tracking-wide">
            <Tag className="w-3 h-3" />
            Tag label
          </label>
          <div className="flex items-center gap-2">
            <input
              id="vc-tag-label"
              type="text"
              value={label}
              maxLength={60}
              onChange={event => setLabel(event.target.value)}
              placeholder="e.g. Golden"
              className="min-w-0 flex-1 rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary focus:outline-none focus:ring-2 focus:ring-accent/50"
            />
            <button
              type="button"
              onClick={() => onSetTag(label, tag?.note ?? null)}
              disabled={!label.trim()}
              className="inline-flex items-center rounded-md border border-default px-2.5 py-1 text-xs font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Save
            </button>
            {tag && onRemoveTag && (
              <button
                type="button"
                onClick={onRemoveTag}
                title="Remove tag"
                aria-label="Remove tag"
                className="inline-flex items-center gap-1 rounded-md border border-default px-2.5 py-1 text-xs font-medium text-muted hover:text-secondary hover:bg-surface-secondary transition-colors"
              >
                <Trash2 className="w-3 h-3" />
                Remove tag
              </button>
            )}
          </div>
        </div>
      )}

      {(detail.parent_version_id || detail.restored_from_version_id || detail.optimizer_run_id) && (
        <div className="flex flex-wrap items-center gap-2">
          {detail.parent_version_id && (
            <span className={CHIP} title={`Parent version ${detail.parent_version_id}`}>Parent {shortId(detail.parent_version_id)}</span>
          )}
          {detail.restored_from_version_id && (
            <span className={CHIP} title={`Restored from ${detail.restored_from_version_id}`}>Restored from {shortId(detail.restored_from_version_id)}</span>
          )}
          {detail.optimizer_run_id && (
            <span className={CHIP}>Optimizer run · Champion {detail.champion_id ?? 'unknown'}</span>
          )}
        </div>
      )}

      {onRestore && (
        <div className="flex items-center justify-end">
          <button
            type="button"
            onClick={onRestore}
            disabled={!restoreEnabled || restoring}
            title={restoreEnabled
              ? 'Apply this version as the live configuration (a new reviewed version; history is preserved)'
              : 'Restore is not enabled on this deployment'}
            className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-default text-sm font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <RotateCcw className="w-4 h-4" />
            {restoring ? 'Restoring…' : 'Restore this version'}
          </button>
        </div>
      )}
      </header>

      {snapshot !== undefined && snapshot !== null && (
        <div className="min-h-0 flex-1 overflow-auto p-4 space-y-2">
          <p className="text-xs font-semibold text-secondary uppercase tracking-wide">Configuration</p>
          <ConfigView snapshot={snapshot} />
        </div>
      )}
    </section>
  )
}
