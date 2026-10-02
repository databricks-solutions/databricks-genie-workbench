import { useEffect, useRef, useState } from 'react'
import { Bot, Check, ChevronRight, ChevronUp, Copy, Pencil, RotateCcw, Tag, Trash2, User, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { VersionDetail, VersionTag } from '@/types/version-control'
import { ConfigView } from './config-view'
import { absoluteTime, friendlyActor, isHumanActor, originMeta, relativeTime, shortId } from './version-format'

const CHIP = 'inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-0.5 text-xs text-muted'
const CURRENT_PILL = 'inline-flex items-center rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[11px] font-medium text-accent'
// Common labels operators reach for; clicking one fills the label input (still editable).
const TAG_PRESETS = ['Champion', 'Challenger', 'Baseline', 'v1'] as const

function Fingerprint({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-3">
      <span className="shrink-0 text-xs text-muted">{label}</span>
      <span className="min-w-0 break-all text-right font-mono text-xs text-secondary" title={value}>{value}</span>
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
  // Inline restore confirmation (rendered right next to the trigger, not far below the
  // grid). When `awaitingConfirm` is true the panel swaps the "Restore this version" button
  // for a compact confirm row (message + Cancel + Confirm + error).
  awaitingConfirm?: boolean
  restoreError?: string | null
  onConfirmRestore?: () => void
  onCancelRestore?: () => void
  // Tag editor (Task 5): existing tag for this version + set/remove handlers. The whole
  // editor block is gated on `onSetTag` being provided. Handlers may return a boolean
  // (true = success) so the panel can show a transient "Saved ✓" cue and reset on remove;
  // returning void is treated as success.
  tag?: VersionTag
  onSetTag?: (label: string, note: string | null) => void | boolean | Promise<void | boolean>
  onRemoveTag?: () => void | boolean | Promise<void | boolean>
}

export function VersionDetailPanel({ detail, onClose, onRestore, restoreEnabled, restoring, isCurrent, awaitingConfirm, restoreError, onConfirmRestore, onCancelRestore, tag, onSetTag, onRemoveTag }: VersionDetailPanelProps) {
  const meta = originMeta(detail.origin)
  const { Icon } = meta
  const ActorIcon = isHumanActor(detail.observed_by) ? User : Bot
  const snapshot = detail.snapshot
  // Seeded once per mount from the version's tag. The parent remounts this panel per
  // version_id (via a React `key`), so a version switch gives a fresh instance and the
  // label/note always reflect the CURRENT version's tag (empty for an untagged version).
  const [label, setLabel] = useState(tag?.label ?? '')
  const [note, setNote] = useState(tag?.note ?? '')
  // Collapsed by default so the tag/comment editor does not steal permanent space from the
  // scrollable configuration below — a tagged version shows only a compact summary line.
  const [editing, setEditing] = useState(false)
  // Transient inline "Saved ✓" confirmation next to Save (~2s), cleared on unmount.
  const [saved, setSaved] = useState(false)
  const savedTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => () => { if (savedTimer.current) clearTimeout(savedTimer.current) }, [])

  const handleSave = async () => {
    if (!onSetTag) return
    const result = await onSetTag(label.trim(), note.trim() || null)
    if (result === false) return  // explicit failure surfaces via the parent banner; no cue
    setSaved(true)
    if (savedTimer.current) clearTimeout(savedTimer.current)
    savedTimer.current = setTimeout(() => setSaved(false), 2000)
  }

  const handleRemove = async () => {
    if (!onRemoveTag) return
    const result = await onRemoveTag()
    if (result === false) return
    setLabel('')
    setNote('')
    setEditing(false)
  }
  return (
    <section aria-label="Version detail" className="flex h-full flex-col rounded-xl border border-default bg-surface">
      {/* Sticky metadata header — stays put while the configuration body scrolls below.
          Kept intentionally shallow (tight spacing + fingerprints behind a disclosure) so
          it does not push the scrollable Configuration off-screen. */}
      <header className="shrink-0 space-y-3 border-b border-default p-3">
      <div className="flex items-center gap-2 flex-wrap">
        <Badge variant={meta.variant} className="gap-1">
          <Icon className="w-3 h-3" />
          {meta.label}
        </Badge>
        {isCurrent && <span className={CURRENT_PILL}>Current</span>}
        {/* Parent version moved up here to save a whole chips row below (issue #1). */}
        {detail.parent_version_id && (
          <span className={CHIP} title={`Parent version ${detail.parent_version_id}`}>
            Parent {shortId(detail.parent_version_id)}
          </span>
        )}
        <button
          type="button"
          onClick={onClose}
          aria-label="Close version detail"
          className="ml-auto text-muted hover:text-secondary transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 text-xs">
        {/* Full version id (issue #2): shown in full with a copy affordance, no truncation. */}
        <div className="col-span-2 flex items-start justify-between gap-3">
          <span className="shrink-0 text-muted">Version</span>
          <span className="flex min-w-0 items-start gap-1">
            <span className="break-all text-right font-mono text-secondary" title={detail.version_id}>{detail.version_id}</span>
            <button
              type="button"
              onClick={() => { void navigator.clipboard?.writeText(detail.version_id) }}
              title="Copy full version id"
              aria-label="Copy full version id"
              className="shrink-0 text-muted hover:text-secondary transition-colors"
            >
              <Copy className="w-3 h-3" />
            </button>
          </span>
        </div>
        <div className="col-span-2 flex items-start justify-between gap-3">
          <span className="text-muted">Captured</span>
          <span className="text-right text-secondary">
            {absoluteTime(detail.observed_at)}
            <span className="text-muted"> · {relativeTime(detail.observed_at)}</span>
          </span>
        </div>
        <div className="col-span-2 flex items-center justify-between gap-3">
          <span className="text-muted">By</span>
          <span className="flex items-center gap-1 text-secondary truncate" title={detail.observed_by}>
            <ActorIcon className="w-3 h-3 shrink-0" />
            {friendlyActor(detail.observed_by)}
          </span>
        </div>
      </div>

      {/* Fingerprints + canonicalizer are diagnostic detail, rarely needed inline — tuck
          them behind a disclosure so they don't permanently crowd the header (issue #3). */}
      <details className="group text-xs">
        <summary className="flex cursor-pointer list-none items-center gap-1 font-semibold uppercase tracking-wide text-secondary">
          <ChevronRight className="w-3 h-3 shrink-0 transition-transform group-open:rotate-90" />
          Fingerprints
        </summary>
        <div className="mt-1.5 space-y-1.5">
          <Fingerprint label="Config" value={detail.fingerprints.config} />
          <Fingerprint label="Benchmark" value={detail.fingerprints.benchmark} />
          <Fingerprint label="Metadata" value={detail.fingerprints.metadata} />
          <div className="flex items-center justify-between gap-3">
            <span className="text-muted">Canonicalizer</span>
            <span className="font-mono text-secondary">{detail.fingerprints.canonicalizer_version}</span>
          </div>
        </div>
      </details>

      {onSetTag && (
        <div className="space-y-1.5">
          {!editing ? (
            // Collapsed: a single compact line. Tagged versions show label + note preview;
            // untagged versions show an "Add tag" affordance. Either expands the editor.
            <button
              type="button"
              onClick={() => setEditing(true)}
              aria-label={tag ? 'Edit tag' : 'Add tag'}
              className="flex w-full items-center gap-2 rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary hover:bg-surface-secondary transition-colors"
            >
              <Tag className="w-3 h-3 shrink-0 text-muted" />
              {tag ? (
                <>
                  <Badge variant="info" className="shrink-0">{tag.label}</Badge>
                  {tag.note && <span className="min-w-0 truncate text-muted" title={tag.note}>{tag.note}</span>}
                  <Pencil className="ml-auto w-3 h-3 shrink-0 text-muted" />
                </>
              ) : (
                <span className="text-muted">Add tag &amp; comment</span>
              )}
            </button>
          ) : (
            <>
              <div className="flex items-center gap-1 text-xs font-semibold text-secondary uppercase tracking-wide">
                <Tag className="w-3 h-3" />
                <label htmlFor="vc-tag-label">Tag &amp; comment</label>
                <button
                  type="button"
                  onClick={() => setEditing(false)}
                  aria-label="Collapse tag editor"
                  className="ml-auto text-muted hover:text-secondary transition-colors"
                >
                  <ChevronUp className="w-3.5 h-3.5" />
                </button>
              </div>
              {/* Quick-pick presets fill the label; the note is still free text. */}
              <div className="flex flex-wrap gap-1">
                {TAG_PRESETS.map(preset => (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => setLabel(preset)}
                    aria-pressed={label === preset}
                    className={`rounded-full border px-2 py-0.5 text-[11px] font-medium transition-colors ${
                      label === preset
                        ? 'border-accent/50 bg-accent/10 text-accent'
                        : 'border-default text-muted hover:bg-surface-secondary'}`}
                  >
                    {preset}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-2">
                <input
                  id="vc-tag-label"
                  type="text"
                  value={label}
                  maxLength={60}
                  onChange={event => setLabel(event.target.value)}
                  placeholder="e.g. Champion"
                  className="min-w-0 flex-1 rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary focus:outline-none focus:ring-2 focus:ring-accent/50"
                />
                <button
                  type="button"
                  onClick={() => { void handleSave() }}
                  disabled={!label.trim()}
                  className="inline-flex items-center rounded-md border border-default px-2.5 py-1 text-xs font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Save
                </button>
                {saved && (
                  <span role="status" className="inline-flex items-center gap-1 text-xs font-medium text-green-400">
                    <Check className="w-3 h-3" />
                    Saved
                  </span>
                )}
                {tag && onRemoveTag && (
                  <button
                    type="button"
                    onClick={() => { void handleRemove() }}
                    title="Remove tag"
                    aria-label="Remove tag"
                    className="inline-flex items-center gap-1 rounded-md border border-default px-2.5 py-1 text-xs font-medium text-muted hover:text-secondary hover:bg-surface-secondary transition-colors"
                  >
                    <Trash2 className="w-3 h-3" />
                    Remove tag
                  </button>
                )}
              </div>
              <textarea
                aria-label="Tag comment"
                value={note}
                maxLength={500}
                rows={2}
                onChange={event => setNote(event.target.value)}
                placeholder="Optional comment — why this version matters"
                className="w-full resize-y rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary focus:outline-none focus:ring-2 focus:ring-accent/50"
              />
            </>
          )}
        </div>
      )}

      {(detail.restored_from_version_id || detail.optimizer_run_id) && (
        <div className="flex flex-wrap items-center gap-2">
          {detail.restored_from_version_id && (
            <span className={CHIP} title={`Restored from ${detail.restored_from_version_id}`}>Restored from {shortId(detail.restored_from_version_id)}</span>
          )}
          {detail.optimizer_run_id && (
            <span className={CHIP}>Optimizer run · Champion {detail.champion_id ?? 'unknown'}</span>
          )}
        </div>
      )}

      {onRestore && (
        awaitingConfirm ? (
          // Confirmation renders right here, next to the trigger (issue #1) — not far below
          // the grid. Solid amber Confirm for readable contrast in both themes (issue #2).
          <div className="space-y-2 rounded-lg border border-amber-500/40 bg-amber-500/10 p-3">
            {restoring ? (
              // Honest, indeterminate progress: the apply is a single synchronous call
              // (OBO PATCH + capture, ~15-30s), so the UI can freeze. Tell the user what is
              // happening and that it can take a while (issue #2).
              <p className="flex items-center gap-2 text-xs text-secondary">
                <RotateCcw className="w-3.5 h-3.5 animate-spin" aria-hidden />
                Applying to the live space, then recording a new version — this can take up to a minute.
              </p>
            ) : (
              <p className="text-xs text-secondary">
                Restore this version as the live configuration? It is applied as a new version —
                history is preserved.
              </p>
            )}
            {restoreError && (
              <div role="alert" className="rounded-md border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-xs text-red-400">
                {restoreError}
              </div>
            )}
            <div className="flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={onCancelRestore}
                disabled={restoring}
                className="rounded-lg border border-default px-3 py-1.5 text-sm font-medium text-secondary transition-colors hover:bg-surface-secondary disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={onConfirmRestore}
                disabled={restoring}
                className="inline-flex items-center gap-2 rounded-lg bg-amber-600 px-3 py-1.5 text-sm font-semibold text-white transition-colors hover:bg-amber-700 disabled:opacity-50"
              >
                <RotateCcw className={`w-4 h-4 ${restoring ? 'animate-spin' : ''}`} />
                {restoring ? 'Restoring…' : 'Confirm restore'}
              </button>
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-end">
            <button
              type="button"
              onClick={onRestore}
              disabled={!restoreEnabled || restoring || isCurrent}
              title={isCurrent
                ? 'This version is already the live configuration'
                : restoreEnabled
                  ? 'Apply this version as the live configuration (a new reviewed version; history is preserved)'
                  : 'Restore is not enabled on this deployment'}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-default text-sm font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <RotateCcw className="w-4 h-4" />
              {restoring ? 'Restoring…' : 'Restore this version'}
            </button>
          </div>
        )
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
