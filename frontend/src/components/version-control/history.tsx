import { useState } from 'react'
import { Bot, Copy, GitBranch, Sparkles, Tag, User } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import type { VersionPage, VersionSummary, VersionTag, VersionTagMap } from '@/types/version-control'
import { absoluteTime, dayGroupLabel, friendlyActor, isHumanActor, originMeta, relativeTime, shortId } from './version-format'

interface HistoryProps {
  page: VersionPage
  loading?: boolean
  // The open version (active row highlight) and the observed head (Current pill).
  selectedId?: string | null
  currentId?: string | null
  onNext: () => void
  onSelect: (version: VersionSummary) => void
  // Legacy dropdown compare (M02 demo surface). The space tab uses checkbox multi-select
  // instead — pass `onToggleCompare` (and `compareIds`) for that mode.
  onCompare?: (left: string, right: string) => void
  compareIds?: string[]
  onToggleCompare?: (versionId: string) => void
  // Optional per-version tags, keyed by version_id (Task 5).
  tags?: VersionTagMap
}

const CHIP = 'inline-flex items-center gap-1 rounded-md border border-default bg-surface px-2 py-0.5 text-xs text-muted'
const CURRENT_PILL = 'inline-flex items-center rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[11px] font-medium text-accent'

// Group the flat, newest-first page into day buckets for a scannable timeline.
function groupByDay(items: VersionSummary[]): { label: string; items: VersionSummary[] }[] {
  const groups: { label: string; items: VersionSummary[] }[] = []
  for (const version of items) {
    const label = dayGroupLabel(version.observed_at)
    const last = groups[groups.length - 1]
    if (last && last.label === label) last.items.push(version)
    else groups.push({ label, items: [version] })
  }
  return groups
}

interface VersionRowProps {
  version: VersionSummary
  isCurrent: boolean
  isActive: boolean
  onSelect: (version: VersionSummary) => void
  compareChecked?: boolean
  onToggleCompare?: (versionId: string) => void
  tag?: VersionTag
}

function VersionRow({ version, isCurrent, isActive, onSelect, compareChecked, onToggleCompare, tag }: VersionRowProps) {
  const meta = originMeta(version.origin)
  const { Icon } = meta
  const ActorIcon = isHumanActor(version.observed_by) ? User : Bot
  const hasLineage = version.parent_version_id || version.restored_from_version_id || version.optimizer_run_id
  const stop = (event: { stopPropagation: () => void }) => event.stopPropagation()
  return (
    <li>
      {/* The whole card is the click target; nested copy/link controls stop propagation. */}
      <div
        role="button"
        tabIndex={0}
        aria-current={isActive ? 'true' : undefined}
        onClick={() => onSelect(version)}
        onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(version) } }}
        className={cn(
          'cursor-pointer rounded-lg border p-3 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-accent/50',
          isActive
            ? 'border-accent/60 bg-surface-secondary ring-1 ring-accent/30'
            : 'border-default bg-surface-secondary/40 hover:bg-surface-secondary/70',
        )}
      >
        <div className="flex items-center gap-2 flex-wrap">
          {onToggleCompare && (
            <input
              type="checkbox"
              checked={!!compareChecked}
              onClick={stop}
              onChange={() => onToggleCompare(version.version_id)}
              aria-label={`Select version ${shortId(version.version_id)} to compare`}
              title="Select to compare"
              className="h-4 w-4 cursor-pointer accent-accent"
            />
          )}
          <Badge variant={meta.variant} className="gap-1">
            <Icon className="w-3 h-3" />
            {meta.label}
          </Badge>
          {isCurrent && <span className={CURRENT_PILL}>Current</span>}
          <span className="font-mono text-xs text-secondary" title={version.version_id}>
            {shortId(version.version_id)}
          </span>
          <button
            type="button"
            onClick={event => { stop(event); void navigator.clipboard?.writeText(version.version_id) }}
            title="Copy full version id"
            aria-label="Copy full version id"
            className="text-muted hover:text-secondary transition-colors"
          >
            <Copy className="w-3 h-3" />
          </button>
          {tag && (
            <Badge variant="secondary" className="gap-1" title={tag.note ?? tag.label}>
              <Tag className="w-3 h-3" />
              {tag.label}
            </Badge>
          )}
          <span className="ml-auto text-xs text-muted" title={absoluteTime(version.observed_at)}>
            {relativeTime(version.observed_at)}
          </span>
        </div>

        <p className="mt-1.5 flex items-center gap-1 text-xs text-muted">
          <ActorIcon className="w-3 h-3" />
          by <span className="text-secondary" title={version.observed_by}>{friendlyActor(version.observed_by)}</span>
        </p>

        {hasLineage && (
          <div className="mt-2 flex flex-wrap items-center gap-2">
            {version.parent_version_id && (
              <span className={CHIP} title={`Parent version ${version.parent_version_id}`}>
                Parent {shortId(version.parent_version_id)}
              </span>
            )}
            {version.restored_from_version_id && (
              <span className={CHIP} title={`Restored from ${version.restored_from_version_id}`}>
                Restored from {shortId(version.restored_from_version_id)}
              </span>
            )}
            {version.optimizer_run_id && (
              <a
                href={`?view=detail&space=${encodeURIComponent(version.binding_id)}&tab=optimize&run=${encodeURIComponent(version.optimizer_run_id)}`}
                onClick={stop}
                className={`${CHIP} hover:text-accent hover:border-accent/40 transition-colors`}
                title={`Optimizer run ${version.optimizer_run_id}`}
              >
                <Sparkles className="w-3 h-3" />
                Optimizer run · Champion {version.champion_id ?? 'unknown'}
              </a>
            )}
          </div>
        )}
      </div>
    </li>
  )
}

function LoadingRows() {
  return (
    <div className="space-y-2" aria-hidden="true">
      {[0, 1, 2].map(i => (
        <div key={i} className="flex items-start gap-3 rounded-lg border border-default p-3">
          <Skeleton className="h-5 w-20 rounded-full" />
          <div className="flex-1 space-y-2">
            <Skeleton className="h-3 w-40" />
            <Skeleton className="h-3 w-24" />
          </div>
        </div>
      ))}
    </div>
  )
}

function EmptyState() {
  return (
    <div className="text-center py-16 text-muted">
      <GitBranch className="w-8 h-8 mx-auto mb-3 opacity-50" />
      <p className="text-secondary font-medium">No versions captured yet</p>
      <p className="text-sm mt-1">Opening this tab auto-captures the current configuration. If nothing appears, use “Capture current state” to save one now.</p>
    </div>
  )
}

function compareLabel(version: VersionSummary): string {
  return `${originMeta(version.origin).label} · ${relativeTime(version.observed_at)} · ${shortId(version.version_id)}`
}

export function History({ page, loading, selectedId, currentId, onNext, onSelect, onCompare, compareIds, onToggleCompare, tags }: HistoryProps) {
  const [left, setLeft] = useState('')
  const [right, setRight] = useState('')

  if (loading && !page.items.length) return <LoadingRows />
  if (!page.items.length) return <EmptyState />

  const checkboxMode = !!onToggleCompare
  const selectedCount = compareIds?.length ?? 0
  const compareHint = selectedCount === 0
    ? 'Select two versions to compare'
    : selectedCount === 1
      ? '1 selected — pick one more to compare'
      : '2 selected — comparing'
  const headId = currentId ?? page.items[0]?.version_id ?? null
  const groups = groupByDay(page.items)
  const selectClass = 'rounded-md border border-default bg-surface px-2 py-1 text-xs text-secondary'
  const actionClass = 'inline-flex items-center gap-1 rounded-md border border-default px-2.5 py-1 text-xs font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50 disabled:cursor-not-allowed'

  return (
    <section aria-label="Captured history" className="space-y-3">
      <ol className="space-y-4">
        {groups.map(group => (
          <li key={group.label}>
            <div className="sticky top-0 z-10 mb-2 bg-surface/95 py-1 text-[11px] font-semibold uppercase tracking-wide text-muted backdrop-blur">
              {group.label}
            </div>
            <ol className="space-y-2">
              {group.items.map(version => (
                <VersionRow
                  key={version.version_id}
                  version={version}
                  isCurrent={version.version_id === headId}
                  isActive={version.version_id === selectedId}
                  onSelect={onSelect}
                  compareChecked={compareIds?.includes(version.version_id)}
                  onToggleCompare={onToggleCompare}
                  tag={tags?.[version.version_id]}
                />
              ))}
            </ol>
          </li>
        ))}
      </ol>

      <div className="flex flex-wrap items-center gap-2 border-t border-default pt-3">
        {checkboxMode ? (
          <span className="text-xs text-muted">{compareHint}</span>
        ) : onCompare ? (
          <>
            <span className="text-xs uppercase tracking-wide text-muted">Compare</span>
            <label className="sr-only" htmlFor="vc-compare-left">Compare from</label>
            <select id="vc-compare-left" className={selectClass} value={left} onChange={event => setLeft(event.target.value)}>
              <option value="">Select version</option>
              {page.items.map(version => <option key={version.version_id} value={version.version_id}>{compareLabel(version)}</option>)}
            </select>
            <span className="text-xs text-muted">to</span>
            <label className="sr-only" htmlFor="vc-compare-right">Compare to</label>
            <select id="vc-compare-right" className={selectClass} value={right} onChange={event => setRight(event.target.value)}>
              <option value="">Select version</option>
              {page.items.map(version => <option key={version.version_id} value={version.version_id}>{compareLabel(version)}</option>)}
            </select>
            <button type="button" className={actionClass} disabled={!left || !right || left === right} onClick={() => onCompare(left, right)}>
              Compare versions
            </button>
          </>
        ) : null}

        {page.next_cursor && (
          <button type="button" className={`${actionClass} ml-auto`} disabled={loading} onClick={onNext}>
            {loading ? 'Loading…' : 'Load more'}
          </button>
        )}
      </div>
    </section>
  )
}
