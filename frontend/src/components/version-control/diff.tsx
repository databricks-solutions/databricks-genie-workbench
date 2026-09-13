import { useState } from 'react'
import ReactDiffViewer, { DiffMethod } from 'react-diff-viewer-continued'
import { Columns2, Minus, Pencil, Plus, Rows2 } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { BadgeProps } from '@/components/ui/badge'
import type { LucideIcon } from 'lucide-react'
import type { DiffItem, SemanticDiff } from '@/types/version-control'
import { diffViewerStyles } from '@/lib/diffViewerStyles'
import { useTheme } from '@/hooks/useTheme'
import { categoryLabel, categoryOrder, friendlyPath } from './diff-format'

type DiffChangeValue = DiffItem['change']

const CHANGE_META: Record<DiffChangeValue, { variant: BadgeProps['variant']; Icon: LucideIcon }> = {
  added: { variant: 'success', Icon: Plus },
  removed: { variant: 'danger', Icon: Minus },
  modified: { variant: 'warning', Icon: Pencil },
}

function toText(value: unknown): string {
  if (value == null) return ''
  if (typeof value === 'string') return value
  return JSON.stringify(value, null, 2)
}

// Side-by-side (or unified) value diff with word-level highlighting — reuses the shared
// react-diff-viewer-continued setup (same styles/theme as SqlDiffView) with correct
// Before/After column titles for a version-to-version comparison.
function ValueDiff({ before, after, split }: { before: string; after: string; split: boolean }) {
  const { isDark } = useTheme()
  return (
    <div className="overflow-hidden rounded-md border border-default text-xs">
      <ReactDiffViewer
        oldValue={before}
        newValue={after}
        splitView={split}
        useDarkTheme={isDark}
        compareMethod={DiffMethod.WORDS}
        styles={diffViewerStyles}
        leftTitle="Before"
        rightTitle="After"
        hideLineNumbers={false}
        showDiffOnly={false}
      />
    </div>
  )
}

function ValueBlock({ label, value }: { label: string; value: string }) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted">{label}</p>
      <pre className="max-h-48 overflow-auto rounded-md bg-surface px-2 py-1.5 text-xs text-secondary whitespace-pre-wrap break-words">{value}</pre>
    </div>
  )
}

function DiffItemCard({ item, split }: { item: DiffItem; split: boolean }) {
  const change = CHANGE_META[item.change]
  const { Icon } = change
  return (
    <article className="rounded-lg border border-default bg-surface-secondary/40 p-3 space-y-2">
      <div className="flex items-center gap-2 flex-wrap">
        <Badge variant={change.variant} className="gap-1">
          <Icon className="w-3 h-3" />
          {item.change}
        </Badge>
        <span className="text-xs text-secondary break-all" title={item.path}>{friendlyPath(item.path)}</span>
      </div>
      {item.review_required && (
        <p role="note" className="text-xs text-amber-400">
          {item.category === 'sql' ? 'Manual SQL review required' : 'Manual review required'}
        </p>
      )}
      {item.change === 'modified'
        ? <ValueDiff before={toText(item.before)} after={toText(item.after)} split={split} />
        : item.change === 'added'
          ? <ValueBlock label="New" value={toText(item.after)} />
          : <ValueBlock label="Removed" value={toText(item.before)} />}
    </article>
  )
}

// Order review-required items first within a category, then by change kind for stability.
function orderItems(items: DiffItem[]): DiffItem[] {
  return [...items].sort((a, b) =>
    Number(b.review_required) - Number(a.review_required) || a.change.localeCompare(b.change) || a.path.localeCompare(b.path),
  )
}

function counts(items: DiffItem[]): string {
  const added = items.filter(i => i.change === 'added').length
  const modified = items.filter(i => i.change === 'modified').length
  const removed = items.filter(i => i.change === 'removed').length
  return [added && `+${added}`, modified && `~${modified}`, removed && `−${removed}`].filter(Boolean).join(' ')
}

export function SemanticDiffView({ diff }: { diff: SemanticDiff }) {
  const [split, setSplit] = useState(true)

  if (diff.comparison === 'unknown') {
    return (
      <section aria-label="Semantic diff" className="rounded-xl border border-default bg-surface p-4">
        <p role="status" className="text-sm text-muted">
          Unknown comparison — evidence is unavailable; not proof of equality.
        </p>
      </section>
    )
  }

  const categories = [...new Set(diff.items.map(item => item.category))].sort(
    (a, b) => categoryOrder(a) - categoryOrder(b),
  )
  const ToggleIcon = split ? Rows2 : Columns2
  const hasModified = diff.items.some(item => item.change === 'modified')

  return (
    <section aria-label="Semantic diff" className="rounded-xl border border-default bg-surface p-4 space-y-4">
      <div className="flex items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide">Semantic diff</h3>
        {diff.items.length > 0 && hasModified && (
          <button
            type="button"
            onClick={() => setSplit(value => !value)}
            className="inline-flex items-center gap-1 rounded-md border border-default px-2 py-1 text-xs font-medium text-secondary hover:bg-surface-secondary transition-colors"
            title={split ? 'Switch to unified diff' : 'Switch to side-by-side diff'}
          >
            <ToggleIcon className="w-3 h-3" />
            {split ? 'Unified' : 'Side-by-side'}
          </button>
        )}
      </div>

      {diff.comparison === 'different' && !diff.items.length && (
        <p role="status" className="text-sm text-amber-400">
          Comparison incomplete — differences reported but no items returned.
        </p>
      )}
      {diff.comparison === 'equal' && (
        <p className="text-sm text-muted">No changes in this comparison.</p>
      )}

      {categories.map(category => {
        const items = orderItems(diff.items.filter(item => item.category === category))
        return (
          <section key={category} aria-label={category} className="space-y-2">
            <h4 className="flex items-baseline gap-2 text-xs font-semibold uppercase tracking-wide text-secondary">
              {categoryLabel(category)}
              <span className="text-muted normal-case">{items.length} {items.length === 1 ? 'change' : 'changes'}{counts(items) && ` · ${counts(items)}`}</span>
            </h4>
            {items.map((item, index) => (
              <DiffItemCard key={`${item.path}-${index}`} item={item} split={split} />
            ))}
          </section>
        )
      })}
    </section>
  )
}
