import { useCallback, useEffect, useRef, useState } from 'react'
import { Camera, GitBranch, Info, RefreshCw } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Tooltip } from '@/components/ui/tooltip'
import { VersionControlApi, VersionControlError } from '@/lib/version-control-api'
import type { ObservationResult, SemanticDiff, VersionDetail, VersionPage, VersionSummary, VersionTagMap } from '@/types/version-control'
import { type CaptureNotice, describeCaptureError, describeObservation } from './capture-notice'
import { History } from './history'
import { VersionDetailPanel } from './version-detail-panel'
import { SemanticDiffView } from './diff'
import { shortId } from './version-format'
import { chronoPair } from './compare-order'

function errorMessage(err: unknown, fallback: string): string {
  return err instanceof VersionControlError ? err.message : fallback
}

// Real (non-demo) Version Control client. The observe surface is fully fail-closed on
// the backend: unless the deployment enables the VC flags, these calls return 503 and
// the tab renders an informative empty state.
const api = new VersionControlApi((input, init) => fetch(input, init))

const EMPTY_PAGE: VersionPage = { items: [], next_cursor: null }

// Static "read once" explainer — surfaced via an info tooltip (and aria-label) instead of a
// permanent multi-line paragraph, to keep the header compact and the config area tall.
const CAPTURE_EXPLAINER =
  'A version is auto-captured whenever you open this tab and after each optimizer run. ' +
  'Edits made directly in Genie are captured the next time you open this tab — there is no background watcher.'

interface Props {
  spaceId: string
}

export function SpaceVersionControlTab({ spaceId }: Props) {
  const [page, setPage] = useState<VersionPage>(EMPTY_PAGE)
  const [tags, setTags] = useState<VersionTagMap>({})
  const [loading, setLoading] = useState(false)
  const [capturing, setCapturing] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<CaptureNotice | null>(null)
  const [detail, setDetail] = useState<VersionDetail | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const detailRef = useRef<HTMLDivElement | null>(null)
  const [diff, setDiff] = useState<SemanticDiff | null>(null)
  const [diffError, setDiffError] = useState<string | null>(null)
  // Checkbox multi-select compare (shown in the right pane, independent of the restore diff).
  const [compareIds, setCompareIds] = useState<string[]>([])
  const [compareDiff, setCompareDiff] = useState<SemanticDiff | null>(null)
  const [compareDiffError, setCompareDiffError] = useState<string | null>(null)
  const [compareLoading, setCompareLoading] = useState(false)
  const [restoreEnabled, setRestoreEnabled] = useState(false)
  const [pendingRestore, setPendingRestore] = useState<VersionSummary | null>(null)
  const [restoring, setRestoring] = useState(false)
  const [restoreError, setRestoreError] = useState<string | null>(null)

  const load = useCallback(async (cursor?: string): Promise<VersionPage | null> => {
    setLoading(true)
    setError(null)
    try {
      const next = await api.spaceVersions(spaceId, cursor)
      setPage(prev => cursor
        ? { items: [...prev.items, ...next.items], next_cursor: next.next_cursor }
        : next)
      return next
    } catch (err) {
      setPage(EMPTY_PAGE)
      setError(err instanceof VersionControlError ? err.message : 'Failed to load version history.')
      return null
    } finally {
      setLoading(false)
    }
  }, [spaceId])

  // Auto-capture on open: fire a single observation when the tab opens (or the space
  // switches) so edits made outside the workbench surface without a manual click. It is
  // idempotent — the backend dedups on the config fingerprint, so an unchanged space appends
  // no new version. Unlike the earlier silent version, this shows a visible "checking…"
  // status and a terminal notice, so the user is never left staring at an unexplained lag
  // (the slow step is the OBO live GET of the Genie space). The history read below remains
  // the source of truth for what renders.
  const syncOnOpen = useCallback(async () => {
    // Paint the persisted history immediately — the fast read is the source of truth for
    // what renders. The observe below is the slow step (an OBO live GET of the Genie space);
    // it runs in the background and appends a new version only if the live config drifted.
    // Snapshot the ids we already have BEFORE observing: the observer's dedup branch returns
    // the existing head as `captured_version` on an unchanged open, so newness is decided by
    // "is this id one we didn't already have", not "did a version object come back".
    const before = await load()
    const knownIds = new Set((before?.items ?? []).map(version => version.version_id))
    setSyncing(true)
    setNotice(null)
    try {
      const result = await api.spaceObserve(spaceId, crypto.randomUUID())
      setNotice(describeObservation(result, knownIds))
      await load()
    } catch (err) {
      setNotice(describeCaptureError(err))
    } finally {
      setSyncing(false)
    }
  }, [spaceId, load])

  useEffect(() => {
    setNotice(null)
    setDetail(null)
    setSelectedId(null)
    setDetailError(null)
    setDiff(null)
    setDiffError(null)
    setCompareIds([])
    setCompareDiff(null)
    setCompareDiffError(null)
    setPendingRestore(null)
    setRestoreError(null)
    void syncOnOpen()
  }, [syncOnOpen])

  // Bring the detail into view when opening a version (matters on stacked/small layouts).
  useEffect(() => {
    if (selectedId) detailRef.current?.scrollIntoView({ block: 'nearest' })
  }, [selectedId])

  // Auto-dismiss transient success/info notices; keep errors on screen until the next action.
  useEffect(() => {
    if (!notice || notice.tone === 'error') return
    const timer = setTimeout(() => setNotice(null), 5000)
    return () => clearTimeout(timer)
  }, [notice])

  // Deployment flags (whether restore is enabled) — best-effort; default disabled.
  useEffect(() => {
    let live = true
    api.config().then(cfg => { if (live) setRestoreEnabled(cfg.restore_enabled) }).catch(() => {})
    return () => { live = false }
  }, [])

  // Version tags — best-effort; a read failure never blocks the rail. Guarded so a slow
  // response for a previous space cannot set stale tags after a rapid spaceId switch.
  useEffect(() => {
    let live = true
    setTags({})
    api.spaceTags(spaceId).then(next => { if (live) setTags(next) }).catch(() => {})
    return () => { live = false }
  }, [spaceId])

  const selectVersion = useCallback(async (version: VersionSummary) => {
    // Toggle: clicking the open row collapses it.
    if (selectedId === version.version_id) {
      setSelectedId(null)
      setDetail(null)
      setDetailError(null)
      return
    }
    // Mark active immediately; keep the previous detail visible (dimmed) while the next
    // one loads so the panel never blanks to nothing between selections.
    setSelectedId(version.version_id)
    setDetailError(null)
    setDetailLoading(true)
    try {
      setDetail(await api.version(version.binding_id, version.version_id))
    } catch (err) {
      setDetail(null)
      setDetailError(errorMessage(err, 'Failed to load version detail.'))
    } finally {
      setDetailLoading(false)
    }
  }, [selectedId])

  const closeDetail = useCallback(() => {
    setSelectedId(null)
    setDetail(null)
    setDetailError(null)
  }, [])

  // Tag write handlers: mutate via the Task 4 endpoints, then refresh the whole tag map
  // so the rail badge and detail editor reflect the authoritative server state.
  const setTag = useCallback(async (versionId: string, label: string, note: string | null): Promise<boolean> => {
    try {
      await api.setVersionTag(spaceId, versionId, { label, note })
      setTags(await api.spaceTags(spaceId))
      setNotice({ tone: 'success', message: 'Tag saved.' })
      return true
    } catch (err) {
      setNotice({ tone: 'error', message: errorMessage(err, 'Failed to save the tag.') })
      return false
    }
  }, [spaceId])
  const removeTag = useCallback(async (versionId: string): Promise<boolean> => {
    try {
      await api.deleteVersionTag(spaceId, versionId)
      setTags(await api.spaceTags(spaceId))
      setNotice({ tone: 'success', message: 'Tag removed.' })
      return true
    } catch (err) {
      setNotice({ tone: 'error', message: errorMessage(err, 'Failed to remove the tag.') })
      return false
    }
  }, [spaceId])

  // Toggle a version into the compare set; cap at two (rolling — the newest two win).
  const toggleCompare = useCallback((versionId: string) => {
    setCompareIds(prev => prev.includes(versionId)
      ? prev.filter(id => id !== versionId)
      : [...prev, versionId].slice(-2))
  }, [])

  // Fetch the diff for the right pane whenever exactly two versions are selected.
  useEffect(() => {
    if (compareIds.length !== 2) { setCompareDiff(null); setCompareDiffError(null); return }
    const bindingId = page.items[0]?.binding_id
    if (!bindingId) return
    let live = true
    setCompareDiff(null)
    setCompareDiffError(null)
    setCompareLoading(true)
    const [olderId, newerId] = chronoPair(compareIds, page.items)
    api.diff(bindingId, olderId, newerId)
      .then(d => { if (live) setCompareDiff(d) })
      .catch(err => { if (live) setCompareDiffError(errorMessage(err, 'Failed to compare versions.')) })
      .finally(() => { if (live) setCompareLoading(false) })
    return () => { live = false }
  }, [compareIds, page.items])

  const compareVersions = useCallback(async (left: string, right: string) => {
    const bindingId = page.items[0]?.binding_id
    if (!bindingId) return
    setDiff(null)
    setDiffError(null)
    try {
      setDiff(await api.diff(bindingId, left, right))
    } catch (err) {
      setDiffError(errorMessage(err, 'Failed to compare versions.'))
    }
  }, [page.items])

  // Publish = in-workspace restore (CUJ-1 §4.5): preview current→selected, confirm, apply.
  const beginRestore = useCallback((version: VersionSummary) => {
    const current = page.items[0]?.version_id
    setRestoreError(null)
    setPendingRestore(version)
    if (current && current !== version.version_id) void compareVersions(current, version.version_id)
    else setDiff(null)
  }, [page.items, compareVersions])

  const confirmRestore = useCallback(async () => {
    const current = page.items[0]?.version_id
    if (!pendingRestore || !current) return
    setRestoring(true)
    setRestoreError(null)
    try {
      const result = await api.spaceRestore(
        spaceId,
        { version_id: pendingRestore.version_id, expected_current_version_id: current },
        crypto.randomUUID(),
      )
      setNotice(result.captured_version
        ? { tone: 'success', message: 'Restored this version as the live configuration.' }
        : { tone: 'info', message: 'The live space already matches this version.' })
      setPendingRestore(null)
      setDetail(null)
      setSelectedId(null)
      setDiff(null)
      await load()
    } catch (err) {
      // Surface the server's SPECIFIC reason (which component drifted, or a stale view)
      // rather than a blanket sentence. On a 409 the user's view is out of date, so reload
      // the history and re-open the diff against the true current head so they can act on it.
      setRestoreError(errorMessage(err, 'Restore failed.'))
      if (err instanceof VersionControlError && err.status === 409) {
        const refreshed = await load()
        const head = refreshed?.items[0]?.version_id
        if (head && pendingRestore && head !== pendingRestore.version_id) {
          void compareVersions(head, pendingRestore.version_id)
        }
      }
    } finally {
      setRestoring(false)
    }
  }, [spaceId, pendingRestore, page.items, load, compareVersions])

  const capture = useCallback(async () => {
    setCapturing(true)
    setError(null)
    setNotice(null)
    // Ids already shown before this manual capture — an unchanged capture returns the head
    // (already in this set) and must read as "no changes", not "saved a new version".
    const knownIds = new Set(page.items.map(version => version.version_id))
    try {
      const result: ObservationResult = await api.spaceObserve(spaceId, crypto.randomUUID())
      setNotice(describeObservation(result, knownIds))
      await load()
    } catch (err) {
      setNotice(describeCaptureError(err))
    } finally {
      setCapturing(false)
    }
  }, [spaceId, page.items, load])

  return (
    <div className="space-y-3">
      {/* One-row header: identity + info tooltip on the left, live-status + actions on the
          right. Keeps the config master–detail as high on the page as possible. */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2 min-w-0">
          <span className="shrink-0 p-2 rounded-lg border border-default bg-surface-secondary text-muted">
            <GitBranch className="w-4 h-4" />
          </span>
          <h3 className="text-lg font-display font-semibold text-primary">Version Control</h3>
          <Tooltip content={<span className="block max-w-xs text-left">{CAPTURE_EXPLAINER}</span>}>
            <span
              aria-label={CAPTURE_EXPLAINER}
              className="text-muted hover:text-secondary transition-colors cursor-help"
            >
              <Info className="w-4 h-4" />
            </span>
          </Tooltip>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {(syncing || capturing) && (
            <span role="status" className="inline-flex items-center gap-1.5 text-xs text-muted">
              <RefreshCw className="w-3.5 h-3.5 animate-spin" />
              {capturing ? 'Capturing…' : 'Checking…'}
            </span>
          )}
            {/* The compact icon and the labeled button run the same action — observe the
                live Genie space (source-check + record). The icon is the quick repeat
                affordance; "Capture current state" is the explicit, discoverable primary. */}
            <button
              onClick={capture}
              disabled={capturing || syncing}
              className="p-2 rounded-lg border border-default text-muted hover:text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50"
              title="Check the live space for changes"
            >
              <RefreshCw className={`w-4 h-4 ${(capturing || syncing) ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={capture}
              disabled={capturing || syncing}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-lg border border-default text-sm font-medium text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50"
            >
              <Camera className="w-4 h-4" />
              {capturing ? 'Capturing…' : 'Capture current state'}
            </button>
          </div>
      </div>

      <div className="space-y-3">
          {error && (
            <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
              {error}
            </div>
          )}
          {notice && !syncing && !capturing && (
            <div
              role="status"
              className={cn(
                'text-sm rounded-lg border px-3 py-2',
                notice.tone === 'success'
                  ? 'border-green-500/30 bg-green-500/10 text-green-400'
                  : notice.tone === 'error'
                    ? 'border-red-500/30 bg-red-500/10 text-red-400'
                    : 'border-default bg-surface-secondary text-muted',
              )}
            >
              {notice.message}
            </div>
          )}

          {/* Master–detail: narrow versions rail (left) + wide detail (right). Each pane
              scrolls on its own at lg+; below lg they stack and the page scrolls. */}
          <div className="grid gap-4 lg:grid-cols-[320px_minmax(0,1fr)] lg:items-start">
            <div className="rounded-xl border border-default bg-surface p-3 lg:h-[78vh] lg:overflow-auto">
              <History
                page={page}
                loading={loading}
                selectedId={selectedId}
                currentId={page.items[0]?.version_id ?? null}
                onNext={() => { if (page.next_cursor) void load(page.next_cursor) }}
                onSelect={version => { void selectVersion(version) }}
                compareIds={compareIds}
                onToggleCompare={toggleCompare}
                tags={tags}
              />
            </div>

            <div ref={detailRef} className="min-w-0 lg:h-[78vh]">
              {detailError && (
                <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
                  {detailError}
                </div>
              )}
              {compareIds.length === 2 ? (
                <div className="flex h-full flex-col rounded-xl border border-default bg-surface">
                  <header className="shrink-0 flex flex-wrap items-center gap-2 border-b border-default p-4">
                    <span className="text-xs font-semibold uppercase tracking-wide text-secondary">Comparing</span>
                    {(() => {
                      const [beforeId, afterId] = chronoPair(compareIds, page.items)
                      return <span className="font-mono text-xs text-secondary">{shortId(beforeId)} → {shortId(afterId)}</span>
                    })()}
                    <button
                      type="button"
                      onClick={() => setCompareIds([])}
                      className="ml-auto inline-flex items-center gap-1 rounded-md border border-default px-2.5 py-1 text-xs font-medium text-secondary hover:bg-surface-secondary transition-colors"
                    >
                      Clear selection
                    </button>
                  </header>
                  <div className="min-h-0 flex-1 overflow-auto p-4">
                    {compareDiffError ? (
                      <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
                        {compareDiffError}
                      </div>
                    ) : compareLoading ? (
                      <p className="text-sm text-muted">Comparing…</p>
                    ) : compareDiff ? (
                      <SemanticDiffView diff={compareDiff} />
                    ) : null}
                  </div>
                </div>
              ) : detail ? (
                <div className={cn('h-full', detailLoading ? 'opacity-60 transition-opacity' : 'transition-opacity')}>
                  <VersionDetailPanel
                    key={detail.version_id}
                    detail={detail}
                    isCurrent={detail.version_id === page.items[0]?.version_id}
                    onClose={closeDetail}
                    onRestore={() => beginRestore(detail)}
                    restoreEnabled={restoreEnabled}
                    restoring={restoring && pendingRestore?.version_id === detail.version_id}
                    awaitingConfirm={pendingRestore?.version_id === detail.version_id}
                    restoreError={pendingRestore?.version_id === detail.version_id ? restoreError : null}
                    onConfirmRestore={() => { void confirmRestore() }}
                    onCancelRestore={() => { setPendingRestore(null); setRestoreError(null) }}
                    tag={tags[detail.version_id]}
                    onSetTag={(label, note) => setTag(detail.version_id, label, note)}
                    onRemoveTag={() => removeTag(detail.version_id)}
                  />
                </div>
              ) : !detailError && (
                <div className="flex h-full items-center justify-center rounded-xl border border-dashed border-default bg-surface p-6 text-center text-sm text-muted">
                  {detailLoading ? 'Loading version…' : 'Select a version to inspect its configuration and restore it, or tick two versions to compare.'}
                </div>
              )}
            </div>
          </div>

          {/* Restore confirmation now renders inline inside VersionDetailPanel, adjacent to
              its trigger (issue #1). The diff preview (current -> selected) stays here in the
              wide area below the grid where there is room for it. */}
          {diffError && (
            <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
              {diffError}
            </div>
          )}
          {diff && <SemanticDiffView diff={diff} />}
      </div>
    </div>
  )
}
