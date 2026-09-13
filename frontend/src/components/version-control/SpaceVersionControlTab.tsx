import { useCallback, useEffect, useRef, useState } from 'react'
import { Camera, GitBranch, RefreshCw, Upload } from 'lucide-react'
import { cn } from '@/lib/utils'
import { VersionControlApi, VersionControlError } from '@/lib/version-control-api'
import type { ObservationResult, SemanticDiff, VersionDetail, VersionPage, VersionSummary, VersionTagMap } from '@/types/version-control'
import { type CaptureNotice, describeCaptureError, describeObservation } from './capture-notice'
import { History } from './history'
import { VersionDetailPanel } from './version-detail-panel'
import { SemanticDiffView } from './diff'
import { shortId } from './version-format'
import { chronoPair } from './compare-order'

type SubTab = 'versions' | 'promote'

function errorMessage(err: unknown, fallback: string): string {
  return err instanceof VersionControlError ? err.message : fallback
}

// Real (non-demo) Version Control client. The observe surface is fully fail-closed on
// the backend: unless the deployment enables the VC flags, these calls return 503 and
// the tab renders an informative empty state.
const api = new VersionControlApi((input, init) => fetch(input, init))

const EMPTY_PAGE: VersionPage = { items: [], next_cursor: null }

interface Props {
  spaceId: string
}

export function SpaceVersionControlTab({ spaceId }: Props) {
  const [subTab, setSubTab] = useState<SubTab>('versions')
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

  const load = useCallback(async (cursor?: string) => {
    setLoading(true)
    setError(null)
    try {
      const next = await api.spaceVersions(spaceId, cursor)
      setPage(prev => cursor
        ? { items: [...prev.items, ...next.items], next_cursor: next.next_cursor }
        : next)
    } catch (err) {
      setPage(EMPTY_PAGE)
      setError(err instanceof VersionControlError ? err.message : 'Failed to load version history.')
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
    await load()
    setSyncing(true)
    setNotice(null)
    try {
      setNotice(describeObservation(await api.spaceObserve(spaceId, crypto.randomUUID())))
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
  const setTag = useCallback(async (versionId: string, label: string, note: string | null) => {
    try {
      await api.setVersionTag(spaceId, versionId, { label, note })
      setTags(await api.spaceTags(spaceId))
    } catch (err) {
      setNotice({ tone: 'error', message: errorMessage(err, 'Failed to save the tag.') })
    }
  }, [spaceId])
  const removeTag = useCallback(async (versionId: string) => {
    try {
      await api.deleteVersionTag(spaceId, versionId)
      setTags(await api.spaceTags(spaceId))
    } catch (err) {
      setNotice({ tone: 'error', message: errorMessage(err, 'Failed to remove the tag.') })
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
      setRestoreError(err instanceof VersionControlError && err.status === 409
        ? 'The space changed since you opened this view. Refresh the history and try again.'
        : errorMessage(err, 'Restore failed.'))
    } finally {
      setRestoring(false)
    }
  }, [spaceId, pendingRestore, page.items, load])

  const capture = useCallback(async () => {
    setCapturing(true)
    setError(null)
    setNotice(null)
    try {
      const result: ObservationResult = await api.spaceObserve(spaceId, crypto.randomUUID())
      setNotice(describeObservation(result))
      await load()
    } catch (err) {
      setNotice(describeCaptureError(err))
    } finally {
      setCapturing(false)
    }
  }, [spaceId, load])

  const subTabs: { id: SubTab; label: string }[] = [
    { id: 'versions', label: 'Versions' },
    { id: 'promote', label: 'Promote' },
  ]

  return (
    <div className="space-y-4">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 p-2 rounded-lg border border-default bg-surface-secondary text-muted">
          <GitBranch className="w-4 h-4" />
        </span>
        <div>
          <h3 className="text-lg font-display font-semibold text-primary">Version Control</h3>
          <p className="text-sm text-muted mt-1 max-w-2xl">
            Manage this agent's configuration versions in-workspace, and promote an approved
            version to another workspace you manage.
          </p>
          <p className="text-xs text-muted mt-1.5 max-w-2xl">
            A version is auto-captured whenever you open this tab and after each optimizer run.
            Edits made directly in Genie are captured the next time you open this tab — there is
            no background watcher.
          </p>
        </div>
      </div>

      <div className="inline-flex rounded-lg border border-default bg-surface-secondary p-0.5">
        {subTabs.map(tab => (
          <button
            key={tab.id}
            type="button"
            onClick={() => setSubTab(tab.id)}
            className={cn(
              'px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
              subTab === tab.id
                ? 'bg-surface text-primary shadow-sm'
                : 'text-muted hover:text-secondary',
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {subTab === 'versions' && (
        <div className="space-y-4">
          <div className="flex items-center justify-end gap-2">
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

          {(syncing || capturing) && (
            <div role="status" className="flex items-center gap-2 text-sm rounded-lg border border-default bg-surface-secondary text-muted px-3 py-2">
              <RefreshCw className="w-4 h-4 animate-spin" />
              {capturing ? 'Capturing the current configuration…' : 'Checking the live configuration for changes…'}
            </div>
          )}
          {error && (
            <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
              {error}
            </div>
          )}
          {notice && (
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
            <div className="rounded-xl border border-default bg-surface p-3 lg:h-[70vh] lg:overflow-auto">
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

            <div ref={detailRef} className="min-w-0 lg:h-[70vh]">
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
                    tag={tags[detail.version_id]}
                    onSetTag={(label, note) => { void setTag(detail.version_id, label, note) }}
                    onRemoveTag={() => { void removeTag(detail.version_id) }}
                  />
                </div>
              ) : !detailError && (
                <div className="flex h-full items-center justify-center rounded-xl border border-dashed border-default bg-surface p-6 text-center text-sm text-muted">
                  {detailLoading ? 'Loading version…' : 'Select a version to inspect its configuration and restore it, or tick two versions to compare.'}
                </div>
              )}
            </div>
          </div>

          {pendingRestore && (
            <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 space-y-3">
              <p className="text-sm text-secondary">
                Restore version <span className="font-mono text-primary">{shortId(pendingRestore.version_id)}</span> as
                the live configuration? It is applied as a new version — history is preserved. Review the changes below.
              </p>
              {restoreError && (
                <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
                  {restoreError}
                </div>
              )}
              <div className="flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => { setPendingRestore(null); setRestoreError(null) }}
                  disabled={restoring}
                  className="px-3 py-2 rounded-lg border border-default text-sm font-medium text-muted hover:text-secondary hover:bg-surface-secondary transition-colors disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => { void confirmRestore() }}
                  disabled={restoring}
                  className="px-3 py-2 rounded-lg bg-amber-500/20 border border-amber-500/40 text-sm font-medium text-amber-300 hover:bg-amber-500/30 transition-colors disabled:opacity-50"
                >
                  {restoring ? 'Restoring…' : 'Confirm restore'}
                </button>
              </div>
            </div>
          )}

          {diffError && (
            <div role="alert" className="text-sm rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 px-3 py-2">
              {diffError}
            </div>
          )}
          {diff && <SemanticDiffView diff={diff} />}
        </div>
      )}

      {subTab === 'promote' && (
        <div className="rounded-xl border border-default bg-surface p-4">
          <div className="text-center py-16 text-muted">
            <Upload className="w-8 h-8 mx-auto mb-3 opacity-50" />
            <p className="text-secondary font-medium">Cross-workspace promotion</p>
            <p className="text-sm mt-1 max-w-md mx-auto">
              Promote an approved version to another workspace you manage, with two-person
              approval and a durable deployment receipt. This surface is not enabled on this
              deployment yet.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
