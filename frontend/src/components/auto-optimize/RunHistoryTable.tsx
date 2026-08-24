import { useEffect, useState } from "react"
import { RotateCcw, Loader2, AlertCircle, AlertTriangle, CheckCircle2, Info, Trash2 } from "lucide-react"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table"
import {
  ApiError,
  getAutoOptimizeRevertOptions,
  getAutoOptimizeRunsForSpace,
  getCurrentVersion,
  removeAutoOptimizeRunFromHistory,
  revertAutoOptimizeRun,
} from "@/lib/api"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  championAccuracyText,
  defaultRevertTargets,
  hasActiveOptimizationRun,
  hasRevertibleChampion,
  humanizeTerminalReason,
} from "@/components/auto-optimize/runHistory"
import type {
  CurrentVersionResponse,
  GSORevertBenchmarkDiff,
  GSORevertBenchmarkTarget,
  GSORevertConfigTarget,
  GSORevertOptions,
  GSORunSummary,
  VersionMatch,
} from "@/types"

interface RunHistoryTableProps {
  spaceId: string
  onSelectRun: (runId: string) => void
  onLiveStateChanged?: (runId: string) => Promise<boolean>
}

const STATUS_VARIANT: Record<string, "default" | "success" | "warning" | "danger" | "info" | "secondary"> = {
  CONVERGED: "success",
  APPLIED: "success",
  STALLED: "warning",
  MAX_ITERATIONS: "warning",
  FAILED: "danger",
  CANCELLED: "secondary",
  DISCARDED: "secondary",
  SKIPPED: "warning",
  IN_PROGRESS: "info",
  RUNNING: "info",
  QUEUED: "secondary",
}

const HISTORY_ACTION_BUTTON_CLASS =
  "inline-flex w-[10.75rem] items-center justify-center gap-1.5 whitespace-nowrap rounded-md border px-2.5 py-1.5 text-xs font-medium transition-colors"

// Revert is a live-space mutation — only offer it on runs that are no longer
// mutating the space. Reverting to a still-running run's snapshot would race
// the active pipeline (and the backend refuses it with a 409 anyway).
export function RunHistoryTable({ spaceId, onSelectRun, onLiveStateChanged }: RunHistoryTableProps) {
  const [runs, setRuns] = useState<GSORunSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [currentVersion, setCurrentVersion] = useState<CurrentVersionResponse | null>(null)
  const hasActiveRun = hasActiveOptimizationRun(runs)

  useEffect(() => {
    let cancelled = false
    let versionRequestInFlight = false

    function loadCurrentVersion(refresh = false) {
      if (versionRequestInFlight) return
      versionRequestInFlight = true
      getCurrentVersion(spaceId, refresh)
        .then((res) => {
          if (!cancelled) setCurrentVersion(res)
        })
        .catch(() => {
          if (!cancelled) setCurrentVersion(null)
        })
        .finally(() => {
          versionRequestInFlight = false
        })
    }

    getAutoOptimizeRunsForSpace(spaceId)
      .then((res) => {
        if (!cancelled) setRuns(res)
      })
      .catch(() => {
        if (!cancelled) setRuns([])
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    // Independent fetch — the table renders immediately; the live-version
    // badge resolves when the fingerprint check completes.
    loadCurrentVersion()

    // Genie opens in a separate tab for direct edits. Force a fresh live GET
    // when the user returns so an external config or benchmark change is not
    // hidden by the short backend cache or the component's previous result.
    function handlePageReturn() {
      if (document.visibilityState === "visible") loadCurrentVersion(true)
    }
    window.addEventListener("focus", handlePageReturn)
    document.addEventListener("visibilitychange", handlePageReturn)

    return () => {
      cancelled = true
      window.removeEventListener("focus", handlePageReturn)
      document.removeEventListener("visibilitychange", handlePageReturn)
    }
  }, [spaceId])

  function refreshRuns() {
    getAutoOptimizeRunsForSpace(spaceId)
      .then(setRuns)
      .catch(() => setRuns([]))
    // The backend invalidates its live-fingerprint cache on revert, so this
    // refetch moves the badge to the reverted row immediately.
    getCurrentVersion(spaceId)
      .then(setCurrentVersion)
      .catch(() => setCurrentVersion(null))
  }

  function removeRunFromView(runId: string) {
    setRuns((current) => current.filter((run) => run.run_id !== runId))
  }

  const liveMatch =
    currentVersion?.status === "matched" ? currentVersion.current ?? null : null
  const showDimensionMatches =
    currentVersion?.status === "mixed" || currentVersion?.status === "drifted"
  const configMatch = showDimensionMatches ? currentVersion.config_match ?? null : null
  const benchmarkMatch = showDimensionMatches ? currentVersion.benchmark_match ?? null : null

  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization History</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <p className="text-muted text-sm py-4">Loading...</p>
        ) : runs.length === 0 ? (
          <p className="text-muted text-sm py-4">No optimization runs yet.</p>
        ) : (
          <>
          {currentVersion?.status === "drifted" && (
            <DriftBanner
              dimensions={currentVersion.drifted_dimensions ?? []}
              liveUpdateTime={currentVersion.live_update_time}
            />
          )}
          {currentVersion?.status === "mixed" &&
            currentVersion.config_match &&
            currentVersion.benchmark_match && (
              <MixedStateBanner
                configMatch={currentVersion.config_match}
                benchmarkMatch={currentVersion.benchmark_match}
              />
            )}
          {currentVersion?.status === "history_incomplete" && (
            <HistoryIncompleteBanner />
          )}
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Run</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Champion accuracy</TableHead>
                <TableHead>Benchmark handling</TableHead>
                <TableHead className="w-[11.5rem] whitespace-nowrap">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.run_id}>
                  <TableCell className="align-top">
                    <RunMetadataCell run={run} />
                  </TableCell>
                  <TableCell className="align-top">
                    <div className="flex flex-col items-start gap-1">
                      <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>
                        {run.status}
                      </Badge>
                      {liveMatch?.run_id === run.run_id && (
                        <LiveVersionBadge
                          current={liveMatch}
                          equivalents={currentVersion?.also_matches ?? []}
                        />
                      )}
                      {configMatch?.run_id === run.run_id && (
                        <LiveDimensionBadge dimension="config" current={configMatch} />
                      )}
                      {benchmarkMatch?.run_id === run.run_id && (
                        <LiveDimensionBadge dimension="benchmarks" current={benchmarkMatch} />
                      )}
                    </div>
                  </TableCell>
                  <TableCell
                    className="max-w-[14rem] truncate align-top text-sm text-muted"
                    title={humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  >
                    {humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  </TableCell>
                  <TableCell className="align-top text-sm">
                    {championAccuracyText(run.best_accuracy)}
                  </TableCell>
                  <TableCell className="align-top">
                    <BenchmarkPolicyCell run={run} />
                  </TableCell>
                  <TableCell className="w-[11.5rem] align-top">
                    <div className="flex w-[10.75rem] flex-col gap-2">
                      <button
                        onClick={() => onSelectRun(run.run_id)}
                        className={`${HISTORY_ACTION_BUTTON_CLASS} border-default text-primary hover:bg-elevated`}
                      >
                        View Details
                      </button>
                      {(hasRevertibleChampion(run) || run.has_config_snapshot !== false) && (
                        <RevertOptionsButton
                          run={run}
                          disabled={hasActiveRun}
                          onReverted={() => {
                            refreshRuns()
                            void onLiveStateChanged?.(run.run_id)
                          }}
                        />
                      )}
                      <RemoveHistoryButton
                        run={run}
                        disabled={hasActiveOptimizationRun([run])}
                        onRemoved={removeRunFromView}
                      />
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function formatRunDate(iso?: string | null): string {
  if (!iso) return "—"
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "—"
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

/** Compact run provenance so the table can reserve width for outcome/actions. */
export function RunMetadataCell({ run }: { run: GSORunSummary }) {
  return (
    <div className="min-w-[11rem] max-w-[15rem] space-y-0.5">
      <p className="text-sm font-medium text-primary">{formatRunDate(run.started_at)}</p>
      <p className="truncate text-xs text-muted" title={run.llm_model ?? undefined}>
        <span className="font-medium text-secondary">Model:</span> {run.llm_model ?? "—"}
      </p>
      <p className="truncate text-xs text-muted" title={run.triggered_by ?? undefined}>
        <span className="font-medium text-secondary">Triggered by:</span> {run.triggered_by ?? "—"}
      </p>
    </div>
  )
}

interface RevertOptionsButtonProps {
  run: GSORunSummary
  disabled: boolean
  onReverted: () => void
}

interface RemoveHistoryButtonProps {
  run: GSORunSummary
  disabled: boolean
  onRemoved: (runId: string) => void
}

export function BenchmarkPolicyCell({ run }: { run: GSORunSummary }) {
  const count = run.benchmark_mutation_count ?? 0
  if (run.benchmark_policy === "review_only") {
    return (
      <div className="text-xs">
        <span className="font-medium text-primary">Review only</span>
        <p className="text-muted">No live benchmark changes</p>
      </div>
    )
  }
  if (run.benchmark_policy === "repair_allowed") {
    return (
      <div className="text-xs">
        <span className="font-medium text-amber-600 dark:text-amber-400">Repair allowed</span>
        <p className="text-muted">{count === 0 ? "No changes" : `${count} live change${count === 1 ? "" : "s"}`}</p>
      </div>
    )
  }
  return <span className="text-xs text-muted">Legacy / unknown</span>
}

/** Workbench-only removal: preserve the GSO Delta audit trail and live Agent. */
export function RemoveHistoryButton({ run, disabled, onRemoved }: RemoveHistoryButtonProps) {
  const [open, setOpen] = useState(false)
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function doRemove() {
    if (disabled || pending) return
    setPending(true)
    setError(null)
    try {
      await removeAutoOptimizeRunFromHistory(run.run_id)
      setOpen(false)
      onRemoved(run.run_id)
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Failed to remove the optimization run from history.",
      )
    } finally {
      setPending(false)
    }
  }

  return (
    <>
      <button
        onClick={() => {
          if (!disabled) {
            setError(null)
            setOpen(true)
          }
        }}
        disabled={disabled}
        title={disabled ? "Wait for this optimization run to finish before removing it from history." : undefined}
        className={`${HISTORY_ACTION_BUTTON_CLASS} border-danger/40 text-danger-foreground hover:bg-danger/10 disabled:cursor-not-allowed disabled:opacity-50`}
      >
        <Trash2 className="h-3.5 w-3.5" />
        Remove From History
      </button>

      <AlertDialog open={open} onOpenChange={(next) => !pending && setOpen(next)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove optimization run from history?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the run from Workbench Optimization History and the History chart for everyone. It does not change the live Genie Agent, delete the Databricks workflow run, or purge GSO audit records.
            </AlertDialogDescription>
          </AlertDialogHeader>

          {error && (
            <p className="mt-4 flex items-start gap-1.5 text-xs text-danger-foreground">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              {error}
            </p>
          )}

          <AlertDialogFooter>
            <AlertDialogCancel disabled={pending} onClick={() => setOpen(false)}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              disabled={pending}
              onClick={(event) => {
                event.preventDefault()
                void doRemove()
              }}
              className="bg-red-600 hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {pending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Trash2 className="mr-2 h-4 w-4" />}
              Remove from history
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

/** One history action opens both independent revert dimensions. */
export function RevertOptionsButton({ run, disabled, onReverted }: RevertOptionsButtonProps) {
  const [open, setOpen] = useState(false)
  const [options, setOptions] = useState<GSORevertOptions | null>(null)
  const [loading, setLoading] = useState(false)
  const [pending, setPending] = useState(false)
  const [success, setSuccess] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [configTarget, setConfigTarget] = useState<GSORevertConfigTarget>(
    hasRevertibleChampion(run) ? "champion" : "baseline",
  )
  const [benchmarkTarget, setBenchmarkTarget] = useState<GSORevertBenchmarkTarget>("champion")

  function showOptions() {
    if (disabled) return
    setOpen(true)
    setLoading(true)
    setError(null)
    setOptions(null)
    getAutoOptimizeRevertOptions(run.run_id)
      .then((preview) => {
        setOptions(preview)
        const defaults = defaultRevertTargets(preview)
        setConfigTarget(defaults.configTarget)
        setBenchmarkTarget(defaults.benchmarkTarget)
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : "Failed to load revert options.")
      })
      .finally(() => setLoading(false))
  }

  async function doRevert() {
    if (disabled || !options) return
    setPending(true)
    setError(null)
    try {
      await revertAutoOptimizeRun(run.run_id, { configTarget, benchmarkTarget })
      setOpen(false)
      setSuccess(true)
      onReverted()
      setTimeout(() => setSuccess(false), 4000)
    } catch (e) {
      const message =
        e instanceof ApiError
          ? e.message
          : e instanceof Error
            ? e.message
            : "Failed to revert the Genie Agent."
      setError(message)
    } finally {
      setPending(false)
    }
  }

  const selectionAvailable = options != null
    && (configTarget === "champion" ? options.championAvailable : options.baselineAvailable)
    && (
      benchmarkTarget === "current"
      || (benchmarkTarget === "champion" && options.benchmarkChampionAvailable)
      || (benchmarkTarget === "baseline" && options.benchmarkBaselineAvailable)
    )

  if (success) {
    return (
      <span className="flex items-center gap-1 text-xs font-medium text-emerald-600 dark:text-emerald-400">
        <CheckCircle2 className="h-3.5 w-3.5" />
        Reverted
      </span>
    )
  }

  return (
    <>
      <button
        onClick={showOptions}
        disabled={disabled}
        title={disabled ? "Wait for the active optimization on this agent to finish before reverting history." : undefined}
        className={`${HISTORY_ACTION_BUTTON_CLASS} border-default text-primary hover:bg-elevated disabled:cursor-not-allowed disabled:opacity-50`}
      >
        <RotateCcw className="h-3.5 w-3.5" />
        Revert Options
      </button>
      <AlertDialog open={open} onOpenChange={(next) => !pending && setOpen(next)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revert Options</AlertDialogTitle>
            <AlertDialogDescription>
              Choose the agent configuration and benchmark state independently. This overwrites the live Genie Agent.
            </AlertDialogDescription>
          </AlertDialogHeader>

          {loading ? (
            <p className="mt-5 flex items-center gap-2 text-sm text-muted">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading available states…
            </p>
          ) : options ? (
            <div className="mt-5 space-y-5">
              <fieldset className="space-y-2">
                <legend className="text-sm font-semibold text-primary">Agent config</legend>
                <RadioOption
                  name={`config-target-${run.run_id}`}
                  checked={configTarget === "champion"}
                  disabled={!options.championAvailable}
                  onChange={() => setConfigTarget("champion")}
                  label="Champion"
                  detail="Restore this run's winning optimized configuration."
                />
                <RadioOption
                  name={`config-target-${run.run_id}`}
                  checked={configTarget === "baseline"}
                  disabled={!options.baselineAvailable}
                  onChange={() => setConfigTarget("baseline")}
                  label="Pre-run baseline"
                  detail="Restore the configuration captured before this run started."
                />
              </fieldset>

              <fieldset className="space-y-2">
                <legend className="text-sm font-semibold text-primary">Benchmarks</legend>
                <RadioOption
                  name={`benchmark-target-${run.run_id}`}
                  checked={benchmarkTarget === "champion"}
                  disabled={!options.benchmarkChampionAvailable}
                  onChange={() => setBenchmarkTarget("champion")}
                  label="Champion iteration"
                  detail={benchmarkDiffText(options.benchmarkDiffs.champion, "champion")}
                />
                <RadioOption
                  name={`benchmark-target-${run.run_id}`}
                  checked={benchmarkTarget === "current"}
                  onChange={() => setBenchmarkTarget("current")}
                  label="Preserve current benchmarks"
                  detail="Keep the live benchmark set exactly as it is now."
                />
                <RadioOption
                  name={`benchmark-target-${run.run_id}`}
                  checked={benchmarkTarget === "baseline"}
                  disabled={!options.benchmarkBaselineAvailable}
                  onChange={() => setBenchmarkTarget("baseline")}
                  label="Restore pre-run baseline"
                  detail={benchmarkDiffText(options.benchmarkDiffs.baseline, "baseline")}
                />
              </fieldset>

              <div className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-primary">
                The live agent config will be replaced with the selected {configTarget} state
                {benchmarkTarget === "current"
                  ? ", while current benchmarks are preserved."
                  : benchmarkTarget === "champion"
                    ? ", including the champion iteration's benchmarks."
                    : ", including pre-run baseline benchmarks."}
              </div>
            </div>
          ) : null}

          {error && (
            <p className="mt-4 flex items-start gap-1.5 text-xs text-danger-foreground">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              {error}
            </p>
          )}

          <AlertDialogFooter>
            <AlertDialogCancel disabled={pending} onClick={() => setOpen(false)}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={loading || pending || !selectionAvailable}
              onClick={(event) => {
                // Radix closes AlertDialogAction synchronously by default.
                // Keep the dialog mounted so API errors remain visible.
                event.preventDefault()
                void doRevert()
              }}
              className="bg-red-600 hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {pending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RotateCcw className="mr-2 h-4 w-4" />}
              Revert agent
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function RadioOption({
  name,
  checked,
  disabled = false,
  onChange,
  label,
  detail,
}: {
  name: string
  checked: boolean
  disabled?: boolean
  onChange: () => void
  label: string
  detail: string
}) {
  return (
    <label className={`flex items-start gap-2 rounded-md border p-3 ${disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer"}`}>
      <input name={name} type="radio" checked={checked} disabled={disabled} onChange={onChange} className="mt-0.5" />
      <span>
        <span className="block text-sm font-medium text-primary">{label}</span>
        <span className="block text-xs text-muted">{detail}</span>
      </span>
    </label>
  )
}

function benchmarkDiffText(
  diff: GSORevertBenchmarkDiff,
  targetLabel: "champion" | "baseline",
): string {
  if (diff.willAdd === 0 && diff.willRemove === 0 && diff.willChange === 0) {
    return `The ${targetLabel} matches the current ${diff.currentCount} benchmarks.`
  }
  return `Restore ${diff.targetCount} ${targetLabel} benchmarks: add ${diff.willAdd}, remove ${diff.willRemove}, and update ${diff.willChange}.`
}

// ---------------------------------------------------------------------------
// Current-version indicators (live-config fingerprint match)
// ---------------------------------------------------------------------------

function formatMatchDate(iso?: string | null): string {
  if (!iso) return ""
  const d = new Date(iso)
  return isNaN(d.getTime())
    ? ""
    : d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" })
}

/**
 * Green pill on the history row whose config and benchmarks both match the
 * live agent. Equivalent complete versions are listed in the tooltip.
 */
export function LiveVersionBadge({
  current,
  equivalents,
}: {
  current: VersionMatch
  equivalents: VersionMatch[]
}) {
  const equivText = equivalents.length
    ? ` Identical to: ${equivalents
        .map((m) => {
          const when = formatMatchDate(m.started_at)
          return `${m.target} of the ${when ? `${when} ` : ""}run`
        })
        .join("; ")}.`
    : ""
  return (
    <Badge
      variant="success"
      className="gap-1.5"
      title={`The live agent configuration and benchmarks currently match this run's ${current.target} state.${equivText}`}
    >
      <span className="inline-block h-1.5 w-1.5 rounded-full bg-success" />
      Live — {current.target}
    </Badge>
  )
}

/**
 * Pill for one independently matched component of a mixed or partially drifted
 * live state.
 */
export function LiveDimensionBadge({
  dimension,
  current,
}: {
  dimension: "config" | "benchmarks"
  current: VersionMatch
}) {
  const label = dimension === "config" ? "config" : "benchmarks"
  const verb = dimension === "config" ? "matches" : "match"
  return (
    <Badge
      variant={dimension === "config" ? "info" : "secondary"}
      title={`The live agent ${label} ${verb} this run's ${current.target} state.`}
    >
      Live {label} — {current.target}
    </Badge>
  )
}

/** Neutral notice when both live components are known but come from different versions. */
export function MixedStateBanner({
  configMatch,
  benchmarkMatch,
}: {
  configMatch: VersionMatch
  benchmarkMatch: VersionMatch
}) {
  const configDate = formatMatchDate(configMatch.started_at)
  const benchmarkDate = formatMatchDate(benchmarkMatch.started_at)
  return (
    <div className="mb-3 flex items-start gap-2 rounded-md border border-border bg-muted/40 px-3 py-2 text-sm text-primary">
      <Info className="mt-0.5 h-4 w-4 shrink-0 text-info-foreground" />
      <p>
        The live agent combines the {configMatch.target} config
        {configDate ? ` from the ${configDate} run` : ""} with {" "}
        {benchmarkMatch.target} benchmarks
        {benchmarkDate ? ` from the ${benchmarkDate} run` : ""}. Both are known
        states, but they come from different optimization versions.
      </p>
    </div>
  )
}

/** Amber warning when one or both live components match no known version. */
export function DriftBanner({
  dimensions,
  liveUpdateTime,
}: {
  dimensions: Array<"config" | "benchmarks">
  liveUpdateTime?: string | null
}) {
  const when = formatMatchDate(liveUpdateTime)
  const subject =
    dimensions.length === 1 && dimensions[0] === "config"
      ? "configuration"
      : dimensions.length === 1 && dimensions[0] === "benchmarks"
        ? "benchmarks"
        : "configuration and benchmarks"
  const plural = subject !== "configuration"
  return (
    <div className="mb-3 flex items-start gap-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm text-primary">
      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warning-foreground" />
      <p>
        The live agent {subject} {plural ? "don’t" : "doesn’t"} match any known
        optimization version &mdash; {plural ? "they were" : "it was"} changed
        outside Auto-Optimize{when ? ` (last modified ${when})` : ""}.
      </p>
    </div>
  )
}

/** Neutral notice for legacy/partial history where external drift is unknown. */
export function HistoryIncompleteBanner() {
  return (
    <div className="mb-3 flex items-start gap-2 rounded-md border border-border bg-muted/40 px-3 py-2 text-sm text-muted-foreground">
      <Info className="mt-0.5 h-4 w-4 shrink-0" />
      <p>
        Some optimization versions predate authoritative configuration or benchmark capture,
        so the current live state can&rsquo;t be determined reliably.
      </p>
    </div>
  )
}
