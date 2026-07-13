import { useEffect, useState } from "react"
import { RotateCcw, Loader2, AlertCircle, CheckCircle2, Trophy, History } from "lucide-react"
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
import { getAutoOptimizeRunsForSpace, revertAutoOptimizeRun, ApiError } from "@/lib/api"
import {
  championAccuracyText,
  hasActiveOptimizationRun,
  humanizeTerminalReason,
} from "@/components/auto-optimize/runHistory"
import type { GSORunSummary } from "@/types"

interface RunHistoryTableProps {
  spaceId: string
  onSelectRun: (runId: string) => void
}

const STATUS_VARIANT: Record<string, "default" | "success" | "warning" | "danger" | "info" | "secondary"> = {
  CONVERGED: "success",
  APPLIED: "success",
  STALLED: "warning",
  MAX_ITERATIONS: "warning",
  FAILED: "danger",
  CANCELLED: "secondary",
  DISCARDED: "secondary",
  IN_PROGRESS: "info",
  RUNNING: "info",
  QUEUED: "secondary",
}

// Revert is a live-space mutation — only offer it on runs that are no longer
// mutating the space. Reverting to a still-running run's snapshot would race
// the active pipeline (and the backend refuses it with a 409 anyway).
export function RunHistoryTable({ spaceId, onSelectRun }: RunHistoryTableProps) {
  const [runs, setRuns] = useState<GSORunSummary[]>([])
  const [loading, setLoading] = useState(true)
  const hasActiveRun = hasActiveOptimizationRun(runs)

  useEffect(() => {
    let cancelled = false
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
    return () => {
      cancelled = true
    }
  }, [spaceId])

  function refreshRuns() {
    getAutoOptimizeRunsForSpace(spaceId)
      .then(setRuns)
      .catch(() => setRuns([]))
  }

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
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Champion accuracy</TableHead>
                <TableHead>Model</TableHead>
                <TableHead>Triggered By</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.run_id}>
                  <TableCell className="text-sm">
                    {run.started_at
                      ? new Date(run.started_at).toLocaleDateString(undefined, {
                          month: "short",
                          day: "numeric",
                          year: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                        })
                      : "—"}
                  </TableCell>
                  <TableCell>
                    <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>
                      {run.status}
                    </Badge>
                  </TableCell>
                  <TableCell
                    className="text-sm text-muted max-w-[14rem] truncate"
                    title={humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  >
                    {humanizeTerminalReason(run.terminal_reason, run.convergence_reason)}
                  </TableCell>
                  <TableCell className="text-sm">
                    {championAccuracyText(run.best_accuracy)}
                  </TableCell>
                  <TableCell className="text-sm text-muted max-w-[12rem] truncate" title={run.llm_model ?? undefined}>
                    {run.llm_model ?? "—"}
                  </TableCell>
                  <TableCell className="text-sm text-muted">
                    {run.triggered_by ?? "—"}
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-col gap-1.5">
                      <button
                        onClick={() => onSelectRun(run.run_id)}
                        className="text-sm text-accent hover:underline text-left"
                      >
                        View Details
                      </button>
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        {/* Revert to champion — only when a distinct optimized
                            iteration won (best_iteration > 0). When the baseline
                            was the champion (best_iteration === 0/null), this
                            button is hidden and only the baseline revert shows. */}
                        {hasDistinctChampion(run) && (
                          <RevertButton
                            run={run}
                            target="champion"
                            disabled={hasActiveRun}
                            onReverted={refreshRuns}
                          />
                        )}
                        {/* Revert to baseline — shown whenever a config snapshot
                            exists. ``has_config_snapshot`` is undefined on older
                            backends; treat that as "unknown" and still render. */}
                        {run.has_config_snapshot !== false && (
                          <RevertButton
                            run={run}
                            target="baseline"
                            disabled={hasActiveRun}
                            onReverted={refreshRuns}
                          />
                        )}
                      </div>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}

/**
 * A distinct champion exists when an optimized iteration (iteration > 0) beat
 * the baseline. ``best_iteration`` is stamped by ``promote_best_model``; 0 means
 * the baseline itself was the champion (nothing beat it), null means no
 * champion was promoted (failed run / no iterations).
 */
function hasDistinctChampion(run: GSORunSummary): boolean {
  const it = run.best_iteration
  return typeof it === "number" && Number.isFinite(it) && it > 0
}

type RevertTarget = "champion" | "baseline"

type RevertPhase = "idle" | "confirming" | "pending" | "success" | "error"

const REVERT_LABEL: Record<RevertTarget, string> = {
  champion: "Revert to Champion",
  baseline: "Revert to Baseline",
}

const REVERT_HINT: Record<RevertTarget, string> = {
  champion: "Roll the live Genie Space back to this run's champion (winning) configuration.",
  baseline: "Roll the live Genie Space back to this run's starting (pre-optimization) configuration.",
}

const REVERT_CONFIRM: Record<RevertTarget, string> = {
  champion: "Roll space back to champion?",
  baseline: "Roll space back to baseline?",
}

const REVERT_ICON: Record<RevertTarget, React.ReactNode> = {
  champion: <Trophy className="h-3.5 w-3.5" />,
  baseline: <History className="h-3.5 w-3.5" />,
}

interface RevertButtonProps {
  run: GSORunSummary
  target: RevertTarget
  disabled: boolean
  onReverted: () => void
}

/**
 * Per-row revert affordance. Re-PATCHes the live Genie Space with either the
 * run's champion config (``target="champion"``) or its pre-run baseline
 * (``target="baseline"``) via ``POST /auto-optimize/runs/{id}/revert?target=``.
 * Destructive (it overwrites the live space config), so it's gated behind an
 * inline confirm and disabled whenever any run for the Space is active.
 */
export function RevertButton({ run, target, disabled, onReverted }: RevertButtonProps) {
  const [phase, setPhase] = useState<RevertPhase>("idle")
  const [error, setError] = useState<string | null>(null)

  async function doRevert() {
    if (disabled) return
    setPhase("pending")
    setError(null)
    try {
      await revertAutoOptimizeRun(run.run_id, target)
      setPhase("success")
      onReverted()
      // Drop the success banner back to idle after a few seconds so the row
      // returns to its resting affordance.
      setTimeout(() => setPhase("idle"), 4000)
    } catch (e) {
      const message =
        e instanceof ApiError
          ? e.message
          : e instanceof Error
            ? e.message
            : "Failed to revert the Genie Space."
      setError(message)
      setPhase("error")
    }
  }

  if (phase === "success") {
    return (
      <span className="flex items-center gap-1 text-xs font-medium text-emerald-600 dark:text-emerald-400">
        <CheckCircle2 className="h-3.5 w-3.5" />
        Reverted
      </span>
    )
  }

  if (phase === "error") {
    return (
      <span className="flex flex-col items-start gap-0.5">
        <button
          onClick={() => setPhase("confirming")}
          disabled={disabled}
          className="flex items-center gap-1 text-xs text-accent hover:underline disabled:cursor-not-allowed disabled:text-muted disabled:no-underline"
        >
          {REVERT_ICON[target]}
          {REVERT_LABEL[target]}
        </button>
        <span
          className="flex items-center gap-1 text-xs text-red-500 max-w-[18rem] truncate"
          title={error ?? undefined}
        >
          <AlertCircle className="h-3 w-3 shrink-0" />
          {error ?? "Failed"}
        </span>
      </span>
    )
  }

  if (phase === "pending") {
    return (
      <span className="flex items-center gap-1.5 text-xs text-muted">
        <Loader2 className="h-3 w-3 animate-spin" />
        Reverting…
      </span>
    )
  }

  if (phase === "confirming") {
    return (
      <span className="flex items-center gap-1.5">
        <span className="text-xs text-muted">{REVERT_CONFIRM[target]}</span>
        <button
          onClick={doRevert}
          disabled={disabled}
          className="flex items-center gap-1 rounded-md bg-red-600 px-2 py-0.5 text-xs font-medium text-white transition-colors hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          <RotateCcw className="h-3 w-3" />
          Yes
        </button>
        <button
          onClick={() => {
            setPhase("idle")
            setError(null)
          }}
          className="rounded-md border border-default px-2 py-0.5 text-xs font-medium text-muted transition-colors hover:text-primary"
        >
          Cancel
        </button>
      </span>
    )
  }

  // idle
  return (
    <button
      onClick={() => setPhase("confirming")}
      disabled={disabled}
      title={
        disabled
          ? `Wait for the active optimization on this Space to finish before reverting history.`
          : REVERT_HINT[target]
      }
      className="flex items-center gap-1 text-xs text-accent hover:underline disabled:cursor-not-allowed disabled:text-muted disabled:no-underline"
    >
      {REVERT_ICON[target]}
      {REVERT_LABEL[target]}
    </button>
  )
}
