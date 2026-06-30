import { useEffect, useState } from "react"
import { Activity, Crosshair, Layers, ListChecks } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import type { GSOAttempt } from "@/types"
import {
  attemptModeColor,
  attemptModeLabel,
  hypothesisClusterId,
  hypothesisLevers,
  leverFamilyLabels,
} from "@/components/auto-optimize/cockpit"

interface CurrentAttemptStripProps {
  /** The in-flight attempt (highest attempt_no). */
  attempt: GSOAttempt | null
  /** Still-failing question count in the latest full eval (residual failures). */
  residualFailureCount: number | null
  /** Wall-clock (ms) of the last observed loop-state commit; null if unknown. */
  lastCommitAt: number | null
  /** Whether the run is still live (drives the ticking heartbeat). */
  isLive: boolean
}

function useElapsedSeconds(since: number | null, ticking: boolean): number | null {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!ticking || since == null) return
    // The interval (async) drives the tick; the initial `now` covers the first
    // second, with elapsed clamped at 0 so a fresh commit reads "0s ago".
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [ticking, since])
  if (since == null) return null
  return Math.max(0, Math.floor((now - since) / 1000))
}

// Current-attempt focus strip (Phase 12 / arch §7.5) — mode · targeted failure
// cluster · patch family · residual-failure count, plus a loop-state heartbeat
// ("last commit Ns ago") as the live-alive signal. Since the single 03_optimize
// Jobs task can no longer surface per-attempt progress, the heartbeat tracks the
// last observed loop-state commit on the client.
export function CurrentAttemptStrip({
  attempt,
  residualFailureCount,
  lastCommitAt,
  isLive,
}: CurrentAttemptStripProps) {
  const elapsed = useElapsedSeconds(lastCommitAt, isLive)
  if (!attempt) return null

  const hypothesis = attempt.currentHypothesis ?? attempt.nextHypothesis ?? null
  const cluster = hypothesisClusterId(hypothesis)
  const families = leverFamilyLabels(hypothesisLevers(hypothesis))

  return (
    <div className="rounded-xl border border-accent/30 bg-accent/5 px-4 py-3">
      <div className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold uppercase tracking-widest text-muted">
            Current attempt
          </span>
          <span className="flex items-center gap-1.5 text-sm font-medium text-primary">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: attemptModeColor(attempt.attemptMode) }}
            />
            {attemptModeLabel(attempt.attemptMode)}
            {attempt.attemptNo != null && (
              <span className="text-muted">· attempt {attempt.attemptNo}</span>
            )}
          </span>
        </div>
        <span
          className={`flex items-center gap-1.5 text-xs ${isLive ? "text-emerald-600 dark:text-emerald-400" : "text-muted"}`}
          title="Live loop-state heartbeat"
        >
          <Activity className={`h-3.5 w-3.5 ${isLive ? "animate-pulse" : ""}`} />
          {elapsed == null ? "awaiting first commit" : `last commit ${elapsed}s ago`}
        </span>
      </div>

      <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
        <Facet icon={Crosshair} label="Targeted cluster" value={cluster ?? "—"} />
        <Facet
          icon={Layers}
          label="Patch family"
          value={families.length > 0 ? families.join(", ") : "—"}
        />
        <Facet
          icon={ListChecks}
          label="Residual failures"
          value={residualFailureCount == null ? "—" : String(residualFailureCount)}
        />
      </div>

      {attempt.decision && (
        <div className="mt-2">
          <Badge variant="secondary">decision: {attempt.decision}</Badge>
        </div>
      )}
    </div>
  )
}

function Facet({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Crosshair
  label: string
  value: string
}) {
  return (
    <div className="flex items-start gap-2">
      <Icon className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted" />
      <div className="min-w-0">
        <p className="text-[10px] font-semibold uppercase tracking-wide text-muted">{label}</p>
        <p className="truncate text-sm text-primary" title={value}>
          {value}
        </p>
      </div>
    </div>
  )
}
