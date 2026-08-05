import { useEffect, useState, useRef } from "react"
import { Info, Play, BarChart2, ExternalLink, ShieldCheck, Sparkles } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { OptimizationConfig } from "@/components/auto-optimize/OptimizationConfig"
import { OptimizationLoadingStepper } from "@/components/auto-optimize/OptimizationLoadingStepper"
import { RunHistoryTable } from "@/components/auto-optimize/RunHistoryTable"
import { RunDetailView } from "@/components/auto-optimize/RunDetailView"
import { TaskRail } from "@/components/auto-optimize/TaskRail"
import { AttemptLadder } from "@/components/auto-optimize/AttemptLadder"
import { AttemptLedger } from "@/components/auto-optimize/AttemptLedger"
import { TerminalBanner } from "@/components/auto-optimize/TerminalBanner"
import { PublishAuditSummary } from "@/components/auto-optimize/PublishAuditSummary"
import { ResolutionActions } from "@/components/auto-optimize/ResolutionActions"
import { BenchmarkChangesPanel } from "@/components/auto-optimize/BenchmarkChangesPanel"
import { PatchesTable } from "@/components/auto-optimize/PatchesTable"
import { ResourceLinks } from "@/components/auto-optimize/ResourceLinks"
import { RunActivitySection } from "@/components/auto-optimize/RunActivitySection"
import {
  getAutoOptimizeHealth,
  getAutoOptimizeStatus,
  getActiveRunForSpace,
  getAutoOptimizePermissions,
  getAutoOptimizeIterations,
  getAutoOptimizeRun,
  getAutoOptimizeLoopState,
  getAutoOptimizePublishRecord,
  getAutoOptimizeBenchmarkChanges,
} from "@/lib/api"
import { convergenceReasonText } from "@/lib/score-display"
import type {
  GSORunStatus,
  GSOPermissionCheck,
  GSOLoopStateResponse,
  GSOPublishRecord,
  GSOBenchmarkChanges,
  GSOPipelineRun,
  GSOIterationResult,
} from "@/types"

interface AutoOptimizeTabProps {
  spaceId: string
  requestedRunId?: string
  onRunChange?: (runId?: string) => void
  onRescan?: () => void
}

type View = "configure" | "monitoring" | "detail"

const TERMINAL_STATUSES = new Set([
  "CONVERGED",
  "STALLED",
  "MAX_ITERATIONS",
  "FAILED",
  "CANCELLED",
  "APPLIED",
  "DISCARDED",
  "SKIPPED",
])

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

function hasBenchmarkChangesSurface(changes: GSOBenchmarkChanges | null | undefined): boolean {
  return Boolean(changes && (changes.qc != null || changes.counts.total > 0))
}

function formatPct(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : `${value.toFixed(1)}%`
}

function ScoreComparisonCards({
  baselineAccuracy,
  optimizedAccuracy,
}: {
  baselineAccuracy: number | null | undefined
  optimizedAccuracy: number | null | undefined
}) {
  const improvement =
    baselineAccuracy != null && optimizedAccuracy != null
      ? optimizedAccuracy - baselineAccuracy
      : null
  const improvementClass =
    improvement == null || improvement === 0
      ? "text-primary"
      : improvement > 0
        ? "text-emerald-600 dark:text-emerald-400"
        : "text-red-500"
  const improvementText =
    improvement == null ? "—" : `${improvement > 0 ? "+" : ""}${improvement.toFixed(1)}%`

  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
      <div className="rounded-xl border border-default bg-surface p-4">
        <p className="text-xs font-semibold uppercase tracking-widest text-muted">Baseline</p>
        <p className="mt-1 text-2xl font-bold text-primary">{formatPct(baselineAccuracy)}</p>
      </div>
      <div className="rounded-xl border border-indigo-300 bg-indigo-50 p-4 dark:border-indigo-900/50 dark:bg-indigo-950/20">
        <p className="text-xs font-semibold uppercase tracking-widest text-indigo-600 dark:text-indigo-400">
          Optimized
        </p>
        <p className="mt-1 text-2xl font-bold text-indigo-600 dark:text-indigo-400">
          {formatPct(optimizedAccuracy)}
        </p>
        <p className="mt-0.5 text-xs text-muted">Champion accuracy</p>
      </div>
      <div className="rounded-xl border border-default bg-surface p-4">
        <p className="text-xs font-semibold uppercase tracking-widest text-muted">Improvement</p>
        <p className={`mt-1 text-2xl font-bold ${improvementClass}`}>{improvementText}</p>
      </div>
    </div>
  )
}

export function AutoOptimizeTab({ spaceId, requestedRunId, onRunChange, onRescan }: AutoOptimizeTabProps) {
  const [configured, setConfigured] = useState<boolean | null>(null)
  const [healthIssues, setHealthIssues] = useState<string[]>([])
  const [view, setView] = useState<View>(requestedRunId ? "monitoring" : "configure")
  const [activeRunId, setActiveRunId] = useState<string | null>(requestedRunId ?? null)
  const [stepperOpen, setStepperOpen] = useState(false)
  const [stepperComplete, setStepperComplete] = useState(false)
  const [stepperError, setStepperError] = useState<string | null>(null)
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [runStatus, setRunStatus] = useState<GSORunStatus | null>(null)
  const [runDetail, setRunDetail] = useState<GSOPipelineRun | null>(null)
  const [iterations, setIterations] = useState<GSOIterationResult[]>([])
  const [permissions, setPermissions] = useState<GSOPermissionCheck | null>(null)
  const [permsLoading, setPermsLoading] = useState(true)
  // GSO v2 Phase 12 — live cockpit state (loop-state attempts + publish record
  // + benchmark QC for the 01 hard-fail chip). All optional/nullable — legacy
  // 6-step runs degrade gracefully.
  const [loopState, setLoopState] = useState<GSOLoopStateResponse | null>(null)
  const [publishRecord, setPublishRecord] = useState<GSOPublishRecord | null>(null)
  const [benchmarkChanges, setBenchmarkChanges] = useState<GSOBenchmarkChanges | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  function resetRunSurfaces() {
    setRunStatus(null)
    setRunDetail(null)
    setIterations([])
    setLoopState(null)
    setPublishRecord(null)
    setBenchmarkChanges(null)
  }

  function openMonitoring(runId: string) {
    if (runId !== activeRunId) resetRunSurfaces()
    setSelectedRunId(null)
    setActiveRunId(runId)
    setView("monitoring")
    if (requestedRunId !== runId) onRunChange?.(runId)
  }

  function closeMonitoring(isTerminal: boolean) {
    setView("configure")
    if (isTerminal) setActiveRunId(null)
    onRunChange?.(undefined)
  }

  // Health check on mount
  useEffect(() => {
    getAutoOptimizeHealth()
      .then((res) => {
        setConfigured(res.configured)
        setHealthIssues(res.issues || [])
      })
      .catch(() => setConfigured(false))
  }, [])

  // Check for active runs (authoritative Delta table) and permissions on mount
  useEffect(() => {
    if (configured !== true) return
    getActiveRunForSpace(spaceId).then((res) => {
      if (!requestedRunId) {
        setActiveRunId(res.hasActiveRun ? res.activeRunId : null)
      }
    })
    getAutoOptimizePermissions(spaceId)
      .then(setPermissions)
      .catch(() => setPermissions(null))
      .finally(() => setPermsLoading(false))
  }, [spaceId, configured, requestedRunId])

  function refreshPermissions() {
    setPermsLoading(true)
    getAutoOptimizePermissions(spaceId)
      .then(setPermissions)
      .catch(() => setPermissions(null))
      .finally(() => setPermsLoading(false))
  }

  // Polling for active run status + ASI results
  useEffect(() => {
    if (view !== "monitoring" || !activeRunId) return

    const runId = activeRunId
    let cancelled = false

    function poll() {
      // Poll status
      getAutoOptimizeStatus(runId)
        .then((status) => {
          if (cancelled) return
          setRunStatus(status)
          if (TERMINAL_STATUSES.has(status.status)) {
            if (intervalRef.current) {
              clearInterval(intervalRef.current)
              intervalRef.current = null
            }
          }
        })
        .catch(() => {})

      // Full run detail is the source of Databricks resource links and the
      // terminal summary narrative. It has a Delta fallback and is lightweight
      // enough for the monitoring poll.
      getAutoOptimizeRun(runId)
        .then((next) => {
          if (!cancelled) setRunDetail(next)
        })
        .catch(() => {})

      // Poll iterations. Preserve the last non-empty set so a transient empty
      // Lakebase/Delta read never blanks the live cockpit while the notebook is
      // between eval-run commits.
      getAutoOptimizeIterations(runId)
        .then((next) => {
          if (cancelled) return
          if (next.length > 0) setIterations(next)
        })
        .catch(() => {})

      // GSO v2 Phase 12 — controller loop-state (per-attempt rows + run-level
      // aggregate) drives the Attempt Ladder/Ledger + Champion hero.
      getAutoOptimizeLoopState(runId)
        .then((ls) => {
          if (cancelled) return
          setLoopState((prev) => {
            if (!ls) return prev
            if (ls.runId !== runId) return prev
            if (prev?.runId && prev.runId !== runId) return ls
            if ((ls.attempts?.length ?? 0) === 0 && (prev?.attempts?.length ?? 0) > 0) {
              return prev
            }
            return ls
          })
        })
        .catch(() => {})

      // Publish record (terminal banner published vs not) — null until publish.
      getAutoOptimizePublishRecord(runId)
        .then((res) => {
          if (cancelled) return
          const next = res?.publishRecord ?? null
          if (next) setPublishRecord(next)
        })
        .catch(() => {})

      // Benchmark changes + QC — drives the first-class QC surface and the
      // compatibility rail chip for historical BENCHMARK_UNREPAIRABLE runs.
      getAutoOptimizeBenchmarkChanges(runId)
        .then((res) => {
          if (cancelled) return
          setBenchmarkChanges((prev) => {
            if (!res) return prev
            if (res.runId !== runId) return prev
            if (prev?.runId && prev.runId !== runId) return res
            if (!hasBenchmarkChangesSurface(res) && hasBenchmarkChangesSurface(prev)) {
              return prev
            }
            return res
          })
        })
        .catch(() => {})
    }

    poll()
    intervalRef.current = setInterval(poll, 5000)

    return () => {
      cancelled = true
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [view, activeRunId])

  // Loading state
  if (configured === null) {
    return <div className="py-8 text-center text-muted text-sm">Loading...</div>
  }

  // Not configured
  if (!configured) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Info className="w-10 h-10 text-muted mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-primary mb-2">Optimize is not configured</h3>
          <p className="text-muted text-sm">
            Contact your administrator to set GSO_CATALOG and GSO_JOB_ID for this deployment.
          </p>
        </CardContent>
      </Card>
    )
  }

  // Configure view
  if (view === "configure") {
    return (
      <div className="space-y-6">
        {activeRunId && (
          <Card className="border-blue-500/30 bg-blue-500/5">
            <CardContent className="py-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-primary mb-1">
                    Optimization in progress
                  </h3>
                  <p className="text-xs text-muted">
                    An active run is already running for this agent. Wait for it to complete before starting a new one.
                  </p>
                </div>
                <button
                  onClick={() => openMonitoring(activeRunId)}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors shrink-0"
                >
                  <Play className="w-3.5 h-3.5" />
                  View Active Run
                </button>
              </div>
            </CardContent>
          </Card>
        )}
        <OptimizationConfig
          spaceId={spaceId}
          hasActiveRun={!!activeRunId}
          permissions={permissions}
          permsLoading={permsLoading}
          healthIssues={healthIssues}
          onRefreshPermissions={refreshPermissions}
          onTriggerStart={() => {
            setStepperError(null)
            setStepperComplete(false)
            setStepperOpen(true)
          }}
          onTriggerError={(msg) => {
            setStepperError(msg)
          }}
          onStarted={(runId) => {
            setActiveRunId(runId)
            setStepperComplete(true)
          }}
        />
        <OptimizationLoadingStepper
          isOpen={stepperOpen}
          isComplete={stepperComplete}
          error={stepperError}
          onNavigate={() => {
            setStepperOpen(false)
            setStepperComplete(false)
            setStepperError(null)
            if (activeRunId) openMonitoring(activeRunId)
          }}
        />
        <RunHistoryTable
          spaceId={spaceId}
          onSelectRun={(runId) => {
            setSelectedRunId(runId)
            setView("detail")
          }}
        />
      </div>
    )
  }

  // Monitoring view
  if (view === "monitoring" && activeRunId) {
    const statusForRun = runStatus?.runId === activeRunId ? runStatus : null
    const runDetailForRun = runDetail?.runId === activeRunId ? runDetail : null
    const loopStateForRun = loopState?.runId === activeRunId ? loopState : null
    const publishRecordForRun = publishRecord?.runId === activeRunId ? publishRecord : null
    const benchmarkChangesForRun = benchmarkChanges?.runId === activeRunId ? benchmarkChanges : null
    const isTerminal = statusForRun ? TERMINAL_STATUSES.has(statusForRun.status) : false
    const stepsCompleted = statusForRun?.stepsCompleted ?? 0
    const currentStepName = statusForRun?.currentStepName ?? null

    // GSO v2 Phase 12 — the controller attempts drive the live cockpit. Empty
    // for legacy 6-step runs or before the loop commits its first attempt.
    const attempts = loopStateForRun?.attempts ?? []
    const hasAttempts = attempts.length > 0
    const loop = loopStateForRun?.loopState ?? null
    const terminalReason = statusForRun?.terminalReason ?? null
    const benchmarkUnrepairable = benchmarkChangesForRun?.qc?.terminalReason === "BENCHMARK_UNREPAIRABLE"
    const showTypedBanner = (isTerminal && Boolean(terminalReason)) || benchmarkUnrepairable
    const hasBenchmarkSurface = hasBenchmarkChangesSurface(benchmarkChangesForRun)

    const baselineAccuracy = statusForRun?.baselineScore ?? runDetailForRun?.baselineScore ?? null
    // targetAccuracy is normalized to 0–1; prefer the loop-state value.
    const targetUnit = loop?.targetAccuracy ?? statusForRun?.targetAccuracy ?? runDetailForRun?.targetAccuracy ?? null
    const bestAccuracy =
      loop?.bestAccuracy ??
      publishRecordForRun?.championAccuracy ??
      statusForRun?.optimizedScore ??
      runDetailForRun?.optimizedScore ??
      null
    // Baseline is champion only when terminal AND no attempt was flagged
    // champion (nothing beat it) — derived from explicit flags, never idxmax.
    const baselineIsChampion = isTerminal && hasAttempts && !attempts.some((a) => a.isChampion)
    // Keep / Discard-rollback is available once a champion was published to the
    // live space (or the run is already resolved). Published comes from the
    // publish record; fall back to the published-terminal statuses for runs
    // that predate the artifact.
    const resolutionPublished = publishRecordForRun ? publishRecordForRun.published : null
    const showResolution =
      isTerminal &&
      (statusForRun?.status === "APPLIED" ||
        statusForRun?.status === "DISCARDED" ||
        resolutionPublished === true ||
        (resolutionPublished == null &&
          (statusForRun?.status === "CONVERGED" || statusForRun?.status === "MAX_ITERATIONS")))
    const legacyReason = !showTypedBanner && statusForRun
      ? convergenceReasonText({
          baselineScore: statusForRun.baselineScore,
          optimizedScore: statusForRun.optimizedScore,
          bestIteration: statusForRun.bestIteration,
          status: statusForRun.status,
          convergenceReason: statusForRun.convergenceReason,
        })
      : null
    const hasOptimizationSurface =
      isTerminal ||
      benchmarkUnrepairable ||
      showResolution ||
      baselineAccuracy != null ||
      bestAccuracy != null ||
      hasAttempts ||
      Boolean(legacyReason)

    return (
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              onClick={() => closeMonitoring(isTerminal)}
              className="text-sm text-accent hover:underline"
            >
              &larr; Back to configuration
            </button>
            {statusForRun && (
              <Badge variant={STATUS_VARIANT[statusForRun.status] ?? "secondary"}>
                {statusForRun.status}
              </Badge>
            )}
          </div>
        </div>

        {/* 4-task rail */}
        <TaskRail
          stepsCompleted={stepsCompleted}
          currentStepName={currentStepName}
          status={statusForRun?.status ?? null}
          terminalReason={terminalReason}
          benchmarkUnrepairable={benchmarkUnrepairable}
        />

        {/* Task 01: benchmark QC, repair, and evaluation-set handoff. */}
        {hasBenchmarkSurface && (
          <RunActivitySection
            title="Benchmark QC & Repairs"
            description="Reviews benchmark quality, repairs eligible items, and establishes the evaluation set."
            icon={ShieldCheck}
          >
            <BenchmarkChangesPanel
              runId={activeRunId}
              changes={benchmarkChangesForRun}
              showTitle={false}
            />
          </RunActivitySection>
        )}

        {/* Tasks 02–04: evaluation, optimization attempts, and publish outcome. */}
        {hasOptimizationSurface && (
          <RunActivitySection
            title="Optimization"
            description="Compares attempts, selects the champion, and records the configuration patches."
            icon={Sparkles}
          >
            {/* Terminal banner — published vs nothing-published, keyed on reason.
                Concerns are owned by the publish/audit summary below. */}
            {(isTerminal || benchmarkUnrepairable) && (
              <TerminalBanner
                status={statusForRun?.status ?? null}
                terminalReason={terminalReason}
                published={publishRecordForRun ? publishRecordForRun.published : null}
                publishOutcome={publishRecordForRun?.publishOutcome ?? null}
                benchmarkUnrepairable={benchmarkUnrepairable}
                championAccuracy={publishRecordForRun?.championAccuracy ?? bestAccuracy}
              />
            )}

            {/* Publish/audit summary headline — LLM paragraph + concerns callout. */}
            {isTerminal && <PublishAuditSummary publishRecord={publishRecordForRun} />}

            {/* Keep / Discard-rollback affordance (auto-publish model). */}
            {showResolution && (
              <ResolutionActions
                key={activeRunId}
                runId={activeRunId}
                status={statusForRun?.status ?? ""}
                published={resolutionPublished}
                onResolved={(s) =>
                  setRunStatus((prev) => (prev ? { ...prev, status: s } : prev))
                }
              />
            )}

            {(baselineAccuracy != null || bestAccuracy != null) && (
              <ScoreComparisonCards
                baselineAccuracy={baselineAccuracy}
                optimizedAccuracy={bestAccuracy}
              />
            )}

            {hasAttempts ? (
              <>
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                  <AttemptLadder
                    baselineAccuracy={baselineAccuracy}
                    attempts={attempts}
                    targetUnit={targetUnit}
                  />
                  <AttemptLedger
                    baselineAccuracy={baselineAccuracy}
                    attempts={attempts}
                    baselineIsChampion={baselineIsChampion}
                  />
                </div>
                <PatchesTable
                  key={`${activeRunId}:${attempts.length}`}
                  runId={activeRunId}
                  iterations={iterations}
                />
              </>
            ) : null}

            {legacyReason && <p className="text-sm text-muted">Reason: {legacyReason}</p>}

            {/* Re-scan prompt when run reaches terminal state */}
            {isTerminal && onRescan && (
              <div className="flex items-center justify-between rounded-lg border border-blue-500/30 bg-blue-500/5 px-4 py-3">
                <div>
                  <h3 className="text-sm font-semibold text-primary">Optimization complete</h3>
                  <p className="text-xs text-muted mt-0.5">
                    Re-scan to see how your IQ score has changed.
                  </p>
                </div>
                <button
                  onClick={onRescan}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors shrink-0"
                >
                  <BarChart2 className="w-3.5 h-3.5" />
                  Re-scan IQ Score
                </button>
              </div>
            )}
          </RunActivitySection>
        )}

        {runDetailForRun?.links && runDetailForRun.links.length > 0 && (
          <RunActivitySection
            title="Databricks Resources"
            description="Open the Genie Agent and workflow artifacts associated with this run."
            icon={ExternalLink}
          >
            <ResourceLinks links={runDetailForRun.links} showHeading={false} />
          </RunActivitySection>
        )}

        {/* Footer */}
        {!isTerminal && (
          <div className="flex justify-end">
            <p className="text-xs text-muted animate-pulse">Polling every 5 seconds...</p>
          </div>
        )}

      </div>
    )
  }

  // Detail view — placeholder for Layer 2
  if (view === "detail" && selectedRunId) {
    return (
      <div className="space-y-4">
        <RunDetailView key={selectedRunId} runId={selectedRunId} onBack={() => setView("configure")} />
      </div>
    )
  }

  return null
}
