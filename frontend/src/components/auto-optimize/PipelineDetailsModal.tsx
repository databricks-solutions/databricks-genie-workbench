import { Fragment, useEffect, useState, useRef } from "react"
import { X, TrendingUp, Info } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { TaskRail } from "@/components/auto-optimize/TaskRail"
import { AttemptLadder } from "@/components/auto-optimize/AttemptLadder"
import { AttemptLedger } from "@/components/auto-optimize/AttemptLedger"
import { OptimizationLevers } from "@/components/auto-optimize/OptimizationLevers"
import { IterationChart } from "@/components/auto-optimize/IterationChart"
import { StageTimeline } from "@/components/auto-optimize/StageTimeline"
import { ResourceLinks } from "@/components/auto-optimize/ResourceLinks"
import { QuestionJourney } from "@/components/auto-optimize/QuestionJourney"
import { PatchesTable } from "@/components/auto-optimize/PatchesTable"
import { ActivityLog } from "@/components/auto-optimize/ActivityLog"
import { OptimizationNarrative } from "@/components/auto-optimize/OptimizationNarrative"
import { PublishAuditSummary } from "@/components/auto-optimize/PublishAuditSummary"
import { SuggestionsPanel } from "@/components/auto-optimize/SuggestionsPanel"
import {
  getAutoOptimizeRun,
  getAutoOptimizeIterations,
  getAutoOptimizePublishRecord,
  getAutoOptimizeLoopState,
  getAutoOptimizeBenchmarkChanges,
} from "@/lib/api"
import {
  convergenceReasonText,
  formatScorePct,
  presentBaselineScore,
  presentOptimizedScore,
} from "@/lib/score-display"
import { attemptModeLabel, decisionLabel } from "@/components/auto-optimize/cockpit"
import { attemptColumnLabel } from "@/components/auto-optimize/runDetail"
import { deriveRailProgress } from "@/components/auto-optimize/pipelineDetail"
import { Tooltip } from "@/components/ui/tooltip"
import type { GSOPipelineRun, GSOIterationResult, GSOPublishRecord, GSOAttempt } from "@/types"

interface PipelineDetailsModalProps {
  runId: string
  isOpen: boolean
  onClose: () => void
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

function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString("en-US", {
    month: "short", day: "numeric", year: "numeric",
    hour: "numeric", minute: "2-digit",
  })
}

export function PipelineDetailsModal({ runId, isOpen, onClose }: PipelineDetailsModalProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [iterations, setIterations] = useState<GSOIterationResult[]>([])
  const [publishRecord, setPublishRecord] = useState<GSOPublishRecord | null>(null)
  // GSO v2 (Phase 14) — the 03_optimize controller attempts drive the Attempt
  // Ladder/Ledger. Empty for legacy 6-step runs / before the first attempt; the
  // Attempt Explorer then degrades to the re-keyed iteration table.
  const [attempts, setAttempts] = useState<GSOAttempt[]>([])
  // GSO v2 (Phase 14) — benchmark-QC terminal reason drives the 01 rail chip.
  // A BENCHMARK_UNREPAIRABLE hard-stop lives on the QC artifact (not the loop
  // GSOTerminalReason union), mirroring the Phase-12 cockpit derivation.
  const [benchmarkUnrepairable, setBenchmarkUnrepairable] = useState(false)

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    if (!isOpen) return
    setRun(null)
    setIterations([])
    setPublishRecord(null)
    setAttempts([])
    setBenchmarkUnrepairable(false)

    function fetchData() {
      getAutoOptimizeRun(runId).then(setRun).catch(() => {})
      getAutoOptimizePublishRecord(runId)
        .then((res) => setPublishRecord(res?.publishRecord ?? null))
        .catch(() => {})
      getAutoOptimizeLoopState(runId)
        .then((res) => setAttempts(res?.attempts ?? []))
        .catch(() => {})
      getAutoOptimizeBenchmarkChanges(runId)
        .then((res) => setBenchmarkUnrepairable(res?.qc?.terminalReason === "BENCHMARK_UNREPAIRABLE"))
        .catch(() => {})
      getAutoOptimizeIterations(runId)
        .then((its) => setIterations(its.filter((it) =>
          String(it.eval_scope ?? "").toLowerCase() === "full" || it.iteration === 0
        )))
        .catch(() => {})
    }

    fetchData()

    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [runId, isOpen])

  // Poll for live updates when run is not terminal
  const TERMINAL = new Set(["CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED", "APPLIED", "DISCARDED"])
  const runIsTerminal = run ? TERMINAL.has(run.status) : false

  useEffect(() => {
    if (!isOpen || runIsTerminal) {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
      return
    }
    pollRef.current = setInterval(() => {
      getAutoOptimizeRun(runId).then(setRun).catch(() => {})
      getAutoOptimizeLoopState(runId)
        .then((res) => setAttempts(res?.attempts ?? []))
        .catch(() => {})
      getAutoOptimizeBenchmarkChanges(runId)
        .then((res) => setBenchmarkUnrepairable(res?.qc?.terminalReason === "BENCHMARK_UNREPAIRABLE"))
        .catch(() => {})
      getAutoOptimizeIterations(runId)
        .then((its) => setIterations(its.filter((it) =>
          String(it.eval_scope ?? "").toLowerCase() === "full" || it.iteration === 0
        )))
        .catch(() => {})
    }, 10000)
    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [isOpen, runIsTerminal, runId])

  if (!isOpen) return null

  // 5-task rail progress derived from the run's steps (the modal reads a
  // GSOPipelineRun, not the status endpoint). Tolerant of the legacy 6-step
  // shape — TaskRail clamps against GSO_TOTAL_STEPS.
  const { stepsCompleted, currentStepName } = deriveRailProgress(run?.steps)

  const baselinePresentation = presentBaselineScore(run?.baselineScore ?? null)
  const optimizedPresentation = run
    ? presentOptimizedScore({
        baselineScore: run.baselineScore,
        optimizedScore: run.optimizedScore,
        bestIteration: run.bestIteration,
        status: run.status,
      })
    : presentBaselineScore(null)
  const reasonCopy = run
    ? convergenceReasonText({
        baselineScore: run.baselineScore,
        optimizedScore: run.optimizedScore,
        bestIteration: run.bestIteration,
        status: run.status,
        convergenceReason: run.convergenceReason,
      })
    : null
  const improvement =
    baselinePresentation.pct != null && optimizedPresentation.pct != null
      ? optimizedPresentation.pct - baselinePresentation.pct
      : null
  const hasAnyScore =
    baselinePresentation.pct != null || optimizedPresentation.pct != null

  const hasAttempts = attempts.length > 0
  const targetUnit = run?.targetAccuracy ?? null
  // Baseline is champion only when terminal and no attempt was flagged champion
  // (nothing beat it) — from explicit flags, never idxmax (§5).
  const baselineIsChampion = runIsTerminal && hasAttempts && !attempts.some((a) => a.isChampion)
  const hasLevers = (run?.levers?.length ?? 0) > 0

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-surface">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-default shrink-0">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-primary">Optimization Pipeline</h2>
          {run && (
            <>
              <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>{run.status}</Badge>
            </>
          )}
        </div>
        <button
          onClick={onClose}
          className="p-1.5 rounded-lg hover:bg-elevated text-muted hover:text-primary transition-colors"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Scrollable body */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-5xl mx-auto px-6 py-6 space-y-8">
          {!run ? (
            <p className="text-sm text-muted text-center py-12 animate-pulse">Loading pipeline details...</p>
          ) : (
            <>
              {/* Run metadata */}
              <p className="text-sm text-muted">
                Run <span className="font-mono text-primary">{run.runId.slice(0, 7)}</span>
                {" · "}
                Started {formatDateTime(run.startedAt)}
              </p>

              {/* 5-task rail (replaces the 6-step progress bar + step cards) */}
              <TaskRail
                stepsCompleted={stepsCompleted}
                currentStepName={currentStepName}
                status={run.status}
                terminalReason={run.terminalReason ?? null}
                benchmarkUnrepairable={benchmarkUnrepairable}
              />

              {hasAnyScore && (
                <div className="space-y-2">
                  <div className="grid grid-cols-3 gap-4">
                    <div className="rounded-xl border border-default p-5">
                      <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1">Baseline</p>
                      <p className="text-3xl font-bold text-primary">{baselinePresentation.text}</p>
                    </div>
                    <div className="rounded-xl border border-accent/30 bg-accent/5 p-5">
                      <p className="text-xs font-medium text-accent uppercase tracking-wide mb-1">Optimized</p>
                      {optimizedPresentation.tooltip ? (
                        <Tooltip content={optimizedPresentation.tooltip} side="bottom">
                          <p className="text-3xl font-bold text-accent">{optimizedPresentation.text}</p>
                        </Tooltip>
                      ) : (
                        <p className="text-3xl font-bold text-accent">{optimizedPresentation.text}</p>
                      )}
                    </div>
                    <div className={`rounded-xl border p-5 ${improvement != null && improvement > 0 ? "border-emerald-500/30 bg-emerald-500/5" : "border-default"}`}>
                      <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1 flex items-center gap-1">
                        <TrendingUp className="w-3 h-3" />
                        Improvement
                      </p>
                      <p className={`text-3xl font-bold ${improvement != null && improvement > 0 ? "text-emerald-500" : "text-primary"}`}>
                        {improvement != null ? `${improvement > 0 ? "+" : ""}${improvement.toFixed(1)}%` : "—"}
                      </p>
                    </div>
                  </div>
                  {reasonCopy && (
                    <p className="text-xs text-muted italic">Reason: {reasonCopy}</p>
                  )}
                </div>
              )}

              {/* Main tabs: Summary / Attempts / Levers / Suggestions */}
              {runIsTerminal && iterations.length > 0 && (
                <Tabs defaultValue="summary">
                  <TabsList>
                    <TabsTrigger value="summary">Summary</TabsTrigger>
                    <TabsTrigger value="attempts">Attempt Explorer</TabsTrigger>
                    <TabsTrigger value="levers">Levers</TabsTrigger>
                    <TabsTrigger value="suggestions">Suggestions</TabsTrigger>
                  </TabsList>

                  {/* Summary tab with sub-tabs */}
                  <TabsContent value="summary">
                    <Tabs defaultValue="overview">
                      <TabsList className="w-full justify-start">
                        <TabsTrigger value="overview">Overview</TabsTrigger>
                        <TabsTrigger value="questions">Questions</TabsTrigger>
                        <TabsTrigger value="patches">Patches</TabsTrigger>
                        <TabsTrigger value="activity">Activity</TabsTrigger>
                      </TabsList>

                      <TabsContent value="overview">
                        <div className="space-y-6">
                          {/* Charts */}
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                            <IterationChart iterations={iterations} />
                            <StageTimeline stages={run.stages ?? []} />
                          </div>
                          {/* Publish/audit summary headline — LLM paragraph +
                              concerns; the rich per-iteration narrative is
                              demoted to a collapsed expandable detail beneath. */}
                          <PublishAuditSummary publishRecord={publishRecord}>
                            <OptimizationNarrative run={run} iterations={iterations} convergenceReason={run.convergenceReason} />
                          </PublishAuditSummary>
                        </div>
                      </TabsContent>

                      <TabsContent value="questions">
                        {/* Attempt-grouped journey — columns relabeled Baseline ·
                            Coverage · Surgical N (attemptColumnLabel). */}
                        <QuestionJourney runId={runId} iterations={iterations} />
                      </TabsContent>

                      <TabsContent value="patches">
                        {/* Attempt-grouped patches — the "Iter" column is re-keyed
                            onto the coverage/surgical attempt vocabulary. */}
                        <PatchesTable runId={runId} iterations={iterations} />
                      </TabsContent>

                      <TabsContent value="activity">
                        <ActivityLog stages={run.stages ?? []} />
                      </TabsContent>
                    </Tabs>
                  </TabsContent>

                  {/* Attempt Explorer tab — re-keyed from iteration+lever to
                      attempt+attempt_mode+decision. The rich ladder+ledger render
                      off the loop-state attempts; when those are absent we degrade
                      to the re-keyed iteration table (still attempt-centric). */}
                  <TabsContent value="attempts">
                    {hasAttempts ? (
                      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                        <AttemptLadder
                          baselineAccuracy={run.baselineScore}
                          attempts={attempts}
                          targetUnit={targetUnit}
                        />
                        <AttemptLedger
                          baselineAccuracy={run.baselineScore}
                          attempts={attempts}
                          baselineIsChampion={baselineIsChampion}
                        />
                      </div>
                    ) : (
                      <AttemptExplorerTable iterations={iterations} />
                    )}
                  </TabsContent>

                  {/* Levers tab — coverage (lever 0) + surgical levers 1–6. */}
                  <TabsContent value="levers">
                    {hasLevers ? (
                      <OptimizationLevers levers={run.levers} iterations={iterations} />
                    ) : (
                      <p className="text-sm text-muted text-center py-8">No lever activity recorded for this run.</p>
                    )}
                  </TabsContent>

                  {/* Suggestions tab */}
                  <TabsContent value="suggestions">
                    <SuggestionsPanel runId={runId} />
                  </TabsContent>
                </Tabs>
              )}

              {/* Databricks Resources */}
              {run.links && run.links.length > 0 && (
                <ResourceLinks links={run.links} />
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// Re-keyed fallback for the Attempt Explorer when loop-state attempts are absent
// (legacy 6-step runs, or v2 iterations merged without loop-state). Columns move
// from iteration+lever to attempt+attempt_mode+decision, reading the explicit
// is_champion flag (never idxmax). Legacy rows with no attempt metadata degrade
// to "Attempt N" / "—" and simply carry no champion star.
export function AttemptExplorerTable({ iterations }: { iterations: GSOIterationResult[] }) {
  // Highest-accuracy row (first wins on ties) + explicit champion row. When the
  // highest ≠ champion we surface the highest row's rollback/decision reason
  // inline, mirroring AttemptLedger, so a higher-but-not-adopted attempt is
  // explained rather than hidden (champion truth = the explicit flag, §5).
  let highestIdx = -1
  let highestVal = -Infinity
  iterations.forEach((it, i) => {
    if (it.overall_accuracy != null && it.overall_accuracy > highestVal) {
      highestVal = it.overall_accuracy
      highestIdx = i
    }
  })
  const championIdx = iterations.findIndex((it) => it.is_champion === true)
  const divergentIdx =
    championIdx >= 0 && highestIdx >= 0 && championIdx !== highestIdx ? highestIdx : -1
  const divergenceReason =
    divergentIdx >= 0
      ? iterations[divergentIdx].decision_reason ?? "Higher accuracy but not adopted as champion"
      : null

  return (
    <div className="rounded-xl border border-default p-6">
      <h3 className="text-sm font-semibold text-primary mb-4">Attempt Accuracy Progression</h3>
      <div className="overflow-hidden rounded-lg border border-default">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-elevated border-b border-default">
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Attempt</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Mode</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Decision</th>
              <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Accuracy</th>
              <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Questions</th>
              <th className="text-center px-4 py-2.5 text-xs font-medium text-muted">Champion</th>
            </tr>
          </thead>
          <tbody>
            {iterations.map((it, i) => {
              const isBaseline = it.iteration === 0
              const rolledBack = it.rolled_back === true
              const isDivergent = i === divergentIdx
              return (
                <Fragment key={it.iteration}>
                  <tr
                    className={`border-b border-default last:border-0 ${
                      it.is_champion
                        ? "bg-emerald-50 dark:bg-emerald-950/30"
                        : isDivergent
                          ? "bg-amber-50 dark:bg-amber-950/20"
                          : isBaseline
                            ? "bg-elevated/50"
                            : ""
                    }`}
                  >
                    <td className="px-4 py-2.5 font-medium text-primary">{attemptColumnLabel(it)}</td>
                    <td className="px-4 py-2.5 text-muted">
                      {isBaseline ? "—" : attemptModeLabel(it.attempt_mode)}
                    </td>
                    <td className="px-4 py-2.5 text-muted">
                      {isBaseline ? "—" : decisionLabel(it.decision, rolledBack)}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-primary">
                      {formatScorePct(it.overall_accuracy)}
                    </td>
                    <td className="px-4 py-2.5 text-right text-muted font-mono">
                      {it.correct_count}/{it.total_questions}
                    </td>
                    <td className="px-4 py-2.5 text-center">
                      {it.is_champion ? (
                        <span title="Champion configuration" className="text-base text-indigo-500">
                          {"★"}
                        </span>
                      ) : (
                        <span className="text-muted">{"—"}</span>
                      )}
                    </td>
                  </tr>
                  {isDivergent && divergenceReason && (
                    <tr>
                      <td colSpan={6} className="px-4 pb-2.5 pt-0">
                        <p className="flex items-start gap-1.5 text-xs text-amber-600 dark:text-amber-400">
                          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                          <span>Highest accuracy, but not the champion: {divergenceReason}</span>
                        </p>
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
