import { useEffect, useState, useRef } from "react"
import { Info, Play, BarChart2 } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { OptimizationConfig } from "@/components/auto-optimize/OptimizationConfig"
import { OptimizationLoadingStepper } from "@/components/auto-optimize/OptimizationLoadingStepper"
import { RunHistoryTable } from "@/components/auto-optimize/RunHistoryTable"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { QuestionList } from "@/components/auto-optimize/QuestionList"
import { QuestionDetail } from "@/components/auto-optimize/QuestionDetail"
import { RunDetailView } from "@/components/auto-optimize/RunDetailView"
import { PipelineDetailsModal } from "@/components/auto-optimize/PipelineDetailsModal"
import { TaskRail } from "@/components/auto-optimize/TaskRail"
import { AttemptLadder } from "@/components/auto-optimize/AttemptLadder"
import { AttemptLedger } from "@/components/auto-optimize/AttemptLedger"
import { ChampionHero } from "@/components/auto-optimize/ChampionHero"
import { CurrentAttemptStrip } from "@/components/auto-optimize/CurrentAttemptStrip"
import { TerminalBanner } from "@/components/auto-optimize/TerminalBanner"
import {
  getAutoOptimizeHealth,
  getAutoOptimizeStatus,
  getActiveRunForSpace,
  getAutoOptimizePermissions,
  getAutoOptimizeIterations,
  getAutoOptimizeEvalResults,
  getAutoOptimizeQuestionResults,
  getAutoOptimizeLoopState,
  getAutoOptimizePublishRecord,
  getAutoOptimizeBenchmarkChanges,
} from "@/lib/api"
import { convergenceReasonText } from "@/lib/score-display"
import { GSO_TOTAL_STEPS } from "@/types"
import type {
  GSORunStatus,
  GSOPermissionCheck,
  GSOQuestionDetail,
  GSOLoopStateResponse,
  GSOPublishRecord,
  GSOBenchmarkQC,
} from "@/types"

interface AutoOptimizeTabProps {
  spaceId: string
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
])

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

export function AutoOptimizeTab({ spaceId, onRescan }: AutoOptimizeTabProps) {
  const [configured, setConfigured] = useState<boolean | null>(null)
  const [healthIssues, setHealthIssues] = useState<string[]>([])
  const [view, setView] = useState<View>("configure")
  const [activeRunId, setActiveRunId] = useState<string | null>(null)
  const [stepperOpen, setStepperOpen] = useState(false)
  const [stepperComplete, setStepperComplete] = useState(false)
  const [stepperError, setStepperError] = useState<string | null>(null)
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [runStatus, setRunStatus] = useState<GSORunStatus | null>(null)
  const [permissions, setPermissions] = useState<GSOPermissionCheck | null>(null)
  const [permsLoading, setPermsLoading] = useState(true)
  const [questions, setQuestions] = useState<GSOQuestionDetail[]>([])
  const [totalQuestions, setTotalQuestions] = useState<number>(0)
  const [selectedQuestionId, setSelectedQuestionId] = useState<string | null>(null)
  const [showPipeline, setShowPipeline] = useState(false)
  // GSO v2 Phase 12 — live cockpit state (loop-state attempts + publish record
  // + benchmark QC for the 01 hard-fail chip + a client-side loop-state
  // heartbeat). All optional/nullable — legacy 6-step runs degrade gracefully.
  const [loopState, setLoopState] = useState<GSOLoopStateResponse | null>(null)
  const [publishRecord, setPublishRecord] = useState<GSOPublishRecord | null>(null)
  const [benchmarkQc, setBenchmarkQc] = useState<GSOBenchmarkQC | null>(null)
  const [lastCommitAt, setLastCommitAt] = useState<number | null>(null)
  const attemptSigRef = useRef<string>("")
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const latestIterRef = useRef<number>(-1)

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
      if (res.hasActiveRun && res.activeRunId) {
        setActiveRunId(res.activeRunId)
        // Stay on "configure" view — the banner there lets users click into monitoring
      }
    })
    setPermsLoading(true)
    getAutoOptimizePermissions(spaceId)
      .then(setPermissions)
      .catch(() => setPermissions(null))
      .finally(() => setPermsLoading(false))
  }, [spaceId, configured])

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

    // Reset the heartbeat signature so a switched/new run re-stamps its first
    // commit (lastCommitAt itself is reset inside the poll handler when the run
    // has no attempts yet — keeps setState out of the effect body).
    attemptSigRef.current = ""

    function poll() {
      // Poll status
      getAutoOptimizeStatus(activeRunId!)
        .then((status) => {
          setRunStatus(status)
          if (TERMINAL_STATUSES.has(status.status)) {
            if (intervalRef.current) {
              clearInterval(intervalRef.current)
              intervalRef.current = null
            }
          }
        })
        .catch(() => {})

      // Poll iterations + question results
      getAutoOptimizeIterations(activeRunId!)
        .then(async (iterations) => {
          if (iterations.length === 0) return
          // Get total questions from the first iteration that has it
          const withTotal = iterations.find((it) => it.total_questions > 0)
          if (withTotal) setTotalQuestions(withTotal.total_questions)
          // Filter to full-scope evaluations only (skip slice/p0 probes)
          const fullIters = iterations.filter((it) => it.eval_scope === "full")
          if (fullIters.length === 0) return
          const maxIter = Math.max(...fullIters.map((it) => it.iteration))
          latestIterRef.current = maxIter

          // Prefer question-results (rows_json) — has full question text, SQL, and arbiter-adjusted pass/fail
          const questionResults = await getAutoOptimizeQuestionResults(activeRunId!, maxIter)
          if (questionResults && questionResults.length > 0) {
            setQuestions(questionResults)
            return
          }

          // Fallback: lightweight official eval-results (assessment +
          // assessment_reasons per question). One row per question on the
          // official path, so no per-judge dedup is needed.
          const evalResults = await getAutoOptimizeEvalResults(activeRunId!, maxIter)
          if (evalResults && evalResults.length > 0) {
            setQuestions(
              evalResults.map((r) => {
                const assessment = (r.assessment ?? "").toString().toUpperCase()
                return {
                  question_id: r.question_id,
                  question: "",
                  generated_sql: null,
                  expected_sql: null,
                  passed: assessment === "GOOD" ? true : assessment === "BAD" ? false : null,
                  assessment: r.assessment ?? null,
                  assessment_reasons: r.assessment_reasons ?? [],
                  match_type: null,
                }
              })
            )
          }
        })
        .catch(() => {})

      // GSO v2 Phase 12 — controller loop-state (per-attempt rows + run-level
      // aggregate) drives the Attempt Ladder/Ledger + Champion hero. The
      // attempt-signature diff feeds the client-side loop-state heartbeat.
      getAutoOptimizeLoopState(activeRunId!)
        .then((ls) => {
          setLoopState(ls)
          const attempts = ls?.attempts ?? []
          if (attempts.length === 0) {
            setLastCommitAt(null)
            return
          }
          const sig = attempts
            .map(
              (a) =>
                `${a.attemptNo}:${a.accuracy}:${a.bestAccuracy}:${a.decision}:${a.rolledBack}:${a.isChampion}`,
            )
            .join("|")
          if (sig !== attemptSigRef.current) {
            attemptSigRef.current = sig
            setLastCommitAt(Date.now())
          }
        })
        .catch(() => {})

      // Publish record (terminal banner published vs not) — null until publish.
      getAutoOptimizePublishRecord(activeRunId!)
        .then((res) => setPublishRecord(res?.publishRecord ?? null))
        .catch(() => {})

      // Benchmark QC — drives the 01 BENCHMARK_UNREPAIRABLE rail hard-fail chip.
      getAutoOptimizeBenchmarkChanges(activeRunId!)
        .then((res) => setBenchmarkQc(res?.qc ?? null))
        .catch(() => {})
    }

    poll()
    intervalRef.current = setInterval(poll, 5000)

    return () => {
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
                    An active run is already running for this space. Wait for it to complete before starting a new one.
                  </p>
                </div>
                <button
                  onClick={() => setView("monitoring")}
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
            if (activeRunId) setView("monitoring")
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
    const isTerminal = runStatus ? TERMINAL_STATUSES.has(runStatus.status) : false
    const assessedCount = questions.length
    const selectedQuestion = questions.find((q) => q.question_id === selectedQuestionId) ?? null
    const stepsCompleted = runStatus?.stepsCompleted ?? 0
    const totalSteps = runStatus?.totalSteps ?? GSO_TOTAL_STEPS
    const currentStepName = runStatus?.currentStepName ?? null

    // GSO v2 Phase 12 — the controller attempts drive the live cockpit. Empty
    // for legacy 6-step runs or before the loop commits its first attempt; the
    // view degrades to the classic ScoreSummary in that case.
    const attempts = loopState?.attempts ?? []
    const hasAttempts = attempts.length > 0
    const loop = loopState?.loopState ?? null
    const terminalReason = runStatus?.terminalReason ?? null
    const benchmarkUnrepairable = benchmarkQc?.terminalReason === "BENCHMARK_UNREPAIRABLE"
    const showTypedBanner = (isTerminal && Boolean(terminalReason)) || benchmarkUnrepairable

    const baselineAccuracy = runStatus?.baselineScore ?? null
    // targetAccuracy is normalized to 0–1; prefer the loop-state value.
    const targetUnit = loop?.targetAccuracy ?? runStatus?.targetAccuracy ?? null
    const bestAccuracy =
      loop?.bestAccuracy ?? publishRecord?.championAccuracy ?? runStatus?.optimizedScore ?? null
    // Baseline is champion only when terminal AND no attempt was flagged
    // champion (nothing beat it) — derived from explicit flags, never idxmax.
    const baselineIsChampion = isTerminal && hasAttempts && !attempts.some((a) => a.isChampion)
    // Residual failures = still-failing questions in the latest full eval.
    const residualFailureCount =
      questions.length > 0 ? questions.filter((q) => q.passed === false).length : null
    const latestAttempt = hasAttempts ? attempts[attempts.length - 1] : null

    return (
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              onClick={() => {
                setView("configure")
                if (isTerminal) setActiveRunId(null)
              }}
              className="text-sm text-accent hover:underline"
            >
              &larr; Back to configuration
            </button>
            {runStatus && (
              <Badge variant={STATUS_VARIANT[runStatus.status] ?? "secondary"}>
                {runStatus.status}
              </Badge>
            )}
          </div>
          {totalQuestions > 0 && (
            <span className="text-sm text-muted">
              {assessedCount} of {totalQuestions} assessed
            </span>
          )}
        </div>

        {/* 5-task rail (replaces the 6-step progress bar) */}
        <TaskRail
          stepsCompleted={stepsCompleted}
          currentStepName={currentStepName}
          totalSteps={totalSteps}
          status={runStatus?.status ?? null}
          terminalReason={terminalReason}
          benchmarkUnrepairable={benchmarkUnrepairable}
          onShowDetails={() => setShowPipeline(true)}
        />

        {/* Terminal banner — published vs nothing-published, keyed on reason */}
        {(isTerminal || benchmarkUnrepairable) && (
          <TerminalBanner
            status={runStatus?.status ?? null}
            terminalReason={terminalReason}
            published={publishRecord ? publishRecord.published : null}
            publishOutcome={publishRecord?.publishOutcome ?? null}
            benchmarkUnrepairable={benchmarkUnrepairable}
            championAccuracy={publishRecord?.championAccuracy ?? bestAccuracy}
            concerns={publishRecord?.concerns ?? []}
          />
        )}

        {hasAttempts ? (
          <>
            <ChampionHero
              baselineAccuracy={baselineAccuracy}
              bestAccuracy={bestAccuracy}
              targetUnit={targetUnit}
            />
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
            {!isTerminal && (
              <CurrentAttemptStrip
                attempt={latestAttempt}
                residualFailureCount={residualFailureCount}
                lastCommitAt={lastCommitAt}
                isLive={!isTerminal}
              />
            )}
          </>
        ) : (
          runStatus && (
            <ScoreSummary
              baselineScore={runStatus.baselineScore}
              optimizedScore={runStatus.optimizedScore}
              bestIteration={runStatus.bestIteration}
              status={runStatus.status}
            />
          )
        )}

        {/* Legacy / free-text convergence reason — only when no typed banner */}
        {!showTypedBanner && runStatus && (() => {
          const reason = convergenceReasonText({
            baselineScore: runStatus.baselineScore,
            optimizedScore: runStatus.optimizedScore,
            bestIteration: runStatus.bestIteration,
            status: runStatus.status,
            convergenceReason: runStatus.convergenceReason,
          })
          return reason ? (
            <p className="text-sm text-muted">Reason: {reason}</p>
          ) : null
        })()}

        {/* Two-column question layout */}
        <div className="grid grid-cols-3 gap-4 min-h-[450px]">
          <Card className="col-span-1">
            <CardContent className="p-4 h-full">
              {assessedCount === 0 ? (
                <div className="flex items-center justify-center h-full text-muted text-sm">
                  {!isTerminal ? (
                    <span className="animate-pulse">Waiting for evaluation results...</span>
                  ) : (
                    "No evaluation results available"
                  )}
                </div>
              ) : (
                <QuestionList
                  questions={questions}
                  selectedId={selectedQuestionId}
                  onSelect={setSelectedQuestionId}
                />
              )}
            </CardContent>
          </Card>

          <Card className="col-span-2">
            <CardContent className="p-6">
              <QuestionDetail question={selectedQuestion} />
            </CardContent>
          </Card>
        </div>

        {/* Footer */}
        {!isTerminal && (
          <div className="flex justify-end">
            <p className="text-xs text-muted animate-pulse">Polling every 5 seconds...</p>
          </div>
        )}

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

        {/* Pipeline Details Modal */}
        <PipelineDetailsModal runId={activeRunId} isOpen={showPipeline} onClose={() => setShowPipeline(false)} />
      </div>
    )
  }

  // Detail view — placeholder for Layer 2
  if (view === "detail" && selectedRunId) {
    return (
      <div className="space-y-4">
        <RunDetailView runId={selectedRunId} onBack={() => setView("configure")} />
      </div>
    )
  }

  return null
}
