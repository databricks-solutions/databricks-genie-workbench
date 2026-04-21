import { useEffect, useState, useRef } from "react"
import { Info, Play, Cog, BarChart2 } from "lucide-react"
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
import {
  getAutoOptimizeHealth,
  getAutoOptimizeStatus,
  getActiveRunForSpace,
  getAutoOptimizePermissions,
  getAutoOptimizeIterations,
  getAutoOptimizeAsiResults,
  getAutoOptimizeQuestionResults,
} from "@/lib/api"
import type { GSORunStatus, GSOPermissionCheck, GSOQuestionDetail } from "@/types"

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
    // Bypass the backend's TTL probe cache: the user clicked Re-check
    // because they just fixed something in the workspace; serving a
    // stale "unavailable" result would be confusing.
    getAutoOptimizePermissions(spaceId, { refresh: true })
      .then(setPermissions)
      .catch(() => setPermissions(null))
      .finally(() => setPermsLoading(false))
  }

  // Polling for active run status + ASI results
  useEffect(() => {
    if (view !== "monitoring" || !activeRunId) return

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
          // Filter to full-scope evaluations only (skip slice/p0/held_out)
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

          // Fallback: ASI results (lightweight, available before rows_json is written)
          const asiResults = await getAutoOptimizeAsiResults(activeRunId!, maxIter)
          if (asiResults && asiResults.length > 0) {
            const seen = new Map<string, typeof asiResults[0]>()
            for (const r of asiResults) {
              if (!seen.has(r.question_id) || (r.failure_type == null)) {
                seen.set(r.question_id, r)
              }
            }
            setQuestions(
              Array.from(seen.values()).map((r) => ({
                question_id: r.question_id,
                question: "",
                generated_sql: null,
                expected_sql: null,
                passed: r.failure_type == null || r.failure_type === "",
                match_type: null,
              }))
            )
          }
        })
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
    const totalSteps = runStatus?.totalSteps ?? 6
    const progressPct = Math.round((stepsCompleted / totalSteps) * 100)
    const allComplete = stepsCompleted === totalSteps
    const currentStepName = runStatus?.currentStepName ?? null

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
          <div className="flex items-center gap-4">
            {totalQuestions > 0 && (
              <span className="text-sm text-muted">
                {assessedCount} of {totalQuestions} assessed
              </span>
            )}
            {runStatus && (
              <ScoreSummary
                baselineScore={runStatus.baselineScore}
                optimizedScore={runStatus.optimizedScore}
              />
            )}
          </div>
        </div>

        {/* Progress bar */}
        <div className="space-y-1.5">
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted">
              {currentStepName && !allComplete ? (
                <>{currentStepName}{!isTerminal && <span className="animate-pulse">...</span>}</>
              ) : allComplete ? (
                "All steps complete"
              ) : (
                "Starting optimization..."
              )}
            </span>
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted">{stepsCompleted}/{totalSteps} steps</span>
              <button
                onClick={() => setShowPipeline(true)}
                className="p-1.5 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
                title="Pipeline Details"
              >
                <Cog className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
          <div className="h-1.5 rounded-full bg-elevated overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${allComplete ? "bg-emerald-500" : "bg-accent"}`}
              style={{ width: `${progressPct}%` }}
            />
          </div>
        </div>

        {runStatus?.convergenceReason && (
          <p className="text-sm text-muted">
            Reason: {runStatus.convergenceReason}
          </p>
        )}

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
