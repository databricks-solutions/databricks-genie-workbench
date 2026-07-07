import { useEffect, useMemo, useState } from "react"
import { ArrowLeft, Cog, Star } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { QuestionList } from "@/components/auto-optimize/QuestionList"
import { QuestionDetail } from "@/components/auto-optimize/QuestionDetail"
import { PipelineDetailsModal } from "@/components/auto-optimize/PipelineDetailsModal"
import { PublishAuditSummary } from "@/components/auto-optimize/PublishAuditSummary"
import { OptimizationNarrative } from "@/components/auto-optimize/OptimizationNarrative"
import { ResolutionActions } from "@/components/auto-optimize/ResolutionActions"
import { BenchmarkChangesPanel } from "@/components/auto-optimize/BenchmarkChangesPanel"
import {
  getAutoOptimizeRun,
  getAutoOptimizeQuestionResults,
  getAutoOptimizeIterations,
  getAutoOptimizePublishRecord,
} from "@/lib/api"
import type { GSOPipelineRun, GSOQuestionDetail, GSOIterationResult, GSOPublishRecord } from "@/types"
import { evalCountsFromIteration } from "@/lib/eval-counts"
import { buildAttemptOptions, questionCacheKey, selectCachedQuestions } from "@/components/auto-optimize/runDetail"

interface RunDetailViewProps {
  runId: string
  onBack: () => void
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

const TERMINAL_STATUSES = new Set([
  "CONVERGED",
  "STALLED",
  "MAX_ITERATIONS",
  "FAILED",
  "CANCELLED",
  "APPLIED",
  "DISCARDED",
])

export function RunDetailView({ runId, onBack }: RunDetailViewProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [iterations, setIterations] = useState<GSOIterationResult[]>([])
  const [publishRecord, setPublishRecord] = useState<GSOPublishRecord | null>(null)
  // Per-attempt question results, keyed by `${runId}:${iteration}` (composite so
  // the same iteration number in two runs can't collide), lazy-loaded.
  const [questionsByIter, setQuestionsByIter] = useState<Map<string, GSOQuestionDetail[]>>(new Map())
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [selectedQuestionId, setSelectedQuestionId] = useState<string | null>(null)
  const [showPipeline, setShowPipeline] = useState(false)

  // Reset on runId change is handled by remount: the parent keys this view by
  // runId (<RunDetailView key={runId} …/>), so every runId gets a fresh
  // instance with fresh state — no run-scoped slice survives across runs. The
  // composite question-cache key below is belt-and-suspenders for the same
  // invariant. This effect therefore only fetches for the current runId.
  useEffect(() => {
    getAutoOptimizeRun(runId).then(setRun).catch(() => {})
    getAutoOptimizeIterations(runId).then(setIterations).catch(() => {})
    getAutoOptimizePublishRecord(runId)
      .then((res) => setPublishRecord(res?.publishRecord ?? null))
      .catch(() => {})
  }, [runId])

  // The per-question Attempt Selector model (Baseline · Patch N · best★),
  // defaulting to the champion. Degrades to Baseline · Final for legacy runs
  // with no attempt metadata.
  const { options, defaultKey } = useMemo(
    () =>
      buildAttemptOptions({
        iterations,
        baselineIteration: run?.baselineIteration ?? null,
        bestIteration: run?.bestIteration ?? null,
      }),
    [iterations, run?.baselineIteration, run?.bestIteration],
  )

  // The selection defaults to the champion (defaultKey) until the user picks an
  // attempt; deriving it avoids a setState-in-effect to seed the default.
  const activeKey = selectedKey ?? defaultKey
  const activeOption = options.find((o) => o.key === activeKey) ?? null
  const activeIteration = activeOption?.iteration ?? null

  // Lazy-load + cache question results for the selected attempt's iteration,
  // under a run-scoped composite key so a cached iter-N from another run can't
  // satisfy this run's lookup.
  useEffect(() => {
    if (activeIteration == null) return
    const cacheKey = questionCacheKey(runId, activeIteration)
    if (questionsByIter.has(cacheKey)) return
    let active = true
    getAutoOptimizeQuestionResults(runId, activeIteration)
      .then((qs) => {
        if (active) setQuestionsByIter((prev) => new Map(prev).set(cacheKey, qs))
      })
      .catch(() => {})
    return () => {
      active = false
    }
  }, [runId, activeIteration, questionsByIter])

  const questions = selectCachedQuestions(questionsByIter, runId, activeIteration)
  const selectedQuestion = questions.find((q) => q.question_id === selectedQuestionId) ?? null

  const fullIterations = iterations.filter(
    (it) => String(it.eval_scope ?? "full").toLowerCase() === "full",
  )
  const activeIterRow =
    activeIteration != null ? fullIterations.find((it) => it.iteration === activeIteration) : undefined
  const activeCounts = evalCountsFromIteration(activeIterRow)

  // ScoreSummary stays run-level (baseline → optimized); needs-review comes from
  // the best/optimized iteration row.
  const bestIterRow =
    run?.bestIteration != null ? fullIterations.find((it) => it.iteration === run.bestIteration) : undefined
  const needsReviewCount =
    bestIterRow?.num_needs_review ??
    (activeIterRow?.num_needs_review ??
      questions.filter((q) => (q.assessment ?? "").toUpperCase() === "NEEDS_REVIEW").length)

  if (!run) {
    return <div className="py-8 text-center text-muted text-sm">Loading run details...</div>
  }

  const isTerminal = TERMINAL_STATUSES.has(run.status)
  const resolutionPublished = publishRecord ? publishRecord.published : null
  const showResolution =
    isTerminal &&
    (run.status === "APPLIED" ||
      run.status === "DISCARDED" ||
      resolutionPublished === true ||
      (resolutionPublished == null && (run.status === "CONVERGED" || run.status === "MAX_ITERATIONS")))

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={onBack}
            className="p-2 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
          </button>
          <div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted">
                {run.startedAt ? new Date(run.startedAt).toLocaleDateString(undefined, {
                  month: "short", day: "numeric", year: "numeric",
                }) : ""}
              </span>
              <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>
                {run.status}
              </Badge>
            </div>
            {activeCounts.evaluated > 0 && activeCounts.accuracyPct != null && (
              <p className="text-lg font-semibold text-primary mt-1">
                {activeCounts.accuracyPct.toFixed(1)}% accurate ({activeCounts.correct}/{activeCounts.evaluated})
                {activeOption && <span className="ml-2 text-sm font-normal text-muted">· {activeOption.label}</span>}
              </p>
            )}
          </div>
        </div>

        <button
          onClick={() => setShowPipeline(true)}
          className="p-2 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
          title="Pipeline Details"
        >
          <Cog className="w-4 h-4" />
        </button>
      </div>

      {/* Publish/audit summary headline (LLM paragraph + concerns), with the
          legacy per-iteration narrative demoted to a collapsed detail. */}
      <PublishAuditSummary publishRecord={publishRecord}>
        <OptimizationNarrative run={run} iterations={iterations} convergenceReason={run.convergenceReason} />
      </PublishAuditSummary>

      {/* Keep / Discard-rollback affordance (auto-publish model). */}
      {showResolution && (
        <ResolutionActions
          key={runId}
          runId={runId}
          status={run.status}
          published={resolutionPublished}
          onResolved={(s) => setRun((prev) => (prev ? { ...prev, status: s } : prev))}
        />
      )}

      <ScoreSummary
        baselineScore={run.baselineScore}
        optimizedScore={run.optimizedScore}
        bestIteration={run.bestIteration}
        status={run.status}
        needsReviewCount={needsReviewCount}
      />

      {/* Benchmark QC & changes — first-class surface under task 01. */}
      <BenchmarkChangesPanel runId={runId} />

      {/* Per-question attempt selector (Baseline · Patch N · best★) */}
      {options.length > 0 && (
        <div className="flex flex-wrap gap-1 border-b border-default">
          {options.map((opt) => {
            const isActive = opt.key === activeKey
            return (
              <button
                key={opt.key}
                onClick={() => {
                  setSelectedKey(opt.key)
                  setSelectedQuestionId(null)
                }}
                className={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                  isActive
                    ? "border-accent text-accent"
                    : "border-transparent text-muted hover:text-primary"
                }`}
              >
                {opt.starred && <Star className="h-3.5 w-3.5 fill-amber-400 text-amber-400" />}
                {opt.label}
                {opt.accuracyPct != null && (
                  <span className="text-xs opacity-75">({opt.accuracyPct.toFixed(1)}%)</span>
                )}
              </button>
            )
          })}
        </div>
      )}

      {/* Two-column layout */}
      <div className="grid grid-cols-3 gap-4 min-h-[400px]">
        {/* Left sidebar: Question list */}
        <Card className="col-span-1">
          <CardContent className="p-4 h-full">
            {questions.length === 0 ? (
              <div className="flex items-center justify-center h-full text-muted text-sm">
                No evaluation results available
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

        {/* Right: Question detail */}
        <Card className="col-span-2">
          <CardContent className="p-6">
            <QuestionDetail question={selectedQuestion} />
          </CardContent>
        </Card>
      </div>

      {/* Pipeline Details Modal */}
      <PipelineDetailsModal runId={runId} isOpen={showPipeline} onClose={() => setShowPipeline(false)} />
    </div>
  )
}
