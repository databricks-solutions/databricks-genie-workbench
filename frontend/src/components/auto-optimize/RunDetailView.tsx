import { useEffect, useState } from "react"
import { ArrowLeft, Settings2 } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { QuestionList } from "@/components/auto-optimize/QuestionList"
import { QuestionDetail } from "@/components/auto-optimize/QuestionDetail"
import { PipelineDetailsModal } from "@/components/auto-optimize/PipelineDetailsModal"
import {
  getAutoOptimizeRun,
  getAutoOptimizeAsiResults,
  applyAutoOptimize,
  discardAutoOptimize,
} from "@/lib/api"
import type { GSOPipelineRun, GSOQuestionResult } from "@/types"

interface RunDetailViewProps {
  runId: string
  onBack: () => void
}

const ACTIONABLE_STATUSES = new Set(["CONVERGED", "STALLED", "MAX_ITERATIONS"])

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

export function RunDetailView({ runId, onBack }: RunDetailViewProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [questions, setQuestions] = useState<GSOQuestionResult[]>([])
  const [selectedQuestionId, setSelectedQuestionId] = useState<string | null>(null)
  const [showPipeline, setShowPipeline] = useState(false)
  const [applying, setApplying] = useState(false)
  const [discarding, setDiscarding] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch run detail
  useEffect(() => {
    getAutoOptimizeRun(runId)
      .then((r) => {
        setRun(r)
        // Fetch ASI results for the best iteration (last completed step)
        const completedSteps = (r.steps ?? []).filter((s) => s.status === "completed")
        const bestIter = completedSteps.length > 0 ? completedSteps[completedSteps.length - 1].stepNumber : 0
        return getAutoOptimizeAsiResults(runId, bestIter)
      })
      .then(setQuestions)
      .catch(() => {})
  }, [runId])

  const selectedQuestion = questions.find((q) => q.question_id === selectedQuestionId) ?? null

  const passingCount = questions.filter(
    (q) => q.failure_type == null || q.failure_type === ""
  ).length
  const totalCount = questions.length

  async function handleApply() {
    setApplying(true)
    setError(null)
    try {
      await applyAutoOptimize(runId)
      onBack()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to apply")
    } finally {
      setApplying(false)
    }
  }

  async function handleDiscard() {
    setDiscarding(true)
    setError(null)
    try {
      await discardAutoOptimize(runId)
      onBack()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to discard")
    } finally {
      setDiscarding(false)
    }
  }

  if (!run) {
    return <div className="py-8 text-center text-muted text-sm">Loading run details...</div>
  }

  const canAct = ACTIONABLE_STATUSES.has(run.status)

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
            {totalCount > 0 && (
              <p className="text-lg font-semibold text-primary mt-1">
                {((passingCount / totalCount) * 100).toFixed(0)}% accurate ({passingCount}/{totalCount})
              </p>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <ScoreSummary baselineScore={run.baselineScore} optimizedScore={run.optimizedScore} />
          <button
            onClick={() => setShowPipeline(true)}
            className="p-2 rounded-lg border border-default hover:bg-elevated text-muted hover:text-primary transition-colors"
            title="Pipeline Details"
          >
            <Settings2 className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Two-column layout */}
      <div className="grid grid-cols-3 gap-4 min-h-[400px]">
        {/* Left sidebar: Question list */}
        <Card className="col-span-1">
          <CardContent className="p-4 h-full">
            <QuestionList
              questions={questions}
              selectedId={selectedQuestionId}
              onSelect={setSelectedQuestionId}
            />
          </CardContent>
        </Card>

        {/* Right: Question detail */}
        <Card className="col-span-2">
          <CardContent className="p-6">
            <QuestionDetail question={selectedQuestion} />
          </CardContent>
        </Card>
      </div>

      {/* Apply / Discard buttons */}
      {canAct && (
        <div className="flex items-center gap-3">
          <button
            onClick={handleApply}
            disabled={applying || discarding}
            className="px-5 py-2.5 rounded-lg bg-emerald-500 text-white font-semibold hover:bg-emerald-600 disabled:opacity-50 transition-colors"
          >
            {applying ? "Applying..." : "Apply Optimization"}
          </button>
          <button
            onClick={handleDiscard}
            disabled={applying || discarding}
            className="px-5 py-2.5 rounded-lg border border-default text-primary font-semibold hover:bg-elevated disabled:opacity-50 transition-colors"
          >
            {discarding ? "Discarding..." : "Discard"}
          </button>
          {error && <span className="text-sm text-danger">{error}</span>}
        </div>
      )}

      {/* Pipeline Details Modal (Layer 3) */}
      <PipelineDetailsModal runId={runId} isOpen={showPipeline} onClose={() => setShowPipeline(false)} />
    </div>
  )
}
