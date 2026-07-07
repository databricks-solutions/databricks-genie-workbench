import { useEffect, useState } from "react"
import { ArrowLeft } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { PublishAuditSummary } from "@/components/auto-optimize/PublishAuditSummary"
import { OptimizationNarrative } from "@/components/auto-optimize/OptimizationNarrative"
import { ResolutionActions } from "@/components/auto-optimize/ResolutionActions"
import { BenchmarkChangesPanel } from "@/components/auto-optimize/BenchmarkChangesPanel"
import { PatchesTable } from "@/components/auto-optimize/PatchesTable"
import { ResourceLinks } from "@/components/auto-optimize/ResourceLinks"
import {
  getAutoOptimizeRun,
  getAutoOptimizeIterations,
  getAutoOptimizePublishRecord,
} from "@/lib/api"
import type { GSOPipelineRun, GSOIterationResult, GSOPublishRecord } from "@/types"

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

  useEffect(() => {
    getAutoOptimizeRun(runId).then(setRun).catch(() => {})
    getAutoOptimizeIterations(runId).then(setIterations).catch(() => {})
    getAutoOptimizePublishRecord(runId)
      .then((res) => setPublishRecord(res?.publishRecord ?? null))
      .catch(() => {})
  }, [runId])

  const fullIterations = iterations.filter(
    (it) => String(it.eval_scope ?? "full").toLowerCase() === "full",
  )

  // ScoreSummary stays run-level (baseline → optimized); needs-review comes from
  // the best/optimized iteration row.
  const bestIterRow =
    run?.bestIteration != null ? fullIterations.find((it) => it.iteration === run.bestIteration) : undefined
  const needsReviewCount = bestIterRow?.num_needs_review ?? 0

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
          <p className="text-lg font-semibold text-primary mt-1">
            Optimization run
          </p>
        </div>
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

      <PatchesTable runId={runId} iterations={iterations} />

      {run.links.length > 0 && <ResourceLinks links={run.links} />}
    </div>
  )
}
