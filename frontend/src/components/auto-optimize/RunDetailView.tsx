import { useEffect, useState } from "react"
import { ArrowLeft, ExternalLink, ShieldCheck, Sparkles } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { ScoreSummary } from "@/components/auto-optimize/ScoreSummary"
import { PublishAuditSummary } from "@/components/auto-optimize/PublishAuditSummary"
import { ResolutionActions } from "@/components/auto-optimize/ResolutionActions"
import { BenchmarkChangesPanel } from "@/components/auto-optimize/BenchmarkChangesPanel"
import { PatchesTable } from "@/components/auto-optimize/PatchesTable"
import { ResourceLinks } from "@/components/auto-optimize/ResourceLinks"
import { AttemptLadder } from "@/components/auto-optimize/AttemptLadder"
import { AttemptLedger } from "@/components/auto-optimize/AttemptLedger"
import { RunActivitySection } from "@/components/auto-optimize/RunActivitySection"
import {
  getAutoOptimizeRun,
  getAutoOptimizeIterations,
  getAutoOptimizePublishRecord,
  getAutoOptimizeLoopState,
} from "@/lib/api"
import { isTerminalStatus } from "@/lib/score-display"
import type { GSOPipelineRun, GSOIterationResult, GSOPublishRecord, GSOAttempt } from "@/types"

interface RunDetailViewProps {
  runId: string
  onBack: () => void
  onRefreshIqScore?: (runId: string, force?: boolean) => Promise<boolean>
}

const STATUS_VARIANT: Record<string, "default" | "success" | "warning" | "danger" | "info" | "secondary"> = {
  CONVERGED: "success",
  APPLIED: "success",
  STALLED: "warning",
  MAX_ITERATIONS: "warning",
  FAILED: "danger",
  CANCELLED: "secondary",
  SKIPPED: "warning",
  DISCARDED: "secondary",
  IN_PROGRESS: "info",
  RUNNING: "info",
  QUEUED: "secondary",
}

export function RunDetailView({ runId, onBack, onRefreshIqScore }: RunDetailViewProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [iterations, setIterations] = useState<GSOIterationResult[]>([])
  const [publishRecord, setPublishRecord] = useState<GSOPublishRecord | null>(null)
  // GSO v2 (Phase 14) — the 03_optimize controller attempts drive the Attempt
  // Ladder + Ledger. These are terminal runs (history), so a one-shot fetch is
  // enough — no polling, mirroring PipelineDetailsModal's terminal path.
  const [attempts, setAttempts] = useState<GSOAttempt[]>([])

  useEffect(() => {
    getAutoOptimizeRun(runId).then(setRun).catch(() => {})
    getAutoOptimizeIterations(runId).then(setIterations).catch(() => {})
    getAutoOptimizePublishRecord(runId)
      .then((res) => setPublishRecord(res?.publishRecord ?? null))
      .catch(() => {})
    getAutoOptimizeLoopState(runId)
      .then((res) => setAttempts(res?.attempts ?? []))
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

  const isTerminal = isTerminalStatus(run.status)
  // Attempt Ladder + Ledger inputs (mirror PipelineDetailsModal / AutoOptimizeTab).
  const hasAttempts = attempts.length > 0
  const targetUnit = run.targetAccuracy ?? null
  // Baseline is champion only when terminal AND nothing beat it — derived from
  // explicit is_champion flags, never idxmax.
  const baselineIsChampion = isTerminal && hasAttempts && !attempts.some((a) => a.isChampion)
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

      <RunActivitySection
        title="Benchmark QC & Repairs"
        description="Reviews benchmark quality, repairs eligible items, and establishes the evaluation set."
        icon={ShieldCheck}
      >
        <BenchmarkChangesPanel runId={runId} showTitle={false} />
      </RunActivitySection>

      <RunActivitySection
        title="Optimization"
        description="Compares attempts, selects the champion, and records the configuration patches."
        icon={Sparkles}
      >
        {/* Publish/audit summary headline (LLM paragraph + concerns). */}
        <PublishAuditSummary publishRecord={publishRecord} />

        {/* Live-state confirmation with optional rollback. */}
        {showResolution && (
          <ResolutionActions
            key={runId}
            runId={runId}
            status={run.status}
            published={resolutionPublished}
            onResolved={(s) => {
              setRun((prev) => (prev ? { ...prev, status: s } : prev))
              if (s === "DISCARDED") void onRefreshIqScore?.(runId, true)
            }}
          />
        )}

        <ScoreSummary
          baselineScore={run.baselineScore}
          optimizedScore={run.optimizedScore}
          bestIteration={run.bestIteration}
          status={run.status}
          needsReviewCount={needsReviewCount}
        />

        {/* Attempt Ladder + Ledger — the per-attempt staircase and decision ledger
            from the 03_optimize controller loop-state. Guarded by hasAttempts so
            legacy 6-step runs (no loop-state) render nothing. */}
        {hasAttempts && (
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
        )}

        <PatchesTable runId={runId} iterations={iterations} />
      </RunActivitySection>

      {run.links.length > 0 && (
        <RunActivitySection
          title="Databricks Resources"
          description="Open the Genie Agent and workflow artifacts associated with this run."
          icon={ExternalLink}
        >
          <ResourceLinks links={run.links} showHeading={false} />
        </RunActivitySection>
      )}
    </div>
  )
}
