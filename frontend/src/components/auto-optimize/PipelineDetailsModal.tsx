import { useEffect, useState } from "react"
import { X, TrendingUp } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { PipelineStepCard } from "@/components/auto-optimize/PipelineStepCard"
import { getAutoOptimizeRun, getAutoOptimizeIterations } from "@/lib/api"
import type { GSOPipelineRun, GSOIterationResult } from "@/types"

interface PipelineDetailsModalProps {
  runId: string
  isOpen: boolean
  onClose: () => void
}

const STEP_DESCRIPTIONS: Record<number, { name: string; description: string }> = {
  1: { name: "Preflight",             description: "Reads config and queries Unity Catalog for metadata" },
  2: { name: "Baseline Evaluation",   description: "Runs benchmarks through 9 evaluation judges" },
  3: { name: "Proactive Enrichment",  description: "Enriches descriptions, joins, and instructions" },
  4: { name: "Adaptive Optimization", description: "Applies optimization levers with 3-gate eval" },
  5: { name: "Finalization",          description: "Repeatability checks and model promotion" },
  6: { name: "Deploy",                description: "Deploys optimized config to target" },
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

const LEVER_NAMES: Record<number, string> = {
  1: "Tables & Columns",
  2: "Metric Views",
  3: "TVFs",
  4: "Joins",
  5: "Instructions",
}

function toPct(v: number | string | null | undefined): string {
  if (v == null) return "—"
  const n = Number(v)
  return `${(n > 1 ? n : n * 100).toFixed(1)}%`
}

function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString("en-US", {
    month: "short", day: "numeric", year: "numeric",
    hour: "numeric", minute: "2-digit",
  })
}

function parseJudgeScores(scoresJson: string): Record<string, number> | null {
  try {
    return JSON.parse(scoresJson)
  } catch {
    return null
  }
}

export function PipelineDetailsModal({ runId, isOpen, onClose }: PipelineDetailsModalProps) {
  const [run, setRun] = useState<GSOPipelineRun | null>(null)
  const [iterations, setIterations] = useState<GSOIterationResult[]>([])

  useEffect(() => {
    if (!isOpen) return
    setRun(null)
    setIterations([])
    getAutoOptimizeRun(runId).then(setRun).catch(() => {})
    getAutoOptimizeIterations(runId)
      .then((its) => setIterations(its.filter((it) => it.eval_scope === "full")))
      .catch(() => {})
  }, [runId, isOpen])

  if (!isOpen) return null

  const stepsCompleted = run?.steps?.filter((s) => s.status === "completed").length ?? 0
  const totalSteps = 6
  const progressPct = Math.round((stepsCompleted / totalSteps) * 100)
  const allComplete = stepsCompleted === totalSteps

  const baselineScore = run?.baselineScore != null ? Number(run.baselineScore) : null
  const optimizedScore = run?.optimizedScore != null ? Number(run.optimizedScore) : null
  const improvement = baselineScore != null && optimizedScore != null
    ? (optimizedScore > 1 ? optimizedScore : optimizedScore * 100) - (baselineScore > 1 ? baselineScore : baselineScore * 100)
    : null

  // Best non-baseline iteration for per-judge scores
  const nonBaselineIters = iterations.filter((it) => it.iteration > 0)
  const bestIter = nonBaselineIters.length > 0
    ? nonBaselineIters.reduce((a, b) => a.overall_accuracy > b.overall_accuracy ? a : b)
    : null
  const judgeScores = bestIter?.scores_json ? parseJudgeScores(bestIter.scores_json) : null

  const baselineIter = iterations.find((it) => it.iteration === 0)
  const baselineJudgeScores = baselineIter?.scores_json ? parseJudgeScores(baselineIter.scores_json) : null

  const bestAccuracy = nonBaselineIters.length > 0
    ? Math.max(...nonBaselineIters.map((it) => it.overall_accuracy))
    : null

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-surface">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-default shrink-0">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-primary">Optimization Pipeline</h2>
          {run && (
            <>
              <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>{run.status}</Badge>
              {run.completedAt === null && (
                <span className="text-xs text-info animate-pulse">Live</span>
              )}
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
        <div className="max-w-4xl mx-auto px-6 py-6 space-y-8">
          {!run ? (
            <p className="text-sm text-muted text-center py-12">Loading pipeline details...</p>
          ) : (
            <>
              {/* Run metadata + progress */}
              <div className="space-y-3">
                <p className="text-sm text-muted">
                  Run <span className="font-mono text-primary">{run.runId.slice(0, 7)}</span>
                  {" · "}
                  Started {formatDateTime(run.startedAt)}
                  {run.completedAt && ` · Completed ${formatDateTime(run.completedAt)}`}
                </p>
                <div className="space-y-1">
                  <div className="flex items-center justify-between text-xs text-muted">
                    <span>{allComplete ? "All steps complete" : `${stepsCompleted} of ${totalSteps} steps complete`}</span>
                    <span>{stepsCompleted}/{totalSteps} steps</span>
                  </div>
                  <div className="h-2 rounded-full bg-elevated overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${allComplete ? "bg-emerald-500" : "bg-accent"}`}
                      style={{ width: `${progressPct}%` }}
                    />
                  </div>
                </div>
              </div>

              {/* Score KPI cards */}
              {(baselineScore != null || optimizedScore != null) && (
                <div className="grid grid-cols-3 gap-4">
                  <div className="rounded-xl border border-default p-4">
                    <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1">Baseline</p>
                    <p className="text-3xl font-bold text-primary">{toPct(baselineScore)}</p>
                  </div>
                  <div className="rounded-xl border border-accent/30 bg-accent/5 p-4">
                    <p className="text-xs font-medium text-accent uppercase tracking-wide mb-1">Optimized</p>
                    <p className="text-3xl font-bold text-accent">{toPct(optimizedScore)}</p>
                  </div>
                  <div className={`rounded-xl border p-4 ${improvement != null && improvement > 0 ? "border-emerald-500/30 bg-emerald-500/5" : "border-default"}`}>
                    <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1 flex items-center gap-1">
                      <TrendingUp className="w-3 h-3" />
                      Improvement
                    </p>
                    <p className={`text-3xl font-bold ${improvement != null && improvement > 0 ? "text-emerald-500" : "text-primary"}`}>
                      {improvement != null ? `${improvement > 0 ? "+" : ""}${improvement.toFixed(1)}%` : "—"}
                    </p>
                  </div>
                </div>
              )}

              {/* Iteration Accuracy Progression */}
              {iterations.length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-primary mb-3">Iteration Accuracy Progression</h3>
                  <div className="overflow-hidden rounded-lg border border-default">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-elevated border-b border-default">
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Iteration</th>
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Lever</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Accuracy</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Questions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {iterations.map((it) => {
                          const isBaseline = it.iteration === 0
                          const isBest = bestAccuracy !== null && it.overall_accuracy === bestAccuracy && !isBaseline
                          return (
                            <tr
                              key={it.iteration}
                              className={`border-b border-default last:border-0 ${
                                isBest
                                  ? "bg-emerald-50 dark:bg-emerald-950/30"
                                  : isBaseline
                                  ? "bg-elevated/50"
                                  : ""
                              }`}
                            >
                              <td className="px-4 py-2.5">
                                {isBaseline ? (
                                  <span className="font-medium text-primary">0 (Baseline)</span>
                                ) : (
                                  <span className="text-muted">{it.iteration}</span>
                                )}
                              </td>
                              <td className="px-4 py-2.5 text-muted">
                                {it.lever != null ? (LEVER_NAMES[it.lever] ?? `Lever ${it.lever}`) : "—"}
                              </td>
                              <td className="px-4 py-2.5 text-right font-mono">
                                <span className={isBest ? "text-emerald-600 dark:text-emerald-400 font-semibold" : "text-primary"}>
                                  {toPct(it.overall_accuracy)}
                                </span>
                                {isBest && (
                                  <span className="ml-1.5 text-xs text-emerald-600 dark:text-emerald-400">← best</span>
                                )}
                              </td>
                              <td className="px-4 py-2.5 text-right text-muted font-mono">
                                {it.correct_count}/{it.total_questions}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Per-Judge Score Breakdown */}
              {judgeScores && Object.keys(judgeScores).length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-primary mb-3">Per-Judge Score Breakdown</h3>
                  <div className="overflow-hidden rounded-lg border border-default">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-elevated border-b border-default">
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Judge</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Baseline</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Best</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Delta</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(judgeScores).sort(([a], [b]) => a.localeCompare(b)).map(([judge, score]) => {
                          const baseScore = baselineJudgeScores?.[judge]
                          const delta = baseScore != null ? score - baseScore : null
                          return (
                            <tr key={judge} className="border-b border-default last:border-0">
                              <td className="px-4 py-2.5 text-primary font-medium">{judge}</td>
                              <td className="px-4 py-2.5 text-right text-muted font-mono">
                                {baseScore != null ? toPct(baseScore) : "—"}
                              </td>
                              <td className="px-4 py-2.5 text-right font-mono text-primary">{toPct(score)}</td>
                              <td className="px-4 py-2.5 text-right font-mono">
                                {delta != null ? (
                                  <span className={delta > 0 ? "text-emerald-500" : delta < 0 ? "text-danger" : "text-muted"}>
                                    {delta > 0 ? "+" : ""}{(delta * (Math.abs(score) <= 1 ? 100 : 1)).toFixed(1)}%
                                  </span>
                                ) : "—"}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Optimization Narrative */}
              {run.convergenceReason && (
                <div>
                  <h3 className="text-sm font-semibold text-primary mb-3">Optimization Narrative</h3>
                  <div className="rounded-lg border border-accent/20 bg-accent/5 px-4 py-3">
                    <p className="text-sm text-primary leading-relaxed">{run.convergenceReason}</p>
                  </div>
                </div>
              )}

              {/* Pipeline Steps */}
              <div>
                <h3 className="text-sm font-semibold text-primary mb-3">Pipeline Steps</h3>
                <div className="space-y-3">
                  {Array.from({ length: 6 }, (_, i) => {
                    const stepNum = i + 1
                    const step = run.steps?.find((s) => s.stepNumber === stepNum)
                    const meta = STEP_DESCRIPTIONS[stepNum]
                    return (
                      <PipelineStepCard
                        key={stepNum}
                        stepNumber={stepNum}
                        name={step?.name ?? meta?.name ?? `Step ${stepNum}`}
                        status={step?.status ?? "pending"}
                        durationSeconds={step?.durationSeconds ?? null}
                        description={meta?.description ?? ""}
                        summary={step?.summary ?? null}
                      />
                    )
                  })}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
