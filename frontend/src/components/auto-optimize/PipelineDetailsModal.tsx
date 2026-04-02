import { useEffect, useState, useRef } from "react"
import { X, TrendingUp, Pen } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { PipelineStepCard } from "@/components/auto-optimize/PipelineStepCard"
import { StepDetailContent } from "@/components/auto-optimize/StepDetailContent"
import { OptimizationLevers } from "@/components/auto-optimize/OptimizationLevers"
import { IterationChart } from "@/components/auto-optimize/IterationChart"
import { StageTimeline } from "@/components/auto-optimize/StageTimeline"
import { ResourceLinks } from "@/components/auto-optimize/ResourceLinks"
import { QuestionJourney } from "@/components/auto-optimize/QuestionJourney"
import { PatchesTable } from "@/components/auto-optimize/PatchesTable"
import { ActivityLog } from "@/components/auto-optimize/ActivityLog"
import { JudgePassRates } from "@/components/auto-optimize/JudgePassRates"
import { OptimizationNarrative } from "@/components/auto-optimize/OptimizationNarrative"
import { SuggestionsPanel } from "@/components/auto-optimize/SuggestionsPanel"
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

function toPct(v: number | null | undefined): string {
  if (v == null) return "\u2014"
  const n = Number(v)
  return `${(n > 1 ? n : n * 100).toFixed(1)}%`
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

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    if (!isOpen) return
    setRun(null)
    setIterations([])

    function fetchData() {
      getAutoOptimizeRun(runId).then(setRun).catch(() => {})
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

  const stepsCompleted = run?.steps?.filter((s) => s.status === "completed" || s.status === "skipped").length ?? 0
  const totalSteps = 6
  const progressPct = Math.round((stepsCompleted / totalSteps) * 100)
  const allComplete = stepsCompleted === totalSteps

  const baselineScore = run?.baselineScore != null ? Number(run.baselineScore) : null
  const optimizedScore = run?.optimizedScore != null ? Number(run.optimizedScore) : null
  const bNorm = baselineScore != null ? (baselineScore > 1 ? baselineScore : baselineScore * 100) : null
  const oNorm = optimizedScore != null ? (optimizedScore > 1 ? optimizedScore : optimizedScore * 100) : null
  const improvement = bNorm != null && oNorm != null ? oNorm - bNorm : null

  // Extract baseline judge scores from step 2 (Baseline Evaluation) outputs
  const baselineStep = run?.steps?.find((s) => s.stepNumber === 2)
  const baselineJudgeScores = (baselineStep?.outputs?.judgeScores as Record<string, number | null> | undefined) ?? null

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-surface">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-default shrink-0">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-primary">Optimization Pipeline</h2>
          {run && (
            <>
              <Badge variant={STATUS_VARIANT[run.status] ?? "secondary"}>{run.status}</Badge>
              {run.deploymentStatus && (
                <Badge variant="secondary">
                  <Pen className="h-3 w-3 mr-1" />
                  {run.deploymentStatus === "skipped" ? "Deploy skipped" : `Deploy: ${run.deploymentStatus}`}
                </Badge>
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
        <div className="max-w-5xl mx-auto px-6 py-6 space-y-8">
          {!run ? (
            <p className="text-sm text-muted text-center py-12 animate-pulse">Loading pipeline details...</p>
          ) : (
            <>
              {/* Run metadata + progress bar */}
              <div className="space-y-3">
                <p className="text-sm text-muted">
                  Run <span className="font-mono text-primary">{run.runId.slice(0, 7)}</span>
                  {" \u00B7 "}
                  Started {formatDateTime(run.startedAt)}
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
                  <div className="rounded-xl border border-default p-5">
                    <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1">Baseline</p>
                    <p className="text-3xl font-bold text-primary">{toPct(baselineScore)}</p>
                  </div>
                  <div className="rounded-xl border border-accent/30 bg-accent/5 p-5">
                    <p className="text-xs font-medium text-accent uppercase tracking-wide mb-1">Optimized</p>
                    <p className="text-3xl font-bold text-accent">{toPct(optimizedScore)}</p>
                  </div>
                  <div className={`rounded-xl border p-5 ${improvement != null && improvement > 0 ? "border-emerald-500/30 bg-emerald-500/5" : "border-default"}`}>
                    <p className="text-xs font-medium text-muted uppercase tracking-wide mb-1 flex items-center gap-1">
                      <TrendingUp className="w-3 h-3" />
                      Improvement
                    </p>
                    <p className={`text-3xl font-bold ${improvement != null && improvement > 0 ? "text-emerald-500" : "text-primary"}`}>
                      {improvement != null ? `${improvement > 0 ? "+" : ""}${improvement.toFixed(1)}%` : "\u2014"}
                    </p>
                  </div>
                </div>
              )}

              {/* Main tabs: Summary / Iteration Explorer / Suggestions */}
              {runIsTerminal && iterations.length > 0 && (
                <Tabs defaultValue="summary">
                  <TabsList>
                    <TabsTrigger value="summary">Summary</TabsTrigger>
                    <TabsTrigger value="iterations">Iteration Explorer</TabsTrigger>
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
                        <TabsTrigger value="judges">Judges</TabsTrigger>
                      </TabsList>

                      <TabsContent value="overview">
                        <div className="space-y-6">
                          {/* Charts */}
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                            <IterationChart iterations={iterations} />
                            <StageTimeline stages={run.stages ?? []} />
                          </div>
                          {/* Per-Judge Score Progression */}
                          <JudgePassRates iterations={iterations} baselineJudgeScores={baselineJudgeScores} />
                          {/* Optimization Narrative — rich per-iteration reflections */}
                          <OptimizationNarrative run={run} iterations={iterations} convergenceReason={run.convergenceReason} />
                        </div>
                      </TabsContent>

                      <TabsContent value="questions">
                        <QuestionJourney runId={runId} iterations={iterations} />
                      </TabsContent>

                      <TabsContent value="patches">
                        <PatchesTable runId={runId} />
                      </TabsContent>

                      <TabsContent value="activity">
                        <ActivityLog stages={run.stages ?? []} />
                      </TabsContent>

                      <TabsContent value="judges">
                        <JudgePassRates iterations={iterations} baselineJudgeScores={baselineJudgeScores} />
                      </TabsContent>
                    </Tabs>
                  </TabsContent>

                  {/* Iteration Explorer tab */}
                  <TabsContent value="iterations">
                    <div className="rounded-xl border border-default p-6">
                      <h3 className="text-sm font-semibold text-primary mb-4">Iteration Accuracy Progression</h3>
                      <div className="overflow-hidden rounded-lg border border-default">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="bg-elevated border-b border-default">
                              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Iteration</th>
                              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Lever</th>
                              <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Accuracy</th>
                              <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Questions</th>
                              <th className="text-center px-4 py-2.5 text-xs font-medium text-muted">Gates Met</th>
                            </tr>
                          </thead>
                          <tbody>
                            {iterations.map((it) => {
                              const isBaseline = it.iteration === 0
                              const bestAccuracy = Math.max(...iterations.filter(i => i.iteration > 0).map(i => i.overall_accuracy))
                              const isBest = !isBaseline && it.overall_accuracy === bestAccuracy
                              const LEVER_NAMES: Record<number, string> = {
                                1: "Tables & Columns", 2: "Metric Views", 3: "SQL Queries", 4: "Joins", 5: "Text Instructions", 6: "SQL Expressions",
                              }
                              return (
                                <tr
                                  key={it.iteration}
                                  className={`border-b border-default last:border-0 ${
                                    isBest ? "bg-emerald-50 dark:bg-emerald-950/30" : isBaseline ? "bg-elevated/50" : ""
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
                                    {it.lever != null ? (LEVER_NAMES[it.lever] ?? `Lever ${it.lever}`) : "\u2014"}
                                  </td>
                                  <td className="px-4 py-2.5 text-right font-mono">
                                    <span className={isBest ? "text-emerald-600 dark:text-emerald-400 font-semibold" : "text-primary"}>
                                      {toPct(it.overall_accuracy)}
                                    </span>
                                    {isBest && <span className="ml-1.5 text-xs text-emerald-600 dark:text-emerald-400">\u2190 best</span>}
                                  </td>
                                  <td className="px-4 py-2.5 text-right text-muted font-mono">
                                    {it.correct_count}/{it.total_questions}
                                  </td>
                                  <td className="px-4 py-2.5 text-center">
                                    {it.thresholds_met ? (
                                      <span className="text-emerald-500 text-xs font-medium">Yes</span>
                                    ) : (
                                      <span className="text-muted text-xs">\u2014</span>
                                    )}
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </TabsContent>

                  {/* Suggestions tab */}
                  <TabsContent value="suggestions">
                    <SuggestionsPanel runId={runId} />
                  </TabsContent>
                </Tabs>
              )}

              {/* Pipeline Steps */}
              <div>
                <h3 className="text-sm font-semibold text-primary mb-3">Pipeline Steps</h3>
                <div className="space-y-3">
                  {Array.from({ length: 6 }, (_, i) => {
                    const stepNum = i + 1
                    const step = run.steps?.find((s) => s.stepNumber === stepNum)
                    const meta = STEP_DESCRIPTIONS[stepNum]

                    // Filter levers for this step
                    const stepLevers = stepNum === 3
                      ? run.levers?.filter(l => l.lever === 0) ?? []
                      : stepNum === 4
                      ? run.levers?.filter(l => l.lever >= 1) ?? []
                      : []

                    return (
                      <div key={stepNum}>
                        <PipelineStepCard
                          stepNumber={stepNum}
                          name={step?.name ?? meta?.name ?? `Step ${stepNum}`}
                          status={stepNum === 6 ? "skipped" : (step?.status ?? "pending")}
                          durationSeconds={step?.durationSeconds ?? null}
                          description={meta?.description ?? ""}
                          summary={step?.summary ?? null}
                        >
                          {step?.outputs && <StepDetailContent step={step} />}
                        </PipelineStepCard>
                        {stepLevers.length > 0 && (
                          <OptimizationLevers levers={stepLevers} />
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>

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
