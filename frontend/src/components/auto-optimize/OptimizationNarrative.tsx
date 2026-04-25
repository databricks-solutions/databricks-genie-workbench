import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import {
  CheckCircle2,
  XCircle,
  ChevronDown,
  ChevronRight,
  ArrowRight,
  Lightbulb,
  RotateCcw,
  BookOpen,
  RefreshCw,
} from "lucide-react"
import type { GSOIterationResult, GSOPipelineRun } from "@/types"
import { Tooltip } from "@/components/ui/tooltip"
import {
  convergenceReasonText,
  presentBaselineScore,
  presentOptimizedScore,
} from "@/lib/score-display"

// ── types ────────────────────────────────────────────────────────────────────

interface ReflectionEntry {
  iteration: number
  accepted: boolean
  reflectionText: string
  accuracyDelta: number | null
  fixedQuestions: string[]
  newRegressions: string[]
  rollbackReason: string | null
}

// ── parsing helpers ───────────────────────────────────────────────────────────

function parseReflection(it: GSOIterationResult): ReflectionEntry | null {
  let ref = it.reflection_json
  if (!ref) return null
  if (typeof ref === "string") {
    try {
      ref = JSON.parse(ref)
    } catch {
      return null
    }
  }
  if (typeof ref !== "object" || ref === null) return null
  const r = ref as Record<string, any>
  if (!r.reflectionText && !r.reflection_text) return null

  return {
    iteration: r.iteration ?? it.iteration,
    accepted: r.accepted ?? r.status === "accepted",
    reflectionText: r.reflectionText ?? r.reflection_text ?? "",
    accuracyDelta: r.accuracyDelta ?? r.accuracy_delta ?? null,
    fixedQuestions: r.fixedQuestions ?? r.fixed_questions ?? [],
    newRegressions: r.newRegressions ?? r.new_regressions ?? [],
    rollbackReason: r.rollbackReason ?? r.rollback_reason ?? null,
  }
}

/**
 * Strip the LLM's own status prefix to avoid "Accepted: Accepted:" duplicates.
 * The reflectionText stored in the DB often already starts with "Accepted:" or
 * "Rolled back:" because the LLM includes it, and the UI used to prepend it again.
 */
function stripStatusPrefix(text: string): string {
  return text.replace(/^(Accepted|Rolled\s*back|Rollback)[:\s]+/i, "").trim()
}

// ── outcome mapping ───────────────────────────────────────────────────────────

function outcomeInfo(status: string): {
  text: string
  variant: "success" | "warning" | "danger" | "secondary"
} {
  switch (status) {
    case "CONVERGED":
      return { text: "Converged — all accuracy thresholds met", variant: "success" }
    case "APPLIED":
      return { text: "Optimization applied", variant: "success" }
    case "MAX_ITERATIONS":
      return { text: "Reached iteration limit without full convergence", variant: "warning" }
    case "STALLED":
      return { text: "Stalled — no further improvement found", variant: "warning" }
    case "FAILED":
      return { text: "Run failed", variant: "danger" }
    default:
      return { text: status, variant: "secondary" }
  }
}

// ── RunNarrative ──────────────────────────────────────────────────────────────

function RunNarrative({
  run,
  reflections,
}: {
  run: GSOPipelineRun
  reflections: ReflectionEntry[]
}) {
  const baseline = presentBaselineScore(run.baselineScore)
  const optimized = presentOptimizedScore({
    baselineScore: run.baselineScore,
    optimizedScore: run.optimizedScore,
    bestIteration: run.bestIteration,
    status: run.status,
  })
  const delta =
    baseline.pct != null && optimized.pct != null
      ? optimized.pct - baseline.pct
      : null

  const acceptedCount = reflections.filter((r) => r.accepted).length
  const rolledBackCount = reflections.filter((r) => !r.accepted).length
  const isNoOpRun = reflections.length === 0

  const insights = reflections
    .filter((r) => r.accepted && r.reflectionText.trim().length > 0)
    .sort((a, b) => b.fixedQuestions.length - a.fixedQuestions.length)
    .slice(0, 3)

  const { text: outcomeText, variant: outcomeVariant } = outcomeInfo(run.status)
  const reasonCopy = convergenceReasonText({
    baselineScore: run.baselineScore,
    optimizedScore: run.optimizedScore,
    bestIteration: run.bestIteration,
    status: run.status,
    convergenceReason: run.convergenceReason,
  })

  const optimizedNumber = (
    <p className="text-xl font-bold text-blue-600 dark:text-blue-400">
      {optimized.text}
    </p>
  )

  return (
    <div className="rounded-xl border border-default bg-surface p-5 space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <Badge variant={outcomeVariant}>{outcomeText}</Badge>
        {!isNoOpRun && (
          <span className="text-xs text-muted">
            {reflections.length} iteration{reflections.length !== 1 ? "s" : ""}
            {acceptedCount > 0 && ` · ${acceptedCount} applied`}
            {rolledBackCount > 0 && ` · ${rolledBackCount} rolled back`}
          </span>
        )}
      </div>

      {(baseline.pct != null || optimized.pct != null) && (
        <div className="flex items-center gap-3">
          <div className="text-center">
            <p className="text-[10px] font-semibold uppercase tracking-widest text-muted mb-0.5">
              Baseline
            </p>
            <p className="text-xl font-bold text-primary">{baseline.text}</p>
          </div>
          <ArrowRight
            className={`h-4 w-4 shrink-0 ${
              delta == null
                ? "text-muted"
                : delta > 0
                ? "text-emerald-500"
                : delta < 0
                ? "text-red-500"
                : "text-muted"
            }`}
          />
          <div className="text-center">
            <p className="text-[10px] font-semibold uppercase tracking-widest text-blue-500 mb-0.5">
              Optimized
            </p>
            {optimized.tooltip ? (
              <Tooltip content={optimized.tooltip} side="bottom">
                {optimizedNumber}
              </Tooltip>
            ) : (
              optimizedNumber
            )}
          </div>
          {delta != null && (
            <div
              className={`ml-1 px-2.5 py-1 rounded-lg text-sm font-bold ${
                delta > 0
                  ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                  : delta < 0
                  ? "bg-red-500/10 text-red-500"
                  : "bg-elevated text-muted"
              }`}
            >
              {delta > 0 ? "+" : ""}
              {delta.toFixed(1)}%
            </div>
          )}
        </div>
      )}

      {reasonCopy && (
        <p className="text-xs text-muted italic">{reasonCopy}</p>
      )}

      {isNoOpRun && (
        <p className="text-sm text-muted">
          No optimization iterations were needed — the baseline already met all accuracy
          thresholds.
        </p>
      )}

      {/* Key insights from accepted iterations */}
      {insights.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-primary flex items-center gap-1.5">
            <Lightbulb className="h-3.5 w-3.5 text-amber-500" />
            What the optimizer fixed
          </p>
          <ul className="space-y-1.5">
            {insights.map((r) => {
              const text = stripStatusPrefix(r.reflectionText)
              const truncated = text.length > 160 ? text.slice(0, 157) + "…" : text
              return (
                <li key={r.iteration} className="flex items-start gap-2">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 shrink-0 mt-0.5" />
                  <span className="text-xs text-primary leading-relaxed flex-1">
                    {truncated}
                  </span>
                  {r.fixedQuestions.length > 0 && (
                    <span className="text-[10px] text-emerald-600 dark:text-emerald-400 font-medium shrink-0 mt-0.5">
                      {r.fixedQuestions.length} fixed
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        </div>
      )}

      {/* Footer: rolled-back patches and/or human review flag */}
      {(rolledBackCount > 0 || run.labelingSessionUrl != null) && (
        <div className="flex items-center gap-4 flex-wrap pt-2 border-t border-default">
          {rolledBackCount > 0 && (
            <span className="text-[11px] text-muted flex items-center gap-1">
              <RotateCcw className="h-3 w-3" />
              {rolledBackCount} patch{rolledBackCount !== 1 ? "es" : ""} rolled back
            </span>
          )}
          {run.labelingSessionUrl != null && (
            <span className="text-[11px] text-amber-600 dark:text-amber-400 flex items-center gap-1">
              <BookOpen className="h-3 w-3" />
              Questions flagged for human review
            </span>
          )}
        </div>
      )}
    </div>
  )
}

// ── IterationLogDisclosure ────────────────────────────────────────────────────

function IterationEntry({ entry }: { entry: ReflectionEntry }) {
  const cleanText = stripStatusPrefix(entry.reflectionText)

  return (
    <div
      className={`rounded-lg border px-4 py-3 ${
        entry.accepted
          ? "border-emerald-500/20 bg-emerald-500/5"
          : "border-red-500/20 bg-red-500/5"
      }`}
    >
      <div className="flex items-start gap-3">
        <Badge
          variant={entry.accepted ? "success" : "danger"}
          className="text-[10px] py-0 shrink-0 mt-0.5"
        >
          {entry.accepted ? (
            <CheckCircle2 className="h-2.5 w-2.5 mr-0.5" />
          ) : (
            <XCircle className="h-2.5 w-2.5 mr-0.5" />
          )}
          Iter {entry.iteration}
        </Badge>
        <div className="flex-1 min-w-0">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-wide mb-1">
            {entry.accepted ? "Accepted" : "Rolled back"}
          </p>
          <p className="text-xs text-primary leading-relaxed">{cleanText}</p>
          <div className="flex items-center gap-2 mt-1.5 flex-wrap">
            {entry.fixedQuestions.length > 0 && (
              <span className="text-[10px] text-emerald-600 dark:text-emerald-400 font-medium">
                {entry.fixedQuestions.length} fixed
              </span>
            )}
            {entry.newRegressions.length > 0 && (
              <Badge variant="danger" className="text-[10px] py-0 px-1.5">
                {entry.newRegressions.length} regressed
              </Badge>
            )}
            {entry.rollbackReason && (
              <span className="text-[10px] text-muted italic">{entry.rollbackReason}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function IterationLogDisclosure({ reflections }: { reflections: ReflectionEntry[] }) {
  const [open, setOpen] = useState(false)

  return (
    <div>
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 text-xs text-muted hover:text-primary transition-colors py-1"
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5" />
        )}
        {open ? "Hide" : "Show"} iteration detail ({reflections.length} iteration
        {reflections.length !== 1 ? "s" : ""})
      </button>
      {open && (
        <div className="mt-2 space-y-2">
          {reflections.map((entry) => (
            <IterationEntry key={entry.iteration} entry={entry} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── main export ───────────────────────────────────────────────────────────────

interface OptimizationNarrativeProps {
  run: GSOPipelineRun
  iterations: GSOIterationResult[]
  /**
   * @deprecated The narrative now reads ``convergenceReason`` directly from
   * ``run`` so it can be combined with the canonical ``bestIteration``
   * signal (e.g. "Baseline retained — no improvement after 3 attempts").
   * Kept on the props for back-compat with existing callers.
   */
  convergenceReason?: string | null
}

export function OptimizationNarrative({ run, iterations }: OptimizationNarrativeProps) {
  const reflections = iterations
    .filter((it) => it.iteration > 0)
    .map(parseReflection)
    .filter((r): r is ReflectionEntry => r !== null)

  return (
    <div className="space-y-3">
      <h3 className="text-sm font-semibold text-primary flex items-center gap-1.5">
        <RefreshCw className="h-3.5 w-3.5" />
        Optimization Summary
      </h3>
      <RunNarrative run={run} reflections={reflections} />
      {reflections.length > 0 && (
        <IterationLogDisclosure reflections={reflections} />
      )}
    </div>
  )
}
