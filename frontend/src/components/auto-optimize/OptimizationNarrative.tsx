import { Badge } from "@/components/ui/badge"
import { CheckCircle2, XCircle, RefreshCw } from "lucide-react"
import type { GSOIterationResult } from "@/types"

interface OptimizationNarrativeProps {
  iterations: GSOIterationResult[]
  convergenceReason: string | null
}

interface ReflectionEntry {
  iteration: number
  accepted: boolean
  reflectionText: string
  accuracyDelta: number | null
  fixedQuestions: string[]
  newRegressions: string[]
  rollbackReason: string | null
}

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

function NarrativeEntry({ entry }: { entry: ReflectionEntry }) {
  const deltaPct = entry.accuracyDelta != null
    ? (Math.abs(entry.accuracyDelta) > 1 ? entry.accuracyDelta : entry.accuracyDelta * 100)
    : null

  return (
    <div className={`rounded-lg border px-4 py-3 ${entry.accepted ? "border-emerald-500/20 bg-emerald-500/5" : "border-red-500/20 bg-red-500/5"}`}>
      <div className="flex items-start gap-3">
        <div className="flex items-center gap-2 shrink-0 mt-0.5">
          <Badge variant={entry.accepted ? "success" : "danger"} className="text-[10px] py-0">
            {entry.accepted ? <CheckCircle2 className="h-2.5 w-2.5 mr-0.5" /> : <XCircle className="h-2.5 w-2.5 mr-0.5" />}
            Iter {entry.iteration}
          </Badge>
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-xs text-primary leading-relaxed">
            {entry.accepted ? "Accepted" : "Rolled back"}: {entry.reflectionText}
          </p>
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
          </div>
        </div>
        {deltaPct != null && (
          <span className={`text-xs font-mono font-medium shrink-0 ${deltaPct >= 0 ? "text-emerald-600" : "text-red-500"}`}>
            {deltaPct >= 0 ? "+" : ""}{deltaPct.toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  )
}

export function OptimizationNarrative({ iterations, convergenceReason }: OptimizationNarrativeProps) {
  // Parse reflection entries from iterations > 0 that have reflection_json
  const reflections = iterations
    .filter((it) => it.iteration > 0)
    .map(parseReflection)
    .filter((r): r is ReflectionEntry => r !== null)

  return (
    <div>
      <h3 className="text-sm font-semibold text-primary mb-3 flex items-center gap-1.5">
        <RefreshCw className="h-3.5 w-3.5" />
        Optimization Narrative
      </h3>
      {reflections.length > 0 ? (
        <div className="space-y-2">
          {reflections.map((entry) => (
            <NarrativeEntry key={entry.iteration} entry={entry} />
          ))}
        </div>
      ) : convergenceReason ? (
        <div className="rounded-lg border border-accent/20 bg-accent/5 px-4 py-3">
          <p className="text-sm text-primary leading-relaxed">{convergenceReason}</p>
        </div>
      ) : null}
    </div>
  )
}
