import { FlaskConical, Zap, TrendingUp, TrendingDown, Minus } from "lucide-react"
import { Tooltip } from "@/components/ui/tooltip"
import {
  presentBaselineScore,
  presentOptimizedScore,
} from "@/lib/score-display"

interface ScoreSummaryProps {
  baselineScore: number | null
  optimizedScore: number | null
  /**
   * ``0`` means no iter > 0 has strictly improved on baseline (mid-run, or
   * the run completed with baseline as best). ``N > 0`` is the iteration
   * that achieved ``optimizedScore``. ``null`` means the backend did not
   * supply this signal (older API), in which case we treat it like an
   * old-style "just trust the numbers" caller.
   */
  bestIteration?: number | null
  /**
   * Run status — needed to disambiguate "in progress" (show "—" + tooltip)
   * from "ran to completion and baseline won" (show baseline).
   */
  status?: string | null
}

export function ScoreSummary({
  baselineScore,
  optimizedScore,
  bestIteration = null,
  status = null,
}: ScoreSummaryProps) {
  const baseline = presentBaselineScore(baselineScore)
  const optimized = presentOptimizedScore({
    baselineScore,
    optimizedScore,
    bestIteration,
    status,
  })

  // Improvement is meaningful only when both numbers are renderable. While
  // we're showing "—" for optimized, we don't compute a delta.
  const delta =
    baseline.pct != null && optimized.pct != null
      ? optimized.pct - baseline.pct
      : null

  const DeltaIcon =
    delta == null ? Minus : delta > 0 ? TrendingUp : delta < 0 ? TrendingDown : Minus
  const deltaColor =
    delta == null
      ? "text-muted"
      : delta > 0
      ? "text-emerald-600 dark:text-emerald-400"
      : delta < 0
      ? "text-red-500"
      : "text-muted"
  const deltaSign = delta != null && delta > 0 ? "+" : ""

  const optimizedNumber = (
    <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
      {optimized.text}
    </p>
  )

  return (
    <div className="grid grid-cols-3 gap-3 w-full">
      <div className="rounded-xl border border-default bg-surface px-4 py-3">
        <div className="flex items-center gap-1.5 mb-1.5">
          <FlaskConical className="w-3.5 h-3.5 text-muted" />
          <span className="text-xs font-semibold tracking-widest text-muted uppercase">Baseline</span>
        </div>
        <p className="text-2xl font-bold text-primary">{baseline.text}</p>
      </div>

      <div className="rounded-xl border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-950/20 px-4 py-3">
        <div className="flex items-center gap-1.5 mb-1.5">
          <Zap className="w-3.5 h-3.5 text-blue-500" />
          <span className="text-xs font-semibold tracking-widest text-blue-500 uppercase">Optimized</span>
        </div>
        {optimized.tooltip ? (
          <Tooltip content={optimized.tooltip} side="bottom">
            {optimizedNumber}
          </Tooltip>
        ) : (
          optimizedNumber
        )}
      </div>

      <div className={`rounded-xl border px-4 py-3 ${
        delta == null
          ? "border-default bg-surface"
          : delta > 0
          ? "border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/20"
          : delta < 0
          ? "border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/20"
          : "border-default bg-surface"
      }`}>
        <div className="flex items-center gap-1.5 mb-1.5">
          <DeltaIcon className={`w-3.5 h-3.5 ${deltaColor}`} />
          <span className={`text-xs font-semibold tracking-widest uppercase ${deltaColor}`}>Improvement</span>
        </div>
        <p className={`text-2xl font-bold ${deltaColor}`}>
          {delta != null ? `${deltaSign}${delta.toFixed(1)}%` : "—"}
        </p>
      </div>
    </div>
  )
}
