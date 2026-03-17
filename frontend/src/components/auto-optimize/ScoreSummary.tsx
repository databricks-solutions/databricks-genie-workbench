import { ArrowRight, TrendingUp, TrendingDown, Minus } from "lucide-react"

interface ScoreSummaryProps {
  baselineScore: number | null
  optimizedScore: number | null
}

export function ScoreSummary({ baselineScore, optimizedScore }: ScoreSummaryProps) {
  const delta =
    baselineScore != null && optimizedScore != null
      ? optimizedScore - baselineScore
      : null

  const deltaColor =
    delta == null ? "text-muted" : delta > 0 ? "text-emerald-400" : delta < 0 ? "text-red-400" : "text-muted"

  const DeltaIcon = delta == null ? Minus : delta > 0 ? TrendingUp : delta < 0 ? TrendingDown : Minus

  function fmt(v: number | null) {
    return v != null ? `${(v * 100).toFixed(0)}%` : "—"
  }

  return (
    <div className="flex items-center gap-3 text-sm">
      <span className="text-muted">Baseline</span>
      <span className="font-semibold text-primary">{fmt(baselineScore)}</span>
      <ArrowRight className="w-4 h-4 text-muted" />
      <span className="font-semibold text-primary">{fmt(optimizedScore)}</span>
      {delta != null && (
        <span className={`flex items-center gap-1 font-medium ${deltaColor}`}>
          <DeltaIcon className="w-3.5 h-3.5" />
          {delta > 0 ? "+" : ""}
          {(delta * 100).toFixed(0)}%
        </span>
      )}
    </div>
  )
}
