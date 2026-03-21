import { FlaskConical, Zap, TrendingUp, TrendingDown, Minus } from "lucide-react"

interface ScoreSummaryProps {
  baselineScore: number | null
  optimizedScore: number | null
}

/** Normalise a score to 0-100 percentage scale. */
function toPct(v: number | string): number {
  const n = typeof v === "string" ? parseFloat(v) : v
  if (isNaN(n)) return 0
  return n > 1 ? n : n * 100
}

export function ScoreSummary({ baselineScore, optimizedScore }: ScoreSummaryProps) {
  const basePct = baselineScore != null ? toPct(baselineScore) : null
  const optPct = optimizedScore != null ? toPct(optimizedScore) : null
  const delta = basePct != null && optPct != null ? optPct - basePct : null

  function fmt(v: number | null) {
    return v != null ? `${v.toFixed(1)}%` : "—"
  }

  const DeltaIcon = delta == null ? Minus : delta > 0 ? TrendingUp : delta < 0 ? TrendingDown : Minus
  const deltaColor = delta == null ? "text-muted" : delta > 0 ? "text-emerald-600 dark:text-emerald-400" : delta < 0 ? "text-red-500" : "text-muted"
  const deltaSign = delta != null && delta > 0 ? "+" : ""

  return (
    <div className="grid grid-cols-3 gap-3 w-full">
      {/* Baseline */}
      <div className="rounded-xl border border-default bg-surface px-4 py-3">
        <div className="flex items-center gap-1.5 mb-1.5">
          <FlaskConical className="w-3.5 h-3.5 text-muted" />
          <span className="text-xs font-semibold tracking-widest text-muted uppercase">Baseline</span>
        </div>
        <p className="text-2xl font-bold text-primary">{fmt(basePct)}</p>
      </div>

      {/* Optimized */}
      <div className="rounded-xl border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-950/20 px-4 py-3">
        <div className="flex items-center gap-1.5 mb-1.5">
          <Zap className="w-3.5 h-3.5 text-blue-500" />
          <span className="text-xs font-semibold tracking-widest text-blue-500 uppercase">Optimized</span>
        </div>
        <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">{fmt(optPct)}</p>
      </div>

      {/* Improvement */}
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
