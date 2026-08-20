import { Trophy, TrendingUp, TrendingDown, Minus, Target } from "lucide-react"
import { toAccuracyPct, targetToPct, progressToTarget } from "@/components/auto-optimize/cockpit"

interface ChampionHeroProps {
  baselineAccuracy: number | null | undefined
  /** Best-so-far champion accuracy (0–100), from loop-state / publish record. */
  bestAccuracy: number | null | undefined
  /** target_accuracy on the 0–1 request scale. */
  targetUnit: number | null | undefined
}

// Champion hero (Phase 12) — the large best_accuracy headline + Δ vs baseline +
// progress-to-target. Supersedes ScoreSummary's "Optimized" / derived-best
// framing for the live cockpit (best comes from the explicit loop-state best,
// never an idxmax of the iterations).
export function ChampionHero({ baselineAccuracy, bestAccuracy, targetUnit }: ChampionHeroProps) {
  const base = toAccuracyPct(baselineAccuracy)
  const best = toAccuracyPct(bestAccuracy)
  const target = targetToPct(targetUnit)
  const delta = base != null && best != null ? best - base : null
  const progress = progressToTarget({ baselineAccuracy: base, bestAccuracy: best, targetPct: target })

  const DeltaIcon = delta == null ? Minus : delta > 0 ? TrendingUp : delta < 0 ? TrendingDown : Minus
  const deltaColor =
    delta == null
      ? "text-muted"
      : delta > 0
        ? "text-emerald-600 dark:text-emerald-400"
        : delta < 0
          ? "text-red-500"
          : "text-muted"
  const deltaSign = delta != null && delta > 0 ? "+" : ""
  const reachedTarget = best != null && target != null && best >= target

  return (
    <div className="rounded-xl border border-indigo-200 bg-gradient-to-br from-indigo-50 to-surface px-5 py-4 dark:border-indigo-900/40 dark:from-indigo-950/20 dark:to-surface">
      <div className="flex flex-wrap items-center justify-between gap-4">
        {/* Best accuracy headline */}
        <div className="flex items-center gap-3">
          <span className="flex h-11 w-11 items-center justify-center rounded-full bg-indigo-500/10">
            <Trophy className="h-5 w-5 text-indigo-500" />
          </span>
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-muted">Champion accuracy</p>
            <p className="text-3xl font-bold text-indigo-600 dark:text-indigo-400">
              {best == null ? "—" : `${best.toFixed(1)}%`}
            </p>
          </div>
        </div>

        {/* Δ vs baseline */}
        <div className="text-right">
          <p className="text-xs font-semibold uppercase tracking-widest text-muted">Δ vs baseline</p>
          <p className={`flex items-center justify-end gap-1 text-2xl font-bold ${deltaColor}`}>
            <DeltaIcon className="h-5 w-5" />
            {delta == null ? "—" : `${deltaSign}${delta.toFixed(1)}%`}
          </p>
          {base != null && (
            <p className="text-xs text-muted">from {base.toFixed(1)}% baseline</p>
          )}
        </div>
      </div>

      {/* Progress to target */}
      {target != null && (
        <div className="mt-4 space-y-1.5">
          <div className="flex items-center justify-between text-xs">
            <span className="flex items-center gap-1 text-muted">
              <Target className="h-3.5 w-3.5" />
              Progress to target ({target.toFixed(0)}%)
            </span>
            <span className={reachedTarget ? "font-semibold text-emerald-600 dark:text-emerald-400" : "text-muted"}>
              {progress == null ? "—" : `${Math.round(progress * 100)}%`}
              {reachedTarget && " · reached"}
            </span>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-elevated">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                reachedTarget ? "bg-emerald-500" : "bg-indigo-500"
              }`}
              style={{ width: `${Math.round((progress ?? 0) * 100)}%` }}
            />
          </div>
        </div>
      )}
    </div>
  )
}
