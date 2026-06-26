/**
 * Bug #2 / GSO v2 Phase 6 — Shared denominator math for the Workbench frontend.
 *
 * Two regimes, decided per-iteration:
 *
 *   OFFICIAL (native EvalRunner; row carries eval_run_id/eval_run_status):
 *     accuracyPct = num_correct / num_questions * 100
 *   LEGACY (in-process, Bug #2; no eval-run metadata):
 *     accuracyPct = correct_count / evaluated_count * 100
 *     evaluated_count = total_questions - excluded_count   (backend invariant)
 *
 * The official denominator is the whole benchmark (num_questions), so a partial
 * run (num_done < num_questions) is never inflated by dividing by num_done.
 *
 * The KPI card (ScoreSummary) and the tab labels (Baseline/Final evaluation
 * in RunDetailView) must agree to the percentage point. The regression that
 * motivated this helper was RunDetailView recomputing the denominator inline
 * as `questions.filter(q => q.passed !== null)` while `run.baselineScore`
 * came from the server-derived `overall_accuracy`. When the inline filter
 * disagreed with the server's `evaluated_count` (legacy rows, the server
 * upgrading its exclusion semantics, or an arbiter override flipping a
 * `passed` bit), the two surfaces drifted.
 *
 * Everything that needs an accuracy percentage in the auto-optimize UI goes
 * through `evalCountsFromIteration`. Question-list rendering still looks at
 * `q.excluded` / `q.passed` directly, but never to compute the denominator.
 */

import type { GSOIterationResult, GSORunStatus } from "@/types"

export interface EvalCounts {
  /** Pre-exclusion question count (denominator we DO NOT use). */
  total: number
  /** Authoritative denominator for `accuracyPct`. */
  evaluated: number
  /** Passing (arbiter-adjusted) questions — the numerator. */
  correct: number
  /** Runtime exclusions (ground-truth excluded, both_empty, …). */
  excluded: number
  /** Derived accuracy on a 0–100 scale, or null if denominator is 0. */
  accuracyPct: number | null
  /** The raw `overall_accuracy` value the backend stored, on a 0–100 scale. */
  storedAccuracyPct: number | null
  /** True when derived and stored disagree by >0.5pp — useful for diagnostics. */
  hasDrift: boolean
}

const DRIFT_THRESHOLD_PCT = 0.5

function toPct(value: number | null | undefined): number | null {
  if (value == null || !Number.isFinite(value)) return null
  // Be generous about scale. Evaluation code and legacy rows have historically
  // stored overall_accuracy on different scales (fractional 0.826 vs percent
  // 82.6). Detect by magnitude: anything ≤1 is a fraction, >1 is already in
  // percent. 100 is intentionally the boundary — 1.0 == "100%" is a valid
  // fraction, so we tip that into percent by treating values > 1 as percent.
  return value > 1 ? value : value * 100
}

function safeInt(value: number | null | undefined): number {
  if (value == null || !Number.isFinite(value)) return 0
  return Math.max(0, Math.trunc(value))
}

/**
 * Compute canonical evaluation counts from an iteration row or run status
 * object. Both shapes can be passed because AutoOptimizeTab holds live
 * status (GSORunStatus) while RunDetailView holds post-run iteration rows
 * (GSOIterationResult).
 *
 * Back-compat rules (match `_resolve_eval_counts`):
 *   - evaluated_count is preferred; missing → total_questions - excluded_count,
 *     clamped ≥ 0.
 *   - excluded_count defaults to 0.
 *   - correct_count is echoed directly.
 *   - If evaluated is 0, accuracyPct is null (don't render NaN or 0%).
 */
export function evalCountsFromIteration(
  source: GSOIterationResult | GSORunStatus | null | undefined,
): EvalCounts {
  if (!source) {
    return {
      total: 0,
      evaluated: 0,
      correct: 0,
      excluded: 0,
      accuracyPct: null,
      storedAccuracyPct: null,
      hasDrift: false,
    }
  }

  const isIteration = "total_questions" in source

  if (isIteration) {
    const it = source as GSOIterationResult
    const total = safeInt(it.total_questions)
    const excluded = safeInt(it.excluded_count ?? 0)
    const storedAccuracyPct = toPct(it.overall_accuracy)

    // GSO v2 Phase 6 — OFFICIAL path. An iteration produced by the native
    // EvalRunner carries eval-run metadata; for it the accuracy denominator is
    // num_questions (the whole benchmark), NOT the legacy evaluated_count
    // (= total − runtime exclusions). They diverge on a partial run where
    // num_done < num_questions, and only num_correct / num_questions is the
    // headline metric the contract mandates.
    const isOfficial = it.eval_run_status != null || it.eval_run_id != null
    if (
      isOfficial &&
      it.num_correct != null &&
      it.num_questions != null &&
      it.num_questions > 0
    ) {
      const correct = safeInt(it.num_correct)
      const evaluated = safeInt(it.num_questions)
      const accuracyPct = (correct / evaluated) * 100
      const hasDrift =
        storedAccuracyPct != null &&
        Math.abs(accuracyPct - storedAccuracyPct) > DRIFT_THRESHOLD_PCT
      return {
        total: total || evaluated,
        evaluated,
        correct,
        excluded,
        accuracyPct,
        storedAccuracyPct,
        hasDrift,
      }
    }

    // Legacy fallback (Bug #2) — correct_count / evaluated_count, where
    // evaluated_count = total_questions − runtime exclusions.
    const correct = safeInt(it.correct_count)
    const evaluatedRaw = it.evaluated_count
    let evaluated: number
    if (evaluatedRaw == null) {
      const derived = total - excluded
      evaluated = derived >= 0 ? derived : total
    } else {
      evaluated = safeInt(evaluatedRaw)
    }

    const accuracyPct =
      evaluated > 0 ? (correct / evaluated) * 100 : null
    const hasDrift =
      accuracyPct != null &&
      storedAccuracyPct != null &&
      Math.abs(accuracyPct - storedAccuracyPct) > DRIFT_THRESHOLD_PCT

    return {
      total,
      evaluated,
      correct,
      excluded,
      accuracyPct,
      storedAccuracyPct,
      hasDrift,
    }
  }

  // GSORunStatus path — live polling. We only have the stored score; there
  // are no per-iteration counts on this shape, so derived accuracy is whatever
  // the server already decided.
  const run = source as GSORunStatus
  const stored = toPct(run.baselineScore)
  return {
    total: 0,
    evaluated: 0,
    correct: 0,
    excluded: 0,
    accuracyPct: stored,
    storedAccuracyPct: stored,
    hasDrift: false,
  }
}

/**
 * Format a derived accuracy for UI display. Uses one decimal to stay
 * consistent with ScoreSummary's `%.1f%%` rendering — single source of
 * rounding, so tab labels and score cards can never round differently.
 */
export function formatAccuracyPct(pct: number | null): string {
  if (pct == null) return "—"
  return `${pct.toFixed(1)}%`
}
