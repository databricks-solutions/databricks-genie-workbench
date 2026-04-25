/**
 * Canonical "Optimized score" display rules.
 *
 * Single source of truth for HOW the {baseline, optimized, bestIteration,
 * status} tuple maps to the UI strings the user sees:
 *
 *   - The headline number ("82.5%" vs "—")
 *   - The headline tooltip ("Optimization in progress" vs the convergence
 *     reason vs nothing)
 *   - The convergence-reason copy under the progress bar
 *
 * Locks down the customer-visible contract from PR description:
 *
 *   "The optimized score is the arbiter adjusted accuracy. And we shouldn't
 *    show regression here — cos regressions don't get posted. So they should
 *    either stay as baseline or an improvement."
 *
 * The backend canonical helper
 * (``genie_space_optimizer.common.accuracy.compute_run_scores``) already
 * guarantees ``optimized >= baseline`` on the wire, plus a
 * ``best_iteration == 0`` signal that disambiguates "in progress" from
 * "ran to completion and baseline won". This module turns those signals
 * into the strings.
 *
 * Keep this dumb and pure — easy to unit test, easy to swap the ScoreCard
 * primitive without re-deriving the rules in five places.
 */

const TERMINAL_STATUSES: ReadonlySet<string> = new Set([
  "CONVERGED",
  "STALLED",
  "MAX_ITERATIONS",
  "FAILED",
  "CANCELLED",
  "APPLIED",
  "DISCARDED",
])

export function isTerminalStatus(status: string | null | undefined): boolean {
  if (!status) return false
  return TERMINAL_STATUSES.has(status)
}

/** Tooltip text for the "—" headline when optimization is still running. */
export const OPTIMIZATION_IN_PROGRESS_TOOLTIP = "Optimization in progress"

/**
 * Customer-visible label that replaces the convergence reason when the
 * baseline ended up being the best plan (no iter > 0 strictly improved).
 *
 * The user asked us to "wire that label into the existing convergence-reason
 * copy" so it shows in the same slot as "Reason: ...".
 */
export const BASELINE_RETAINED_LABEL = "Baseline retained"

/** Normalise a wire score to a 0-100 percentage. */
function toPct(value: number | null | undefined): number | null {
  if (value == null || !Number.isFinite(value)) return null
  // Defensive against legacy callers / fixtures that still send 0-1
  // floats. The Pydantic ``ScorePct`` validator rejects 0-1 values now,
  // but keeping the rescaling here stops a single rogue fixture from
  // making the page render "0.8%" as the headline.
  return value > 1 ? value : value * 100
}

/**
 * Format any wire-format score value as a "82.5%" string.
 *
 * Use this for raw per-iteration accuracy cells, leaderboard rows, and
 * other places where the strict canonical baseline/optimized rules don't
 * apply but you still want consistent rendering and "—" for null/NaN.
 */
export function formatScorePct(
  value: number | null | undefined,
  options: { fractionDigits?: number } = {},
): string {
  const pct = toPct(value)
  if (pct == null) return "—"
  return `${pct.toFixed(options.fractionDigits ?? 1)}%`
}

export interface ScorePresentation {
  /** Pre-formatted headline string ("82.5%" or "—"). */
  text: string
  /** Tooltip for the headline. ``null`` when no tooltip is needed. */
  tooltip: string | null
  /** Numeric percent, ``null`` when we render "—". */
  pct: number | null
}

export interface OptimizedScoreInputs {
  baselineScore: number | null
  optimizedScore: number | null
  bestIteration: number | null
  status: string | null | undefined
}

/**
 * Decide what the "Optimized" headline cell should render.
 *
 * Branches:
 *
 *   1. baseline is null → "—" (nothing yet, no tooltip — there's literally
 *      no score to show).
 *   2. ``bestIteration == 0`` AND run is NOT terminal → "—" with the
 *      "Optimization in progress" tooltip. The user explicitly asked for
 *      this rendering rather than echoing the baseline, because the latter
 *      makes mid-run UI look like it converged at baseline.
 *   3. ``bestIteration == 0`` AND run IS terminal → render the baseline
 *      number. The "Baseline retained" copy is wired into the
 *      convergence-reason slot via :func:`convergenceReasonText` so the
 *      headline cell stays a clean number.
 *   4. ``bestIteration > 0`` → render the optimized number. Floor-at-baseline
 *      is enforced server-side, so we just trust the value.
 */
export function presentOptimizedScore(
  inputs: OptimizedScoreInputs,
): ScorePresentation {
  const basePct = toPct(inputs.baselineScore)
  const optPct = toPct(inputs.optimizedScore)
  const isTerminal = isTerminalStatus(inputs.status)
  const bestIter = inputs.bestIteration

  if (basePct == null) {
    return { text: "—", tooltip: null, pct: null }
  }

  // Mid-run with no accepted iter > 0 yet. Show "—" instead of echoing
  // the baseline number, so the card doesn't look "done".
  if (bestIter === 0 && !isTerminal) {
    return {
      text: "—",
      tooltip: OPTIMIZATION_IN_PROGRESS_TOOLTIP,
      pct: null,
    }
  }

  // Terminal with bestIter == 0, OR bestIter > 0. Either way the number
  // is meaningful — terminal-baseline-retained shows the baseline, real
  // improvement shows the optimized. ``optimizedScore`` is server-clamped
  // to ``>= baseline`` so we just render it.
  const numeric = optPct ?? basePct
  return {
    text: `${numeric.toFixed(1)}%`,
    tooltip: null,
    pct: numeric,
  }
}

export function presentBaselineScore(
  baselineScore: number | null,
): ScorePresentation {
  const basePct = toPct(baselineScore)
  if (basePct == null) {
    return { text: "—", tooltip: null, pct: null }
  }
  return {
    text: `${basePct.toFixed(1)}%`,
    tooltip: null,
    pct: basePct,
  }
}

/**
 * Choose what to render in the "Reason:" slot under the progress bar.
 *
 * Rules:
 *   - ``bestIteration == 0`` AND terminal → "Baseline retained" wins. This
 *     is the user's request: surface the "we kept baseline" outcome in the
 *     same copy slot that today shows convergence reason. If the backend
 *     ALSO supplied a convergence_reason (e.g. STALLED → "no improvement
 *     after 3 attempts"), we suffix it for the operator: "Baseline
 *     retained — no improvement after 3 attempts".
 *   - ``bestIteration == 0`` AND not terminal → null (the headline is "—"
 *     with its own tooltip; we don't need a second copy).
 *   - Otherwise → the convergence reason as-is, or null when absent.
 */
export function convergenceReasonText(
  inputs: OptimizedScoreInputs & { convergenceReason: string | null },
): string | null {
  const { convergenceReason, bestIteration, status } = inputs
  const terminal = isTerminalStatus(status)

  if (bestIteration === 0) {
    if (!terminal) return null
    if (convergenceReason && convergenceReason.trim().length > 0) {
      return `${BASELINE_RETAINED_LABEL} — ${convergenceReason.trim()}`
    }
    return BASELINE_RETAINED_LABEL
  }

  return convergenceReason && convergenceReason.trim().length > 0
    ? convergenceReason
    : null
}
