import type { GSOIterationResult, GSOQuestionDetail } from "@/types"
import { evalCountsFromIteration } from "@/lib/eval-counts"

// ---------------------------------------------------------------------------
// Pure helpers for the GSO v2 post-run resolution surfaces (Phase 13). Kept out
// of the component files (react-refresh/only-export-components) and unit-tested
// without a DOM: the per-question Attempt Selector model + the QuestionJourney
// column relabel (Iter → attempts).
// ---------------------------------------------------------------------------

export type AttemptOptionMode = "baseline" | "legacy" | "patch"

// One selectable attempt in the per-question Attempt Selector. ``iteration`` is
// the genie_opt_iterations row id used to fetch that attempt's question
// results. ``starred`` marks the champion (explicit is_champion flag) — or, on
// legacy runs with no flag, the best iteration ("best★").
export interface AttemptOption {
  key: string
  label: string
  iteration: number
  mode: AttemptOptionMode
  accuracyPct: number | null
  isChampion: boolean
  starred: boolean
}

export interface AttemptSelectorModel {
  options: AttemptOption[]
  defaultKey: string
}

function isFullScope(it: GSOIterationResult): boolean {
  return String(it.eval_scope ?? "full").toLowerCase() === "full" || it.iteration === 0
}

function modeOf(it: GSOIterationResult): AttemptOptionMode {
  const m = (it.attempt_mode ?? "").toString().toLowerCase()
  if (m === "coverage" || m === "enrichment") return "legacy"
  return "patch"
}

function labelFor(mode: AttemptOptionMode, it: GSOIterationResult): string {
  if (mode === "baseline") return "Baseline"
  if (mode === "legacy") return "Pre-loop enrichment"
  const n = it.attempt_no ?? it.iteration
  return `Patch ${n}`
}

/**
 * Build the per-question Attempt Selector model (Phase 13, item 4) from the
 * iteration rows.
 *
 * - v2 runs (iterations carry ``attempt_mode``/``attempt_no``): one option per
 *   full-scope iteration — Baseline · Patch N — with the champion
 *   read from the explicit ``is_champion`` flag (never idxmax), defaulting the
 *   selection to that champion.
 * - Older runs (no attempt metadata): degrades to the classic
 *   Baseline · Final pair, defaulting to Final (the best iteration).
 *
 * Champion fallback: when no row carries ``is_champion`` we star ``bestIteration``
 * so the selector still has a meaningful "best★" default.
 */
export function buildAttemptOptions(args: {
  iterations: GSOIterationResult[]
  baselineIteration: number | null
  bestIteration: number | null
}): AttemptSelectorModel {
  const { baselineIteration, bestIteration } = args
  const fullIters = args.iterations
    .filter(isFullScope)
    .slice()
    .sort((a, b) => a.iteration - b.iteration)

  if (fullIters.length === 0) {
    return { options: [], defaultKey: "" }
  }

  // Resolve the baseline row: prefer the explicit baselineIteration, else
  // iteration 0, else the lowest full iteration.
  const baselineRow =
    (baselineIteration != null
      ? fullIters.find((it) => it.iteration === baselineIteration)
      : undefined) ??
    fullIters.find((it) => it.iteration === 0) ??
    fullIters[0]

  const hasAttemptInfo = fullIters.some(
    (it) => it.iteration > 0 && (it.attempt_mode != null || it.attempt_no != null),
  )

  // Champion iteration: explicit is_champion flag wins; otherwise fall back to
  // bestIteration so the default selection ("best★") is still meaningful.
  const flagged = fullIters.find((it) => it.is_champion === true)
  const championIter =
    flagged?.iteration ??
    (bestIteration != null && fullIters.some((it) => it.iteration === bestIteration)
      ? bestIteration
      : null)

  const options: AttemptOption[] = []
  const pushOption = (it: GSOIterationResult, mode: AttemptOptionMode) => {
    const accuracyPct = evalCountsFromIteration(it).accuracyPct
    options.push({
      key: `iter-${it.iteration}`,
      label: labelFor(mode, it),
      iteration: it.iteration,
      mode,
      accuracyPct,
      isChampion: it.is_champion === true,
      starred: championIter != null && it.iteration === championIter,
    })
  }

  if (!hasAttemptInfo) {
    // Legacy: Baseline · Final.
    pushOption(baselineRow, "baseline")
    const finalRow =
      (bestIteration != null
        ? fullIters.find((it) => it.iteration === bestIteration)
        : undefined) ?? fullIters[fullIters.length - 1]
    if (finalRow && finalRow.iteration !== baselineRow.iteration) {
      const accuracyPct = evalCountsFromIteration(finalRow).accuracyPct
      options.push({
        key: `iter-${finalRow.iteration}`,
        label: "Final",
        iteration: finalRow.iteration,
        mode: "patch",
        accuracyPct,
        isChampion: finalRow.is_champion === true,
        starred: championIter != null && finalRow.iteration === championIter
          ? true
          : bestIteration != null && finalRow.iteration === bestIteration,
      })
    }
  } else {
    for (const it of fullIters) {
      const mode = it.iteration === baselineRow.iteration ? "baseline" : modeOf(it)
      pushOption(it, mode)
    }
  }

  // Default selection = the champion / best option, else the last option.
  const championKey = options.find((o) => o.starred)?.key
  const defaultKey = championKey ?? options[options.length - 1]?.key ?? options[0]?.key ?? ""

  return { options, defaultKey }
}

// ---------------------------------------------------------------------------
// Per-attempt question-result cache keying (Phase 13). The RunDetailView lazily
// fetches question results per attempt and caches them; keying by iteration
// number ALONE leaks across runs when the view is reused for a different runId
// and both runs share an iteration number (e.g. both have iter-2). Compose the
// runId into the key so entries from run A can never satisfy a lookup for run B.
// ---------------------------------------------------------------------------

/** Composite cache key: an iteration number is only meaningful within a run. */
export function questionCacheKey(runId: string, iteration: number): string {
  return `${runId}:${iteration}`
}

/**
 * Read the cached question results for a run+iteration. Returns ``[]`` when the
 * attempt hasn't been fetched — and, critically, when a stale cache from a
 * *different* run holds the same iteration number, so no cross-run leakage.
 */
export function selectCachedQuestions(
  cache: Map<string, GSOQuestionDetail[]>,
  runId: string,
  iteration: number | null,
): GSOQuestionDetail[] {
  if (iteration == null) return []
  return cache.get(questionCacheKey(runId, iteration)) ?? []
}

/**
 * Column header label for the QuestionJourney attempt grid (Phase 13, item 4):
 * relabels the old "Iter N" to attempt-centric copy. Baseline · Patch N when
 * attempt metadata is present, else "Attempt N".
 */
export function attemptColumnLabel(it: GSOIterationResult): string {
  if (it.iteration === 0) return "Baseline"
  const m = (it.attempt_mode ?? "").toString().toLowerCase()
  if (m === "coverage" || m === "enrichment") return "Pre-loop enrichment"
  if (m === "surgical" || m === "llm_patch" || m === "patch") return `Patch ${it.attempt_no ?? it.iteration}`
  return `Attempt ${it.attempt_no ?? it.iteration}`
}
