import type { GSORunSummary, GSOTerminalReason } from "@/types"

const ACTIVE_RUN_STATUSES = new Set(["IN_PROGRESS", "RUNNING", "QUEUED"])

export function hasActiveOptimizationRun(runs: GSORunSummary[]): boolean {
  return runs.some((run) => ACTIVE_RUN_STATUSES.has(run.status.toUpperCase()))
}

/**
 * Whether a run points at a scored champion that can be offered as a revert
 * target. Iteration 0 is valid here: proactive enrichment and Optimize-task
 * recovery can produce a champion at iteration 0 that is distinct from the
 * pre-run baseline snapshot. A null iteration/accuracy means no champion was
 * established yet.
 */
export function hasRevertibleChampion(run: GSORunSummary): boolean {
  const iteration = run.best_iteration
  const accuracy = run.best_accuracy
  return (
    typeof iteration === "number"
    && Number.isFinite(iteration)
    && iteration >= 0
    && typeof accuracy === "number"
    && Number.isFinite(accuracy)
  )
}

// ---------------------------------------------------------------------------
// Pure helpers for the GSO v2 optimization history table (Phase 14, item 3).
// Kept out of RunHistoryTable.tsx so the component file stays react-refresh
// clean and the terminal-reason humanizer + champion-accuracy formatter are
// unit-testable without a DOM.
// ---------------------------------------------------------------------------

// Compact, table-cell-sized labels for the typed loop terminal reason (arch
// §7.4). The full-sentence copy lives in cockpit.ts `classifyTerminal` for the
// banner; this is the terse column form.
const TERMINAL_REASON_LABELS: Record<GSOTerminalReason, string> = {
  TARGET_REACHED: "Target reached",
  MAX_ATTEMPTS: "Max attempts",
  NO_NEW_HYPOTHESIS: "No new hypothesis",
  EVAL_INVALID: "Eval invalid",
  LOOP_STATE_INVALID: "Loop state invalid",
  EVAL_BUDGET_EXHAUSTED: "Budget exhausted",
}

/**
 * Humanize the typed ``terminal_reason`` for the history "Outcome" column. When
 * the reason is one of the closed set it maps to a compact label; otherwise
 * (legacy runs / in-progress rows with no typed reason) it degrades to the
 * free-text ``convergence_reason`` (trimmed) or an em dash.
 *
 * ``BENCHMARK_UNREPAIRABLE`` is a QC-side (task 01) hard-stop, NOT a loop
 * ``GSOTerminalReason`` member (it lives on the benchmark-QC artifact), so it
 * arrives here as raw text on either field — handle it defensively rather than
 * letting the raw token leak into the column.
 */
export function humanizeTerminalReason(
  reason: GSOTerminalReason | null | undefined,
  convergenceReason?: string | null,
): string {
  if (reason && reason in TERMINAL_REASON_LABELS) {
    return TERMINAL_REASON_LABELS[reason]
  }
  const raw = String(reason ?? convergenceReason ?? "").trim()
  if (raw.toUpperCase() === "BENCHMARK_UNREPAIRABLE") return "Benchmark unrepairable"
  const fallback = (convergenceReason ?? "").trim()
  return fallback.length > 0 ? fallback : "—"
}

/**
 * Format the champion (best-so-far) accuracy for the history table. The run
 * summary's ``best_accuracy`` is the champion's full-benchmark accuracy on the
 * 0–100 scale (Phase 12 scale contract), so render it identity-on-0–100 as an
 * integer percent — NO ``≤ 1 ⇒ ×100`` heuristic, which would corrupt a true
 * sub-1% champion (0.9 → wrongly 90%). Null / non-finite renders as an em dash.
 */
export function championAccuracyText(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—"
  return `${Number(v).toFixed(0)}%`
}
