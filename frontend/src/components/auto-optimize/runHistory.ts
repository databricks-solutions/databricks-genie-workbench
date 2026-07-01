import type { GSOTerminalReason } from "@/types"

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
 */
export function humanizeTerminalReason(
  reason: GSOTerminalReason | null | undefined,
  convergenceReason?: string | null,
): string {
  if (reason && reason in TERMINAL_REASON_LABELS) {
    return TERMINAL_REASON_LABELS[reason]
  }
  const fallback = (convergenceReason ?? "").trim()
  return fallback.length > 0 ? fallback : "—"
}

/**
 * Format the champion (best-so-far) accuracy for the history table. The run
 * summary's ``best_accuracy`` is the champion's full-benchmark accuracy; it may
 * arrive on either the 0–1 or 0–100 scale depending on the writer, so normalize
 * defensively (values ≤ 1 are treated as fractions) and render as an integer
 * percent. Null (no champion / no baseline yet) renders as an em dash.
 */
export function championAccuracyText(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—"
  const n = Number(v)
  return `${(n > 1 ? n : n * 100).toFixed(0)}%`
}
