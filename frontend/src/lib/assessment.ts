/**
 * GSO v2 Phase 6 — canonical per-question display state.
 *
 * The native Benchmark API verdict is three-valued (GOOD / BAD /
 * NEEDS_REVIEW), so the UI must NOT collapse a question to a plain pass/fail
 * boolean — that mislabels review-pending and bad rows as the same "fail".
 * Every per-question surface (QuestionList / QuestionDetail / QuestionJourney)
 * resolves state through these helpers.
 */

export type QuestionState = "passing" | "failing" | "needs_review" | "excluded"

/** Minimal shape needed to resolve state — satisfied by GSOQuestionDetail. */
interface AssessableQuestion {
  assessment?: string | null
  passed?: boolean | null
  excluded?: boolean
}

/**
 * Resolve the canonical display state. Prefers the official `assessment`;
 * falls back to the derived `passed` boolean for legacy/pre-Phase-6 rows
 * (`passed === null` with no exclusion ⇒ review-pending).
 */
export function questionState(q: AssessableQuestion): QuestionState {
  if (q.excluded) return "excluded"
  const a = (q.assessment ?? "").toUpperCase()
  if (a === "GOOD") return "passing"
  if (a === "BAD") return "failing"
  if (a === "NEEDS_REVIEW") return "needs_review"
  // Legacy fallback (no assessment present).
  if (q.passed === true) return "passing"
  if (q.passed === false) return "failing"
  return "needs_review"
}

const STATE_LABELS: Record<QuestionState, string> = {
  passing: "Pass",
  failing: "Fail",
  needs_review: "Needs Review",
  excluded: "Excluded",
}

export function questionStateLabel(state: QuestionState): string {
  return STATE_LABELS[state]
}

/** Human-readable label for a raw assessment_reason enum (best-effort). */
export function formatAssessmentReason(reason: string): string {
  return reason
    .replace(/^LLM_JUDGE_/, "")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase())
}
