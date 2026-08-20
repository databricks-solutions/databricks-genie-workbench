import { applyAutoOptimize, discardAutoOptimize, ApiError } from "@/lib/api"

// ---------------------------------------------------------------------------
// Pure async wiring for the Keep / Discard-rollback affordance (Phase 13,
// item 2). Extracted from the component so the rollback PATCH path and the
// "already resolved" (409) handling are unit-testable without a DOM.
// ---------------------------------------------------------------------------

export type ResolutionAction = "apply" | "discard"

export type ResolvedStatus = "APPLIED" | "DISCARDED"

export type ResolutionResult =
  | { kind: "resolved"; status: ResolvedStatus }
  | { kind: "error"; message: string }

/**
 * Read an unambiguous resolved state out of a free-text field. The backend's
 * 409 messages describe the run's *current* terminal state — e.g.
 * "Run already discarded." or "Cannot apply run in status DISCARDED. Must be
 * one of: [...]" — and the terminal set they enumerate (CONVERGED /
 * MAX_ITERATIONS / STALLED) contains neither substring, so a single keyword
 * match is safe. Returns ``null`` when the text names both states or neither.
 */
function statusFromText(text: unknown): ResolvedStatus | null {
  const s = (typeof text === "string" ? text : "").toLowerCase()
  const hasApplied = s.includes("applied")
  const hasDiscarded = s.includes("discarded")
  if (hasApplied && !hasDiscarded) return "APPLIED"
  if (hasDiscarded && !hasApplied) return "DISCARDED"
  return null
}

/**
 * Map a 409 (run already resolved elsewhere) to the run's *current* resolved
 * state — never a hard error (acceptance contract: a 409 is a resolved state).
 *
 * Precedence, so the banner reflects reality rather than the attempted action:
 *   1. structured ``detail`` payload from the backend, if one is present;
 *   2. the error message text (covers the cross-action case — e.g. Keep on an
 *      already-discarded run reports DISCARDED, not the Keep banner);
 *   3. fall back to the attempted action's resolved state.
 */
function resolvedStatusFrom409(e: ApiError, action: ResolutionAction): ResolvedStatus {
  const detail = e.detail
  if (detail) {
    for (const field of ["status", "current_status", "final_status", "reason_code", "message"]) {
      const derived = statusFromText(detail[field])
      if (derived) return derived
    }
  }
  const fromMsg = statusFromText(e.message)
  if (fromMsg) return fromMsg
  return action === "apply" ? "APPLIED" : "DISCARDED"
}

/**
 * Keep (``apply``) marks the run APPLIED; Discard (``discard``) re-PATCHes the
 * original ``space_snapshot`` to roll the live Genie Agent back (the backend
 * ``/discard`` endpoint). A 409 means the run was already resolved elsewhere —
 * surface that terminal state instead of a raw error.
 */
export async function performResolution(
  action: ResolutionAction,
  runId: string,
): Promise<ResolutionResult> {
  try {
    if (action === "apply") {
      await applyAutoOptimize(runId)
      return { kind: "resolved", status: "APPLIED" }
    }
    await discardAutoOptimize(runId)
    return { kind: "resolved", status: "DISCARDED" }
  } catch (e) {
    if (e instanceof ApiError && e.status === 409) {
      return { kind: "resolved", status: resolvedStatusFrom409(e, action) }
    }
    return {
      kind: "error",
      message:
        e instanceof Error
          ? e.message
          : action === "apply"
            ? "Failed to keep the optimization."
            : "Failed to roll back the optimization.",
    }
  }
}
