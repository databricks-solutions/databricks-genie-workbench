import { applyAutoOptimize, discardAutoOptimize, ApiError } from "@/lib/api"

// ---------------------------------------------------------------------------
// Pure async wiring for the Keep / Discard-rollback affordance (Phase 13,
// item 2). Extracted from the component so the rollback PATCH path and the
// "already resolved" (409) handling are unit-testable without a DOM.
// ---------------------------------------------------------------------------

export type ResolutionAction = "apply" | "discard"

export type ResolutionResult =
  | { kind: "resolved"; status: "APPLIED" | "DISCARDED" }
  | { kind: "error"; message: string }

/**
 * Keep (``apply``) marks the run APPLIED; Discard (``discard``) re-PATCHes the
 * original ``space_snapshot`` to roll the live Genie Space back (the backend
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
      const msg = (e.message || "").toLowerCase()
      if (msg.includes("applied")) return { kind: "resolved", status: "APPLIED" }
      if (msg.includes("discarded")) return { kind: "resolved", status: "DISCARDED" }
      return { kind: "error", message: e.message || "This run was already resolved." }
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
