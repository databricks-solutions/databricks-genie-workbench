/**
 * Ontology "Refresh" poll semantics (pure). Kept out of the component file so it's unit-testable
 * without a DOM/timer harness and so the component module exports only its component (Fast Refresh).
 */
import type { RefreshState } from "@/ontology/types"

/**
 * Decide what a poll tick does given the latest refresh `state` and whether the completion
 * callback has already fired for this run. `settled` = the run left running/queued (any terminal
 * state: cold/fresh/stale/failed/skipped) so polling can stop; `fireComplete` gates the
 * parent-reload callback to EXACTLY ONCE per run — never while still running/queued, never on a
 * repeat poll.
 */
export function pollSettleAction(
  state: RefreshState,
  alreadyFired: boolean,
): { settled: boolean; fireComplete: boolean } {
  const settled = state !== "running" && state !== "queued"
  return { settled, fireComplete: settled && !alreadyFired }
}
