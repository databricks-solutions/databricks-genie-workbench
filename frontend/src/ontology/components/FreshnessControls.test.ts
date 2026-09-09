import { describe, expect, it, vi } from "vitest"
import { pollSettleAction } from "@/ontology/refreshPolling"
import type { RefreshState } from "@/ontology/types"

// `pollSettleAction` is the exact decision the 4s poll loop uses; testing it proves the
// running/queued → terminal transition fires the parent-reload callback ONCE and never while the
// run is still in flight or on a repeat poll. (The component wiring — timer + fetch — needs a DOM
// env the node suite lacks; this covers the semantics the goal specifies.)

describe("pollSettleAction — refresh completion gating", () => {
  it("does not settle or fire while running", () => {
    expect(pollSettleAction("running", false)).toEqual({ settled: false, fireComplete: false })
  })

  it("does not settle or fire while queued", () => {
    expect(pollSettleAction("queued", false)).toEqual({ settled: false, fireComplete: false })
  })

  it("settles AND fires once on each terminal state (first time)", () => {
    const terminal: RefreshState[] = ["fresh", "stale", "failed", "skipped", "cold"]
    for (const s of terminal) {
      expect(pollSettleAction(s, false)).toEqual({ settled: true, fireComplete: true })
    }
  })

  it("still settles but does NOT re-fire once already fired (no double-fire / per-poll)", () => {
    expect(pollSettleAction("fresh", true)).toEqual({ settled: true, fireComplete: false })
  })

  it("fires exactly once across a running → running → done poll sequence", () => {
    const onComplete = vi.fn()
    let fired = false
    // Simulate the poll loop applying the same decision each tick.
    for (const state of ["running", "running", "fresh", "fresh"] as RefreshState[]) {
      const { fireComplete } = pollSettleAction(state, fired)
      if (fireComplete) {
        fired = true
        onComplete()
      }
    }
    expect(onComplete).toHaveBeenCalledTimes(1)
  })
})
