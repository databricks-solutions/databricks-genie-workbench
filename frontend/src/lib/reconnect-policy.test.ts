import { describe, it, expect } from "vitest"
import {
  MAX_AUTO_RECONNECTS,
  shouldResetReconnectBudget,
  decideReconnect,
} from "./reconnect-policy"

describe("shouldResetReconnectBudget", () => {
  it("resets on a genuine new user turn", () => {
    expect(shouldResetReconnectBudget(false)).toBe(true)
  })
  it("does NOT reset on a continuation (the RCA bug)", () => {
    // The backend re-emits `session` on every reconnect continuation; resetting
    // here is what made the cap unreachable and caused the infinite loop.
    expect(shouldResetReconnectBudget(true)).toBe(false)
  })
})

describe("decideReconnect", () => {
  it("reconnects while under the cap", () => {
    expect(decideReconnect({ connectionLost: true, hasSession: true, spaceAlreadyCreated: false, currentCount: 0 }))
      .toEqual({ action: "reconnect", attempt: 1 })
    expect(decideReconnect({ connectionLost: true, hasSession: true, spaceAlreadyCreated: false, currentCount: 2 }))
      .toEqual({ action: "reconnect", attempt: 3 })
  })

  it("stops once the cap is exceeded", () => {
    expect(decideReconnect({ connectionLost: true, hasSession: true, spaceAlreadyCreated: false, currentCount: 3 }))
      .toEqual({ action: "stop", reason: "cap-reached" })
  })

  it("stops immediately if a space was already created (no duplicate creates)", () => {
    expect(decideReconnect({ connectionLost: true, hasSession: true, spaceAlreadyCreated: true, currentCount: 0 }))
      .toEqual({ action: "stop", reason: "space-already-created" })
  })

  it("does not reconnect on a clean (non-drop) stream end", () => {
    expect(decideReconnect({ connectionLost: false, hasSession: true, spaceAlreadyCreated: false, currentCount: 0 }))
      .toEqual({ action: "stop", reason: "not-a-drop" })
  })

  it("does not reconnect without a session to resume", () => {
    expect(decideReconnect({ connectionLost: true, hasSession: false, spaceAlreadyCreated: false, currentCount: 0 }))
      .toEqual({ action: "stop", reason: "not-a-drop" })
  })
})

describe("adversarial: the reconnect loop must terminate", () => {
  // Regression test for the field bug. Drives the full loop the way the
  // component does — including the backend re-emitting `session` on every
  // reconnect — and asserts it cannot run forever.
  function runLoop(opts: { createSucceedsServerSide: boolean; buggyReset: boolean }) {
    let count = 0
    let spacesCreated = 0
    let spaceAlreadyCreated = false
    let iterations = 0
    let isContinuation = false

    while (true) {
      iterations++
      if (iterations > 1000) return { spacesCreated, iterations, terminated: false }

      if (opts.buggyReset) count = 0
      else if (shouldResetReconnectBudget(isContinuation)) count = 0

      spacesCreated++
      if (opts.createSucceedsServerSide) spaceAlreadyCreated = true

      const decision = decideReconnect({
        connectionLost: true, hasSession: true, spaceAlreadyCreated, currentCount: count,
      })
      if (decision.action === "stop") {
        return { spacesCreated, iterations, terminated: true, reason: decision.reason }
      }
      count = decision.attempt
      isContinuation = true
    }
  }

  it("BUG repro: unconditional reset loops forever (>1000)", () => {
    expect(runLoop({ createSucceedsServerSide: false, buggyReset: true }).terminated).toBe(false)
  })

  it("FIX: bounded to MAX_AUTO_RECONNECTS when create keeps failing", () => {
    const r = runLoop({ createSucceedsServerSide: false, buggyReset: false })
    expect(r.terminated).toBe(true)
    expect(r.reason).toBe("cap-reached")
    expect(r.spacesCreated).toBe(MAX_AUTO_RECONNECTS + 1)
  })

  it("FIX: exactly ONE space when create succeeded server-side", () => {
    const r = runLoop({ createSucceedsServerSide: true, buggyReset: false })
    expect(r.terminated).toBe(true)
    expect(r.reason).toBe("space-already-created")
    expect(r.spacesCreated).toBe(1)
  })
})
