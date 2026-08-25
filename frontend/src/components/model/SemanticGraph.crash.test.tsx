/**
 * SemanticGraph — 12c Part 1 crash-fix regression coverage.
 *
 * The v1 bug: onPointerMove read `drag.current!.tx` with non-null assertions,
 * so a pointerup that nulled the ref mid-move crashed the tab with "Cannot read
 * properties of null (reading 'tx')" and no boundary caught it. These tests pin
 * the two halves of the fix that ARE deterministic in the repo's node-only test
 * env: the null-safe pan math, and the boundary's failure-state transition.
 * The interactive pointer-race + rendered-fallback test lands with 12c Part 2's
 * jsdom harness (the redesign), per the 12c Tests line.
 */
import { describe, expect, it } from "vitest"
import { GraphErrorBoundary, panDelta } from "./SemanticGraph"

describe("panDelta — the null-safe pan translation (crash-race fix)", () => {
  it("returns null when the drag anchor is null (the pointerup race), never throws", () => {
    expect(() => panDelta(null, 100, 200)).not.toThrow()
    expect(panDelta(null, 100, 200)).toBeNull()
  })

  it("computes the delta from anchor to current pointer", () => {
    expect(panDelta({ x: 10, y: 20 }, 35, 50)).toEqual({ dx: 25, dy: 30 })
    expect(panDelta({ x: 10, y: 20 }, 5, 12)).toEqual({ dx: -5, dy: -8 })
  })
})

describe("GraphErrorBoundary — a viz throw never takes the page down", () => {
  it("transitions to the failed state on a caught error", () => {
    expect(GraphErrorBoundary.getDerivedStateFromError()).toEqual({ failed: true })
  })
})
