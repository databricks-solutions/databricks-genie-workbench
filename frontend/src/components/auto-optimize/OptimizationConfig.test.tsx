import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { OptimizationConfig } from "./OptimizationConfig"
import {
  buildOptimizationTriggerRequest,
  parseMaxAttempts,
  parseTargetAccuracy,
} from "./optimizationRequest"

describe("OptimizationConfig coverage row", () => {
  // (a) Coverage (attempt 1) is automatic — it must render as an always-on
  // row, never a toggle. We assert the row text is present AND that the only
  // checkboxes on the surface are the six surgical levers (1..6), proving the
  // coverage row added no 7th checkbox.
  it("renders coverage as an always-on row, not a checkbox", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={null}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Coverage pass")
    expect(markup).toContain("Automatic")
    expect(markup).toContain("Surgical optimization scope (attempts 2+)")

    const checkboxCount = (markup.match(/type="checkbox"/g) ?? []).length
    expect(checkboxCount).toBe(6) // exactly the six surgical levers, none for coverage
  })

  it("surfaces the two stopping-criteria knobs with their default values", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={null}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Target accuracy")
    expect(markup).toContain("Max surgical attempts")
    expect(markup).toContain("whichever comes first")
    // Defaults reflect the job defaults (0.90 -> 90%, 3 attempts).
    expect(markup).toContain('value="90"')
    expect(markup).toContain('value="3"')
  })
})

describe("stopping-criteria parsing", () => {
  it("converts the target-accuracy percentage to the 0–1 scale", () => {
    expect(parseTargetAccuracy("90")).toBeCloseTo(0.9)
    expect(parseTargetAccuracy("100")).toBe(1)
    expect(parseTargetAccuracy("0.5")).toBeCloseTo(0.005)
  })

  it("rejects out-of-range or non-numeric target accuracy", () => {
    expect(parseTargetAccuracy("0")).toBeNull()
    expect(parseTargetAccuracy("150")).toBeNull()
    expect(parseTargetAccuracy("")).toBeNull()
    expect(parseTargetAccuracy("abc")).toBeNull()
  })

  it("accepts only positive integers for max attempts", () => {
    expect(parseMaxAttempts("3")).toBe(3)
    expect(parseMaxAttempts("1")).toBe(1)
    expect(parseMaxAttempts("0")).toBeNull()
    expect(parseMaxAttempts("-1")).toBeNull()
    expect(parseMaxAttempts("2.5")).toBeNull()
    expect(parseMaxAttempts("")).toBeNull()
  })
})

describe("buildOptimizationTriggerRequest (trigger payload)", () => {
  // (b) target_accuracy (0–1) and max_attempts must be passed through. The
  // component sends exactly this object to triggerAutoOptimize in handleStart;
  // api.test.ts independently verifies triggerAutoOptimize forwards both fields.
  it("forwards target_accuracy on the 0–1 scale and max_attempts as an integer", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([1, 2, 3, 4, 5, 6]),
      selectedModel: "claude-opus",
      targetAccuracy: parseTargetAccuracy("90")!,
      maxAttempts: parseMaxAttempts("3")!,
    })

    expect(req).toEqual({
      space_id: "space-1",
      apply_mode: "genie_config",
      levers: [1, 2, 3, 4, 5, 6],
      llm_model: "claude-opus",
      target_accuracy: 0.9,
      max_attempts: 3,
    })
    expect(req.target_accuracy).toBeLessThanOrEqual(1)
  })

  it("never includes the coverage pass (lever 0) in the levers payload", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([3, 1]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
    })

    expect(req.levers).toEqual([1, 3]) // sorted subset of {1..6}
    expect(req.levers).not.toContain(0)
  })
})
