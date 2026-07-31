import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { OptimizationConfig } from "./OptimizationConfig"
import {
  buildOptimizationTriggerRequest,
  parseMaxAttempts,
  parseTargetAccuracy,
} from "./optimizationRequest"

describe("OptimizationConfig lever scope", () => {
  // The current 4-task flow exposes only the six patch levers (1..6). Retired
  // lever 0 is a legacy data concern, not a selectable control.
  it("renders only the six patch levers", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={null}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Optimization Scope")
    expect(markup).toContain("Select which changes the optimizer may make")
    expect(markup).toContain("Optimization Config")
    expect(markup).toContain("Model selection")
    expect(markup).toContain("lg:grid-cols-2")
    expect(markup).not.toContain("lg:grid-cols-3")
    expect(markup).not.toContain("Coverage pass")
    expect(markup).not.toContain("Surgical optimization scope")

    const checkboxCount = (markup.match(/type="checkbox"/g) ?? []).length
    expect(checkboxCount).toBe(7)
    expect(markup).toContain("Allow GSO to repair and add benchmarks")
    expect(markup).toContain("Off by default")
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
    expect(markup).toContain("Max patch attempts")
    expect(markup).toContain("whichever comes first")
    // Defaults reflect the job defaults (0.90 -> 90%, 3 attempts).
    expect(markup).toContain('value="90"')
    expect(markup).toContain('value="3"')
  })

  it("explains the non-blocking fallback when query usage is unavailable", () => {
    const markup = renderToStaticMarkup(
      <OptimizationConfig
        spaceId="space-1"
        onStarted={() => {}}
        hasActiveRun={false}
        permissions={{
          sp_display_name: "gso-service-principal",
          sp_application_id: "application-id",
          sp_has_manage: true,
          schemas: [],
          can_start: true,
          errors: [],
          query_usage_signal: {
            status: "unavailable",
            system_table_available: false,
            warehouse_api_available: false,
            warehouses: [],
            inaccessible_warehouses: [],
            system_grant_sql: null,
          },
        }}
        permsLoading={false}
      />,
    )

    expect(markup).toContain("Query usage signal")
    expect(markup).toContain("Optimization will still run")
    expect(markup).toContain("local profiling evidence")
  })
})

describe("stopping-criteria parsing", () => {
  it("converts the target-accuracy percentage to the 0–1 scale", () => {
    expect(parseTargetAccuracy("90")).toBeCloseTo(0.9)
    expect(parseTargetAccuracy("100")).toBe(1)
    expect(parseTargetAccuracy("80")).toBeCloseTo(0.8) // lower boundary (80% floor)
  })

  it("rejects out-of-range or non-numeric target accuracy", () => {
    // The 80% floor: a lower optimization target isn't useful, so <80 is invalid.
    expect(parseTargetAccuracy("79")).toBeNull() // just below the 80% floor
    expect(parseTargetAccuracy("1")).toBeNull()
    expect(parseTargetAccuracy("0")).toBeNull()
    expect(parseTargetAccuracy("0.5")).toBeNull()
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
      benchmarkPolicy: "review_only",
    })

    expect(req).toEqual({
      space_id: "space-1",
      apply_mode: "genie_config",
      levers: [1, 2, 3, 4, 5, 6],
      llm_model: "claude-opus",
      target_accuracy: 0.9,
      max_attempts: 3,
      workload_warehouse_ids: [],
      benchmark_policy: "review_only",
    })
    expect(req.target_accuracy).toBeLessThanOrEqual(1)
  })

  it("forwards workload warehouses for optional query-history evidence", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([1]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
      workloadWarehouseIds: ["warehouse-b", "warehouse-a"],
    })

    expect(req.workload_warehouse_ids).toEqual(["warehouse-b", "warehouse-a"])
  })

  it("never includes retired lever 0 in the levers payload", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([3, 1]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
    })

    expect(req.levers).toEqual([1, 3]) // sorted subset of {1..6}
    expect(req.levers).not.toContain(0)
  })

  it("filters out-of-range lever ids (retired 0, 7+) before sending", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([0, 1, 7]),
      selectedModel: null,
      targetAccuracy: 0.9,
      maxAttempts: 3,
      benchmarkPolicy: "review_only",
    })

    expect(req.levers).toEqual([1]) // 0 (retired) and 7 (out of range) dropped
  })
})
