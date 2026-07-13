import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it, vi } from "vitest"
import type { GSORunSummary } from "@/types"

vi.mock("@/lib/api", () => ({
  getAutoOptimizeRunsForSpace: vi.fn(() => Promise.resolve([])),
  revertAutoOptimizeRun: vi.fn(),
  ApiError: class ApiError extends Error {},
}))

import { RevertButton } from "./RunHistoryTable"
import { hasActiveOptimizationRun } from "./runHistory"

function run(overrides: Partial<GSORunSummary>): GSORunSummary {
  return {
    run_id: "run-1",
    space_id: "space-1",
    status: "CONVERGED",
    started_at: "2026-07-13T00:00:00Z",
    completed_at: "2026-07-13T00:05:00Z",
    best_accuracy: 90,
    best_iteration: 1,
    convergence_reason: "TARGET_REACHED",
    triggered_by: "user@example.com",
    ...overrides,
  }
}

describe("RunHistoryTable revert safety", () => {
  it("treats any active run for the Space as a global history lock", () => {
    expect(hasActiveOptimizationRun([
      run({ run_id: "old", status: "CONVERGED" }),
      run({ run_id: "current", status: "IN_PROGRESS" }),
    ])).toBe(true)
    expect(hasActiveOptimizationRun([run({ status: "DISCARDED" })])).toBe(false)
  })

  it("disables a terminal row's revert control while another run is active", () => {
    const markup = renderToStaticMarkup(
      <RevertButton
        run={run({ run_id: "old", status: "CONVERGED" })}
        target="baseline"
        disabled={true}
        onReverted={() => undefined}
      />,
    )

    expect(markup).toContain("disabled")
    expect(markup).toContain("active optimization on this Space")
    expect(markup).toContain("Revert to Baseline")
  })
})
