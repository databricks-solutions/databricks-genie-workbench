import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it, vi } from "vitest"
import type { GSORunSummary, VersionMatch } from "@/types"

vi.mock("@/lib/api", () => ({
  getAutoOptimizeRunsForSpace: vi.fn(() => Promise.resolve([])),
  getCurrentVersion: vi.fn(() => Promise.resolve({ status: "no_known_versions" })),
  revertAutoOptimizeRun: vi.fn(),
  ApiError: class ApiError extends Error {},
}))

import { DriftBanner, LiveVersionBadge, RevertButton } from "./RunHistoryTable"
import { hasActiveOptimizationRun, hasRevertibleChampion } from "./runHistory"

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
  it("treats any active run for the agent as a global history lock", () => {
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
    expect(markup).toContain("active optimization on this agent")
    expect(markup).toContain("Revert to Baseline")
  })

  it("offers an iteration-0 champion when enrichment or recovery won", () => {
    expect(hasRevertibleChampion(run({
      status: "APPLIED",
      best_iteration: 0,
      best_accuracy: 94.1,
    }))).toBe(true)
  })

  it("does not offer a champion before one has been scored", () => {
    expect(hasRevertibleChampion(run({
      status: "FAILED",
      best_iteration: null,
      best_accuracy: null,
    }))).toBe(false)
  })

  it("continues to offer champions from later iterations", () => {
    expect(hasRevertibleChampion(run({
      best_iteration: 2,
      best_accuracy: 96,
    }))).toBe(true)
  })
})

// ── Current-version indicators ──────────────────────────────────────────

function match(overrides: Partial<VersionMatch>): VersionMatch {
  return {
    run_id: "run-1",
    target: "baseline",
    started_at: "2026-07-13T00:00:00Z",
    best_accuracy: 90,
    ...overrides,
  }
}

describe("LiveVersionBadge", () => {
  it("labels the live baseline and champion", () => {
    const baseline = renderToStaticMarkup(
      <LiveVersionBadge current={match({ target: "baseline" })} equivalents={[]} />,
    )
    expect(baseline).toContain("Live — baseline")

    const champion = renderToStaticMarkup(
      <LiveVersionBadge current={match({ target: "champion" })} equivalents={[]} />,
    )
    expect(champion).toContain("Live — champion")
  })

  it("explains the match in the tooltip", () => {
    const markup = renderToStaticMarkup(
      <LiveVersionBadge current={match({ target: "baseline" })} equivalents={[]} />,
    )
    expect(markup).toContain("currently matches this run&#x27;s baseline config")
  })

  it("lists byte-identical equivalents in the tooltip", () => {
    const markup = renderToStaticMarkup(
      <LiveVersionBadge
        current={match({ run_id: "run-2", target: "baseline", started_at: "2026-07-20T00:00:00Z" })}
        equivalents={[match({ run_id: "run-1", target: "champion", started_at: "2026-07-13T00:00:00Z" })]}
      />,
    )
    const expectedDate = new Date("2026-07-13T00:00:00Z").toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    })
    expect(markup).toContain("Identical to:")
    expect(markup).toContain("champion of the")
    expect(markup).toContain(expectedDate)
  })
})

describe("DriftBanner", () => {
  it("warns that the config changed outside Auto-Optimize", () => {
    const markup = renderToStaticMarkup(<DriftBanner liveUpdateTime={null} />)
    expect(markup).toContain("changed outside Auto-Optimize")
    expect(markup).not.toContain("last modified")
  })

  it("includes the last-modified date when known", () => {
    const markup = renderToStaticMarkup(<DriftBanner liveUpdateTime="2026-07-28T16:00:00Z" />)
    const expectedDate = new Date("2026-07-28T16:00:00Z").toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    })
    expect(markup).toContain("last modified")
    expect(markup).toContain(expectedDate)
  })
})
