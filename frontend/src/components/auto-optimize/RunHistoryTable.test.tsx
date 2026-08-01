import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it, vi } from "vitest"
import type { GSORunSummary, VersionMatch } from "@/types"

vi.mock("@/lib/api", () => ({
  getAutoOptimizeRunsForSpace: vi.fn(() => Promise.resolve([])),
  getCurrentVersion: vi.fn(() => Promise.resolve({ status: "no_known_versions" })),
  getAutoOptimizeRevertOptions: vi.fn(),
  removeAutoOptimizeRunFromHistory: vi.fn(),
  revertAutoOptimizeRun: vi.fn(),
  ApiError: class ApiError extends Error {},
}))

import {
  BenchmarkPolicyCell,
  DriftBanner,
  HistoryIncompleteBanner,
  LiveDimensionBadge,
  LiveVersionBadge,
  MixedStateBanner,
  RemoveHistoryButton,
  RevertOptionsButton,
  RunMetadataCell,
} from "./RunHistoryTable"
import {
  defaultRevertTargets,
  hasActiveOptimizationRun,
  hasRevertibleChampion,
} from "./runHistory"

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
  it("defaults a history restore to the champion config and its benchmarks", () => {
    expect(defaultRevertTargets({
      runId: "run-1",
      spaceId: "space-1",
      championAvailable: true,
      baselineAvailable: true,
      benchmarkChampionAvailable: true,
      benchmarkBaselineAvailable: true,
      benchmarkDiffs: {
        champion: { currentCount: 12, targetCount: 10, willAdd: 0, willRemove: 2, willChange: 0 },
        baseline: { currentCount: 12, targetCount: 8, willAdd: 0, willRemove: 4, willChange: 0 },
      },
    })).toEqual({ configTarget: "champion", benchmarkTarget: "champion" })
  })

  it("treats any active run for the agent as a global history lock", () => {
    expect(hasActiveOptimizationRun([
      run({ run_id: "old", status: "CONVERGED" }),
      run({ run_id: "current", status: "IN_PROGRESS" }),
    ])).toBe(true)
    expect(hasActiveOptimizationRun([run({ status: "DISCARDED" })])).toBe(false)
  })

  it("disables a terminal row's revert control while another run is active", () => {
    const markup = renderToStaticMarkup(
      <RevertOptionsButton
        run={run({ run_id: "old", status: "CONVERGED" })}
        disabled={true}
        onReverted={() => undefined}
      />,
    )

    expect(markup).toContain("disabled")
    expect(markup).toContain("active optimization on this agent")
    expect(markup).toContain("Revert Options")
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

describe("RemoveHistoryButton", () => {
  it("offers Workbench-only removal for a terminal run", () => {
    const markup = renderToStaticMarkup(
      <RemoveHistoryButton
        run={run({ status: "CONVERGED" })}
        disabled={false}
        onRemoved={() => undefined}
      />,
    )

    expect(markup).toContain("Remove from history")
    expect(markup).not.toContain(' disabled=""')
  })

  it("is disabled while its run is active", () => {
    const markup = renderToStaticMarkup(
      <RemoveHistoryButton
        run={run({ status: "IN_PROGRESS" })}
        disabled={true}
        onRemoved={() => undefined}
      />,
    )

    expect(markup).toContain("disabled")
    expect(markup).toContain("Wait for this optimization run to finish")
  })
})

describe("BenchmarkPolicyCell", () => {
  it("distinguishes review-only from repair-enabled runs", () => {
    const review = renderToStaticMarkup(
      <BenchmarkPolicyCell run={run({ benchmark_policy: "review_only", benchmark_mutation_count: 0 })} />,
    )
    expect(review).toContain("Review only")
    expect(review).toContain("No live benchmark changes")

    const repair = renderToStaticMarkup(
      <BenchmarkPolicyCell run={run({ benchmark_policy: "repair_allowed", benchmark_mutation_count: 2 })} />,
    )
    expect(repair).toContain("Repair allowed")
    expect(repair).toContain("2 live changes")
  })
})

describe("RunMetadataCell", () => {
  it("groups the run date, model, and triggering user", () => {
    const markup = renderToStaticMarkup(
      <RunMetadataCell run={run({
        llm_model: "databricks-claude-sonnet-4-6",
        triggered_by: "optimizer@example.com",
      })} />,
    )

    expect(markup).toContain("Jul")
    expect(markup).toContain("Model:")
    expect(markup).toContain("databricks-claude-sonnet-4-6")
    expect(markup).toContain("Triggered by:")
    expect(markup).toContain("optimizer@example.com")
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
    expect(markup).toContain("configuration and benchmarks currently match")
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
    const markup = renderToStaticMarkup(
      <DriftBanner dimensions={["config"]} liveUpdateTime={null} />,
    )
    expect(markup).toContain("live agent configuration")
    expect(markup).toContain("changed outside Auto-Optimize")
    expect(markup).not.toContain("last modified")
  })

  it("identifies benchmark-only drift", () => {
    const markup = renderToStaticMarkup(
      <DriftBanner dimensions={["benchmarks"]} liveUpdateTime={null} />,
    )
    expect(markup).toContain("live agent benchmarks")
    expect(markup).toContain("don’t match any known optimization version")
    expect(markup).toContain("they were changed outside Auto-Optimize")
  })

  it("includes the last-modified date when known", () => {
    const markup = renderToStaticMarkup(
      <DriftBanner
        dimensions={["config", "benchmarks"]}
        liveUpdateTime="2026-07-28T16:00:00Z"
      />,
    )
    const expectedDate = new Date("2026-07-28T16:00:00Z").toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    })
    expect(markup).toContain("last modified")
    expect(markup).toContain(expectedDate)
  })
})

describe("MixedStateBanner", () => {
  it("explains that config and benchmarks come from different known versions", () => {
    const markup = renderToStaticMarkup(
      <MixedStateBanner
        configMatch={match({ target: "champion" })}
        benchmarkMatch={match({ target: "baseline" })}
      />,
    )
    expect(markup).toContain("champion config")
    expect(markup).toContain("baseline benchmarks")
    expect(markup).toContain("different optimization versions")
    expect(markup).not.toContain("changed outside Auto-Optimize")
  })
})

describe("LiveDimensionBadge", () => {
  it("labels independently matched config and benchmark states", () => {
    const config = renderToStaticMarkup(
      <LiveDimensionBadge
        dimension="config"
        current={match({ target: "champion" })}
      />,
    )
    const benchmarks = renderToStaticMarkup(
      <LiveDimensionBadge
        dimension="benchmarks"
        current={match({ target: "baseline" })}
      />,
    )
    expect(config).toContain("Live config — champion")
    expect(benchmarks).toContain("Live benchmarks — baseline")
  })
})

describe("HistoryIncompleteBanner", () => {
  it("does not claim the config changed outside Auto-Optimize", () => {
    const markup = renderToStaticMarkup(<HistoryIncompleteBanner />)
    expect(markup).toContain("determined reliably")
    expect(markup).not.toContain("changed outside Auto-Optimize")
  })
})
