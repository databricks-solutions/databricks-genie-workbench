import { describe, expect, it, vi, beforeEach } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import type {
  GSOIterationResult,
  GSOBenchmarkChanges,
  GSOBenchmarkQC,
  GSOPublishRecord,
} from "@/types"

// Mock the API module before importing anything that pulls it in (resolution.ts
// and BenchmarkChangesPanel both import from @/lib/api).
vi.mock("@/lib/api", () => {
  class ApiError extends Error {
    status: number
    constructor(message: string, status: number) {
      super(message)
      this.name = "ApiError"
      this.status = status
    }
  }
  return {
    applyAutoOptimize: vi.fn(() => Promise.resolve({ status: "applied", runId: "r", message: "" })),
    discardAutoOptimize: vi.fn(() => Promise.resolve({ status: "discarded", runId: "r", message: "" })),
    getAutoOptimizeBenchmarkChanges: vi.fn(() => Promise.resolve(null)),
    ApiError,
  }
})

import { applyAutoOptimize, discardAutoOptimize, ApiError } from "@/lib/api"
import { performResolution } from "./resolution"
import { buildAttemptOptions, attemptColumnLabel } from "./runDetail"
import { PublishAuditSummary } from "./PublishAuditSummary"
import { ResolutionActions } from "./ResolutionActions"
import { BenchmarkChangesPanel } from "./BenchmarkChangesPanel"
import { OptimizationNarrative } from "./OptimizationNarrative"

// A fully-populated iteration row; tests override the fields they care about.
function iter(overrides: Partial<GSOIterationResult>): GSOIterationResult {
  return {
    iteration: 0,
    lever: null,
    eval_scope: "full",
    overall_accuracy: 70,
    total_questions: 30,
    correct_count: 21,
    ...overrides,
  }
}

// ---------------------------------------------------------------------------
// Item 4 — Attempt selector logic
// ---------------------------------------------------------------------------

describe("buildAttemptOptions — v2 runs (Baseline · Coverage · Surgical N · best★)", () => {
  const iterations = [
    iter({ iteration: 0, overall_accuracy: 70, correct_count: 21, attempt_no: 0 }),
    iter({ iteration: 1, overall_accuracy: 72, correct_count: 22, attempt_no: 1, attempt_mode: "coverage" }),
    iter({
      iteration: 2,
      overall_accuracy: 80,
      correct_count: 24,
      attempt_no: 2,
      attempt_mode: "surgical",
      is_champion: true,
    }),
  ]

  it("builds one option per full iteration, labeled by mode", () => {
    const { options } = buildAttemptOptions({ iterations, baselineIteration: 0, bestIteration: 2 })
    expect(options.map((o) => o.label)).toEqual(["Baseline", "Coverage", "Surgical 2"])
    expect(options.map((o) => o.iteration)).toEqual([0, 1, 2])
  })

  it("stars the explicit champion and defaults the selection to it (never idxmax)", () => {
    const { options, defaultKey } = buildAttemptOptions({
      iterations,
      baselineIteration: 0,
      bestIteration: 2,
    })
    const champ = options.find((o) => o.starred)!
    expect(champ.label).toBe("Surgical 2")
    expect(champ.isChampion).toBe(true)
    expect(defaultKey).toBe(champ.key)
  })

  it("falls back to bestIteration for the star when no row is flagged champion", () => {
    const noFlag = iterations.map((it) => ({ ...it, is_champion: false }))
    const { options, defaultKey } = buildAttemptOptions({
      iterations: noFlag,
      baselineIteration: 0,
      bestIteration: 1,
    })
    const starred = options.find((o) => o.starred)!
    expect(starred.iteration).toBe(1)
    expect(defaultKey).toBe(starred.key)
  })

  it("carries each attempt's own accuracy onto its option", () => {
    const { options } = buildAttemptOptions({ iterations, baselineIteration: 0, bestIteration: 2 })
    expect(options[0].accuracyPct).toBeCloseTo(70)
    expect(options[2].accuracyPct).toBeCloseTo(80)
  })
})

describe("buildAttemptOptions — legacy runs degrade to Baseline · Final", () => {
  const legacy = [
    iter({ iteration: 0, overall_accuracy: 60, correct_count: 18 }),
    iter({ iteration: 4, overall_accuracy: 75, correct_count: 22 }),
  ]

  it("produces only Baseline and Final when there is no attempt metadata", () => {
    const { options, defaultKey } = buildAttemptOptions({
      iterations: legacy,
      baselineIteration: 0,
      bestIteration: 4,
    })
    expect(options.map((o) => o.label)).toEqual(["Baseline", "Final"])
    // Default to Final (best★).
    expect(options.find((o) => o.key === defaultKey)!.label).toBe("Final")
    expect(options[1].starred).toBe(true)
  })

  it("returns a single Baseline option when there is no distinct best iteration", () => {
    const { options } = buildAttemptOptions({
      iterations: [iter({ iteration: 0, overall_accuracy: 60 })],
      baselineIteration: 0,
      bestIteration: 0,
    })
    expect(options).toHaveLength(1)
    expect(options[0].label).toBe("Baseline")
  })

  it("returns no options when there are no iterations", () => {
    const { options, defaultKey } = buildAttemptOptions({
      iterations: [],
      baselineIteration: null,
      bestIteration: null,
    })
    expect(options).toHaveLength(0)
    expect(defaultKey).toBe("")
  })
})

describe("attemptColumnLabel — QuestionJourney 'Iter' → attempts relabel", () => {
  it("labels by mode when attempt metadata is present", () => {
    expect(attemptColumnLabel(iter({ iteration: 0 }))).toBe("Baseline")
    expect(attemptColumnLabel(iter({ iteration: 1, attempt_mode: "coverage" }))).toBe("Coverage")
    expect(attemptColumnLabel(iter({ iteration: 2, attempt_mode: "surgical", attempt_no: 2 }))).toBe(
      "Surgical 2",
    )
  })

  it("falls back to 'Attempt N' (never 'Iter N') for unlabeled iterations", () => {
    expect(attemptColumnLabel(iter({ iteration: 3 }))).toBe("Attempt 3")
  })
})

// ---------------------------------------------------------------------------
// Item 2 — Keep / Discard wiring (incl. the rollback re-PATCH path)
// ---------------------------------------------------------------------------

describe("performResolution — Keep / Discard wiring", () => {
  beforeEach(() => {
    vi.mocked(applyAutoOptimize).mockClear()
    vi.mocked(discardAutoOptimize).mockClear()
    vi.mocked(applyAutoOptimize).mockResolvedValue({ status: "applied", runId: "run-1", message: "" })
    vi.mocked(discardAutoOptimize).mockResolvedValue({ status: "discarded", runId: "run-1", message: "" })
  })

  it("Keep calls applyAutoOptimize and resolves APPLIED", async () => {
    const res = await performResolution("apply", "run-1")
    expect(applyAutoOptimize).toHaveBeenCalledWith("run-1")
    expect(discardAutoOptimize).not.toHaveBeenCalled()
    expect(res).toEqual({ kind: "resolved", status: "APPLIED" })
  })

  it("Discard calls discardAutoOptimize (the space_snapshot rollback re-PATCH) and resolves DISCARDED", async () => {
    const res = await performResolution("discard", "run-1")
    expect(discardAutoOptimize).toHaveBeenCalledWith("run-1")
    expect(applyAutoOptimize).not.toHaveBeenCalled()
    expect(res).toEqual({ kind: "resolved", status: "DISCARDED" })
  })

  it("maps a 409 'already applied' to the resolved APPLIED state, not an error", async () => {
    vi.mocked(applyAutoOptimize).mockRejectedValueOnce(new ApiError("Run already applied.", 409))
    const res = await performResolution("apply", "run-1")
    expect(res).toEqual({ kind: "resolved", status: "APPLIED" })
  })

  it("maps a 409 'already discarded' to the resolved DISCARDED state", async () => {
    vi.mocked(discardAutoOptimize).mockRejectedValueOnce(new ApiError("Run already discarded.", 409))
    const res = await performResolution("discard", "run-1")
    expect(res).toEqual({ kind: "resolved", status: "DISCARDED" })
  })

  it("surfaces a generic failure as an error result", async () => {
    vi.mocked(discardAutoOptimize).mockRejectedValueOnce(new Error("network down"))
    const res = await performResolution("discard", "run-1")
    expect(res).toEqual({ kind: "error", message: "network down" })
  })
})

describe("ResolutionActions — rendered states", () => {
  it("renders Keep + Discard when a champion was published", () => {
    const markup = renderToStaticMarkup(
      <ResolutionActions runId="r" status="CONVERGED" published={true} />,
    )
    expect(markup).toContain("Keep changes")
    expect(markup).toContain("Discard")
    expect(markup).toContain("roll the space back")
  })

  it("renders the kept banner when already APPLIED", () => {
    const markup = renderToStaticMarkup(
      <ResolutionActions runId="r" status="APPLIED" published={true} />,
    )
    expect(markup).toContain("Changes kept")
    expect(markup).not.toContain("Keep changes")
  })

  it("renders the discarded/rolled-back banner when already DISCARDED", () => {
    const markup = renderToStaticMarkup(
      <ResolutionActions runId="r" status="DISCARDED" published={true} />,
    )
    expect(markup).toContain("Changes discarded")
    expect(markup).toContain("rolled back")
  })

  it("renders nothing when nothing was published (nothing to keep / roll back)", () => {
    const markup = renderToStaticMarkup(
      <ResolutionActions runId="r" status="FAILED" published={false} />,
    )
    expect(markup).toBe("")
  })
})

// ---------------------------------------------------------------------------
// Item 1 — Publish/audit summary headline + concerns
// ---------------------------------------------------------------------------

function publishRecord(overrides: Partial<GSOPublishRecord>): GSOPublishRecord {
  return {
    runId: "r",
    spaceId: "s",
    finalStatus: "CONVERGED",
    terminalReason: "TARGET_REACHED",
    published: true,
    publishOutcome: "published",
    championIteration: 2,
    championAccuracy: 91,
    championConfigVersionId: null,
    targetAccuracy: 0.9,
    maxAttempts: 3,
    auditSummary: null,
    improvementTrajectory: [],
    concerns: [],
    ...overrides,
  }
}

describe("PublishAuditSummary — LLM headline + concerns callout (§7.3)", () => {
  it("renders the LLM summary paragraph as the headline", () => {
    const markup = renderToStaticMarkup(
      <PublishAuditSummary
        publishRecord={publishRecord({ auditSummary: "Lifted accuracy from 70% to 91% over two attempts." })}
      />,
    )
    expect(markup).toContain("Optimization summary")
    expect(markup).toContain("Lifted accuracy from 70% to 91%")
  })

  it("renders the concerns callout", () => {
    const markup = renderToStaticMarkup(
      <PublishAuditSummary
        publishRecord={publishRecord({
          auditSummary: "Done.",
          concerns: ["Stopped on budget with the spend cluster still failing."],
        })}
      />,
    )
    expect(markup).toContain("Concerns (1)")
    expect(markup).toContain("spend cluster still failing")
  })

  it("demotes the narrative to a collapsed detail (not visible) when a headline is present", () => {
    const markup = renderToStaticMarkup(
      <PublishAuditSummary publishRecord={publishRecord({ auditSummary: "Summary text." })}>
        <div>NARRATIVE_BODY</div>
      </PublishAuditSummary>,
    )
    // The toggle exists, but the collapsed body is not rendered until expanded.
    expect(markup).toContain("per-iteration detail")
    expect(markup).not.toContain("NARRATIVE_BODY")
  })

  it("keeps the legacy narrative visible when there is no publish record", () => {
    const markup = renderToStaticMarkup(
      <PublishAuditSummary publishRecord={null}>
        <div>NARRATIVE_BODY</div>
      </PublishAuditSummary>,
    )
    expect(markup).toContain("NARRATIVE_BODY")
    expect(markup).not.toContain("Optimization summary")
  })

  it("renders nothing when there is neither a record nor a narrative", () => {
    expect(renderToStaticMarkup(<PublishAuditSummary publishRecord={null} />)).toBe("")
  })

  it("composes with OptimizationNarrative without throwing", () => {
    const run = {
      runId: "r",
      spaceId: "s",
      status: "CONVERGED",
      startedAt: new Date().toISOString(),
      completedAt: null,
      baselineScore: 70,
      optimizedScore: 91,
      baselineIteration: 0,
      bestIteration: 2,
      steps: [],
      stages: [],
      levers: [],
      links: [],
      convergenceReason: null,
      deploymentStatus: null,
    }
    expect(() =>
      renderToStaticMarkup(
        <PublishAuditSummary publishRecord={publishRecord({ auditSummary: "x" })}>
          <OptimizationNarrative run={run} iterations={[]} />
        </PublishAuditSummary>,
      ),
    ).not.toThrow()
  })
})

// ---------------------------------------------------------------------------
// Item 3 — Benchmark QC meter (window + repair tries) + zero-mutation render
// ---------------------------------------------------------------------------

function qc(overrides: Partial<GSOBenchmarkQC>): GSOBenchmarkQC {
  return {
    validCount: 32,
    persistedCount: 32,
    repairTriesUsed: 1,
    repairMaxTries: 3,
    repairedIds: ["q1"],
    repairSweeps: null,
    finalValidity: true,
    window: null,
    windowTargetMin: 30,
    windowTargetMax: 40,
    gtCorrectionCandidates: [],
    terminalReason: null,
    stillInvalidIds: [],
    ...overrides,
  }
}

function changes(overrides: Partial<GSOBenchmarkChanges>): GSOBenchmarkChanges {
  return {
    runId: "r",
    added: [],
    removed: [],
    changed: [],
    pruneRecommended: [],
    items: [],
    counts: { added: 0, removed: 0, changed: 0, pruneRecommended: 0, total: 0 },
    qc: null,
    ...overrides,
  }
}

describe("BenchmarkChangesPanel — QC window meter + repair-tries indicator", () => {
  it("renders the 30–40 window meter and repair-sweeps indicator from qc", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel runId="r" changes={changes({ qc: qc({}) })} />,
    )
    expect(markup).toContain("Working-set window")
    expect(markup).toContain("target 30–40")
    expect(markup).toContain("In window")
    expect(markup).toContain("Repair sweeps")
    expect(markup).toContain("1 / 3")
    expect(markup).toContain("Benchmark valid")
  })

  it("renders the QC meter even when there are zero benchmark mutations", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel runId="r" changes={changes({ qc: qc({}) })} />,
    )
    // No mutation groups, but the window meter is still present (not the empty
    // "no changes" placeholder).
    expect(markup).toContain("Working-set window")
    expect(markup).not.toContain("GSO made no changes to this space's benchmark set.")
  })

  it("flags a below-window count as top-up recommended", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel runId="r" changes={changes({ qc: qc({ persistedCount: 20, validCount: 20 }) })} />,
    )
    expect(markup).toContain("Below window")
  })

  it("surfaces the BENCHMARK_UNREPAIRABLE terminal state", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel
        runId="r"
        changes={changes({ qc: qc({ finalValidity: false, terminalReason: "BENCHMARK_UNREPAIRABLE", stillInvalidIds: ["q9"] }) })}
      />,
    )
    expect(markup).toContain("could not be repaired")
    expect(markup).toContain("Benchmark invalid")
  })

  it("shows the empty placeholder when there is neither qc nor mutations", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel runId="r" changes={changes({})} />,
    )
    // renderToStaticMarkup HTML-escapes the apostrophe (' → &#x27;).
    expect(markup).toContain("GSO made no changes to this space")
    expect(markup).not.toContain("Working-set window")
  })
})
