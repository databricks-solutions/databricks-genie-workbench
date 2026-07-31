import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import type { GSOBenchmarkChanges, GSOBenchmarkQC } from "@/types"

import { BenchmarkChangesPanel } from "./BenchmarkChangesPanel"

function qc(overrides: Partial<GSOBenchmarkQC> = {}): GSOBenchmarkQC {
  return {
    validCount: 9,
    persistedCount: 9,
    repairTriesUsed: 0,
    repairMaxTries: 3,
    repairedIds: [],
    repairSweeps: [],
    finalValidity: true,
    window: { status: "under_window", count: 9 },
    windowTargetMin: 30,
    windowTargetMax: 40,
    gtCorrectionCandidates: [],
    terminalReason: "INSUFFICIENT_VALID_BENCHMARKS",
    stillInvalidIds: null,
    qualityCounts: null,
    qualityFindings: [],
    proposedChanges: [],
    ...overrides,
  }
}

function changes(qcPayload: GSOBenchmarkQC): GSOBenchmarkChanges {
  return {
    runId: "run-1",
    added: [],
    removed: [],
    changed: [],
    pruneRecommended: [],
    items: [],
    counts: { added: 0, removed: 0, changed: 0, pruneRecommended: 0, total: 0 },
    qc: qcPayload,
  }
}

describe("BenchmarkChangesPanel policy and gate summary", () => {
  it("explains a review-only skip without claiming the valid subset is invalid", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel
        runId="run-1"
        changes={changes(qc({
          benchmarkPolicy: "review_only",
          optimizationEligible: false,
          minimumValidCount: 15,
        }))}
      />,
    )

    expect(markup).toContain("Review only · live benchmarks preserved")
    expect(markup).toContain("Optimization was skipped")
    expect(markup).toContain("at least 15 are required")
    expect(markup).toContain("No evaluation or configuration patch ran")
    expect(markup).toContain("SQL valid")
    expect(markup).not.toContain("SQL invalid")
  })

  it("shows repair usage for repair-enabled runs", () => {
    const markup = renderToStaticMarkup(
      <BenchmarkChangesPanel
        runId="run-2"
        changes={changes(qc({
          validCount: 30,
          persistedCount: 30,
          repairTriesUsed: 1,
          repairedIds: ["q1"],
          terminalReason: null,
          benchmarkPolicy: "repair_allowed",
          optimizationEligible: true,
        }))}
      />,
    )

    expect(markup).toContain("Repair sweeps:")
    expect(markup).toContain(">1</span> repaired")
    expect(markup).not.toContain("Optimization was skipped")
  })
})
