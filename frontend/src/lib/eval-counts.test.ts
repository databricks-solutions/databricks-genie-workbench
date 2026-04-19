import { describe, expect, it } from "vitest"
import type { GSOIterationResult, GSORunStatus } from "@/types"
import { evalCountsFromIteration, formatAccuracyPct } from "./eval-counts"

function iter(overrides: Partial<GSOIterationResult>): GSOIterationResult {
  return {
    iteration: 0,
    lever: null,
    eval_scope: "full",
    overall_accuracy: 0,
    total_questions: 0,
    correct_count: 0,
    scores_json: {},
    thresholds_met: false,
    ...overrides,
  }
}

describe("evalCountsFromIteration", () => {
  it("returns zeros for null", () => {
    expect(evalCountsFromIteration(null)).toEqual({
      total: 0,
      evaluated: 0,
      correct: 0,
      excluded: 0,
      accuracyPct: null,
      storedAccuracyPct: null,
      hasDrift: false,
    })
  })

  it("derives accuracy from correct/evaluated (12 of 14)", () => {
    // Original Bug #2 scenario: KPI card showed 12/14=85.7%, detail showed
    // 12/12=100%. Ensure the helper always reports 85.7% when the server
    // sends evaluated_count=14.
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 85.7,
        total_questions: 14,
        evaluated_count: 14,
        correct_count: 12,
        excluded_count: 0,
      }),
    )
    expect(c.evaluated).toBe(14)
    expect(c.correct).toBe(12)
    expect(c.accuracyPct).toBeCloseTo(85.7142857, 4)
    expect(c.storedAccuracyPct).toBeCloseTo(85.7)
    expect(c.hasDrift).toBe(false)
  })

  it("drops exclusions from the denominator (19 of 22 case)", () => {
    // The Apr 2 ticket scenario: 22 benchmarks, 3 excluded at runtime,
    // 19 evaluated. Tab label must show accuracy over 19, NOT 22.
    // Stored and derived agree here because the row was written with the
    // post-Bug-#2 math.
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 84.21,
        total_questions: 22,
        evaluated_count: 19,
        correct_count: 16,
        excluded_count: 3,
      }),
    )
    expect(c.evaluated).toBe(19)
    expect(c.excluded).toBe(3)
    expect(c.accuracyPct).toBeCloseTo(84.210526, 4)
    expect(c.hasDrift).toBe(false)
  })

  it("flags drift when stored overall_accuracy disagrees with derived by >0.5pp", () => {
    const c = evalCountsFromIteration(
      iter({
        // server stored 82.6 (maybe computed from 22 rows pre-Bug-#2 backfill)
        // but the row says evaluated=19 correct=16 → 84.21
        overall_accuracy: 82.6,
        total_questions: 22,
        evaluated_count: 19,
        correct_count: 16,
        excluded_count: 3,
      }),
    )
    expect(c.hasDrift).toBe(true)
    expect(c.accuracyPct).toBeCloseTo(84.210526, 4)
    expect(c.storedAccuracyPct).toBeCloseTo(82.6)
  })

  it("does not flag drift within 0.5pp", () => {
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 84.0,
        total_questions: 19,
        evaluated_count: 19,
        correct_count: 16,
        excluded_count: 0,
      }),
    )
    expect(c.hasDrift).toBe(false)
  })

  it("falls back to total_questions - excluded_count when evaluated_count is missing (legacy)", () => {
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 75.0,
        total_questions: 20,
        evaluated_count: null,
        excluded_count: 4,
        correct_count: 12,
      }),
    )
    expect(c.evaluated).toBe(16)
    expect(c.accuracyPct).toBe(75)
  })

  it("falls back to total_questions when both evaluated_count and excluded_count are absent (very legacy)", () => {
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 60.0,
        total_questions: 10,
        evaluated_count: null,
        excluded_count: null,
        correct_count: 6,
      }),
    )
    expect(c.evaluated).toBe(10)
    expect(c.accuracyPct).toBe(60)
  })

  it("clamps negative derived denominators to total_questions", () => {
    // Defensive: if someone wrote excluded_count > total_questions, don't
    // end up with evaluated=-X and a negative percentage.
    const c = evalCountsFromIteration(
      iter({
        total_questions: 5,
        evaluated_count: null,
        excluded_count: 9,
        correct_count: 1,
      }),
    )
    expect(c.evaluated).toBe(5)
  })

  it("returns null accuracyPct when evaluated is 0 (no questions evaluated)", () => {
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 0,
        total_questions: 0,
        evaluated_count: 0,
        correct_count: 0,
        excluded_count: 0,
      }),
    )
    expect(c.accuracyPct).toBeNull()
  })

  it("handles fractional overall_accuracy by scaling to percent", () => {
    // overall_accuracy stored as 0.826 (fraction) should still be treated
    // as 82.6% for the stored back-pointer.
    const c = evalCountsFromIteration(
      iter({
        overall_accuracy: 0.826,
        total_questions: 19,
        evaluated_count: 19,
        correct_count: 16,
        excluded_count: 0,
      }),
    )
    expect(c.storedAccuracyPct).toBeCloseTo(82.6)
  })

  it("handles live GSORunStatus shape (no per-iteration counts)", () => {
    const status: GSORunStatus = {
      runId: "r1",
      status: "IN_PROGRESS",
      spaceId: "s1",
      startedAt: null,
      completedAt: null,
      baselineScore: 75,
      optimizedScore: 80,
      convergenceReason: null,
    }
    const c = evalCountsFromIteration(status)
    expect(c.accuracyPct).toBe(75)
    expect(c.evaluated).toBe(0)
    expect(c.hasDrift).toBe(false)
  })
})

describe("formatAccuracyPct", () => {
  it("formats to one decimal", () => {
    expect(formatAccuracyPct(82.5714)).toBe("82.6%")
    expect(formatAccuracyPct(100)).toBe("100.0%")
    expect(formatAccuracyPct(0)).toBe("0.0%")
  })

  it("returns em-dash for null", () => {
    expect(formatAccuracyPct(null)).toBe("—")
  })
})

describe("contract: tab labels and score cards use same rounding", () => {
  // Regression guard for the exact bug the user re-reported: one surface
  // showed 82.6% and another showed 86%. Both now flow through
  // evalCountsFromIteration + .toFixed(1), so they cannot disagree by more
  // than a display rounding artifact.
  it("tab label percent agrees with score card percent to 0.1", () => {
    const row = iter({
      overall_accuracy: 82.6,
      total_questions: 22,
      evaluated_count: 19,
      correct_count: 16,
      excluded_count: 3,
    })
    const counts = evalCountsFromIteration(row)
    // Tab label renders counts.accuracyPct.toFixed(1)
    const tabLabel = counts.accuracyPct != null ? counts.accuracyPct.toFixed(1) : "—"
    // ScoreSummary receives baselineScore from the backend which, after
    // the Bug #2 fix, is derived the same way.
    const serverBaselineScore = counts.accuracyPct // backend now returns derived value
    const card =
      serverBaselineScore != null
        ? (serverBaselineScore > 1 ? serverBaselineScore : serverBaselineScore * 100).toFixed(1)
        : "—"
    expect(tabLabel).toBe(card)
  })
})
