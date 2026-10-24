/**
 * The score chart's attach step (MV-D121, M7d Ruling 5): a kept metric-view
 * attach is its own point right after the baseline, keyed "0-mv", and is
 * never drawn without a baseline point.
 */
import { describe, expect, it } from "vitest"
import { buildChartData } from "./IterationChart"
import type { GSOIterationResult } from "@/types"

function row(iteration: number, overall_accuracy: number, lever: number | null = null): GSOIterationResult {
  return {
    iteration,
    lever,
    eval_scope: "full",
    overall_accuracy,
    total_questions: 15,
    correct_count: Math.round(overall_accuracy * 15),
  }
}

describe("buildChartData — the metric-view attach step", () => {
  it("the attach is a point right after the baseline", () => {
    const points = buildChartData([row(1, 0.933, 1), row(0, 0.8667)], 90)
    expect(points.map((p) => p.key)).toEqual(["0", "0-mv", "1"])
    expect(points.map((p) => p.leverLabel)).toEqual(["Baseline", "Metric view", "Tables & Columns"])
    expect(points[1].accuracy).toBe(90)
    expect(points[1].totalQuestions).toBe(points[0].totalQuestions)
  })

  it("a run whose only gain is the attach has two points", () => {
    const points = buildChartData([row(0, 0.8667)], 90)
    expect(points).toHaveLength(2)
  })

  it("no attach, no point", () => {
    const points = buildChartData([row(0, 0.8667), row(1, 0.933, 1)], null)
    expect(points.map((p) => p.key)).toEqual(["0", "1"])
  })

  it("no baseline, no attach point", () => {
    const points = buildChartData([row(1, 0.933, 1)], 90)
    expect(points.map((p) => p.key)).toEqual(["1"])
  })

  it("a 0–1 attach accuracy is scaled like the rows", () => {
    const points = buildChartData([row(0, 0.8667)], 0.9)
    expect(points[1].accuracy).toBe(90)
  })
})
