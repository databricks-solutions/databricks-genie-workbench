import { describe, expect, it } from "vitest"
import {
  BASELINE_RETAINED_LABEL,
  OPTIMIZATION_IN_PROGRESS_TOOLTIP,
  convergenceReasonText,
  isTerminalStatus,
  presentBaselineScore,
  presentOptimizedScore,
} from "./score-display"

describe("isTerminalStatus", () => {
  it("returns true for terminal statuses", () => {
    expect(isTerminalStatus("CONVERGED")).toBe(true)
    expect(isTerminalStatus("STALLED")).toBe(true)
    expect(isTerminalStatus("FAILED")).toBe(true)
    expect(isTerminalStatus("APPLIED")).toBe(true)
    expect(isTerminalStatus("DISCARDED")).toBe(true)
  })

  it("returns false for in-progress statuses", () => {
    expect(isTerminalStatus("RUNNING")).toBe(false)
    expect(isTerminalStatus("BASELINE_EVAL")).toBe(false)
    expect(isTerminalStatus("STARTED")).toBe(false)
  })

  it("returns false for null/empty", () => {
    expect(isTerminalStatus(null)).toBe(false)
    expect(isTerminalStatus(undefined)).toBe(false)
    expect(isTerminalStatus("")).toBe(false)
  })
})

describe("presentOptimizedScore", () => {
  it("renders '—' (no tooltip) when baseline is null", () => {
    const result = presentOptimizedScore({
      baselineScore: null,
      optimizedScore: null,
      bestIteration: null,
      status: "RUNNING",
    })
    expect(result.text).toBe("—")
    expect(result.tooltip).toBeNull()
    expect(result.pct).toBeNull()
  })

  it("renders '—' with 'in progress' tooltip when bestIteration==0 and not terminal", () => {
    // The exact screenshot bug: Baseline Evaluation finished, no iter > 0
    // yet. Pre-fix this rendered "100%". Post-fix: "—" + tooltip.
    const result = presentOptimizedScore({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "RUNNING",
    })
    expect(result.text).toBe("—")
    expect(result.tooltip).toBe(OPTIMIZATION_IN_PROGRESS_TOOLTIP)
    expect(result.pct).toBeNull()
  })

  it("renders the baseline number when bestIteration==0 and terminal", () => {
    // Run completed without any iter > 0 strictly improving on baseline.
    // The number IS the baseline; the "Baseline retained" label is
    // surfaced via convergenceReasonText, not the headline.
    const result = presentOptimizedScore({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "STALLED",
    })
    expect(result.text).toBe("80.0%")
    expect(result.tooltip).toBeNull()
    expect(result.pct).toBe(80.0)
  })

  it("renders the optimized number when bestIteration > 0", () => {
    const result = presentOptimizedScore({
      baselineScore: 80.0,
      optimizedScore: 92.5,
      bestIteration: 3,
      status: "CONVERGED",
    })
    expect(result.text).toBe("92.5%")
    expect(result.tooltip).toBeNull()
    expect(result.pct).toBe(92.5)
  })

  it("falls back to baseline when optimizedScore is null but baseline exists", () => {
    // Defensive — older backends might omit optimizedScore entirely.
    const result = presentOptimizedScore({
      baselineScore: 80.0,
      optimizedScore: null,
      bestIteration: null,
      status: "FAILED",
    })
    expect(result.text).toBe("80.0%")
    expect(result.pct).toBe(80.0)
  })

  it("rescales legacy 0-1 fractional scores to percentage", () => {
    // Defensive against fixtures or older API responses on the 0-1 scale.
    const result = presentOptimizedScore({
      baselineScore: 0.8,
      optimizedScore: 0.925,
      bestIteration: 1,
      status: "CONVERGED",
    })
    expect(result.text).toBe("92.5%")
    expect(result.pct).toBe(92.5)
  })
})

describe("presentBaselineScore", () => {
  it("renders '—' when null", () => {
    const result = presentBaselineScore(null)
    expect(result.text).toBe("—")
    expect(result.pct).toBeNull()
  })

  it("renders the percentage with one decimal", () => {
    expect(presentBaselineScore(80.0).text).toBe("80.0%")
    expect(presentBaselineScore(82.567).text).toBe("82.6%")
  })

  it("rescales 0-1 fractional input", () => {
    expect(presentBaselineScore(0.825).text).toBe("82.5%")
  })
})

describe("convergenceReasonText", () => {
  it("returns null for in-progress run with no iter > 0 yet", () => {
    // Mid-run, headline is "—" + its own tooltip — no need for a second copy.
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "RUNNING",
      convergenceReason: null,
    })
    expect(result).toBeNull()
  })

  it("returns 'Baseline retained' (alone) for terminal run with no improvement and no reason", () => {
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "STALLED",
      convergenceReason: null,
    })
    expect(result).toBe(BASELINE_RETAINED_LABEL)
  })

  it("suffixes the convergence reason after 'Baseline retained' for terminal+0+reason", () => {
    // Customer's words: "wire that label into the existing convergence-reason
    // copy". When the backend reports a reason, we keep both.
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "STALLED",
      convergenceReason: "no improvement after 3 attempts",
    })
    expect(result).toBe(`${BASELINE_RETAINED_LABEL} — no improvement after 3 attempts`)
  })

  it("returns convergence reason as-is for terminal run with improvement", () => {
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 92.0,
      bestIteration: 3,
      status: "CONVERGED",
      convergenceReason: "all accuracy thresholds met",
    })
    expect(result).toBe("all accuracy thresholds met")
  })

  it("returns null when no convergence reason and there was an improvement", () => {
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 92.0,
      bestIteration: 3,
      status: "CONVERGED",
      convergenceReason: null,
    })
    expect(result).toBeNull()
  })

  it("treats whitespace-only convergence reason as missing", () => {
    const result = convergenceReasonText({
      baselineScore: 80.0,
      optimizedScore: 80.0,
      bestIteration: 0,
      status: "STALLED",
      convergenceReason: "   ",
    })
    expect(result).toBe(BASELINE_RETAINED_LABEL)
  })
})
