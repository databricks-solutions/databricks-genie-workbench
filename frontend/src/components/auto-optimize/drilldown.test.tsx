import { describe, expect, it, vi } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import type { GSOIterationResult, GSOPipelineStep, GSOLeverStatus, GSOTerminalReason } from "@/types"

// Mock the API module before importing PipelineDetailsModal (which — and whose
// child components — pull in @/lib/api). We only render prop-driven pieces, so
// none of these are actually invoked; the mock just keeps import-time clean and
// deterministic.
vi.mock("@/lib/api", () => ({
  getAutoOptimizeRun: vi.fn(() => Promise.resolve(null)),
  getAutoOptimizeIterations: vi.fn(() => Promise.resolve([])),
  getAutoOptimizePublishRecord: vi.fn(() => Promise.resolve(null)),
  getAutoOptimizeLoopState: vi.fn(() => Promise.resolve(null)),
  getAutoOptimizeQuestionResults: vi.fn(() => Promise.resolve([])),
  getAutoOptimizePatches: vi.fn(() => Promise.resolve([])),
  getAutoOptimizeBenchmarkChanges: vi.fn(() => Promise.resolve(null)),
  ApiError: class ApiError extends Error {},
}))

import { deriveRailProgress, patchAttemptLabel, buildLeverIterationLabels } from "./pipelineDetail"
import { humanizeTerminalReason, championAccuracyText } from "./runHistory"
import { AttemptExplorerTable } from "./PipelineDetailsModal"
import { OptimizationLevers } from "./OptimizationLevers"
import { AutoOptimizeContent } from "@/pages/HowItWorks"

// Minimal builders — override just the fields under test.
function step(overrides: Partial<GSOPipelineStep>): GSOPipelineStep {
  return {
    stepNumber: 1,
    name: "Intake & Snapshot",
    status: "pending",
    durationSeconds: null,
    summary: null,
    inputs: null,
    outputs: null,
    ...overrides,
  }
}

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
// Item 1 — deriveRailProgress (4-task rail inputs from a run's steps)
// ---------------------------------------------------------------------------

describe("deriveRailProgress — 4-task rail inputs from run.steps", () => {
  it("counts completed/skipped as done and reports the first running step", () => {
    const steps = [
      step({ stepNumber: 1, name: "Intake & Snapshot", status: "completed" }),
      step({ stepNumber: 2, name: "Benchmark QC & Repair", status: "skipped" }),
      step({ stepNumber: 3, name: "Optimize", status: "running" }),
      step({ stepNumber: 4, name: "Publish & Audit", status: "pending" }),
    ]
    expect(deriveRailProgress(steps)).toEqual({
      stepsCompleted: 2,
      currentStepName: "Optimize",
    })
  })

  it("treats a terminal all-done run as fully complete with no current step", () => {
    const steps = [
      step({ stepNumber: 1, status: "completed" }),
      step({ stepNumber: 2, status: "completed" }),
      step({ stepNumber: 3, status: "success" }),
      step({ stepNumber: 4, status: "completed" }),
    ]
    expect(deriveRailProgress(steps)).toEqual({ stepsCompleted: 4, currentStepName: null })
  })

  it("degrades to zero for empty / missing steps (legacy runs)", () => {
    expect(deriveRailProgress([])).toEqual({ stepsCompleted: 0, currentStepName: null })
    expect(deriveRailProgress(null)).toEqual({ stepsCompleted: 0, currentStepName: null })
    expect(deriveRailProgress(undefined)).toEqual({ stepsCompleted: 0, currentStepName: null })
  })
})

// ---------------------------------------------------------------------------
// Item 1 — patchAttemptLabel (attempt-grouped patches re-key)
// ---------------------------------------------------------------------------

describe("patchAttemptLabel — Iter → attempt re-key", () => {
  const iterations = [
    iter({ iteration: 0 }),
    iter({ iteration: 1, attempt_no: 1, attempt_mode: "llm_patch" }),
    iter({ iteration: 2, attempt_no: 2, attempt_mode: "llm_patch" }),
  ]

  it("maps iterations onto the patch attempt vocabulary", () => {
    expect(patchAttemptLabel(0, iterations)).toBe("Baseline")
    expect(patchAttemptLabel(1, iterations)).toBe("Patch 1")
    expect(patchAttemptLabel(2, iterations)).toBe("Patch 2")
  })

  it("falls back for unknown iterations and null (legacy / orphan patches)", () => {
    expect(patchAttemptLabel(7, iterations)).toBe("Iteration 7")
    expect(patchAttemptLabel(3, [])).toBe("Iteration 3")
    expect(patchAttemptLabel(0, [])).toBe("Baseline")
    expect(patchAttemptLabel(null, iterations)).toBe("—")
  })
})

// ---------------------------------------------------------------------------
// Item 3 — humanizeTerminalReason + championAccuracyText (history columns)
// ---------------------------------------------------------------------------

describe("humanizeTerminalReason — typed reason → compact Outcome label", () => {
  it("maps each typed reason to a terse label", () => {
    expect(humanizeTerminalReason("TARGET_REACHED")).toBe("Target reached")
    expect(humanizeTerminalReason("MAX_ATTEMPTS")).toBe("Max attempts")
    expect(humanizeTerminalReason("NO_NEW_HYPOTHESIS")).toBe("No new hypothesis")
    expect(humanizeTerminalReason("EVAL_INVALID")).toBe("Eval invalid")
    expect(humanizeTerminalReason("CONFIG_VALIDATION_FAILED")).toBe("Config validation failed")
    expect(humanizeTerminalReason("LOOP_STATE_INVALID")).toBe("Loop state invalid")
    expect(humanizeTerminalReason("EVAL_BUDGET_EXHAUSTED")).toBe("Budget exhausted")
  })

  it("humanizes the QC-side BENCHMARK_UNREPAIRABLE hard-stop from either field", () => {
    // Not a GSOTerminalReason member — the 01 hard-stop lands on the run
    // summary as free-text convergence_reason (or, defensively, on `reason`).
    expect(humanizeTerminalReason(null, "BENCHMARK_UNREPAIRABLE")).toBe("Benchmark unrepairable")
    expect(
      humanizeTerminalReason("BENCHMARK_UNREPAIRABLE" as unknown as GSOTerminalReason, null),
    ).toBe("Benchmark unrepairable")
  })

  it("degrades to the free-text convergence reason, then an em dash", () => {
    expect(humanizeTerminalReason(null, "converged early")).toBe("converged early")
    expect(humanizeTerminalReason(null, "  ")).toBe("—")
    expect(humanizeTerminalReason(null)).toBe("—")
    expect(humanizeTerminalReason(undefined, null)).toBe("—")
  })
})

describe("championAccuracyText — identity on 0–100 (no ×100 heuristic)", () => {
  it("renders a 0–100 accuracy as an integer percent", () => {
    expect(championAccuracyText(85)).toBe("85%")
    expect(championAccuracyText(100)).toBe("100%")
    expect(championAccuracyText(0)).toBe("0%")
  })

  it("does NOT ×100 a true sub-1% champion (the Phase-12 corruption class)", () => {
    expect(championAccuracyText(0.9)).toBe("1%")
    expect(championAccuracyText(0.9)).not.toBe("90%")
  })

  it("renders an em dash for null / non-finite", () => {
    expect(championAccuracyText(null)).toBe("—")
    expect(championAccuracyText(undefined)).toBe("—")
    expect(championAccuracyText(Number.NaN)).toBe("—")
  })
})

// ---------------------------------------------------------------------------
// Item 1 — AttemptExplorerTable (re-keyed iteration+lever → attempt+mode+decision)
// ---------------------------------------------------------------------------

describe("AttemptExplorerTable — re-keyed Attempt Explorer (fallback)", () => {
  it("renders attempt-mode/decision columns and stars the explicit champion", () => {
    const iterations = [
      iter({ iteration: 0, overall_accuracy: 70, correct_count: 21 }),
      iter({
        iteration: 1,
        attempt_no: 1,
        attempt_mode: "llm_patch",
        decision: "reject",
        rolled_back: true,
        overall_accuracy: 70,
        correct_count: 21,
      }),
      iter({
        iteration: 2,
        attempt_no: 2,
        attempt_mode: "llm_patch",
        decision: "accept",
        is_champion: true,
        overall_accuracy: 84,
        correct_count: 25,
      }),
    ]
    const markup = renderToStaticMarkup(<AttemptExplorerTable iterations={iterations} />)
    expect(markup).toContain("Attempt Accuracy Progression")
    expect(markup).toContain("Baseline")
    expect(markup).toContain("Patch 1")
    expect(markup).toContain("Patch 2")
    expect(markup).toContain("Accepted")
    // Patch 1 was rolled back → decision reads "Rolled back" regardless of token.
    expect(markup).toContain("Rolled back")
    // Champion star present (explicit is_champion flag, never idxmax).
    expect(markup).toContain("★")
  })

  it("degrades gracefully for legacy iterations with no attempt metadata", () => {
    const iterations = [
      iter({ iteration: 0, overall_accuracy: 70, correct_count: 21 }),
      iter({ iteration: 1, overall_accuracy: 74, correct_count: 22 }),
    ]
    const markup = renderToStaticMarkup(<AttemptExplorerTable iterations={iterations} />)
    // No attempt metadata → "Attempt N" label, no champion star.
    expect(markup).toContain("Attempt 1")
    expect(markup).not.toContain("★")
  })

  it("surfaces the highest-vs-champion reason inline (champion = explicit flag, not idxmax)", () => {
    // The highest-accuracy row (iter 2, 88%) was rolled back and is NOT the
    // champion; a lower row (iter 3, 80%) carries the explicit is_champion flag.
    // An idxmax impl would star iter 2 and hide the reason — this asserts the
    // opposite.
    const iterations = [
      iter({ iteration: 0, overall_accuracy: 70, correct_count: 21 }),
      iter({
        iteration: 2,
        attempt_no: 2,
        attempt_mode: "llm_patch",
        decision: "reject",
        rolled_back: true,
        decision_reason: "regressed on the priority cluster",
        overall_accuracy: 88,
        correct_count: 26,
      }),
      iter({
        iteration: 3,
        attempt_no: 3,
        attempt_mode: "llm_patch",
        decision: "accept",
        is_champion: true,
        overall_accuracy: 80,
        correct_count: 24,
      }),
    ]
    const markup = renderToStaticMarkup(<AttemptExplorerTable iterations={iterations} />)
    // Exactly one champion star, and it is on the lower (80%) row — assert the
    // star renders and the divergence explanation is surfaced inline.
    expect(markup).toContain("★")
    expect(markup).toContain("Highest accuracy, but not the champion")
    expect(markup).toContain("regressed on the priority cluster")
  })
})

// ---------------------------------------------------------------------------
// Item 2 — HowItWorks Auto-Optimize prose (4-task DAG + patch/eval loop)
// ---------------------------------------------------------------------------

describe("AutoOptimizeContent — 4-task DAG + patch/eval loop prose", () => {
  const markup = renderToStaticMarkup(<AutoOptimizeContent />)

  it("describes the 4-task pipeline (standalone baseline/deploy dropped)", () => {
    expect(markup).toContain("4-Task Pipeline")
    expect(markup).toContain("Intake &amp; Snapshot")
    expect(markup).toContain("Benchmark QC &amp; Repair")
    expect(markup).toContain("02 Optimize")
    expect(markup).toContain("Publish &amp; Audit")
    expect(markup).not.toContain("6-Task Pipeline")
    expect(markup).not.toContain("Baseline Eval &amp; Triage")
  })

  it("explains the patch/eval loop and the stop conditions", () => {
    expect(markup).toContain("Iteration 0")
    expect(markup).toContain("Patch/eval loop")
    expect(markup).toContain("target accuracy")
    expect(markup).toContain("max attempts")
    expect(markup).toContain("max_attempts bounds")
    expect(markup).not.toContain("Coverage")
    expect(markup).not.toContain("Surgical")
  })
})

// ---------------------------------------------------------------------------
// Item 1 (B1) — attempt-grouped Levers provenance labels
// ---------------------------------------------------------------------------

describe("buildLeverIterationLabels — attempt-grouped lever provenance", () => {
  it("maps iterations to Patch N (+ decision) when attempt metadata is present", () => {
    const iterations = [
      iter({ iteration: 0 }),
      iter({ iteration: 1, attempt_no: 1, attempt_mode: "llm_patch", decision: "reject", rolled_back: true }),
      iter({ iteration: 2, attempt_no: 2, attempt_mode: "llm_patch", decision: "accept" }),
    ]
    const labels = buildLeverIterationLabels(iterations)
    expect(labels.get(1)).toBe("Patch 1 · Rolled back")
    expect(labels.get(2)).toBe("Patch 2 · Accepted")
  })

  it("returns an empty map (⇒ 'Iteration N' fallback) for legacy runs with no attempt metadata", () => {
    const labels = buildLeverIterationLabels([iter({ iteration: 1 }), iter({ iteration: 2 })])
    expect(labels.size).toBe(0)
    expect(buildLeverIterationLabels(undefined).size).toBe(0)
  })
})

describe("OptimizationLevers — attempt-labeled provenance vs legacy Iteration N", () => {
  // A lever with two patched iterations so the multi-iteration branch (which
  // renders the per-iteration provenance sub-labels) is exercised.
  const patch = {
    patchType: "update_instruction",
    scope: "",
    riskLevel: "",
    targetObject: null,
    rolledBack: false,
    rollbackReason: null,
    command: null,
    patch: null,
    appliedAt: null,
  }
  const lever: GSOLeverStatus = {
    lever: 5,
    name: "Instructions & Examples",
    status: "accepted",
    patchCount: 2,
    scoreBefore: null,
    scoreAfter: null,
    scoreDelta: null,
    rollbackReason: null,
    patches: [],
    iterations: [
      { iteration: 1, status: "rolled_back", patchCount: 1, patchTypes: [], scoreBefore: null, scoreAfter: null, scoreDelta: null, rollbackReason: null, patches: [patch] },
      { iteration: 2, status: "accepted", patchCount: 1, patchTypes: [], scoreBefore: null, scoreAfter: null, scoreDelta: null, rollbackReason: null, patches: [patch] },
    ],
  }
  const iterations = [
    iter({ iteration: 1, attempt_no: 1, attempt_mode: "llm_patch", decision: "reject", rolled_back: true }),
    iter({ iteration: 2, attempt_no: 2, attempt_mode: "llm_patch", decision: "accept" }),
  ]

  it("re-keys provenance sub-labels to Patch N when attempt metadata is present", () => {
    const markup = renderToStaticMarkup(<OptimizationLevers levers={[lever]} iterations={iterations} />)
    expect(markup).toContain("Patch 1")
    expect(markup).toContain("Patch 2")
    expect(markup).not.toContain("Iteration 2")
  })

  it("degrades to 'Iteration N' when no iterations prop / attempt metadata is provided", () => {
    const markup = renderToStaticMarkup(<OptimizationLevers levers={[lever]} />)
    expect(markup).toContain("Iteration 2")
    expect(markup).not.toContain("Patch 2")
  })

  it("keeps example SQL patches under their stamped lever", () => {
    const examplePatch = {
      patchType: "add_example_sql",
      scope: "genie_config",
      riskLevel: "low",
      targetObject: null,
      rolledBack: false,
      rollbackReason: null,
      command: JSON.stringify({
        op: "add",
        section: "example_question_sqls",
        question: "How many orders by region?",
        sql: "SELECT region, COUNT(*) FROM orders GROUP BY 1",
      }),
      patch: null,
      appliedAt: null,
    }
    const leverWithExample: GSOLeverStatus = {
      lever: 5,
      name: "Instructions & Examples",
      status: "accepted",
      patchCount: 1,
      scoreBefore: null,
      scoreAfter: null,
      scoreDelta: null,
      rollbackReason: null,
      patches: [examplePatch],
      iterations: [],
    }

    const markup = renderToStaticMarkup(<OptimizationLevers levers={[leverWithExample]} />)
    const instructionsIdx = markup.indexOf("Instructions &amp; Examples")
    const exampleIdx = markup.indexOf("How many orders by region?")
    const expressionsIdx = markup.indexOf("SQL Expressions")

    expect(markup).toContain("Example SQL")
    expect(instructionsIdx).toBeGreaterThan(-1)
    expect(exampleIdx).toBeGreaterThan(instructionsIdx)
    expect(expressionsIdx).toBeGreaterThan(exampleIdx)
  })
})
