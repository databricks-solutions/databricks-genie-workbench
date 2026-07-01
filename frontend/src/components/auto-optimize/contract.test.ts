import { describe, expect, expectTypeOf, it } from "vitest"
import {
  GSO_PIPELINE_STEPS,
  GSO_TOTAL_STEPS,
  type GSOAttempt,
  type GSOAttemptMode,
  type GSOLoopState,
  type GSOLoopStateResponse,
  type GSOPipelineRun,
  type GSOTriggerRequest,
  type GSOTerminalReason,
  type GSOLeverIteration,
} from "@/types"
import { buildOptimizationTriggerRequest } from "./optimizationRequest"

describe("GSO v2 frontend contract", () => {
  it("exposes the canonical 5-task DAG with no deploy task", () => {
    expect(GSO_TOTAL_STEPS).toBe(5)
    expect(GSO_PIPELINE_STEPS.map((s) => s.name)).toEqual([
      "Intake & Snapshot",
      "Benchmark QC & Repair",
      "Baseline Eval & Triage",
      "Optimize",
      "Publish & Audit",
    ])
    expect(GSO_PIPELINE_STEPS.some((s) => /deploy/i.test(s.name))).toBe(false)
  })

  it("builds the trigger request without retired deploy target fields", () => {
    const req = buildOptimizationTriggerRequest({
      spaceId: "space-1",
      applyMode: "genie_config",
      selectedLevers: new Set([6, 1, 0, 9]),
      selectedModel: "model-a",
      targetAccuracy: 0.9,
      maxAttempts: 3,
    })

    expect(req).toEqual({
      space_id: "space-1",
      apply_mode: "genie_config",
      levers: [1, 6],
      llm_model: "model-a",
      target_accuracy: 0.9,
      max_attempts: 3,
    })
    expect("deploy_target" in req).toBe(false)
  })

  it("keeps the loop-state legacy fallback contract explicit", () => {
    const response: GSOLoopStateResponse = {
      runId: "run-1",
      loopState: null,
      attempts: [],
    }

    expect(response.loopState).toBeNull()
    expect(response.attempts).toEqual([])

    const attempt: GSOAttempt = {
      attemptNo: 1,
      attemptMode: "coverage",
      iteration: 1,
      evalScope: "full",
      lever: null,
      accuracy: 80,
      bestAccuracy: 80,
      decision: "accept",
      decisionReason: null,
      rolledBack: false,
      rollbackReason: null,
      isChampion: true,
      currentHypothesis: null,
      bestConfigVersionId: "cfg-1",
      nextHypothesis: null,
      doNotRepeat: [],
      terminalReason: "TARGET_REACHED",
    }
    expect(attempt.attemptMode).toBe("coverage")
    expect(attempt.isChampion).toBe(true)
  })

  it("runtime fixtures no longer carry retired frontend fields", () => {
    const leverIteration: GSOLeverIteration = {
      iteration: 1,
      status: "accepted",
      patchCount: 0,
      patchTypes: [],
      scoreBefore: null,
      scoreAfter: null,
      scoreDelta: null,
      rollbackReason: null,
      patches: [],
    }
    const run: GSOPipelineRun = {
      runId: "run-1",
      spaceId: "space-1",
      status: "CONVERGED",
      startedAt: new Date(0).toISOString(),
      completedAt: null,
      baselineScore: 70,
      optimizedScore: 80,
      baselineIteration: 0,
      bestIteration: 1,
      steps: [],
      stages: [],
      levers: [],
      links: [],
      convergenceReason: null,
    }

    expect("mlflowRunId" in leverIteration).toBe(false)
    expect("deploymentStatus" in run).toBe(false)
  })

  it("type-checks the typed v2 loop/run fields", () => {
    expectTypeOf<GSOLoopStateResponse["loopState"]>().toEqualTypeOf<GSOLoopState | null>()
    expectTypeOf<GSOAttempt["attemptMode"]>().toEqualTypeOf<GSOAttemptMode | string | null>()
    expectTypeOf<GSOAttempt["terminalReason"]>().toEqualTypeOf<GSOTerminalReason | null>()
    expectTypeOf<GSOPipelineRun["terminalReason"]>().toEqualTypeOf<GSOTerminalReason | null | undefined>()
    expectTypeOf<GSOTriggerRequest["target_accuracy"]>().toEqualTypeOf<number | null | undefined>()
  })
})
