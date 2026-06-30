import { describe, expect, it } from "vitest"
import { renderToStaticMarkup } from "react-dom/server"
import { GSO_PIPELINE_STEPS, type GSOAttempt } from "@/types"
import {
  buildLadderModel,
  buildLedgerModel,
  buildTaskRail,
  classifyTerminal,
  progressToTarget,
  targetToPct,
  toAccuracyPct,
} from "./cockpit"
import { AttemptLadder } from "./AttemptLadder"
import { AttemptLedger } from "./AttemptLedger"
import { TaskRail } from "./TaskRail"
import { TerminalBanner } from "./TerminalBanner"
import { ChampionHero } from "./ChampionHero"

// A fully-populated GSOAttempt with sensible defaults; tests override the
// fields they care about.
function attempt(overrides: Partial<GSOAttempt>): GSOAttempt {
  return {
    attemptNo: 1,
    attemptMode: "coverage",
    iteration: 1,
    evalScope: "full",
    lever: null,
    accuracy: null,
    bestAccuracy: null,
    decision: null,
    decisionReason: null,
    rolledBack: false,
    rollbackReason: null,
    isChampion: false,
    currentHypothesis: null,
    terminalReason: null,
    ...overrides,
  }
}

describe("accuracy scale helpers", () => {
  it("keeps 0–100 accuracy and rescales a defensive 0–1 float", () => {
    expect(toAccuracyPct(85)).toBe(85)
    expect(toAccuracyPct(0.85)).toBeCloseTo(85)
    expect(toAccuracyPct(null)).toBeNull()
    expect(toAccuracyPct(undefined)).toBeNull()
  })

  it("converts the 0–1 target accuracy to the 0–100 chart scale (×100)", () => {
    expect(targetToPct(0.9)).toBeCloseTo(90)
    expect(targetToPct(1)).toBeCloseTo(100)
    expect(targetToPct(90)).toBe(90) // defensive: already 0–100
    expect(targetToPct(null)).toBeNull()
  })
})

describe("progressToTarget", () => {
  it("measures how far best has climbed from baseline toward target", () => {
    expect(progressToTarget({ baselineAccuracy: 70, bestAccuracy: 80, targetPct: 90 })).toBeCloseTo(0.5)
    expect(progressToTarget({ baselineAccuracy: 70, bestAccuracy: 70, targetPct: 90 })).toBe(0)
    expect(progressToTarget({ baselineAccuracy: 70, bestAccuracy: 95, targetPct: 90 })).toBe(1) // clamped
    expect(progressToTarget({ baselineAccuracy: null, bestAccuracy: null, targetPct: 90 })).toBeNull()
  })
})

describe("Attempt Ladder — coverage rung always renders at zero lift (§5)", () => {
  it("renders the coverage rung as a flat amber rung at the baseline floor when rolled back", () => {
    const attempts = [
      attempt({
        attemptNo: 1,
        attemptMode: "coverage",
        accuracy: 70, // below baseline → no lift
        bestAccuracy: 72,
        rolledBack: true,
        decision: "reject",
      }),
    ]
    const model = buildLadderModel({ baselineAccuracy: 72, attempts, targetUnit: 0.9 })

    // baseline + coverage rungs both present (coverage never hidden).
    expect(model.rungs).toHaveLength(2)
    const cov = model.rungs[1]
    expect(cov.mode).toBe("coverage")
    expect(cov.bestSoFar).toBe(72) // champion staircase stayed at the baseline floor
    expect(cov.note).toMatch(/no change|rolled back/)
    expect(model.summit).toBeCloseTo(90)
  })

  it("pins an unmeasured coverage rung to the baseline floor (never hidden)", () => {
    const attempts = [attempt({ attemptNo: 1, attemptMode: "coverage", accuracy: null, rolledBack: true })]
    const model = buildLadderModel({ baselineAccuracy: 80, attempts, targetUnit: 0.9 })
    expect(model.rungs[1].markerY).toBe(80)
  })

  it("renders an SVG with the coverage 'no change / rolled back' annotation", () => {
    const attempts = [
      attempt({ attemptNo: 1, attemptMode: "coverage", accuracy: 72, bestAccuracy: 72, rolledBack: true, decision: "reject" }),
    ]
    const markup = renderToStaticMarkup(
      <AttemptLadder baselineAccuracy={72} attempts={attempts} targetUnit={0.9} />,
    )
    expect(markup).toContain("<svg")
    expect(markup).toContain("Coverage")
    expect(markup).toContain("rolled back")
    expect(markup).toContain("target")
  })

  it("keeps the champion staircase monotone non-decreasing", () => {
    const attempts = [
      attempt({ attemptNo: 1, attemptMode: "coverage", accuracy: 60, bestAccuracy: 70, rolledBack: true, decision: "reject" }),
      attempt({ attemptNo: 2, attemptMode: "surgical", accuracy: 78, bestAccuracy: 78, decision: "accept", isChampion: true }),
    ]
    const model = buildLadderModel({ baselineAccuracy: 70, attempts, targetUnit: 0.9 })
    const best = model.rungs.map((r) => r.bestSoFar)
    expect(best).toEqual([70, 70, 78])
    for (let i = 1; i < best.length; i++) {
      expect(best[i]!).toBeGreaterThanOrEqual(best[i - 1]!)
    }
  })
})

describe("Attempt Ledger — highest-accuracy highlight + champion from is_champion (§5)", () => {
  // The coverage attempt has the HIGHEST accuracy but was rolled back; the
  // surgical attempt is the explicit champion at a LOWER accuracy. The ledger
  // must highlight the highest row, star the champion from is_champion (NOT
  // idxmax), and surface the rollback reason so the divergence is explained.
  const attempts = [
    attempt({
      attemptNo: 1,
      attemptMode: "coverage",
      accuracy: 85,
      bestAccuracy: 70,
      rolledBack: true,
      decision: "reject",
      rollbackReason: "regressed on cohort spend cluster",
    }),
    attempt({
      attemptNo: 2,
      attemptMode: "surgical",
      accuracy: 80,
      bestAccuracy: 80,
      decision: "accept",
      isChampion: true,
    }),
  ]

  it("marks the highest-accuracy row and the champion separately", () => {
    const rows = buildLedgerModel({ baselineAccuracy: 70, attempts })
    const cov = rows.find((r) => r.key === "attempt-1")!
    const surg = rows.find((r) => r.key === "attempt-2")!

    expect(cov.isHighest).toBe(true) // 85% is the highest
    expect(cov.isChampion).toBe(false)
    expect(surg.isChampion).toBe(true) // champion is the explicit is_champion=true row
    expect(surg.isHighest).toBe(false)
    expect(cov.deltaVsBaseline).toBeCloseTo(15)
  })

  it("explains the divergence on the highest row, clears it on the champion", () => {
    const rows = buildLedgerModel({ baselineAccuracy: 70, attempts })
    const cov = rows.find((r) => r.key === "attempt-1")!
    const surg = rows.find((r) => r.key === "attempt-2")!
    expect(cov.divergenceReason).toContain("regressed on cohort spend cluster")
    expect(surg.divergenceReason).toBeNull()
  })

  it("renders the champion star, the highest tag, and the divergence reason", () => {
    const markup = renderToStaticMarkup(<AttemptLedger baselineAccuracy={70} attempts={attempts} />)
    expect(markup).toContain("★")
    expect(markup).toContain("highest")
    expect(markup).toContain("regressed on cohort spend cluster")
  })

  it("marks baseline as champion only when explicitly nothing beat it", () => {
    const rolledOnly = [
      attempt({ attemptNo: 1, attemptMode: "coverage", accuracy: 65, bestAccuracy: 70, rolledBack: true, decision: "reject" }),
    ]
    const rows = buildLedgerModel({ baselineAccuracy: 70, attempts: rolledOnly, baselineIsChampion: true })
    expect(rows[0].key).toBe("baseline")
    expect(rows[0].isChampion).toBe(true)
  })
})

describe("Task Rail — 5 nodes, default fallback 5, 01 hard-fail chip", () => {
  it("always renders the 5 canonical DAG nodes", () => {
    const nodes = buildTaskRail({})
    expect(nodes).toHaveLength(5)
    expect(nodes.map((n) => n.name)).toEqual(GSO_PIPELINE_STEPS.map((s) => s.name))
  })

  it("drives completed/current state off stepsCompleted (defaults to 5 tasks)", () => {
    const nodes = buildTaskRail({ stepsCompleted: 3, status: "RUNNING" })
    expect(nodes).toHaveLength(5)
    expect(nodes[0].state).toBe("completed")
    expect(nodes[2].state).toBe("completed")
    expect(nodes[3].state).toBe("current")
    expect(nodes[4].state).toBe("upcoming")
  })

  it("branches the 01 node to a BENCHMARK_UNREPAIRABLE hard-fail chip", () => {
    const nodes = buildTaskRail({ status: "FAILED", benchmarkUnrepairable: true })
    const qc = nodes.find((n) => n.stepNumber === 2)!
    expect(qc.state).toBe("failed")
    expect(qc.chip).toBe("BENCHMARK_UNREPAIRABLE")

    const markup = renderToStaticMarkup(<TaskRail status="FAILED" benchmarkUnrepairable />)
    expect(markup).toContain("BENCHMARK_UNREPAIRABLE")
    // renderToStaticMarkup HTML-escapes "&" → "&amp;" in the step names.
    for (const step of GSO_PIPELINE_STEPS) {
      expect(markup).toContain(step.name.replace(/&/g, "&amp;"))
    }
  })

  it("marks the Optimize node failed on a loop hard-fail terminal reason", () => {
    const nodes = buildTaskRail({ status: "FAILED", terminalReason: "LOOP_STATE_INVALID" })
    expect(nodes.find((n) => n.stepNumber === 4)!.state).toBe("failed")
  })
})

describe("Terminal Banner — published vs nothing-published", () => {
  it("classifies TARGET_REACHED / MAX_ATTEMPTS as published", () => {
    expect(classifyTerminal({ terminalReason: "TARGET_REACHED" })!.published).toBe(true)
    expect(classifyTerminal({ terminalReason: "MAX_ATTEMPTS", published: true })!.published).toBe(true)
  })

  it("classifies EVAL_INVALID / LOOP_STATE_INVALID as stopped — nothing published", () => {
    const ev = classifyTerminal({ terminalReason: "EVAL_INVALID" })!
    expect(ev.published).toBe(false)
    expect(ev.tone).toBe("danger")
    const ls = classifyTerminal({ terminalReason: "LOOP_STATE_INVALID" })!
    expect(ls.published).toBe(false)
  })

  it("treats a benchmark hard-fail as stopped — nothing published", () => {
    const unrep = classifyTerminal({ benchmarkUnrepairable: true })!
    expect(unrep.published).toBe(false)
    expect(unrep.title).toMatch(/unrepairable/i)
  })

  it("renders the published banner with the champion accuracy", () => {
    const markup = renderToStaticMarkup(
      <TerminalBanner status="CONVERGED" terminalReason="TARGET_REACHED" published={true} championAccuracy={91.2} />,
    )
    expect(markup).toContain("Published")
    expect(markup).toContain("target accuracy")
    expect(markup).toContain("91.2%")
  })

  it("renders the not-published banner for EVAL_INVALID", () => {
    const markup = renderToStaticMarkup(
      <TerminalBanner status="FAILED" terminalReason="EVAL_INVALID" published={false} />,
    )
    expect(markup).toContain("Not published")
    expect(markup).toContain("evaluation invalid")
  })
})

describe("graceful degradation — legacy run / no attempts", () => {
  it("returns no typed banner for a legacy free-text terminal run", () => {
    expect(classifyTerminal({ status: "CONVERGED", terminalReason: null })).toBeNull()
    // TerminalBanner self-hides (renders nothing) so the legacy reason copy shows.
    expect(renderToStaticMarkup(<TerminalBanner status="CONVERGED" terminalReason={null} />)).toBe("")
  })

  it("builds a baseline-only ladder/ledger when there are no attempts", () => {
    const ladder = buildLadderModel({ baselineAccuracy: 75, attempts: [], targetUnit: 0.9 })
    expect(ladder.rungs).toHaveLength(1)
    expect(ladder.rungs[0].key).toBe("baseline")

    const rows = buildLedgerModel({ baselineAccuracy: 75, attempts: [] })
    expect(rows).toHaveLength(1)
    expect(rows[0].key).toBe("baseline")
  })

  it("never throws on null baseline + empty attempts + null target", () => {
    expect(() => buildLadderModel({ baselineAccuracy: null, attempts: [], targetUnit: null })).not.toThrow()
    expect(() =>
      renderToStaticMarkup(<AttemptLadder baselineAccuracy={null} attempts={[]} targetUnit={null} />),
    ).not.toThrow()
  })
})

describe("Champion hero", () => {
  it("renders the best accuracy, Δ vs baseline, and progress-to-target", () => {
    const markup = renderToStaticMarkup(
      <ChampionHero baselineAccuracy={70} bestAccuracy={82} targetUnit={0.9} />,
    )
    expect(markup).toContain("82.0%")
    expect(markup).toContain("+12.0%")
    expect(markup).toContain("Progress to target")
  })
})
