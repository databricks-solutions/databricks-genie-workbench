import {
  GSO_PIPELINE_STEPS,
  GSO_TOTAL_STEPS,
  type GSOAttempt,
  type GSOTerminalReason,
} from "@/types"

// ---------------------------------------------------------------------------
// Pure helpers for the GSO v2 live run cockpit (Phase 12). Kept out of the
// component files so those export only components (react-refresh/only-export-
// components) and so the scale/ladder/ledger/banner logic is unit-testable
// without a DOM.
// ---------------------------------------------------------------------------

// Patch-lever display names (levers 1–6). Lever 0 is not user-selectable in the
// 4-task runner, so it has no entry here. Mirrors OptimizationConfig.
export const LEVER_NAMES: Record<number, string> = {
  1: "Tables & Columns",
  2: "Metric Views",
  3: "SQL Queries",
  4: "Joins",
  5: "Text Instructions",
  6: "SQL Expressions",
}

// Marker colors by attempt mode. Amber is reserved for legacy enrichment rows;
// cyan is the current native patch/eval loop.
export const LEGACY_ENRICHMENT_COLOR = "#f59e0b" // amber-500
export const PATCH_ATTEMPT_COLOR = "#06b6d4" // cyan-500

/**
 * Normalize a per-attempt accuracy for the 0–100 chart scale — IDENTITY.
 *
 * Per the Phase-10 hard scale contract, ``GSOAttempt.accuracy``/``bestAccuracy``
 * and ``GSORunStatus.baselineScore`` are ALREADY 0–100; only
 * ``GSOLoopState.targetAccuracy`` is 0–1 (see {@link targetToPct}). This is a
 * pure null/finite guard with NO ×100 rescale — a legitimately low accuracy
 * (e.g. a real 0.85%) must stay 0.85, never be corrupted to 85.
 */
export function toAccuracyPct(value: number | null | undefined): number | null {
  if (value == null || !Number.isFinite(value)) return null
  return value
}

/**
 * Convert the ``targetAccuracy`` (the 0–1 request scale, per the contract) to
 * the 0–100 chart scale so the gold summit line / progress-to-target sit at the
 * right height. This is the ONLY ×100 conversion path in the cockpit. The
 * ``<= 1`` guard keeps a stray already-0–100 value sane (and maps 1.0 → 100).
 */
export function targetToPct(unit: number | null | undefined): number | null {
  if (unit == null || !Number.isFinite(unit)) return null
  return unit <= 1 ? unit * 100 : unit
}

export function attemptModeLabel(mode: string | null | undefined): string {
  if (!mode) return "—"
  const m = mode.toLowerCase()
  if (m === "coverage" || m === "enrichment" || m === "legacy") return "Legacy enrichment"
  if (m === "surgical" || m === "llm_patch" || m === "patch") return "Patch"
  return mode.charAt(0).toUpperCase() + mode.slice(1)
}

export function attemptModeColor(mode: string | null | undefined): string {
  const m = (mode ?? "").toLowerCase()
  return m === "coverage" || m === "enrichment" || m === "legacy" ? LEGACY_ENRICHMENT_COLOR : PATCH_ATTEMPT_COLOR
}

/**
 * Human label for the per-attempt decision. A rolled-back attempt always reads
 * "Rolled back" regardless of the raw ``decision`` token, since rollback is the
 * outcome the operator cares about.
 */
export function decisionLabel(
  decision: string | null | undefined,
  rolledBack: boolean,
): string {
  if (rolledBack) return "Rolled back"
  const d = (decision ?? "").toLowerCase()
  if (d === "accept") return "Accepted"
  if (d === "reject") return "Rejected"
  if (d === "continue") return "Continued"
  return decision ? decision : "—"
}

export type DecisionTone = "success" | "danger" | "warning" | "secondary"

export function decisionTone(
  decision: string | null | undefined,
  rolledBack: boolean,
): DecisionTone {
  if (rolledBack) return "danger"
  const d = (decision ?? "").toLowerCase()
  if (d === "accept") return "success"
  if (d === "reject") return "danger"
  if (d === "continue") return "warning"
  return "secondary"
}

// ---------------------------------------------------------------------------
// Hypothesis extractors (the current-attempt focus strip). The controller
// commits ``current_hypothesis = {ag_id, levers}`` (harness.py:21839); these
// read those fields defensively from the ``Record | string | null`` contract.
// ---------------------------------------------------------------------------

type Hypothesis = Record<string, unknown> | string | null | undefined

export function hypothesisClusterId(h: Hypothesis): string | null {
  if (!h || typeof h === "string") return null
  const ag = h["ag_id"]
  if (typeof ag === "string" && ag.trim()) return ag.trim()
  if (typeof ag === "number") return String(ag)
  return null
}

export function hypothesisLevers(h: Hypothesis): number[] {
  if (!h || typeof h === "string") return []
  const lv = h["levers"]
  if (!Array.isArray(lv)) return []
  return lv
    .map((x) => (typeof x === "number" ? x : Number(x)))
    .filter((n): n is number => Number.isFinite(n) && n >= 1 && n <= 6)
}

export function leverFamilyLabels(levers: number[]): string[] {
  return levers.map((id) => LEVER_NAMES[id] ?? `Lever ${id}`)
}

// ---------------------------------------------------------------------------
// Attempt Ladder model (the signature element). Re-bases the old per-iteration
// score chart onto ATTEMPTS: a best-so-far champion staircase, a gold target
// summit line, and a faint baseline floor.
// ---------------------------------------------------------------------------

export type RungMode = "baseline" | "legacy" | "patch"

export interface LadderRung {
  key: string
  x: number
  label: string
  shortLabel: string
  /** The attempt's OWN full-benchmark accuracy (0–100); null if unmeasured. */
  accuracy: number | null
  /** Where the marker is drawn (own accuracy, falling back to best-so-far). */
  markerY: number | null
  /** Best-so-far champion-staircase value at this rung (0–100, monotone). */
  bestSoFar: number | null
  mode: RungMode
  color: string
  /** Filled marker = accepted (landed on the staircase). */
  accepted: boolean
  /** Hollow marker = rolled back / not adopted (drops below the staircase). */
  rolledBack: boolean
  isChampion: boolean
  /** Annotation shown for a rejected rung, e.g. "rolled back". */
  note: string | null
}

export interface LadderModel {
  rungs: LadderRung[]
  baselineFloor: number | null
  summit: number | null
  yMin: number
  yMax: number
}

export function buildLadderModel(args: {
  baselineAccuracy: number | null | undefined
  attempts: GSOAttempt[]
  targetUnit: number | null | undefined
}): LadderModel {
  const baselineFloor = toAccuracyPct(args.baselineAccuracy)
  const summit = targetToPct(args.targetUnit)
  const rungs: LadderRung[] = []

  let runningBest: number | null = baselineFloor
  let x = 0

  rungs.push({
    key: "baseline",
    x,
    label: "Baseline",
    shortLabel: "Base",
    accuracy: baselineFloor,
    markerY: baselineFloor,
    bestSoFar: baselineFloor,
    mode: "baseline",
    color: "#9ca3af",
    accepted: true,
    rolledBack: false,
    isChampion: false,
    note: null,
  })

  for (const a of args.attempts) {
    x += 1
    const rawMode = (a.attemptMode ?? "").toLowerCase()
    const mode: RungMode = rawMode === "coverage" || rawMode === "enrichment" ? "legacy" : "patch"
    const accuracy = toAccuracyPct(a.accuracy)
    const reportedBest = toAccuracyPct(a.bestAccuracy)
    // Champion staircase is monotone non-decreasing: take the best of the
    // running best and any reported best/own accuracy for accepted rungs.
    const candidates = [runningBest, reportedBest]
    if (!a.rolledBack && accuracy != null) candidates.push(accuracy)
    const bestSoFar = candidates.reduce<number | null>(
      (acc, v) => (v == null ? acc : acc == null ? v : Math.max(acc, v)),
      null,
    )
    runningBest = bestSoFar
    const accepted = !a.rolledBack && (a.decision ?? "").toLowerCase() === "accept"
    let note: string | null = null
    if (a.rolledBack) note = "rolled back"

    rungs.push({
      key: `attempt-${a.attemptNo ?? x}`,
      x,
      label: `${mode === "patch" ? "Patch" : "Legacy enrichment"} ${a.attemptNo ?? x}`,
      shortLabel: mode === "patch" ? `P${a.attemptNo ?? x}` : "Legacy",
      accuracy,
      // An unmeasured rung still renders — pin it to the baseline floor.
      markerY: accuracy ?? bestSoFar ?? baselineFloor,
      bestSoFar,
      mode,
      color: mode === "patch" ? PATCH_ATTEMPT_COLOR : LEGACY_ENRICHMENT_COLOR,
      accepted,
      rolledBack: a.rolledBack,
      isChampion: a.isChampion,
      note,
    })
  }

  // Y domain — fit baseline, summit, all accuracies + best-so-far with padding.
  const values: number[] = []
  for (const r of rungs) {
    if (r.accuracy != null) values.push(r.accuracy)
    if (r.markerY != null) values.push(r.markerY)
    if (r.bestSoFar != null) values.push(r.bestSoFar)
  }
  if (baselineFloor != null) values.push(baselineFloor)
  if (summit != null) values.push(summit)
  const dataMin = values.length ? Math.min(...values) : 0
  const dataMax = values.length ? Math.max(...values) : 100
  const yMin = Math.max(0, Math.floor((dataMin - 8) / 5) * 5)
  const yMax = Math.min(100, Math.ceil((dataMax + 8) / 5) * 5)

  return { rungs, baselineFloor, summit, yMin, yMax: Math.max(yMax, yMin + 5) }
}

// ---------------------------------------------------------------------------
// Attempt Ledger model (the tabular companion). One row per baseline / patch
// attempt. The champion is read from the explicit
// ``isChampion`` flag — NEVER re-derived as idxmax (§5). The highest-accuracy
// row is highlighted separately; when it diverges from the champion, the
// rejection/rollback reason is surfaced so a higher-but-rolled-back attempt is
// explained, not hidden.
// ---------------------------------------------------------------------------

export interface LedgerRow {
  key: string
  label: string
  sublabel: string | null
  mode: RungMode
  accuracy: number | null
  deltaVsBaseline: number | null
  decision: string | null
  decisionDisplay: string
  decisionTone: DecisionTone
  rolledBack: boolean
  isChampion: boolean
  isHighest: boolean
  /** Set on the highest-accuracy row when it is NOT the champion. */
  divergenceReason: string | null
}

export function buildLedgerModel(args: {
  baselineAccuracy: number | null | undefined
  attempts: GSOAttempt[]
  /** Baseline is champion only when explicitly nothing beat it (terminal, no
   * attempt flagged champion) — passed in, never re-derived here. */
  baselineIsChampion?: boolean
}): LedgerRow[] {
  const baselineFloor = toAccuracyPct(args.baselineAccuracy)
  const rows: LedgerRow[] = []

  rows.push({
    key: "baseline",
    label: "Baseline",
    sublabel: "iteration 0",
    mode: "baseline",
    accuracy: baselineFloor,
    deltaVsBaseline: baselineFloor == null ? null : 0,
    decision: null,
    decisionDisplay: "—",
    decisionTone: "secondary",
    rolledBack: false,
    isChampion: Boolean(args.baselineIsChampion),
    isHighest: false,
    divergenceReason: null,
  })

  for (const a of args.attempts) {
    const rawMode = (a.attemptMode ?? "").toLowerCase()
    const mode: RungMode = rawMode === "coverage" || rawMode === "enrichment" ? "legacy" : "patch"
    const accuracy = toAccuracyPct(a.accuracy)
    rows.push({
      key: `attempt-${a.attemptNo ?? rows.length}`,
      label: `${attemptModeLabel(a.attemptMode)}`,
      sublabel: a.attemptNo != null ? `attempt ${a.attemptNo}` : null,
      mode,
      accuracy,
      deltaVsBaseline:
        accuracy != null && baselineFloor != null ? accuracy - baselineFloor : null,
      decision: a.decision ?? null,
      decisionDisplay: decisionLabel(a.decision, a.rolledBack),
      decisionTone: decisionTone(a.decision, a.rolledBack),
      rolledBack: a.rolledBack,
      isChampion: a.isChampion,
      isHighest: false,
      divergenceReason: a.rollbackReason ?? a.decisionReason ?? null,
    })
  }

  // Highlight the single highest-accuracy row (first wins on ties).
  let highestIdx = -1
  let highestVal = -Infinity
  rows.forEach((r, i) => {
    if (r.accuracy != null && r.accuracy > highestVal) {
      highestVal = r.accuracy
      highestIdx = i
    }
  })
  if (highestIdx >= 0) rows[highestIdx].isHighest = true

  // Divergence: when the highest-accuracy row is NOT the champion, keep its
  // rejection/rollback reason; clear the reason on every other row so only the
  // explanatory divergence note remains.
  const championIdx = rows.findIndex((r) => r.isChampion)
  rows.forEach((r, i) => {
    const diverges = i === highestIdx && championIdx >= 0 && championIdx !== highestIdx
    if (!diverges) {
      r.divergenceReason = null
    } else if (!r.divergenceReason) {
      r.divergenceReason = "Higher accuracy but not adopted as champion"
    }
  })

  return rows
}

// ---------------------------------------------------------------------------
// Terminal banner classification — keyed on the typed terminal reason, clearly
// distinguishing "stopped — nothing published" from "champion published". The
// publish-record ``published`` flag is authoritative when present; otherwise we
// infer from the terminal reason (only TARGET_REACHED / MAX_ATTEMPTS publish,
// per Phase 9 gating).
// ---------------------------------------------------------------------------

export type TerminalTone = "success" | "danger" | "warning" | "info" | "secondary"

export interface TerminalClassification {
  published: boolean
  tone: TerminalTone
  title: string
  detail: string
}

const PUBLISHED_REASONS: ReadonlySet<GSOTerminalReason> = new Set([
  "TARGET_REACHED",
  "MAX_ATTEMPTS",
])
const HARD_FAIL_REASONS: ReadonlySet<GSOTerminalReason> = new Set([
  "EVAL_INVALID",
  "LOOP_STATE_INVALID",
])

export function isHardFailReason(reason: GSOTerminalReason | null | undefined): boolean {
  return reason != null && HARD_FAIL_REASONS.has(reason)
}

export function isPublishedReason(reason: GSOTerminalReason | null | undefined): boolean {
  return reason != null && PUBLISHED_REASONS.has(reason)
}

export function classifyTerminal(args: {
  status?: string | null
  terminalReason?: GSOTerminalReason | null
  published?: boolean | null
  publishOutcome?: string | null
  benchmarkUnrepairable?: boolean
}): TerminalClassification | null {
  // The 01 benchmark hard-fail short-circuits before the loop ever runs —
  // nothing is published.
  if (args.benchmarkUnrepairable) {
    return {
      published: false,
      tone: "danger",
      title: "Stopped — benchmark unrepairable",
      detail:
        "Benchmark QC & repair could not produce a valid evaluation set. The optimization loop never ran and nothing was published.",
    }
  }

  const reason = args.terminalReason ?? null
  if (!reason) {
    // Legacy / free-text terminal runs surface via the existing reason copy;
    // no typed banner.
    return null
  }

  const published =
    args.published != null ? args.published : isPublishedReason(reason)

  switch (reason) {
    case "TARGET_REACHED":
      return {
        published: true,
        tone: "success",
        title: "Champion published — target accuracy reached",
        detail:
          "A candidate met the target accuracy. Publish & Audit promoted the champion and wrote the audit record.",
      }
    case "MAX_ATTEMPTS":
      return {
        published: published,
        tone: published ? "success" : "warning",
        title: published
          ? "Champion published — max patch attempts reached"
          : "Stopped — max patch attempts reached",
        detail: published
          ? "The patch/eval loop hit its attempt budget. Publish & Audit promoted the best-so-far champion."
          : "The patch/eval loop hit its attempt budget without a publishable champion.",
      }
    case "EVAL_INVALID":
      return {
        published: false,
        tone: "danger",
        title: "Stopped — evaluation invalid",
        detail:
          "An eval run produced no scorable rows, so the loop stopped fail-closed. Nothing was published.",
      }
    case "LOOP_STATE_INVALID":
      return {
        published: false,
        tone: "danger",
        title: "Stopped — loop state invalid",
        detail:
          "The controller hard-stopped after restoring the frozen baseline (a firewall/rollback guard tripped). Nothing was published.",
      }
    case "NO_NEW_HYPOTHESIS":
      return {
        published: false,
        tone: "warning",
        title: "Stopped — no new hypothesis",
        detail:
          "The strategist ran out of distinct hypotheses to try. Nothing was published.",
      }
    case "EVAL_BUDGET_EXHAUSTED":
      return {
        published: false,
        tone: "warning",
        title: "Stopped — eval budget exhausted",
        detail:
          "The 2-hour eval-wall budget was reached before another attempt could be funded. Nothing was published.",
      }
    default:
      return {
        published,
        tone: published ? "success" : "secondary",
        title: published ? "Champion published" : "Run finished",
        detail: args.publishOutcome ?? "",
      }
  }
}

// ---------------------------------------------------------------------------
// 4-task rail model. State per node is
// driven off run-status stepsCompleted / currentStepName + the typed terminal
// reason; the 01 node can branch to a BENCHMARK_UNREPAIRABLE hard-fail chip.
// ---------------------------------------------------------------------------

export type RailNodeState = "completed" | "current" | "failed" | "upcoming"

export interface RailNode {
  stepNumber: number
  name: string
  state: RailNodeState
  chip: string | null
}

export function buildTaskRail(args: {
  stepsCompleted?: number | null
  currentStepName?: string | null
  status?: string | null
  terminalReason?: GSOTerminalReason | null
  benchmarkUnrepairable?: boolean
}): RailNode[] {
  const completed = Math.max(0, Math.min(args.stepsCompleted ?? 0, GSO_TOTAL_STEPS))
  const current = (args.currentStepName ?? "").trim().toLowerCase()
  const statusUpper = (args.status ?? "").toUpperCase()
  const failed = statusUpper === "FAILED"
  const allDone = completed >= GSO_TOTAL_STEPS

  // Loop hard-fails land on the Optimize node (step 3); the benchmark hard-fail
  // lands on the QC & Repair node (step 2).
  const optimizeFailed = failed && isHardFailReason(args.terminalReason)

  return GSO_PIPELINE_STEPS.map((step) => {
    let state: RailNodeState
    if (args.benchmarkUnrepairable && step.stepNumber === 2) {
      state = "failed"
    } else if (optimizeFailed && step.stepNumber === 3) {
      state = "failed"
    } else if (step.stepNumber <= completed) {
      state = "completed"
    } else if (
      !allDone &&
      !failed &&
      (current === step.name.toLowerCase() || step.stepNumber === completed + 1)
    ) {
      state = "current"
    } else {
      state = "upcoming"
    }
    const chip =
      args.benchmarkUnrepairable && step.stepNumber === 2 ? "BENCHMARK_UNREPAIRABLE" : null
    return { stepNumber: step.stepNumber, name: step.name, state, chip }
  })
}

// Short prefix labels for the rail (e.g. "00 · Intake & Snapshot").
export const RAIL_STEP_PREFIXES: Record<number, string> = {
  1: "00",
  2: "01",
  3: "02",
  4: "03",
}

// Progress-to-target as a 0–1 fraction (how far best has climbed from baseline
// toward the target). Clamped to [0,1]; null when inputs are missing.
export function progressToTarget(args: {
  baselineAccuracy: number | null
  bestAccuracy: number | null
  targetPct: number | null
}): number | null {
  const { baselineAccuracy: base, bestAccuracy: best, targetPct: target } = args
  if (best == null || target == null) return null
  if (base == null) return Math.max(0, Math.min(1, best / target))
  if (target <= base) return best >= target ? 1 : 0
  return Math.max(0, Math.min(1, (best - base) / (target - base)))
}
