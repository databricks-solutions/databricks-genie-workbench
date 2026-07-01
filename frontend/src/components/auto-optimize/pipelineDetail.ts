import type { GSOPipelineStep, GSOIterationResult } from "@/types"
import { attemptColumnLabel } from "@/components/auto-optimize/runDetail"
import { decisionLabel } from "@/components/auto-optimize/cockpit"

// ---------------------------------------------------------------------------
// Pure helpers for the GSO v2 Pipeline Details drill-down (Phase 14, item 1).
// Kept out of PipelineDetailsModal.tsx so the component file stays
// react-refresh clean (only-export-components) and the rail-progress / patch
// attempt-labeling logic is unit-testable without a DOM.
// ---------------------------------------------------------------------------

const DONE_STATES = new Set(["completed", "skipped", "success"])
const ACTIVE_STATES = new Set(["running", "in_progress", "in-progress", "active"])

/**
 * Derive the 5-task rail inputs from a run's ``steps`` array. The modal reads a
 * ``GSOPipelineRun`` (which carries ``steps`` but not the status endpoint's
 * ``stepsCompleted``/``currentStepName``), so we recompute them here:
 *
 * - ``stepsCompleted`` = count of steps whose status is completed/skipped.
 * - ``currentStepName`` = the name of the first running/in-progress step, so
 *   TaskRail can mark the live node (null when nothing is actively running).
 *
 * Tolerant of the legacy 6-step shape (Phase 10 remapped the backend
 * ``_STEP_DEFINITIONS`` to 5, but old runs may still report 6 rows) — the count
 * is clamped by TaskRail against ``GSO_TOTAL_STEPS``.
 */
export function deriveRailProgress(steps: GSOPipelineStep[] | null | undefined): {
  stepsCompleted: number
  currentStepName: string | null
} {
  if (!steps || steps.length === 0) {
    return { stepsCompleted: 0, currentStepName: null }
  }
  let stepsCompleted = 0
  let currentStepName: string | null = null
  for (const s of steps) {
    const status = (s.status ?? "").toLowerCase()
    if (DONE_STATES.has(status)) {
      stepsCompleted += 1
    } else if (currentStepName == null && ACTIVE_STATES.has(status)) {
      currentStepName = s.name ?? null
    }
  }
  return { stepsCompleted, currentStepName }
}

/**
 * Attempt-centric label for a patch's iteration (Phase 14, item 1 — the
 * attempt-grouped Patches surface). Re-keys the raw "Iter N" column onto the
 * coverage/surgical attempt vocabulary by looking the iteration up in the
 * iteration rows (which carry ``attempt_mode``/``attempt_no``) and reusing the
 * shared {@link attemptColumnLabel} relabel. Falls back to "Baseline" for
 * iteration 0 and "Iteration N" when no matching row is present (legacy runs).
 */
export function patchAttemptLabel(
  iteration: number | null | undefined,
  iterations: GSOIterationResult[] | null | undefined,
): string {
  if (iteration == null) return "—"
  const row = iterations?.find((it) => it.iteration === iteration)
  if (row) return attemptColumnLabel(row)
  return iteration === 0 ? "Baseline" : `Iteration ${iteration}`
}

/**
 * Attempt-centric labels for the OptimizationLevers provenance sub-labels
 * (Phase 14, item 1 — attempt-grouped levers). Builds a ``Map<iterationNo,
 * label>`` where each label re-keys the raw "Iteration N" onto the coverage/
 * surgical attempt vocabulary (reusing {@link patchAttemptLabel}) and appends
 * the per-attempt decision (e.g. "Surgical 2 · Accepted", "Coverage · Rolled
 * back"), read from the merged ``attempt_mode``/``attempt_no``/``decision``/
 * ``rolled_back`` iteration columns.
 *
 * When the run carries NO attempt metadata (legacy 6-step runs) the map is
 * EMPTY, so callers degrade to the existing "Iteration N" labels — nothing to
 * override.
 */
export function buildLeverIterationLabels(
  iterations: GSOIterationResult[] | null | undefined,
): Map<number, string> {
  const labels = new Map<number, string>()
  const rows = iterations ?? []
  const hasAttemptInfo = rows.some(
    (it) => it.iteration > 0 && (it.attempt_mode != null || it.attempt_no != null),
  )
  if (!hasAttemptInfo) return labels
  for (const it of rows) {
    const base = patchAttemptLabel(it.iteration, rows)
    const dec = decisionLabel(it.decision, it.rolled_back === true)
    labels.set(it.iteration, dec !== "—" ? `${base} · ${dec}` : base)
  }
  return labels
}
