/**
 * Semantic Blueprint (v4) — Join Advisor, the validated-seed model (§7).
 *
 * A checked candidate is NEVER written as a declared `join_spec`: the
 * Auto-Optimize loop can `add_join_spec`/`update_join_spec` but its allowlist
 * (`unified_loop.py:_ALLOWED_PATCH_TYPES`) drops `remove_join_spec`, so a
 * declared join is effectively locked and a wrong one can't be undone. Instead a
 * selection persists as a **proposed seed** the optimizer re-validates and adds
 * itself. This module is the pure logic — candidate shape, containment-probe
 * verdict, and the weak-probe confirm gate; the overlay edge is drawn by the
 * canvas as `proposed_join` (never a base edge, §2).
 */

/**
 * A data-grounded join candidate, `MvProposal`-shaped so the existing run-seed
 * path can carry it forward. `probe` is the warehouse containment ratio (is
 * `from.fromCol ⊆ to.toCol`?) in [0,1], or `null` when no warehouse could probe
 * it (honest-empty, never a silent 0). `match` records the grounding evidence.
 *
 * Defined canonically in `@/types` (mirrors backend/models.py JoinCandidate);
 * re-exported here so the blueprint modules and their tests keep importing it
 * from one place.
 */
import type { JoinCandidate } from "@/types"
export type { JoinCandidate }

export interface ProbeVerdict {
  level: "validated" | "partial" | "unverified"
  label: string
  /** Percent for the bar (0-100), or null when unverified. */
  pct: number | null
  color: string
}

/** Weak containment (`< 50%`) → the seed action must confirm first (§7 guardrails). */
export const WEAK_PROBE = 0.5

export function verdict(probe: number | null): ProbeVerdict {
  if (probe == null) return { level: "unverified", label: "unverified · no warehouse probe", pct: null, color: "var(--color-danger)" }
  const pct = Math.round(probe * 100)
  if (probe >= 0.9) return { level: "validated", label: `validated · ${pct}% row containment`, pct, color: "var(--color-success)" }
  if (probe >= WEAK_PROBE) return { level: "partial", label: `partial · ${pct}% row containment`, pct, color: "var(--color-warning)" }
  return { level: "unverified", label: `unverified · ${pct}% row containment`, pct, color: "var(--color-danger)" }
}

/** True when turning on the candidate must trip the confirm gate (§7). */
export function isWeak(probe: number | null): boolean {
  return probe == null || probe < WEAK_PROBE
}

/** The pending seed set as the run-input shape handed to Auto-Optimize (§7). */
export function seedPayload(candidates: JoinCandidate[], checked: Set<string>): JoinCandidate[] {
  return candidates.filter((c) => checked.has(c.id))
}
