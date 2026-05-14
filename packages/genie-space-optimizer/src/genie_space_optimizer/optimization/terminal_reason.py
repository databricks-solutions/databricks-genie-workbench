"""Phase 1.2 — closed-vocabulary terminal outcome enum.

Defined by ``docs/final_plan/2026-05-13-final-closeout-contract-spec.md``
Section 3.2. Every iteration of the lever loop that does NOT emit a
kept candidate MUST record exactly one ``TerminalReason`` in its
DecisionRecord stream (``DecisionType.ITERATION_TERMINAL_DECIDED``).

For accepted / rolled-back outcomes, use ``AcceptanceTier`` from
``optimization/acceptance_tier.py`` (owned by Plan B). The two
vocabularies are disjoint and complementary.

Consumers:
  * Phase 0.3 stdout markers (``GSO_ITERATION_NO_CANDIDATE_V1.terminal_reason``)
  * Phase 0.4 candidate ledger (``IterationCandidateLedgerEntry.terminal_reason``)
  * Phase 1.3 retry memory (``TerminalSignature.terminal_reason``)
  * Phase 2.x rejection paths (every gate maps to one value)
"""
from __future__ import annotations

from enum import StrEnum


class TerminalReason(StrEnum):
    """Closed vocabulary for iteration-terminal causes (no-candidate
    paths only). Producers MUST pick the *most specific* applicable
    value; ordering top-to-bottom in the enum definition determines
    precedence when multiple match.
    """

    # ── Pre-proposal terminations ──────────────────────────────────
    NO_RCA_GROUND = "no_rca_ground"
    """The selected cluster's RCA card is absent or ungrounded
    (matches the RcaUngroundedReason taxonomy)."""

    NO_ACTION_GROUP_EMITTED = "no_action_group_emitted"
    """The strategist returned zero AGs after the RCA stage."""

    AG_COLLISION_WITH_FORBIDDEN_SET = "ag_collision_with_forbidden_set"
    """The selected AG's TerminalSignature is already in the
    forbidden set from a prior iteration."""

    # ── Proposal-stage terminations ────────────────────────────────
    NO_STRUCTURAL_CANDIDATE = "no_structural_candidate"
    """Mirrors ReasonCode.NO_STRUCTURAL_CANDIDATE — strategist asked
    for L5/L6 structural lever but no proposal of structural shape
    was generated."""

    PROPOSAL_GENERATION_EMPTY = "proposal_generation_empty"
    """Mirrors ReasonCode.PROPOSAL_GENERATION_EMPTY — strategist
    emitted directives but proposal-generation LLM returned zero
    proposals."""

    STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY = (
        "structural_gate_dropped_instruction_only"
    )
    """Mirrors ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY —
    proposals were generated but all were instruction-only (no
    L5/L6 structural shape) and dropped by the structural-repair
    gate (Task 14)."""

    # ── Gate-rejection terminations ────────────────────────────────
    APPLYABILITY_REJECTED = "applyability_rejected"
    """Proposals passed structural shape but the applyability gate
    rejected every patch (validation_status=invalid)."""

    BLAST_RADIUS_REJECTED = "blast_radius_rejected"
    """Proposals passed applyability but blast-radius dropped every
    patch and narrow-replacement (Task 15) could not synthesize a
    survivor."""

    COLLATERAL_RISK_REJECTED = "collateral_risk_rejected"
    """All survivors carry collateral_risk above policy threshold
    against the AG's target qid set."""

    # ── Apply / eval terminations ──────────────────────────────────
    ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER = (
        "all_selected_patches_dropped_by_applier"
    )
    """Patches passed gates but the Databricks API rejected every
    one at apply time."""

    NO_APPLIED_PATCHES = "no_applied_patches"
    """Mirrors ReasonCode.NO_APPLIED_PATCHES — catch-all for
    zero-applied-patches iterations when no more specific reason
    above applies."""

    # ── Post-eval terminations ─────────────────────────────────────
    TARGET_QIDS_NOT_IMPROVED = "target_qids_not_improved"
    """Patches applied AND aggregate gained, but the named target
    qid stayed hard (attribution drift case). Distinct from
    rollback: this iteration MAY still be accepted under
    AcceptanceTier.ACCEPT_WITH_DEBT or
    AcceptanceTier.ACCEPT_WITH_ATTRIBUTION_DRIFT (Plan B)."""

    CONTENT_REGRESSION_ROLLBACK = "content_regression_rollback"
    """Iteration accepted then rolled back because an out-of-target
    QID regressed past policy. Mirrors RollbackClass.CONTENT_REGRESSION
    rollups."""

    MULTI_PATCH_REGRESSION_NO_ISOLATION = (
        "multi_patch_regression_no_isolation"
    )
    """Subset-isolation tried but could not localize the regression
    to a single patch. Mirrors RollbackClass.MULTI_PATCH_REGRESSION."""

    # ── Contract / invariant terminations ──────────────────────────
    DIRECTIVE_OUTCOME_VIOLATION = "directive_outcome_violation"
    """The directive-outcome coverage invariant fired. Iteration is
    force-terminated even if other steps would have proceeded."""

    INVARIANT_VIOLATION = "invariant_violation"
    """A DecisionRecord invariant (I-series) fired in warn-and-degrade
    or strict mode."""

    # ── Defensive ──────────────────────────────────────────────────
    UNKNOWN = "unknown"
    """Reserved. Producers SHOULD NOT emit this; consumers MAY see
    it during partial rollouts of new producers."""
