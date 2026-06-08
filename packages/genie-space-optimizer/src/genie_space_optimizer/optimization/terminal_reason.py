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

    FALLBACK_NO_NEW_STRATEGY = "fallback_no_new_strategy"
    """Trial 19 A6 — the AG regenerator (or strategist fallback path)
    exhausted every candidate against the union of forbidden +
    insufficient_repair_signatures sets. Emitted by
    ``regenerate_action_groups_with_signatures`` (action_groups.py)
    when every candidate it produced was filtered out, so the
    iteration stops with an informative reason rather than burning
    budget on ``ag_collision_with_forbidden_set`` for a candidate
    we already know is dead."""

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
    BUNDLE_PARTIAL_APPLY = "bundle_partial_apply"
    """Phase 2 P2.3 — at least one but not all patches in a
    ``bundle_id`` applied successfully. The atomic-apply transformer
    rolls back any partial state and terminates the whole bundle so
    that downstream survivor-selection treats the bundle as a single
    unit (either all-or-none). Distinct from
    ``ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER`` which fires when zero
    patches landed."""

    ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER = (
        "all_selected_patches_dropped_by_applier"
    )
    """Patches passed gates but the Databricks API rejected every
    one at apply time."""

    NO_APPLIED_PATCHES = "no_applied_patches"
    """Mirrors ReasonCode.NO_APPLIED_PATCHES — catch-all for
    zero-applied-patches iterations when no more specific reason
    above applies."""

    # ── Trial 22 W4 — actuator-aware iteration terminals ──────────
    SLATE_COMPILER_EMPTY = "slate_compiler_empty"
    """Trial 22 W4 — Stage 3 returned >= 1 proposal but the slate
    compiler dropped every one. Stays closed-vocab; the root-cause
    DropReason lives in :class:`IterationTerminalVerdict.top_drop_reason`
    and ``drop_reason_counts``, NOT in a colon-suffixed enum value.
    Replaces the catch-all ``NO_APPLIED_PATCHES`` whenever Stage 3
    actually returned proposals — see Trial 21 postmortem (every
    proposal dropped as ``bundle_invariant_violated``) for the
    motivation."""

    STAGE3_RETURNED_NONE = "stage3_returned_none"
    """Trial 22 W4 — Stage 3 returned zero proposals. Distinct from
    :attr:`PROPOSAL_GENERATION_EMPTY` (which carries the historical
    semantic of "strategist emitted directives but proposal-gen
    returned zero") in that this value pins the actuator-aware
    "Stage 3 declined to emit anything" branch of
    :func:`compute_iteration_terminal_reason`."""

    APPLIER_NO_OUTCOMES = "applier_no_outcomes"
    """Trial 22 W4 — proposals survived the compiler but the applier
    produced zero outcomes (no apply markers). Distinct from
    :attr:`ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER` (every patch
    rejected by Databricks API) in that this value pins the case
    where the applier was never invoked or never returned outcomes
    at all."""

    KEPT_INSUFFICIENT = "kept_insufficient"
    """Trial 20 B1 — patches applied AND survived the eval gate as
    behaviour-unchanged-but-harmless. The state-machine
    ``acceptance_gate`` transformer records
    ``AcceptanceDecisionRecord.decision == "kept_insufficient"`` for
    these candidates (Trial 18 contract). Trial 20 B2 promotes that
    inner-loop decision to a typed outer-loop iteration terminal:
    if any QID's SM final state recorded ``kept_insufficient`` for
    the iteration, the iteration MUST emit ``KEPT_INSUFFICIENT``
    here instead of the catch-all ``NO_APPLIED_PATCHES``. Trial 20
    B3 makes Plan 12 treat this terminal as a survival failure
    requiring a pivot (``_TERMINATIONS_REQUIRING_PIVOT``)."""

    # ── Post-eval terminations ─────────────────────────────────────
    TARGET_QIDS_NOT_IMPROVED = "target_qids_not_improved"
    """Patches applied AND aggregate gained, but the named target
    qid stayed hard (attribution drift case). Distinct from
    rollback: this iteration MAY still be accepted under
    AcceptanceTier.ACCEPT_WITH_DEBT or
    AcceptanceTier.ACCEPT_WITH_ATTRIBUTION_DRIFT (Plan B)."""

    AGGREGATE_GAIN_TARGET_DEBT = "aggregate_gain_target_debt"
    """P4 C7 — aggregate accuracy gained AND some-but-not-all target
    QIDs were fixed on the accepted iteration. The harness loop-
    control treats this as a *continue* signal while budget remains:
    the target set is not yet drained. Mirrors the new
    ``OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT`` RunOutcome. e943's
    canonical case — aggregate +8.2pp accepted, target gs_009 still
    hard."""

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

    # ── Infrastructure / post-apply-rollback terminations ──────────
    # Trial 30 W30.4(b) — three paths in the harness no-candidate body
    # previously emitted the raw ``"unknown"`` string because no
    # structural-funnel reason covered them. They are real, generalizable
    # failure modes (not per-QID/anchor), so they get typed members; the
    # gso-postmortem skill flagged the raw "unknown" as a terminal-reason
    # taxonomy gap that forced ``architecture_invariants_held=false``.
    INFRASTRUCTURE_PRE_AG_SNAPSHOT_FAILED = (
        "infrastructure_pre_ag_snapshot_failed"
    )
    """Pre-AG metadata snapshot capture failed, so the AG never reached
    the applier. An infrastructure failure, not a strategy failure —
    the AG signature should not be treated as a dead strategy."""

    INFRASTRUCTURE_APPLIER_FAILED = "infrastructure_applier_failed"
    """The applier / Genie API rejected the PATCH payload at apply time
    (SCHEMA_FAILURE or INFRA_FAILURE). Distinct from
    :attr:`ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER` (every patch
    individually rejected by the gate path) in that this pins the
    deploy-call infrastructure failure."""

    SLICE_OR_P0_GATE_REGRESSION_ROLLBACK = (
        "slice_or_p0_gate_regression_rollback"
    )
    """Patches applied then rolled back by a pre-full-eval gate
    (slice_gate / p0_gate) because the candidate regressed at that
    boundary. Distinct from :attr:`CONTENT_REGRESSION_ROLLBACK` (a
    full-eval out-of-target regression) — this pins the pre-full-eval
    slice/p0 rejection that no prior closed-vocab value covered."""

    # ── Defensive ──────────────────────────────────────────────────
    UNKNOWN = "unknown"
    """Reserved. Producers SHOULD NOT emit this; consumers MAY see
    it during partial rollouts of new producers."""
