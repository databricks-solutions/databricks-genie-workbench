"""Failure-bucketing seed catalog — Track 7 (Phase C pre-work).

Phase C's classifier consumes ``SEED_CATALOG`` to bucket loop
failures into one of four top-level categories:

  * ``GATE_OR_CAP_GAP`` — proposals existed but were lost at gate / cap.
  * ``EVIDENCE_GAP`` — judge / RCA evidence ran out for the qid.
  * ``PROPOSAL_GAP`` — no proposal was generated for the failure.
  * ``MODEL_CEILING`` — the strategist / model could not produce a
    canonical fix even with full evidence.

Each entry is one observed pattern from the May-01 ESR / 7Now / 23:04
runs plus the RCA roadmap. Phase C will extend the catalog as new
patterns surface; the contract is stable enums + immutable
dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FailureBucket(Enum):
    GATE_OR_CAP_GAP = "GATE_OR_CAP_GAP"
    EVIDENCE_GAP = "EVIDENCE_GAP"
    PROPOSAL_GAP = "PROPOSAL_GAP"
    MODEL_CEILING = "MODEL_CEILING"
    # Phase D Failure-Bucketing T2: three new buckets covering the
    # remaining links in the RCA invariant chain (see roadmap.md
    # lines 50-53). These complement the four cycle-9 dominant-signal
    # labels, which were always meant to be a subset of the full
    # bucket set.
    RCA_GAP = "RCA_GAP"
    TARGETING_GAP = "TARGETING_GAP"
    APPLY_OR_ROLLBACK_GAP = "APPLY_OR_ROLLBACK_GAP"


@dataclass(frozen=True)
class BucketingSeedPattern:
    """One observed failure pattern.

    Attributes:
        pattern_id: Stable, lowercase, snake_case handle for the
            pattern. Phase C's classifier references patterns by id.
        description: Short prose description of the pattern, suitable
            for an operator banner.
        bucket: Top-level failure bucket.
        sub_bucket: Phase C sub-classification (e.g.,
            ``identity_collision``, ``family_crowding``).
        source_run: Which observed run surfaced this pattern.
        why: One-sentence operator-facing rationale.
    """

    pattern_id: str
    description: str
    bucket: FailureBucket
    sub_bucket: str
    source_run: str
    why: str


SEED_CATALOG: list[BucketingSeedPattern] = [
    BucketingSeedPattern(
        pattern_id="targeted_l6_dropped_at_cap_due_to_identity_collision",
        description=(
            "Targeted lever-6 patch dropped at cap due to "
            "expanded_patch_id collision with split-child"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="identity_collision",
        source_run="ESR",
        why="Direct fix existed and was silently lost.",
    ),
    BucketingSeedPattern(
        pattern_id="ag_level_rewrite_split_children_without_targeted_patch",
        description=(
            "AG-level broad rewrite_instruction split-children applied "
            "without targeted patch in bundle"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="family_crowding",
        source_run="ESR",
        why="Family budget did not preserve direct-fix slot.",
    ),
    BucketingSeedPattern(
        pattern_id="diagnostic_ag_reused_after_rollback_with_same_proposals",
        description=(
            "Diagnostic / coverage AG reused after rollback with same "
            "proposals"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="stale_buffered_ag",
        source_run="ESR",
        why="Reflection validator did not catch buffered reuse.",
    ),
    BucketingSeedPattern(
        pattern_id="execution_error_on_passing_question_after_broad_rewrite",
        description=(
            "Genie-side execution error on a previously passing "
            "question after broad rewrite"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="regression_debt_after_broadcast",
        source_run="ESR",
        why="Broadcast patch produced syntactically invalid SQL elsewhere.",
    ),
    BucketingSeedPattern(
        pattern_id="net_positive_zero_regression_target_unchanged_rolled_back",
        description=(
            "Net-positive candidate, zero regressions, target "
            "unchanged, rolled back"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="accept_attribution_drift",
        source_run="7Now",
        why=(
            "Acceptance predicate rejected before considering "
            "net+zero-regression."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="plateau_termination_while_pending_diagnostic_ag",
        description=(
            "Plateau termination while non-quarantined hard cluster "
            "has a queued diagnostic AG"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="false_plateau_after_rollback",
        source_run="7Now",
        why="Plateau detector ignored pending AG state.",
    ),
    BucketingSeedPattern(
        pattern_id="buffered_ag_ran_against_qid_no_longer_hard",
        description=(
            "Buffered AG ran against a qid that is no longer hard on "
            "the current iteration"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="buffered_ag_signature_drift",
        source_run="23:04",
        why="Cluster IDs (H00N) re-numbered; AG target stale.",
    ),
    BucketingSeedPattern(
        pattern_id="only_remaining_hard_qid_quarantined",
        description="The only remaining hard qid was quarantined",
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="quarantine_singleton_hard_set",
        source_run="23:04",
        why="Quarantine has no singleton-hard floor.",
    ),
    BucketingSeedPattern(
        pattern_id="passing_qid_in_convergence_quarantine",
        description="Passing qid appears in convergence quarantine",
        bucket=FailureBucket.EVIDENCE_GAP,
        sub_bucket="quarantine_attribution_drift",
        source_run="7Now",
        why="Quarantine producer fed bad data.",
    ),
    BucketingSeedPattern(
        pattern_id="just_fixed_target_qid_in_soft_cluster",
        description="Just-fixed target qid appears in soft cluster",
        bucket=FailureBucket.EVIDENCE_GAP,
        sub_bucket="soft_cluster_currency_drift",
        source_run="23:04",
        why="Soft-clustering reads stale ASI.",
    ),
    BucketingSeedPattern(
        pattern_id="eval_row_missing_persisted_trace_id_recovered_by_fallback",
        description=(
            "Eval row missing persisted trace id, recovered by fallback"
        ),
        bucket=FailureBucket.EVIDENCE_GAP,
        sub_bucket="trace_id_fallback_required",
        source_run="7Now / 23:04",
        why="Trace context lost during Genie call.",
    ),
    BucketingSeedPattern(
        pattern_id="hard_qid_with_no_direct_proposal_generated",
        description="Hard qid with no direct proposal generated",
        bucket=FailureBucket.PROPOSAL_GAP,
        sub_bucket="cluster_driven_synthesis_missing",
        source_run="RCA-roadmap",
        why="No patch exists after proposal generation.",
    ),
    BucketingSeedPattern(
        pattern_id="plural_top_n_collapse_no_canonical_template_after_two_iterations",
        description=(
            "Plural-top-N collapse never received a strategist-"
            "generated direct fix despite being hard for two iterations"
        ),
        bucket=FailureBucket.MODEL_CEILING,
        sub_bucket="top_n_template_missing",
        source_run="7Now / 23:04",
        why=(
            "Strategist did not synthesize the canonical LIMIT N / "
            "ROW_NUMBER template."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="kpi_shape_template_missing_for_day_or_mtd_metric",
        description=(
            "Hard qid (gs_005) needs a canonical Day/MTD KPI shape, "
            "not generic metadata"
        ),
        bucket=FailureBucket.MODEL_CEILING,
        sub_bucket="kpi_shape_template_missing",
        source_run="ESR",
        why=(
            "Lever-1 description is too weak for an operational "
            "column-mapping rule."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="qid_receives_targeted_applied_patch_and_still_fails_without_regression",
        description=(
            "QID receives targeted applied patch and still fails "
            "without regression"
        ),
        bucket=FailureBucket.MODEL_CEILING,
        sub_bucket="genuine_ceiling",
        source_run="RCA-roadmap",
        why=(
            "Use only after proposal/gate/cap/apply path is complete."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="hard_cluster_with_proposals_but_all_dropped_at_grounding",
        description=(
            "Hard cluster received proposals but all were dropped at "
            "grounding (no relevance-score survivors)"
        ),
        bucket=FailureBucket.PROPOSAL_GAP,
        sub_bucket="grounding_relevance_floor",
        source_run="ESR",
        why=(
            "Proposals named the wrong assets; relevance check "
            "rejected all of them."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="dead_on_arrival_blocks_buffered_drain",
        description=(
            "AG fails dead-on-arrival or applier-rejection and the "
            "lever loop unconditionally clears pending_action_groups, "
            "discarding buffered AGs targeting other clusters"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="buffered_ag_unrelated_drop",
        source_run="cycle9",
        why=(
            "Selective drain not yet wired; one failed AG took down "
            "two unrelated buffered AGs."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="blast_radius_no_escape_hatch",
        description=(
            "Blast-radius gate dropped every candidate patch; the "
            "strategist re-proposed the same shape on the next "
            "iteration with no constraint on the dropped table"
        ),
        bucket=FailureBucket.GATE_OR_CAP_GAP,
        sub_bucket="blast_radius_dead_end",
        source_run="cycle9",
        why=(
            "No forbid_tables feedback loop into the strategist; "
            "loop spent 5 iterations producing the same dropped "
            "patch shape."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="proposal_direction_inversion",
        description=(
            "Strategist proposal value directly contradicts the "
            "dominant cluster counterfactual (e.g. ADD filter X when "
            "the diagnosis says REMOVE filter X)"
        ),
        bucket=FailureBucket.PROPOSAL_GAP,
        sub_bucket="counterfactual_contradiction",
        source_run="cycle9",
        why=(
            "Proposal grounding does not validate that proposal value "
            "agrees with cluster counterfactual_fix direction."
        ),
    ),
    BucketingSeedPattern(
        pattern_id="union_all_grain_split",
        description=(
            "Generated SQL composed two heterogeneous report shapes "
            "via UNION ALL with NULL filler columns instead of one "
            "result set at the question's expected grain"
        ),
        bucket=FailureBucket.MODEL_CEILING,
        sub_bucket="union_all_grain_split",
        source_run="cycle9",
        why=(
            "Strategist cannot synthesize a single canonical grain; "
            "no template asset exists for compound monthly + pattern "
            "breakdowns."
        ),
    ),
]


def match_pattern_id(pattern_id: str) -> Optional[BucketingSeedPattern]:
    """Return the seed pattern with the given id, or None if unknown."""
    pid = str(pattern_id or "").strip()
    if not pid:
        return None
    for entry in SEED_CATALOG:
        if entry.pattern_id == pid:
            return entry
    return None
