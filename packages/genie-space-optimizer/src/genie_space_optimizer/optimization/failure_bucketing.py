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


# ---------------------------------------------------------------------------
# Phase D Failure-Bucketing T3: classify_unresolved_qid
# ---------------------------------------------------------------------------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord,
        OptimizationTrace,
    )


@dataclass(frozen=True)
class ClassificationResult:
    """Output of ``classify_unresolved_qid``.

    Attributes:
        bucket: The bucket label, or ``None`` for a currently-passing qid.
        reason: Operator-readable next-action prose, ≤ 120 chars.
        earliest_broken_link: Stable identifier for the RCA-chain link
            that broke. One of: ``evidence_to_rca``, ``rca_to_ag``,
            ``ag_to_proposal``, ``proposal_to_target_qids``,
            ``target_qids_to_applied``, ``applied_to_observed``,
            ``observed_to_next_action``, ``none`` (sentinel for passing).
        evidence_record_ids: Tuple of indices into ``trace.decision_records``
            that justified the bucket. Allows the operator transcript
            to render the supporting records by reference.
    """

    bucket: "FailureBucket | None"
    reason: str
    earliest_broken_link: str
    evidence_record_ids: tuple[int, ...] = ()


# Verbatim reason strings per bucket. Operator-facing; keep ≤ 120 chars.
_REASONS: dict[str, str] = {
    "EVIDENCE_GAP": (
        "QID has no eval row or was not clustered. Re-run RCA judges or "
        "promote the qid to benchmark review."
    ),
    "RCA_GAP": (
        "QID's cluster has no grounded RCA finding. Re-run RCA prompt with "
        "broader evidence, or quarantine the cluster."
    ),
    "PROPOSAL_GAP": (
        "RCA exists but no AG or proposal targets the QID. Force strategist "
        "to emit an AG for the cluster's root cause."
    ),
    "TARGETING_GAP": (
        "Proposals exist for the AG but none claim the QID in target_qids. "
        "Stamp target_qids on the proposal site (cycle-8 Bug 1 shape)."
    ),
    "GATE_OR_CAP_GAP": (
        "All proposals for the QID dropped at gates (blast_radius / lever5 / "
        "cap / groundedness). Tighten the gate's escape hatch or rotate lever."
    ),
    "APPLY_OR_ROLLBACK_GAP": (
        "Patch applied but iteration regressed or was rolled back. "
        "Investigate the regression source or split the AG."
    ),
    "MODEL_CEILING": (
        "Patch landed and held but QID still fails. Likely a model ceiling; "
        "consider escalation, schema change, or benchmark review."
    ),
    "PASSING": "qid is currently passing; no bucket required.",
}


_PASS_REASON_CODES: frozenset[str] = frozenset({
    "post_eval_hold_pass",
    "post_eval_fail_to_pass",
})


_HOLD_FAIL_REASON: str = "post_eval_hold_fail"
_PASS_TO_FAIL_REASON: str = "post_eval_pass_to_fail"


def _records_for_iteration(
    trace: "OptimizationTrace", iteration: int,
) -> list["DecisionRecord"]:
    return [
        r for r in trace.decision_records
        if int(r.iteration) == int(iteration)
    ]


def _qid_in_record(qid: str, rec: "DecisionRecord") -> bool:
    if rec.question_id == qid:
        return True
    if qid in (rec.affected_qids or ()):
        return True
    if qid in (rec.target_qids or ()):
        return True
    return False


def classify_unresolved_qid(
    trace: "OptimizationTrace",
    qid: str,
    *,
    iteration: int,
) -> ClassificationResult:
    """Classify an unresolved qid against the RCA invariant chain.

    Walks seven rungs from earliest broken link to latest. The earliest
    rung that matches picks the bucket. Returns the sentinel result with
    ``bucket=None`` when the qid is currently passing.

    The classifier is pure — no side effects, no logging, no MLflow.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
    )

    iter_records = _records_for_iteration(trace, iteration)

    # Sentinel: qid is currently passing → no classification.
    qid_resolution = [
        r for r in iter_records
        if r.decision_type == DecisionType.QID_RESOLUTION
        and r.question_id == qid
    ]
    if qid_resolution and any(
        r.outcome == DecisionOutcome.RESOLVED
        or r.reason_code.value in _PASS_REASON_CODES
        for r in qid_resolution
    ):
        return ClassificationResult(
            bucket=None,
            reason=_REASONS["PASSING"],
            earliest_broken_link="none",
        )

    # Pre-bucket the records once for cheap lookups.
    eval_records = [
        r for r in iter_records
        if r.decision_type == DecisionType.EVAL_CLASSIFIED
        and _qid_in_record(qid, r)
    ]
    cluster_records = [
        r for r in iter_records
        if r.decision_type == DecisionType.CLUSTER_SELECTED
        and _qid_in_record(qid, r)
    ]

    # Rung 1 — EVIDENCE_GAP.
    if not eval_records or not cluster_records:
        ev_ids = tuple(_record_indices(trace, eval_records + cluster_records))
        return ClassificationResult(
            bucket=FailureBucket.EVIDENCE_GAP,
            reason=_REASONS["EVIDENCE_GAP"],
            earliest_broken_link="evidence_to_rca",
            evidence_record_ids=ev_ids,
        )

    cluster_ids = {r.cluster_id for r in cluster_records if r.cluster_id}
    rca_records = [
        r for r in iter_records
        if r.decision_type == DecisionType.RCA_FORMED
        and r.cluster_id in cluster_ids
    ]
    grounded_rca = [r for r in rca_records if r.rca_id]

    # Rung 2 — RCA_GAP.
    if not grounded_rca:
        ev_ids = tuple(_record_indices(trace, cluster_records + rca_records))
        return ClassificationResult(
            bucket=FailureBucket.RCA_GAP,
            reason=_REASONS["RCA_GAP"],
            earliest_broken_link="rca_to_ag",
            evidence_record_ids=ev_ids,
        )

    ag_records = [
        r for r in iter_records
        if r.decision_type == DecisionType.STRATEGIST_AG_EMITTED
        and _qid_in_record(qid, r)
    ]
    ag_ids = {r.ag_id for r in ag_records if r.ag_id}

    # Rung 3 — PROPOSAL_GAP.
    if not ag_ids:
        ev_ids = tuple(_record_indices(trace, ag_records))
        return ClassificationResult(
            bucket=FailureBucket.PROPOSAL_GAP,
            reason=_REASONS["PROPOSAL_GAP"],
            earliest_broken_link="ag_to_proposal",
            evidence_record_ids=ev_ids,
        )

    proposals_for_ags = [
        r for r in iter_records
        if r.decision_type == DecisionType.PROPOSAL_GENERATED
        and r.ag_id in ag_ids
    ]
    if not proposals_for_ags:
        ev_ids = tuple(_record_indices(trace, ag_records))
        return ClassificationResult(
            bucket=FailureBucket.PROPOSAL_GAP,
            reason=_REASONS["PROPOSAL_GAP"],
            earliest_broken_link="ag_to_proposal",
            evidence_record_ids=ev_ids,
        )

    proposals_targeting_qid = [
        r for r in proposals_for_ags
        if qid in (r.target_qids or ())
    ]

    # Rung 4 — TARGETING_GAP.
    if not proposals_targeting_qid:
        ev_ids = tuple(_record_indices(trace, proposals_for_ags))
        return ClassificationResult(
            bucket=FailureBucket.TARGETING_GAP,
            reason=_REASONS["TARGETING_GAP"],
            earliest_broken_link="proposal_to_target_qids",
            evidence_record_ids=ev_ids,
        )

    proposal_ids_targeting_qid = {
        r.proposal_id for r in proposals_targeting_qid if r.proposal_id
    }
    applied_records = [
        r for r in iter_records
        if r.decision_type == DecisionType.PATCH_APPLIED
        and r.proposal_id in proposal_ids_targeting_qid
    ]

    # Rung 5 — GATE_OR_CAP_GAP.
    if not applied_records:
        ev_ids = tuple(_record_indices(
            trace,
            proposals_targeting_qid + [
                r for r in iter_records
                if r.decision_type == DecisionType.GATE_DECISION
                and r.proposal_id in proposal_ids_targeting_qid
            ],
        ))
        return ClassificationResult(
            bucket=FailureBucket.GATE_OR_CAP_GAP,
            reason=_REASONS["GATE_OR_CAP_GAP"],
            earliest_broken_link="target_qids_to_applied",
            evidence_record_ids=ev_ids,
        )

    # Rung 6 — APPLY_OR_ROLLBACK_GAP.
    qid_pass_to_fail = any(
        r.outcome == DecisionOutcome.UNRESOLVED
        and r.reason_code.value == _PASS_TO_FAIL_REASON
        for r in qid_resolution
    )
    ag_rolled_back = any(
        r.decision_type == DecisionType.ACCEPTANCE_DECIDED
        and r.outcome == DecisionOutcome.ROLLED_BACK
        and r.ag_id in ag_ids
        for r in iter_records
    )
    if qid_pass_to_fail or ag_rolled_back:
        ev_ids = tuple(_record_indices(
            trace, applied_records + qid_resolution,
        ))
        return ClassificationResult(
            bucket=FailureBucket.APPLY_OR_ROLLBACK_GAP,
            reason=_REASONS["APPLY_OR_ROLLBACK_GAP"],
            earliest_broken_link="applied_to_observed",
            evidence_record_ids=ev_ids,
        )

    # Rung 7 — MODEL_CEILING (default for unresolved-but-applied qids).
    ev_ids = tuple(_record_indices(trace, applied_records + qid_resolution))
    return ClassificationResult(
        bucket=FailureBucket.MODEL_CEILING,
        reason=_REASONS["MODEL_CEILING"],
        earliest_broken_link="observed_to_next_action",
        evidence_record_ids=ev_ids,
    )


def _record_indices(
    trace: "OptimizationTrace",
    records: list["DecisionRecord"],
) -> list[int]:
    """Look up indices of the given records inside ``trace.decision_records``.

    Uses identity (``is``) so a record that appears twice in ``records`` is
    deduplicated by its trace position.
    """
    indices: list[int] = []
    for idx, candidate in enumerate(trace.decision_records):
        if any(candidate is r for r in records):
            indices.append(idx)
    return indices
