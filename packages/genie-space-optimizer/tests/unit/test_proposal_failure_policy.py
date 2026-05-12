"""Plan P-F — pure policy unit tests.

The policy maps (failure_mode, ProposalFailureContext) to one of six
closed-vocabulary next-action labels. Every supported failure mode
has at least one branch test plus a fallback test for unknown modes.
"""

from __future__ import annotations


def test_decision_record_has_expected_fields() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureDecision,
        ProposalFailureNextAction,
    )

    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG_TEST",
        cluster_id="C1",
        cluster_signature="sig:abc123",
        rca_id="rca_1",
        root_cause="missing_top_n_filter",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
    )
    decision = ProposalFailureDecision(
        next_action=ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING,
        rationale="rca_card_grounded=False",
    )
    assert decision.next_action.value == "request_evidence_gathering"
    assert decision.rationale == "rca_card_grounded=False"
    assert ctx.failure_mode == "proposal_generation_empty"


def test_proposal_generation_empty_with_ungrounded_rca_requests_evidence() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG_1",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="",
        root_cause="",
        lever_set=(1, 3, 5),
        tried_lever_families=(),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert (
        decision.next_action
        == ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING
    )


def test_proposal_generation_empty_with_grounded_rca_rotates_lever() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG_2",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="rca_1",
        root_cause="missing_filter",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert decision.next_action == ProposalFailureNextAction.ROTATE_LEVER_FAMILY


def test_lever6_force_llm_declined_with_no_archetype_marks_evidence_gap() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="lever6_force_llm_declined",
        ag_id="AG_3",
        cluster_id="C2",
        cluster_signature="sig:def",
        rca_id="rca_2",
        root_cause="sql_shape_top_n",
        lever_set=(5, 6),
        tried_lever_families=(5, 6),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert decision.next_action == ProposalFailureNextAction.MARK_EVIDENCE_GAP


def test_all_selected_dropped_with_multi_cluster_ag_narrows_scope() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="all_selected_patches_dropped_by_applier",
        ag_id="AG_4",
        cluster_id="C3",
        cluster_signature="sig:ghi",
        rca_id="rca_3",
        root_cause="anything",
        lever_set=(1, 5),
        tried_lever_families=(1,),
        ag_source_cluster_count=3,
        rca_card_grounded=True,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert decision.next_action == ProposalFailureNextAction.NARROW_AG_SCOPE


def test_no_causal_applyable_patch_with_exhausted_levers_escalates() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="no_causal_applyable_patch",
        ag_id="AG_5",
        cluster_id="C4",
        cluster_signature="sig:jkl",
        rca_id="rca_4",
        root_cause="anything",
        lever_set=(1, 3, 5, 6),
        tried_lever_families=(1, 3, 5, 6),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert (
        decision.next_action
        == ProposalFailureNextAction.ESCALATE_UNSUPPORTED_REPAIR_SHAPE
    )


def test_repeated_failure_blocks_ag_retry_by_signature() -> None:
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="no_applied_patches",
        ag_id="AG_6",
        cluster_id="C5",
        cluster_signature="sig:mno",
        rca_id="rca_5",
        root_cause="anything",
        lever_set=(1, 5),
        tried_lever_families=(1, 5),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=2,
    )

    decision = decide_next_action(ctx)
    assert (
        decision.next_action
        == ProposalFailureNextAction.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE
    )


def test_unknown_failure_mode_falls_back_to_request_evidence() -> None:
    """Defensive: any failure mode not in the closed list defaults to
    request_evidence_gathering so the loop never silently stalls."""
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureNextAction,
        decide_next_action,
    )

    ctx = ProposalFailureContext(
        failure_mode="totally_unknown_mode",
        ag_id="AG_7",
        cluster_id="C6",
        cluster_signature="sig:pqr",
        rca_id="rca_6",
        root_cause="?",
        lever_set=(1,),
        tried_lever_families=(),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
    )

    decision = decide_next_action(ctx)
    assert (
        decision.next_action
        == ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING
    )
