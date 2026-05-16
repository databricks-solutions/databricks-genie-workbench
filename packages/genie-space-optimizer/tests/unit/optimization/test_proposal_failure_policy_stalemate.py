"""``decide_next_action`` must escalate to ``ESCALATE_STALEMATE`` once
the same iteration-failure signature has fired before in the current
AG. Without this, the loop emits the same failure record every
iteration (the Trial-5 Run A symptom)."""

from __future__ import annotations

from genie_space_optimizer.optimization.proposal_failure_policy import (
    ProposalFailureContext,
    ProposalFailureNextAction,
    decide_next_action,
)


def _ctx(prior_identical: int) -> ProposalFailureContext:
    return ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG1",
        cluster_id="C1",
        cluster_signature="sig-C1",
        rca_id="rca-1",
        root_cause="wrong_aggregation",
        lever_set=(2, 6),
        tried_lever_families=(),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
        prior_identical_failure_count=prior_identical,
    )


def test_first_occurrence_does_not_escalate_stalemate():
    """prior_identical_failure_count=0 means this is the first time
    the signature fired — the policy should fall through to
    REQUEST_EVIDENCE_GATHERING (current behaviour for ungrounded
    empty generation)."""
    decision = decide_next_action(_ctx(prior_identical=0))
    assert decision.next_action == ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING


def test_second_identical_failure_escalates_stalemate():
    """prior_identical_failure_count >= 1 means the same signature
    has fired before; further iterations are guaranteed-loops."""
    decision = decide_next_action(_ctx(prior_identical=1))
    assert decision.next_action == ProposalFailureNextAction.ESCALATE_STALEMATE
    assert "stalemate" in decision.rationale.lower()


def test_stalemate_branch_overrides_request_evidence():
    """Even when other branches would normally fire (e.g.,
    rca_card_grounded=False), stalemate escalation takes precedence
    so the loop can terminate."""
    ctx = _ctx(prior_identical=3)
    decision = decide_next_action(ctx)
    assert decision.next_action == ProposalFailureNextAction.ESCALATE_STALEMATE


def test_block_by_signature_still_wins_over_stalemate():
    """``prior_failure_count >= 2`` is the older, narrower block-by-
    signature branch. It must still win — stalemate is the *catch-all*
    for cases where block-by-signature doesn't fire."""
    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG1", cluster_id="C1", cluster_signature="sig-C1",
        rca_id="rca-1", root_cause="wrong_aggregation",
        lever_set=(2, 6), tried_lever_families=(),
        ag_source_cluster_count=1, rca_card_grounded=False,
        prior_failure_count=5, prior_identical_failure_count=2,
    )
    decision = decide_next_action(ctx)
    assert decision.next_action == ProposalFailureNextAction.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE
