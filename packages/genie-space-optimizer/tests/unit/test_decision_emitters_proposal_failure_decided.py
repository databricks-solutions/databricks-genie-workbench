"""Plan P-F — Proposal Failure Taxonomy + Recovery Policy unit tests.

Covers the new DecisionType / ReasonCode vocabulary and the
proposal_failure_decided_record producer helper.

Evidence anchor:
runid_analysis/{ccf1d60d,31ecd96f}/postmortem.md — neither run
carries a typed next-action label on any proposal-phase failure.
"""

from __future__ import annotations


def test_decision_type_has_proposal_failure_decided() -> None:
    """The new DecisionType value exists and is JSON-stable."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    assert DecisionType.PROPOSAL_FAILURE_DECIDED.value == (
        "proposal_failure_decided"
    )


def test_reason_code_has_six_next_action_labels() -> None:
    """ReasonCode carries the six closed-vocabulary labels."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    assert ReasonCode.ROTATE_LEVER_FAMILY.value == "rotate_lever_family"
    assert ReasonCode.NARROW_AG_SCOPE.value == "narrow_ag_scope"
    assert ReasonCode.MARK_EVIDENCE_GAP.value == "mark_evidence_gap"
    assert ReasonCode.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE.value == (
        "block_ag_retry_by_cluster_signature"
    )
    assert ReasonCode.ESCALATE_UNSUPPORTED_REPAIR_SHAPE.value == (
        "escalate_unsupported_repair_shape"
    )
    assert ReasonCode.REQUEST_EVIDENCE_GATHERING.value == (
        "request_evidence_gathering"
    )


def test_type_to_section_includes_proposal_failure_decided() -> None:
    """The new DecisionType maps to SECTION_PROPOSAL_SURVIVAL so the
    section-coverage invariant stays green."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        SECTION_PROPOSAL_SURVIVAL,
        TYPE_TO_SECTION,
    )

    assert (
        TYPE_TO_SECTION[DecisionType.PROPOSAL_FAILURE_DECIDED]
        == SECTION_PROPOSAL_SURVIVAL
    )


def test_proposal_failure_decided_flag_default_off(monkeypatch) -> None:
    """Default-OFF preserves replay byte-stability of pre-P-F fixtures."""
    from genie_space_optimizer.common.config import (
        proposal_failure_decided_enabled,
    )

    monkeypatch.delenv("GSO_PROPOSAL_FAILURE_DECIDED", raising=False)
    assert proposal_failure_decided_enabled() is False


def test_proposal_failure_decided_flag_truthy_values(monkeypatch) -> None:
    from genie_space_optimizer.common.config import (
        proposal_failure_decided_enabled,
    )

    for truthy in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", truthy)
        assert proposal_failure_decided_enabled() is True, truthy


def test_proposal_failure_decided_flag_falsy_values(monkeypatch) -> None:
    from genie_space_optimizer.common.config import (
        proposal_failure_decided_enabled,
    )

    for falsy in ("", "0", "false", "no", "off"):
        monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", falsy)
        assert proposal_failure_decided_enabled() is False, falsy


def test_proposal_failure_decided_record_carries_reason_code_and_metrics() -> None:
    """Producer emits a DecisionType.PROPOSAL_FAILURE_DECIDED record with
    reason_code matching the next-action label."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_failure_decided_record,
    )
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureDecision,
        ProposalFailureNextAction,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG_X",
        cluster_id="C1",
        cluster_signature="sig:abc123",
        rca_id="rca_1",
        root_cause="missing_top_n",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
    )
    decision = ProposalFailureDecision(
        next_action=ProposalFailureNextAction.ROTATE_LEVER_FAMILY,
        rationale="untried_lever_families=[5, 6]",
    )

    rec = proposal_failure_decided_record(
        run_id="run_1",
        iteration=3,
        context=ctx,
        decision=decision,
        target_qids=("q1", "q2"),
    )

    assert rec.decision_type == DecisionType.PROPOSAL_FAILURE_DECIDED
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.ROTATE_LEVER_FAMILY
    assert rec.ag_id == "AG_X"
    assert rec.cluster_id == "C1"
    assert rec.rca_id == "rca_1"
    assert rec.root_cause == "missing_top_n"
    assert "untried_lever_families" in rec.reason_detail
    assert rec.target_qids == ("q1", "q2")
    assert rec.metrics.get("failure_mode") == "proposal_generation_empty"
    assert rec.metrics.get("cluster_signature") == "sig:abc123"
    assert rec.metrics.get("prior_failure_count") == 0


def test_proposal_failure_decided_record_handles_empty_optional_fields() -> None:
    """When the harness lacks rca_id / cluster_id (e.g. ungrounded RCA),
    the record still emits with safe defaults."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_failure_decided_record,
    )
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureContext,
        ProposalFailureDecision,
        ProposalFailureNextAction,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    ctx = ProposalFailureContext(
        failure_mode="proposal_generation_empty",
        ag_id="AG_Y",
        cluster_id="",
        cluster_signature="",
        rca_id="",
        root_cause="",
        lever_set=(),
        tried_lever_families=(),
        ag_source_cluster_count=1,
        rca_card_grounded=False,
        prior_failure_count=0,
    )
    decision = ProposalFailureDecision(
        next_action=ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING,
        rationale="rca_card_grounded=False",
    )

    rec = proposal_failure_decided_record(
        run_id="run_2",
        iteration=1,
        context=ctx,
        decision=decision,
        target_qids=(),
    )

    assert rec.reason_code == ReasonCode.REQUEST_EVIDENCE_GATHERING
    assert rec.cluster_id == ""
    assert rec.rca_id == ""
    assert rec.target_qids == ()
