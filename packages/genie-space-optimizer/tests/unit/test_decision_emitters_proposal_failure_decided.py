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
