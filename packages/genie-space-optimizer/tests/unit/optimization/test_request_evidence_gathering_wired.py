"""When ``decide_next_action`` returns
``ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING``, the harness
MUST invoke ``regenerate_rca_if_policy_permits`` on the offending
cluster. Trial-5 proved the action was a no-op (the loop emitted the
same failure record every iteration without retrying RCA)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.proposal_failure_policy import (
    ProposalFailureContext,
    ProposalFailureDecision,
    ProposalFailureNextAction,
)


def _ctx_requesting_evidence() -> ProposalFailureContext:
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
    )


def test_policy_returns_request_evidence_for_ungrounded_empty_generation():
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        decide_next_action,
    )
    decision = decide_next_action(_ctx_requesting_evidence())
    assert decision.next_action == ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING


def test_harness_invokes_regenerate_rca_on_request_evidence_gathering():
    """This test mocks ``regenerate_rca_if_policy_permits`` and asserts
    the harness handler ``_handle_proposal_failure_next_action`` calls
    it when the decision is REQUEST_EVIDENCE_GATHERING. The handler
    lives in ``harness.py`` and is the new wiring code added in
    Task 2a.2."""
    from genie_space_optimizer.optimization.harness import (
        _handle_proposal_failure_next_action,
    )

    cluster = {
        "cluster_id": "C1",
        "cluster_signature": "sig-C1",
        "question_ids": ["Q42"],
        "rca_card": None,
    }
    decision = ProposalFailureDecision(
        next_action=ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING,
        rationale="rca_card_grounded=False",
    )
    findings: list = []
    evidence_snapshot: dict = {}
    metadata_snapshot: dict = {}

    with patch(
        "genie_space_optimizer.optimization.harness.regenerate_rca_if_policy_permits",
        return_value=[{"decision_type": "rca_regeneration_triggered"}],
    ) as mock_regen:
        records = _handle_proposal_failure_next_action(
            decision=decision,
            cluster=cluster,
            findings=findings,
            evidence_snapshot=evidence_snapshot,
            metadata_snapshot=metadata_snapshot,
            run_id="run-1",
            iteration=2,
            cache=MagicMock(),
            policy=MagicMock(),
            spark=None,
        )

    mock_regen.assert_called_once()
    assert any(
        r.get("decision_type") == "rca_regeneration_triggered" for r in records
    )


def test_handler_returns_empty_records_for_unwired_actions():
    """Non-evidence-gathering actions (ROTATE_LEVER_FAMILY, etc.) are
    handled elsewhere in the harness; this helper must return [] for
    those so it's safe to call unconditionally."""
    from genie_space_optimizer.optimization.harness import (
        _handle_proposal_failure_next_action,
    )
    decision = ProposalFailureDecision(
        next_action=ProposalFailureNextAction.ROTATE_LEVER_FAMILY,
        rationale="untried_lever_families=[6]",
    )
    records = _handle_proposal_failure_next_action(
        decision=decision,
        cluster={"cluster_id": "C1"},
        findings=[], evidence_snapshot={}, metadata_snapshot={},
        run_id="r", iteration=1, cache=MagicMock(), policy=MagicMock(),
        spark=None,
    )
    assert records == []
