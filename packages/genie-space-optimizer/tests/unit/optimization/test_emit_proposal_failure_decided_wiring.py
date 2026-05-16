"""End-to-end test of ``_emit_proposal_failure_decided`` post-refactor.

Verifies (1) the helper bumps the per-AG signature counter, (2)
``prior_identical_failure_count`` is populated on the policy context,
(3) the second identical-signature emit produces an ESCALATE_STALEMATE
record, and (4) the handler is invoked with the cluster from the
call-site context."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.proposal_failure_callsite_context import (
    ProposalFailureCallSiteContext,
    noop_context,
)


def _emit(monkeypatch, callsite_ctx, **overrides):
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )
    iter_inputs: dict = {}
    kwargs = dict(
        run_id="r-1",
        iteration=1,
        ag_id="AG1",
        cluster_id="C1",
        cluster_signature="sig-C1",
        rca_id="rca-1",
        root_cause="wrong_aggregation",
        failure_mode="no_causal_applyable_patch",
        lever_set=(6,),
        tried_lever_families=(6,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("Q42",),
        iter_inputs=iter_inputs,
        callsite_ctx=callsite_ctx,
    )
    kwargs.update(overrides)
    _emit_proposal_failure_decided(**kwargs)
    return iter_inputs


def test_first_emit_does_not_escalate(monkeypatch):
    iter_inputs = _emit(monkeypatch, noop_context())
    records = iter_inputs.get("decision_records", [])
    assert len(records) == 1
    assert records[0]["decision_type"] == "proposal_failure_decided"
    assert records[0]["next_action"] != "escalate_stalemate"


def test_second_identical_emit_escalates_stalemate(monkeypatch):
    counter: dict[str, int] = {}
    ctx = ProposalFailureCallSiteContext(
        cluster={"cluster_id": "C1"},
        findings=[],
        evidence_snapshot={},
        cache=None,
        policy=None,
        signatures_counter=counter,
        metadata_snapshot={},
        spark=None,
    )
    _emit(monkeypatch, ctx)
    iter_inputs = _emit(monkeypatch, ctx, iteration=2)
    records = iter_inputs.get("decision_records", [])
    assert len(records) == 1
    assert records[0]["next_action"] == "escalate_stalemate"


def test_handler_invoked_with_cluster_from_context(monkeypatch):
    cluster = {"cluster_id": "C1", "rca_card": None}
    ctx = ProposalFailureCallSiteContext(
        cluster=cluster,
        findings=[],
        evidence_snapshot={},
        cache=MagicMock(),
        policy=MagicMock(),
        signatures_counter={},
        metadata_snapshot={},
        spark=None,
    )
    with patch(
        "genie_space_optimizer.optimization.harness._handle_proposal_failure_next_action",
        return_value=[{"decision_type": "rca_regeneration_triggered"}],
    ) as mock_handle:
        _emit(
            monkeypatch, ctx,
            failure_mode="proposal_generation_empty",
            rca_card_grounded=False,
        )
    mock_handle.assert_called_once()
    call_kwargs = mock_handle.call_args.kwargs
    assert call_kwargs["cluster"] is cluster


def test_handler_records_appended_to_iter_inputs(monkeypatch):
    ctx = ProposalFailureCallSiteContext(
        cluster={"cluster_id": "C1"},
        findings=[],
        evidence_snapshot={},
        cache=MagicMock(),
        policy=MagicMock(),
        signatures_counter={},
        metadata_snapshot={},
        spark=None,
    )
    extra_rec = {"decision_type": "rca_regeneration_triggered", "iteration": 7}
    with patch(
        "genie_space_optimizer.optimization.harness._handle_proposal_failure_next_action",
        return_value=[extra_rec],
    ):
        iter_inputs = _emit(
            monkeypatch, ctx,
            failure_mode="proposal_generation_empty",
            rca_card_grounded=False,
        )
    types = [r.get("decision_type") for r in iter_inputs.get("decision_records", [])]
    assert types == ["proposal_failure_decided", "rca_regeneration_triggered"]


def test_callsite_ctx_default_is_noop(monkeypatch):
    """For backward compat, callers that have not been refactored yet
    must still produce the same record shape (no handler invocation)."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )
    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="r-1",
        iteration=1,
        ag_id="AG1",
        cluster_id="C1",
        cluster_signature="sig-C1",
        rca_id="rca-1",
        root_cause="wrong_aggregation",
        failure_mode="no_causal_applyable_patch",
        lever_set=(6,),
        tried_lever_families=(6,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("Q42",),
        iter_inputs=iter_inputs,
        # callsite_ctx omitted — defaults to noop_context()
    )
    assert len(iter_inputs.get("decision_records", [])) == 1
