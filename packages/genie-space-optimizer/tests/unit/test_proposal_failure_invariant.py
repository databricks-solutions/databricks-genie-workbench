"""Plan P-F — operator-transcript projection + iteration invariant tests."""

from __future__ import annotations


def test_proposal_generation_stage_projects_failure_decided_type() -> None:
    """Stage 6 (proposal_generation) carries both PROPOSAL_GENERATED and
    PROPOSAL_FAILURE_DECIDED so the operator transcript renders the
    failure record + the typed next-action label adjacent."""
    from genie_space_optimizer.optimization.operator_process_transcript import (
        _STAGE_DECISION_TYPE_MAP,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    types = _STAGE_DECISION_TYPE_MAP.get("proposal_generation", ())
    assert DecisionType.PROPOSAL_GENERATED in types
    assert DecisionType.PROPOSAL_FAILURE_DECIDED in types


def test_emit_proposal_failure_decided_helper_appends_record_and_marker(
    monkeypatch,
) -> None:
    """The internal helper emits exactly one record + one marker into
    iter_inputs when the flag is on."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_x",
        iteration=1,
        ag_id="AG_1",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="rca_1",
        root_cause="missing_filter",
        failure_mode="proposal_generation_empty",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("q1",),
        iter_inputs=iter_inputs,
    )

    records = iter_inputs.get("decision_records") or []
    assert len(records) == 1
    rec = records[0]
    assert rec["decision_type"] == "proposal_failure_decided"
    assert rec["reason_code"] == "rotate_lever_family"

    markers = iter_inputs.get("markers") or []
    assert any(m.startswith("GSO_PROPOSAL_FAILURE_DECIDED_V1 ") for m in markers)


def test_emit_proposal_failure_decided_helper_noop_when_flag_off(
    monkeypatch,
) -> None:
    """Flag-off path appends nothing — replay byte-stability."""
    monkeypatch.delenv("GSO_PROPOSAL_FAILURE_DECIDED", raising=False)

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_x",
        iteration=1,
        ag_id="AG_1",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="rca_1",
        root_cause="missing_filter",
        failure_mode="proposal_generation_empty",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("q1",),
        iter_inputs=iter_inputs,
    )

    assert "decision_records" not in iter_inputs
    assert "markers" not in iter_inputs
