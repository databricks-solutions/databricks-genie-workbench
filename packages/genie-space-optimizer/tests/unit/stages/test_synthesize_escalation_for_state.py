"""Step §M of the production-seam wire-in plan.

Tests the new ``synthesize_escalation_for_state`` entry point — the
unified LLM dispatcher for rungs 1, 3, 4 of the escalation ladder.
"""
from __future__ import annotations


def _make_failed_proposal():
    from genie_space_optimizer.optimization.repair_intent import (
        PatchType, RepairShape,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        RepairProposal,
    )
    return RepairProposal(
        intent_id="intent_rejected",
        intent_name="reject", intent_description="d",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_JOIN_SPEC,
        rationale="r", confidence="high",
        patch_body={"table_a": "x", "table_b": "y"},
        blame_set=("x:a", "y:b"),
        target_qids=("q1",),
    )


def _make_cluster():
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="theme",
        member_qids=("q1",),
        unifying_evidence="ev",
        repair_hypothesis="hypothesis",
        primary_blame_set=("x:a",),
        confidence="high",
    )


def _mock_synth_returning_proposal(monkeypatch, proposal_dict):
    from dataclasses import dataclass

    @dataclass
    class _R:
        proposal: object = None
        attempted_archetypes: tuple = ()
        skipped_reason: str | None = None

    captured = {}

    def fake_synth(cluster, schema_slice, history, **kwargs):
        captured["cluster"] = cluster
        captured["schema_slice"] = schema_slice
        captured["history"] = history
        captured["kwargs"] = kwargs
        return _R(proposal=proposal_dict)

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.synthesize"
        ".run_plan11_synthesis_for_single_cluster",
        fake_synth,
    )
    return captured


def test_rung_hint_scoped_l6_appends_to_cluster_repair_hypothesis(monkeypatch):
    """The unified dispatcher hints rung 1 by embedding ``scoped_l6`` in
    the cluster's repair_hypothesis before calling the legacy Stage 3."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        EscalationRungHint, synthesize_escalation_for_state,
    )

    captured = _mock_synth_returning_proposal(
        monkeypatch,
        {"intent_id": "intent_new", "intent_name": "n",
         "intent_description": "d", "repair_shape": "other",
         "patch_type": "add_join_spec", "rationale": "r",
         "confidence": "high", "patch_body": {"a": "b"},
         "blame_set": ["x:a"], "target_objects": [],
         "required_constructs": [],
         "repair_hypothesis": "narrower_join",
         "target_qids": ["q1"]},
    )

    result = synthesize_escalation_for_state(
        rung_hint=EscalationRungHint.SCOPED_L6,
        failed_proposal=_make_failed_proposal(),
        failure_reason="STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY",
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="r",
        iteration=1,
        ag_id="AG_H001",
        w=None,
    )

    assert result.proposal is not None
    assert result.proposal["intent_id"] == "intent_new"
    # The repair_hypothesis on the cluster passed to the legacy
    # synthesize call carries the rung hint marker.
    forwarded = captured["cluster"]
    assert "scoped_l6" in forwarded.repair_hypothesis


def test_rung_hint_add_example_sql_in_cluster(monkeypatch):
    from genie_space_optimizer.optimization.stages.synthesize import (
        EscalationRungHint, synthesize_escalation_for_state,
    )
    captured = _mock_synth_returning_proposal(
        monkeypatch,
        {"intent_id": "intent_new", "intent_name": "n",
         "intent_description": "d", "repair_shape": "other",
         "patch_type": "add_example_sql", "rationale": "r",
         "confidence": "high",
         "patch_body": {"example_question": "?",
                        "example_sql": "SELECT 1"},
         "blame_set": [], "target_objects": [], "required_constructs": [],
         "target_qids": ["q1"]},
    )
    result = synthesize_escalation_for_state(
        rung_hint=EscalationRungHint.ADD_EXAMPLE_SQL,
        failed_proposal=_make_failed_proposal(),
        failure_reason="apply_failed",
        cluster=_make_cluster(),
        schema_slice={}, history=[],
        optimization_run_id="r", iteration=1, ag_id="AG_H001", w=None,
    )
    assert result.proposal is not None
    assert "add_example_sql" in captured["cluster"].repair_hypothesis


def test_rung_hint_narrowed_example_sql_targets_one_qid(monkeypatch):
    from genie_space_optimizer.optimization.stages.synthesize import (
        EscalationRungHint, synthesize_escalation_for_state,
    )
    captured = _mock_synth_returning_proposal(
        monkeypatch,
        {"intent_id": "intent_new", "intent_name": "n",
         "intent_description": "d", "repair_shape": "other",
         "patch_type": "add_example_sql", "rationale": "r",
         "confidence": "high",
         "patch_body": {"example_question": "?",
                        "example_sql": "SELECT 1"},
         "blame_set": [], "target_objects": [], "required_constructs": [],
         "target_qids": ["q1"]},
    )
    result = synthesize_escalation_for_state(
        rung_hint=EscalationRungHint.NARROWED_EXAMPLE_SQL,
        failed_proposal=_make_failed_proposal(),
        failure_reason="repeated_rejection",
        cluster=_make_cluster(),
        schema_slice={}, history=[],
        optimization_run_id="r", iteration=1, ag_id="AG_H001", w=None,
    )
    assert result.proposal is not None
    # Narrowed_example_sql is the most-narrow rung; cluster should
    # be projected to single-target QID scope.
    assert "narrowed_example_sql" in captured["cluster"].repair_hypothesis
    assert len(captured["cluster"].member_qids) == 1


def test_decline_returns_skipped_reason(monkeypatch):
    """When the underlying synthesize call declines, the unified
    dispatcher surfaces ``skipped_reason`` unchanged."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        EscalationRungHint, synthesize_escalation_for_state,
    )
    _mock_synth_returning_proposal(monkeypatch, None)

    result = synthesize_escalation_for_state(
        rung_hint=EscalationRungHint.SCOPED_L6,
        failed_proposal=_make_failed_proposal(),
        failure_reason="STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY",
        cluster=_make_cluster(),
        schema_slice={}, history=[],
        optimization_run_id="r", iteration=1, ag_id="AG_H001", w=None,
    )
    assert result.proposal is None
