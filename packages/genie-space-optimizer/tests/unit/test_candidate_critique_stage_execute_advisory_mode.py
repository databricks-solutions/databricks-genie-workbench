"""Plan 6 Task 9 — stage execute() in ADVISORY mode (default).

Slate is byte-stable input → output. Verdicts are recorded in
verdict_by_proposal_id. discard verdicts do NOT filter the proposal.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueInput,
    execute,
)


def _ctx(decision_emit_calls: list) -> StageContext:
    return StageContext(
        run_id="run_x", iteration=2, space_id="space_y", domain="sales",
        catalog="main", schema="gso_test", apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: decision_emit_calls.append(rec),
        feature_flags={},
    )


def _intent(intent_id: str) -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="llm_l5b_synthesis",
        cluster_id="H001", target_qids=("gs_009",),
        blame_set=("sales.fact_sales.revenue",),
        rca_card_id="", ag_id="AG3",
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="missing LIMIT",
        generated_sql_issue="x",
        expected_sql_shape="ORDER BY revenue DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="top_n",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _stub_llm_returning(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=800, completion_tokens=200, total_tokens=1000,
    )
    client.chat.completions.create.return_value = completion
    return client


def _discard_envelope() -> str:
    return json.dumps({
        "result": {
            "addresses_target_failure": False,
            "is_overgeneralized": True,
            "likely_neighbor_regressions": ["gs_044"],
            "matches_intended_shape": False,
            "overall_recommendation": "discard",
            "rationale": "overgeneralized; regression risk on gs_044",
        },
        "declined": None,
    })


def _make_input(*, n_proposals: int) -> CritiqueInput:
    proposals = []
    cluster_id_by: dict = {}
    ag_by: dict = {}
    intents_by_id: dict = {}
    risk_by_pid: dict = {}
    for i in range(n_proposals):
        pid = f"prop_{i:03d}"
        intent_id = f"intent_{i:03d}"
        intent = _intent(intent_id)
        proposals.append({
            "proposal_id": pid,
            "example_question": f"question {i}",
            "example_sql": "SELECT 1",
            "intent_id": intent_id,
            "repair_intent": intent.to_json(),
        })
        cluster_id_by[pid] = "H001"
        ag_by[pid] = "AG3"
        intents_by_id[intent_id] = intent
        risk_by_pid[pid] = ("gs_044", "gs_055")
    return CritiqueInput(
        proposals_by_ag={"AG3": tuple(proposals)},
        repair_intents_by_id=intents_by_id,
        rca_evidence_typed_by_cluster={
            "H001": {"gs_009": _evidence("gs_009")},
        },
        passing_qids_at_risk_by_proposal_id=risk_by_pid,
        cluster_semantic_theme_by_cluster={"H001": "x"},
        cluster_id_by_proposal_id=cluster_id_by,
        ag_id_by_proposal_id=ag_by,
    )


def test_advisory_mode_keeps_slate_byte_stable_when_verdict_is_discard(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    inp = _make_input(n_proposals=1)
    decisions: list = []
    ctx = _ctx(decisions)

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_llm_returning(_discard_envelope()),
    ):
        out = execute(ctx, inp)

    # Slate byte-stable in advisory mode (with stamped critique_verdict on each proposal):
    assert "AG3" in out.proposals_by_ag
    assert len(out.proposals_by_ag["AG3"]) == 1
    pid_set = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert pid_set == {"prop_000"}
    # No drops in advisory mode:
    assert out.dropped_by_critique == ()
    # Verdict recorded:
    assert "prop_000" in out.verdict_by_proposal_id
    v = out.verdict_by_proposal_id["prop_000"]
    assert isinstance(v, CritiqueVerdict)
    assert v.overall_recommendation == "discard"
    assert v.is_blocking() is True
    assert out.advised_count == 1


def test_advisory_mode_emits_one_decision_record_per_verdict(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    inp = _make_input(n_proposals=2)
    decisions: list = []
    ctx = _ctx(decisions)

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_llm_returning(_discard_envelope()),
    ):
        execute(ctx, inp)

    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )
    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    assert len(critique_records) == 2


def test_advisory_mode_short_circuits_proposals_without_intent(monkeypatch) -> None:
    """Unstamped proposals (no repair_intent on the dict, no entry in
    repair_intents_by_id either) → driver returns None → slate is
    untouched, no decision record."""
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    proposals = [
        {"proposal_id": "prop_unstamped",
         "example_question": "x", "example_sql": "SELECT 1"},
    ]
    inp = CritiqueInput(
        proposals_by_ag={"AG3": tuple(proposals)},
        repair_intents_by_id={},
        rca_evidence_typed_by_cluster={},
        passing_qids_at_risk_by_proposal_id={"prop_unstamped": ()},
        cluster_semantic_theme_by_cluster={},
        cluster_id_by_proposal_id={"prop_unstamped": "H001"},
        ag_id_by_proposal_id={"prop_unstamped": "AG3"},
    )
    decisions: list = []
    ctx = _ctx(decisions)
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        out = execute(ctx, inp)

    # Slate byte-stable (proposal still present), no LLM call, no verdict, advised_count=0:
    survived = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert survived == {"prop_unstamped"}
    assert out.advised_count == 0
    assert "prop_unstamped" not in out.verdict_by_proposal_id
    assert client.chat.completions.create.call_count == 0
