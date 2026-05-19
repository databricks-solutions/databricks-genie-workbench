"""Plan 6 Task 9 — stage execute() in ENFORCING mode.

GSO_CRITIQUE_GATE_ENFORCING=true → discard verdicts filter the
proposal out of the output slate; dropped_by_critique records the
proposal_id; verdicts still recorded for postmortem.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
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


def _ctx() -> StageContext:
    return StageContext(
        run_id="r", iteration=2, space_id="s", domain="d",
        catalog="c", schema="x", apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: None,
        feature_flags={},
    )


def _intent(intent_id: str) -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=(), blame_set=(),
        rca_card_id="", ag_id="AG3",
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _envelope(recommendation: str) -> str:
    return json.dumps({
        "result": {
            "addresses_target_failure": recommendation != "discard",
            "is_overgeneralized": recommendation == "discard",
            "likely_neighbor_regressions": ["gs_044"] if recommendation == "discard" else [],
            "matches_intended_shape": recommendation != "discard",
            "overall_recommendation": recommendation,
            "rationale": f"{recommendation} rationale",
        },
        "declined": None,
    })


def _stub_sequence(envelopes: list[str]) -> MagicMock:
    """Returns successive envelopes for successive completions calls."""
    client = MagicMock()
    completions = []
    for env in envelopes:
        choice = MagicMock()
        choice.message.content = env
        c = MagicMock()
        c.choices = [choice]
        c.usage = MagicMock(
            prompt_tokens=800, completion_tokens=200, total_tokens=1000,
        )
        completions.append(c)
    client.chat.completions.create.side_effect = completions
    return client


def _make_input(verdicts_by_pid: list[tuple[str, str]]) -> CritiqueInput:
    """verdicts_by_pid: list of (proposal_id, recommendation) — order matters."""
    proposals = []
    cluster_id_by: dict = {}
    ag_by: dict = {}
    intents_by_id: dict = {}
    risk_by_pid: dict = {}
    for pid, _rec in verdicts_by_pid:
        intent_id = f"intent_{pid}"
        intent = _intent(intent_id)
        proposals.append({
            "proposal_id": pid,
            "example_question": f"q for {pid}",
            "example_sql": "SELECT 1",
            "intent_id": intent_id,
            "repair_intent": intent.to_json(),
        })
        cluster_id_by[pid] = "H001"
        ag_by[pid] = "AG3"
        intents_by_id[intent_id] = intent
        risk_by_pid[pid] = ("gs_044",)
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


def test_enforcing_mode_filters_discard_verdicts_out_of_slate(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    inp = _make_input([
        ("prop_001", "proceed"),
        ("prop_002", "discard"),
        ("prop_003", "rework"),
    ])
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([
            _envelope("proceed"),
            _envelope("discard"),
            _envelope("rework"),
        ]),
    ):
        out = execute(_ctx(), inp)

    # prop_002 (discard) filtered out; prop_001 + prop_003 retained:
    survived_ids = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert survived_ids == {"prop_001", "prop_003"}
    assert "prop_002" in out.dropped_by_critique
    assert out.dropped_by_critique == ("prop_002",)
    # All three verdicts still recorded for postmortem:
    assert set(out.verdict_by_proposal_id.keys()) == {
        "prop_001", "prop_002", "prop_003",
    }
    assert out.advised_count == 3


def test_enforcing_mode_keeps_rework_recommendations_through(monkeypatch) -> None:
    """rework is NOT blocking per CritiqueVerdict.is_blocking() — only
    discard is blocking. Verify enforcing mode lets rework through."""
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    inp = _make_input([("prop_001", "rework")])
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([_envelope("rework")]),
    ):
        out = execute(_ctx(), inp)

    survived_ids = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert "prop_001" in survived_ids
    assert out.dropped_by_critique == ()


def test_enforcing_mode_preserves_ag_keys_after_filtering_all_proposals(
    monkeypatch,
) -> None:
    """When EVERY proposal in an AG gets discarded, the AG key remains
    in proposals_by_ag with an empty tuple — downstream stages must
    cope with this (matches GatesInput / GatesOutcome shape)."""
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    inp = _make_input([("prop_001", "discard")])
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([_envelope("discard")]),
    ):
        out = execute(_ctx(), inp)

    assert "AG3" in out.proposals_by_ag
    assert out.proposals_by_ag["AG3"] == ()
    assert out.dropped_by_critique == ("prop_001",)
