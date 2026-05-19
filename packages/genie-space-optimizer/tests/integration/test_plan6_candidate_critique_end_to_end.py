"""Plan 6 Task 15 — end-to-end integration test.

Threads every Plan-6 component (skill folder, validators, driver,
stage execute, registry, decision emission, advisory/enforcing modes)
through the real machinery with only the OpenAI client stubbed.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionType,
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
from genie_space_optimizer.optimization.stages._registry import get_stage
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueInput,
)


def _ev(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="missing top-N",
        generated_sql_issue="no ORDER BY ... LIMIT",
        expected_sql_shape="GROUP BY region ORDER BY revenue DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _intent(intent_id: str, intent_name: str = "top_n_revenue_by_region") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name=intent_name,
        intent_description="add example_sql for top-N revenue",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="cluster blames missing LIMIT/ORDER BY",
        confidence="high", source="llm_l5b_synthesis",
        cluster_id="H001",
        target_qids=("gs_009", "gs_017"),
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        rca_card_id="", ag_id="AG3",
    )


def _ctx(decisions: list, iteration: int = 2) -> StageContext:
    return StageContext(
        run_id="r_int", iteration=iteration, space_id="s_int",
        domain="sales", catalog="main", schema="gso_test",
        apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: decisions.append(rec),
        feature_flags={},
    )


def _stub_sequence(envelopes: list[str]) -> MagicMock:
    client = MagicMock()
    completions = []
    for env in envelopes:
        choice = MagicMock()
        choice.message.content = env
        c = MagicMock()
        c.choices = [choice]
        c.usage = MagicMock(
            prompt_tokens=800, completion_tokens=300, total_tokens=1100,
        )
        completions.append(c)
    client.chat.completions.create.side_effect = completions
    return client


def _envelope(rec: str, regressions: list[str]) -> str:
    return json.dumps({
        "result": {
            "addresses_target_failure": rec != "discard",
            "is_overgeneralized": rec == "discard",
            "likely_neighbor_regressions": regressions,
            "matches_intended_shape": rec != "discard",
            "overall_recommendation": rec,
            "rationale": f"{rec} reasoning",
        },
        "declined": None,
    })


def _build_realistic_input(verdicts_by_pid: list[tuple[str, str]]) -> CritiqueInput:
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
            "intent_id": intent_id,
            "repair_intent": intent.to_json(),
            "example_question": f"What are top 3 regions for {pid}?",
            "example_sql": (
                "SELECT region, SUM(revenue) r FROM sales.fact_sales "
                "GROUP BY region ORDER BY r DESC LIMIT 3"
            ),
            "usage_guidance": "use for top-N",
            "parameters": [],
        })
        cluster_id_by[pid] = "H001"
        ag_by[pid] = "AG3"
        intents_by_id[intent_id] = intent
        risk_by_pid[pid] = ("gs_044", "gs_055")
    return CritiqueInput(
        proposals_by_ag={"AG3": tuple(proposals)},
        repair_intents_by_id=intents_by_id,
        rca_evidence_typed_by_cluster={
            "H001": {"gs_009": _ev("gs_009"), "gs_017": _ev("gs_017")},
        },
        passing_qids_at_risk_by_proposal_id=risk_by_pid,
        cluster_semantic_theme_by_cluster={"H001": "top-N revenue ranking missing"},
        cluster_id_by_proposal_id=cluster_id_by,
        ag_id_by_proposal_id=ag_by,
    )


def test_end_to_end_advisory_mode_records_verdicts_without_filtering(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    inp = _build_realistic_input([
        ("prop_001", "proceed"),
        ("prop_002", "rework"),
        ("prop_003", "discard"),
    ])
    decisions: list = []

    entry = get_stage("candidate_critique")
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([
            _envelope("proceed", []),
            _envelope("rework", ["gs_044"]),
            _envelope("discard", ["gs_044", "gs_055"]),
        ]),
    ):
        out = entry.execute(_ctx(decisions), inp)

    survived_ids = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert survived_ids == {"prop_001", "prop_002", "prop_003"}
    assert out.dropped_by_critique == ()
    assert set(out.verdict_by_proposal_id.keys()) == {
        "prop_001", "prop_002", "prop_003",
    }
    assert out.advised_count == 3
    for p in out.proposals_by_ag["AG3"]:
        assert "critique_verdict" in p
    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    assert len(critique_records) == 3


def test_end_to_end_enforcing_mode_filters_discard_verdicts(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    inp = _build_realistic_input([
        ("prop_001", "proceed"),
        ("prop_002", "discard"),
        ("prop_003", "discard"),
    ])
    decisions: list = []

    entry = get_stage("candidate_critique")
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([
            _envelope("proceed", []),
            _envelope("discard", ["gs_044"]),
            _envelope("discard", ["gs_055"]),
        ]),
    ):
        out = entry.execute(_ctx(decisions), inp)

    survived_ids = {p["proposal_id"] for p in out.proposals_by_ag["AG3"]}
    assert survived_ids == {"prop_001"}
    assert set(out.dropped_by_critique) == {"prop_002", "prop_003"}
    assert len(out.verdict_by_proposal_id) == 3
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
    )
    dropped_recs = [
        r for r in decisions
        if (
            getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
            and getattr(r, "outcome", None) == DecisionOutcome.DROPPED
        )
    ]
    assert len(dropped_recs) == 2


def test_end_to_end_budget_meter_records_actuals(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    budget = IterationTokenBudget(itpm_limit=200_000, otpm_limit=20_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        inp = _build_realistic_input([
            ("prop_001", "proceed"),
            ("prop_002", "proceed"),
        ])
        entry = get_stage("candidate_critique")
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_sequence([
                _envelope("proceed", []),
                _envelope("proceed", []),
            ]),
        ):
            entry.execute(_ctx([]), inp)
        # Two LLM calls × 800 input + 300 output = 1.6k input + 600 output:
        assert budget.actual_input_tokens == 1600
        assert budget.actual_output_tokens == 600
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_proposals_by_ag_shape_compatible_with_gates_input(
    monkeypatch,
) -> None:
    """The CritiqueOutcome.proposals_by_ag must be the exact shape
    that stages.gates.GatesInput.proposals_by_ag expects so the next
    stage reads it unchanged."""
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    inp = _build_realistic_input([
        ("prop_001", "proceed"),
        ("prop_002", "discard"),
    ])
    entry = get_stage("candidate_critique")
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([
            _envelope("proceed", []),
            _envelope("discard", ["gs_044"]),
        ]),
    ):
        out = entry.execute(_ctx([]), inp)

    from genie_space_optimizer.optimization.stages.gates import GatesInput
    gates_input = GatesInput(
        proposals_by_ag=out.proposals_by_ag,
        ags=(),
    )
    assert gates_input.proposals_by_ag == out.proposals_by_ag
    rebuilt = GatesInput.from_json(gates_input.to_json())
    survived_ids = {p["proposal_id"]
                    for p in rebuilt.proposals_by_ag["AG3"]}
    assert survived_ids == {"prop_001"}
