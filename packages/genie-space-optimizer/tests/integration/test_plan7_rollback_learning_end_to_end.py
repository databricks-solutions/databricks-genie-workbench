"""Plan 7 Task 16 — end-to-end integration test.

Threads every Plan-7 component (skill folder, validators, driver,
iteration entry, stamp + filter helpers, Plan-5 consumer integration)
through the real machinery with only the OpenAI client stubbed.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
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
    IntentOutcome,
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    _render_user_prompt as _render_plan5_user_prompt,
)
from genie_space_optimizer.optimization.rollback_learning import (
    apply_forbidden_signatures_to_rollback_fingerprints,
    hypothesize_next_attempts_for_iteration,
    stamp_hypotheses_on_metadata_snapshot,
)
from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages.acceptance import (
    AgOutcome,
    AgOutcomeRecord,
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


def _intent(intent_id: str, cluster_id: str = "H001") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="top_n_revenue_by_region",
        intent_description="add example_sql for top-N revenue",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="llm_l5b_synthesis",
        cluster_id=cluster_id,
        target_qids=("gs_009", "gs_017"),
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        rca_card_id="", ag_id="AG3",
    )


def _outcome(intent_id: str) -> IntentOutcome:
    return IntentOutcome(
        intent_id=intent_id, ag_id="AG3", outcome="rolled_back",
        applied_signature=f"sig_{intent_id}",
        applied_at_iter=2, rollback_reason="out_of_target_regression",
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="missing top-N",
        generated_sql_issue="no ORDER BY ... LIMIT",
        expected_sql_shape="GROUP BY region ORDER BY revenue DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _success_envelope(forbidden: list[str]) -> str:
    return json.dumps({
        "result": {
            "why_failed": "patch was too broad",
            "failure_mode": "overgeneralized_top_n",
            "revised_repair_shape": None,
            "revised_patch_type": "add_sql_snippet_filter",
            "revised_blame_set": ["sales.fact_sales.revenue"],
            "additional_evidence_needed": [
                "data_profile_for_sales.fact_sales.revenue",
            ],
            "forbidden_signatures": forbidden,
            "confidence": "high",
        },
        "declined": None,
    })


def _stub_sequence(envelopes: list[str]) -> MagicMock:
    client = MagicMock()
    completions = []
    for env in envelopes:
        choice = MagicMock()
        choice.message.content = env
        c = MagicMock()
        c.choices = [choice]
        c.usage = MagicMock(
            prompt_tokens=900, completion_tokens=400, total_tokens=1300,
        )
        completions.append(c)
    client.chat.completions.create.side_effect = completions
    return client


def _ag_outcome_rolled_back(*, intent_ids: list[str]) -> AgOutcome:
    intent_outcomes = tuple(_outcome(i) for i in intent_ids)
    rec = AgOutcomeRecord(
        ag_id="AG3", outcome="rolled_back",
        reason_code="out_of_target_regression",
        target_qids=("gs_009", "gs_017"),
        affected_qids=("gs_009", "gs_017"),
        content_fingerprints=tuple(f"sig_{i}" for i in intent_ids),
        intent_outcomes=intent_outcomes,
    )
    return AgOutcome(
        outcomes_by_ag={"AG3": rec},
        qid_resolutions={"gs_009": "hold_fail", "gs_017": "hold_fail"},
        rolled_back_content_fingerprints=frozenset(
            f"sig_{i}" for i in intent_ids
        ),
        intent_outcomes_by_id={i: _outcome(i) for i in intent_ids},
    )


def test_end_to_end_rolled_back_intent_produces_stamped_hypothesis(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")

    ag_outcome = _ag_outcome_rolled_back(intent_ids=["i_001"])
    metadata_snapshot: dict = {"_failure_clusters": []}
    decisions: list = []
    prior_fingerprints = {"sig_other_iteration"}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([_success_envelope(["sig_i_001"])]),
    ):
        hypotheses = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
                "gs_017": _evidence("gs_017"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=({"question_id": "gs_009",
                       "result_correctness": "no"},
                      {"question_id": "gs_017",
                       "result_correctness": "no"}),
            post_rows=({"question_id": "gs_009",
                        "result_correctness": "no"},
                       {"question_id": "gs_017",
                        "result_correctness": "no"}),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
                "sales.fact_sales.region",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )

    assert "H001" in hypotheses
    h = hypotheses["H001"]
    assert h.confidence == "high"
    assert h.failure_mode == "overgeneralized_top_n"
    assert h.revised_patch_type == PatchType.ADD_SQL_SNIPPET_FILTER
    assert h.forbidden_signatures == ("sig_i_001",)

    # Stamp onto metadata_snapshot:
    stamp_hypotheses_on_metadata_snapshot(metadata_snapshot, hypotheses)
    assert "_last_attempt_hypothesis_by_cluster" in metadata_snapshot
    stamped = metadata_snapshot["_last_attempt_hypothesis_by_cluster"]["H001"]
    assert stamped["confidence"] == "high"
    assert metadata_snapshot["_failure_clusters"] == []

    # Union forbidden_signatures into the do-not-retry set:
    new_fingerprints = apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=prior_fingerprints,
        hypotheses_by_cluster_id=hypotheses,
    )
    assert new_fingerprints == {"sig_other_iteration", "sig_i_001"}

    # Plan-5's _render_user_prompt now surfaces the hypothesis:
    plan5_cluster = LlmCluster(
        cluster_id="H001",
        semantic_theme="top-N revenue ranking missing",
        member_qids=("gs_009",),
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )
    rendered = _render_plan5_user_prompt(
        cluster=plan5_cluster,
        rca_evidence_typed={},
        identifier_allowlist={
            "sales.fact_sales.revenue", "sales.fact_sales.region",
        },
        ag_id="AG3",
        iteration=3,
        existing_examples_preview="",
        metadata_snapshot=metadata_snapshot,
    )
    payload = json.loads(rendered)
    assert payload["last_attempt_hypothesis"] is not None
    assert payload["last_attempt_hypothesis"]["confidence"] == "high"
    assert payload["last_attempt_hypothesis"]["failure_mode"] == (
        "overgeneralized_top_n"
    )

    recs = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.NEXT_ATTEMPT_HYPOTHESIZED
    ]
    assert len(recs) == 1


def test_end_to_end_budget_meter_records_actuals(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    budget = IterationTokenBudget(itpm_limit=200_000, otpm_limit=20_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        ag_outcome = _ag_outcome_rolled_back(intent_ids=["i_001"])
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_sequence([_success_envelope([])]),
        ):
            hypothesize_next_attempts_for_iteration(
                ctx=_ctx([]),
                ag_outcome=ag_outcome,
                repair_intents_by_id={"i_001": _intent("i_001")},
                per_qid_evidence_by_cluster={"H001": {
                    "gs_009": _evidence("gs_009"),
                }},
                critique_verdicts_by_intent_id={},
                pre_rows=(), post_rows=(),
                applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
                identifier_allowlist_by_ag={"AG3": {
                    "sales.fact_sales.revenue",
                }},
                cluster_id_by_intent_id={"i_001": "H001"},
            )
        assert budget.actual_input_tokens == 900
        assert budget.actual_output_tokens == 400
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_no_rolled_back_outcomes_is_zero_overhead(monkeypatch) -> None:
    """All AGs accepted → iteration entry returns {} without
    dispatching any LLM call."""
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    rec = AgOutcomeRecord(
        ag_id="AG3", outcome="accepted",
        reason_code="accepted",
        target_qids=("gs_009",), affected_qids=("gs_009",),
        content_fingerprints=("sig_i_001",),
        intent_outcomes=(IntentOutcome(
            intent_id="i_001", ag_id="AG3", outcome="accepted",
            applied_signature="sig_i_001", applied_at_iter=2,
            rollback_reason=None,
        ),),
    )
    ag_outcome = AgOutcome(
        outcomes_by_ag={"AG3": rec},
        qid_resolutions={"gs_009": "fail_to_pass"},
        rolled_back_content_fingerprints=frozenset(),
        intent_outcomes_by_id={"i_001": rec.intent_outcomes[0]},
    )
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    decisions: list = []
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    assert result == {}
    assert client.chat.completions.create.call_count == 0
    assert decisions == []
