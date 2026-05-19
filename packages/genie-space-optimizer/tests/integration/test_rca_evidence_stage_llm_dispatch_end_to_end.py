"""Plan 3 Task 15 — end-to-end test of rca-evidence-extraction stage.

Threads together every Plan-3 component using the real skill folder.
Only the OpenAI client is stubbed; every other component (skill
loader, extractor, dispatch logic, mappers, projector, budget meter)
runs unmocked.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceInput,
    collect,
)


def _success_env(qid: str, family: str, patch_type: str) -> str:
    return json.dumps({
        "result": {
            "qid": qid,
            "observed_failure": f"failure for {qid}",
            "generated_sql_issue": "specific defect",
            "expected_sql_shape": "shape",
            "blame_set": [f"sales.fact_sales.{qid}_col"],
            "suggested_repair_family": family,
            "repair_hint_patch_type": patch_type,
            "confidence": "high",
            "quoted_evidence": [],
        },
        "declined": None,
    })


def _decline_env(reason: str) -> str:
    return json.dumps({
        "result": None,
        "declined": {
            "reason": reason,
            "explanation": "test decline",
            "needed_evidence": ["evidence_type_x"],
            "suggested_next_step": "next_step",
        },
    })


def _make_cycling_client(envelopes: list[str]) -> MagicMock:
    client = MagicMock(name="OpenAIClient")
    completions = []
    for env in envelopes:
        choice = MagicMock()
        choice.message.content = env
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=200, completion_tokens=80, total_tokens=280,
        )
        completions.append(completion)
    client.chat.completions.create.side_effect = completions
    return client


class _StubCtx:
    run_id = "e2e_run"
    iteration = 3
    w = None


def test_end_to_end_three_qids_two_success_one_decline(monkeypatch) -> None:
    """Three qids: first two succeed, third declines — typed sidecar
    has two entries; legacy dict has all three (third via fallback)."""
    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "true")

    inp = RcaEvidenceInput(
        eval_rows=(
            {
                "question_id": "gs_001",
                "genie_sql": "SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1",
            },
            {
                "question_id": "gs_002",
                "genie_sql": "SELECT * FROM crm.customer c, crm.orders o",
            },
            {
                "question_id": "gs_003",
                "genie_sql": "SELECT * FROM finance.payments WHERE status='PENDING'",
            },
        ),
        hard_failure_qids=("gs_001", "gs_002", "gs_003"),
        soft_signal_qids=(),
        per_qid_judge={
            "gs_001": {"verdict": "missing_top_n"},
            "gs_002": {"verdict": "wrong_join_spec"},
            "gs_003": {"verdict": "wrong_filter_condition"},
        },
        asi_metadata={
            "gs_001": {"failure_type": "missing_top_n"},
            "gs_002": {"failure_type": "missing_join"},
            "gs_003": {"failure_type": "wrong_filter_condition"},
        },
    )

    client = _make_cycling_client([
        _success_env("gs_001", "top_n_with_ordering", "add_example_sql"),
        _success_env("gs_002", "join_spec_addition_with_disambiguation", "add_join_spec"),
        _decline_env("ambiguous_failure"),
    ])

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        bundle = collect(_StubCtx(), inp)

    # Post Plan-8 T6: deterministic fallback also stamps a typed
    # PerQidRcaEvidence for the declined-LLM qid (gs_003) so Plan 4
    # clustering and Plan 5 intent synthesis can see fallback'd qids.
    # The LLM-extracted entries (gs_001, gs_002) keep their richer
    # repair_hint_patch_type from the LLM envelope.
    assert set(bundle.per_qid_evidence_typed.keys()) == {
        "gs_001", "gs_002", "gs_003",
    }
    assert isinstance(bundle.per_qid_evidence_typed["gs_001"], PerQidRcaEvidence)
    assert (
        bundle.per_qid_evidence_typed["gs_001"].repair_hint_patch_type
        is PatchType.ADD_EXAMPLE_SQL
    )
    assert isinstance(bundle.per_qid_evidence_typed["gs_003"], PerQidRcaEvidence)
    assert set(bundle.per_qid_evidence.keys()) == {"gs_001", "gs_002", "gs_003"}
    assert (
        bundle.per_qid_evidence["gs_001"]["rca_kind"]
        == RcaKind.TOP_N_CARDINALITY_COLLAPSE.value
    )
    assert bundle.rca_kinds_by_qid["gs_001"] == "top_n_cardinality_collapse"
    assert bundle.rca_kinds_by_qid["gs_002"] == "join_spec_missing_or_wrong"
    assert bundle.rca_kinds_by_qid["gs_003"] == "filter_logic_mismatch"


def test_end_to_end_budget_meter_records_actuals_across_per_qid_calls(
    monkeypatch,
) -> None:
    """Each per-qid call updates the per-iteration token budget."""
    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "true")

    budget = IterationTokenBudget(itpm_limit=200_000, otpm_limit=20_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        inp = RcaEvidenceInput(
            eval_rows=(
                {"question_id": "gs_001", "genie_sql": "x"},
                {"question_id": "gs_002", "genie_sql": "y"},
            ),
            hard_failure_qids=("gs_001", "gs_002"),
            soft_signal_qids=(),
            per_qid_judge={
                "gs_001": {"verdict": "missing_top_n"},
                "gs_002": {"verdict": "missing_top_n"},
            },
            asi_metadata={
                "gs_001": {"failure_type": "missing_top_n"},
                "gs_002": {"failure_type": "missing_top_n"},
            },
        )
        client = _make_cycling_client([
            _success_env("gs_001", "top_n_with_ordering", "add_example_sql"),
            _success_env("gs_002", "top_n_with_ordering", "add_example_sql"),
        ])
        with patch.object(
            optimizer, "_get_openai_client", return_value=client,
        ):
            collect(_StubCtx(), inp)
        assert budget.actual_input_tokens == 400
        assert budget.actual_output_tokens == 160
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_flag_off_uses_deterministic_only(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "0")
    inp = RcaEvidenceInput(
        eval_rows=(
            {"question_id": "gs_001", "genie_sql": "SELECT * FROM t"},
        ),
        hard_failure_qids=("gs_001",),
        soft_signal_qids=(),
        per_qid_judge={"gs_001": {"verdict": "wrong_join_spec"}},
        asi_metadata={"gs_001": {"failure_type": "wrong_join_spec"}},
    )
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        bundle = collect(_StubCtx(), inp)
    assert client.chat.completions.create.call_count == 0
    # Post Plan-8 T6: deterministic-only path stamps a typed
    # PerQidRcaEvidence built from asi_metadata so the typed sidecar
    # is non-empty even when no LLM call fires. The legacy
    # per_qid_evidence dict still gets populated as before.
    assert "gs_001" in bundle.per_qid_evidence_typed
    assert isinstance(
        bundle.per_qid_evidence_typed["gs_001"], PerQidRcaEvidence
    )
    assert "gs_001" in bundle.per_qid_evidence
