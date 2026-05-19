"""Plan 3 Task 9 — extract_evidence_for_qid (success path)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_extractor import (
    extract_evidence_for_qid,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=120, completion_tokens=80, total_tokens=200,
    )
    client.chat.completions.create.return_value = completion
    return client


def _success_envelope_for_top_n(qid: str = "gs_009") -> str:
    return json.dumps({
        "result": {
            "qid": qid,
            "observed_failure": "returned 1 row instead of top 3",
            "generated_sql_issue": "missing LIMIT 3 and ORDER BY revenue DESC",
            "expected_sql_shape": "GROUP BY 1 ORDER BY 2 DESC LIMIT 3",
            "blame_set": ["sales.fact_sales.revenue", "sales.fact_sales.product"],
            "suggested_repair_family": "top_n_with_ordering",
            "repair_hint_patch_type": "add_example_sql",
            "confidence": "high",
            "quoted_evidence": ["judge: 'expected 3 rows, got 1'"],
        },
        "declined": None,
    })


def test_extract_returns_typed_evidence_on_success() -> None:
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope_for_top_n()),
    ):
        result = extract_evidence_for_qid(
            w=None,
            qid="gs_009",
            judge={"verdict": "wrong_top_n_collapse"},
            asi={
                "failure_type": "missing_top_n",
                "wrong_clause": "missing_order_by_and_limit",
            },
            sql="SELECT product, SUM(revenue) FROM sales.fact_sales GROUP BY 1",
            iteration=2,
        )
    assert isinstance(result, PerQidRcaEvidence)
    assert result.qid == "gs_009"
    assert result.suggested_repair_family == "top_n_with_ordering"
    assert result.repair_hint_patch_type is PatchType.ADD_EXAMPLE_SQL
    assert result.confidence == "high"
    assert result.blame_set == (
        "sales.fact_sales.revenue", "sales.fact_sales.product",
    )


def test_extract_request_includes_qid_judge_asi_sql_in_prompt() -> None:
    """The rendered user prompt must contain all per-qid context."""
    captured_create_kwargs: list[dict] = []
    client = _stub_with(_success_envelope_for_top_n())

    def _spy_create(**kwargs):
        captured_create_kwargs.append(kwargs)
        return client.chat.completions.create.return_value

    client.chat.completions.create.side_effect = _spy_create
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        extract_evidence_for_qid(
            w=None,
            qid="gs_009",
            judge={"verdict": "wrong_top_n_collapse"},
            asi={
                "failure_type": "missing_top_n",
                "wrong_clause": "missing_order_by_and_limit",
            },
            sql="SELECT product FROM sales.fact_sales",
            iteration=2,
        )

    assert len(captured_create_kwargs) == 1
    messages = captured_create_kwargs[0]["messages"]
    user_msg = next(m["content"] for m in messages if m["role"] == "user")
    assert "gs_009" in user_msg
    assert "wrong_top_n_collapse" in user_msg
    assert "missing_top_n" in user_msg
    assert "sales.fact_sales" in user_msg


def test_extract_call_id_is_qid_and_iteration_scoped() -> None:
    """The framework records every call by call_id; the extractor MUST
    emit a call_id that names the qid + iteration so postmortems can
    join LLM calls to qids without prompt-SHA inference."""
    from genie_space_optimizer.optimization.rca_evidence_extractor import (
        _build_request_for_qid,
    )
    request = _build_request_for_qid(
        qid="gs_017",
        judge={"verdict": "wrong_filter"},
        asi={"failure_type": "wrong_filter_condition"},
        sql="SELECT * FROM t",
        iteration=4,
    )
    assert request.call_id == "rca_evidence.iter_4.gs_017"
    assert request.skill_id == "rca-evidence-extraction"
    assert request.max_tokens == 800
