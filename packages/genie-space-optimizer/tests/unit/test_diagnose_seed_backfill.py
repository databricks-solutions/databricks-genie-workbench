"""Trial 13h — Stage 1 seed backfill unit tests.

When ``diagnose_failing_qids`` parses the LLM output, the post-schema-filter
``valid_blame`` can be empty in two ways:

1. LLM literally emitted ``blame_set: []``.
2. LLM emitted entries that all failed the ``schema_columns`` filter.

In both cases the pre-13h code constructed ``PerQidDiagnosis`` with
``blame_set=()`` and the non-actionable gate correctly terminated the QID
as ``zero_blame_set``. Trial 13h backfills from the ``blame_set_seed``
already attached to the input card (guaranteed non-empty by the Stage 1
input contract) whenever ``valid_blame`` is empty, and exposes the source
via the ``blame_set_source`` marker field.

These tests pin the four cases of the backfill contract via the
mocked-marker pattern used by ``test_plan11_stage1_diagnose.py``.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse


_SCHEMA = [
    "catalog.schema.orders.revenue",
    "catalog.schema.orders.payment_amt",
]


def _qid_card(qid: str, seed: list[str]) -> dict:
    return {
        "qid": qid,
        "question_text": "Top 10 orders by revenue?",
        "ground_truth_sql": "SELECT * FROM orders ORDER BY revenue DESC LIMIT 10",
        "generated_sql": "SELECT * FROM orders",
        "judge_rationale": "Generated SQL is missing the ORDER BY + LIMIT",
        "blame_set_seed": seed,
    }


def _llm_response(qid: str, blame_set: list[str]) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=True,
        parsed_output={
            "diagnoses": [
                {
                    "qid": qid,
                    "rca_kind_label": "top-N collapsed",
                    "observed_failure": "Query returned wrong rows",
                    "generated_sql_issue": "Missing ORDER BY + LIMIT",
                    "expected_sql_shape": "ORDER BY revenue DESC LIMIT 10",
                    "blame_set": blame_set,
                    "evidence_summary": "Top-N pattern not honored",
                    "confidence": "high",
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=100,
        tokens_output=50,
        duration_ms=1234,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker"
)
def test_llm_emits_schema_valid_blame_no_backfill(mock_marker, MockLlmCall):
    """Healthy path — LLM emits a schema-valid entry, backfill stays dormant."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_llm_response("gs_001", ["catalog.schema.orders.revenue"])
    )

    results = diagnose_failing_qids(
        failing_qids=[_qid_card("gs_001", ["catalog.schema.orders.payment_amt"])],
        schema_columns=_SCHEMA,
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    assert results[0].blame_set == ("catalog.schema.orders.revenue",)
    kw = mock_marker.call_args[1]
    assert kw["blame_set_source"] == "llm"
    assert kw["blame_set_llm_emitted"] == 1
    assert kw["blame_set_post_schema_dropped"] == 0
    assert kw["blame_set_size"] == 1


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker"
)
def test_llm_emits_empty_blame_seed_backfills(mock_marker, MockLlmCall):
    """Trial 13h primary fix — LLM omits blame_set, seed fills in."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_llm_response("gs_009", [])
    )

    results = diagnose_failing_qids(
        failing_qids=[_qid_card("gs_009", ["catalog.schema.orders.payment_amt"])],
        schema_columns=_SCHEMA,
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    assert results[0].blame_set == ("catalog.schema.orders.payment_amt",)
    kw = mock_marker.call_args[1]
    assert kw["blame_set_source"] == "seed_backfill"
    assert kw["blame_set_llm_emitted"] == 0
    assert kw["blame_set_post_schema_dropped"] == 0
    assert kw["blame_set_size"] == 1


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker"
)
def test_llm_emits_only_hallucinated_blame_seed_backfills(mock_marker, MockLlmCall):
    """LLM emits entries that all fail schema filter — seed still rescues."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_llm_response(
            "gs_009",
            [
                "catalog.schema.hallucinated_table.foo",
                "catalog.schema.also_made_up.bar",
            ],
        )
    )

    results = diagnose_failing_qids(
        failing_qids=[_qid_card("gs_009", ["catalog.schema.orders.revenue"])],
        schema_columns=_SCHEMA,
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    assert results[0].blame_set == ("catalog.schema.orders.revenue",)
    kw = mock_marker.call_args[1]
    assert kw["blame_set_source"] == "seed_backfill"
    # Both hallucinated entries were emitted and both were dropped.
    assert kw["blame_set_llm_emitted"] == 2
    assert kw["blame_set_post_schema_dropped"] == 2
    assert kw["blame_set_size"] == 1


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker"
)
def test_llm_empty_and_seed_empty_stays_empty(mock_marker, MockLlmCall):
    """Both LLM and seed are empty — final blame stays empty (will be rejected
    downstream by classify_non_actionable_reason as zero_blame_set; the
    backfill safety net is intentionally not a hallucination factory)."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_llm_response("gs_999", [])
    )

    results = diagnose_failing_qids(
        failing_qids=[_qid_card("gs_999", [])],
        schema_columns=_SCHEMA,
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    assert results[0].blame_set == ()
    kw = mock_marker.call_args[1]
    assert kw["blame_set_source"] == "empty"
    assert kw["blame_set_llm_emitted"] == 0
    assert kw["blame_set_post_schema_dropped"] == 0
    assert kw["blame_set_size"] == 0


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker"
)
def test_seed_filtered_by_schema_columns_before_backfill(mock_marker, MockLlmCall):
    """Edge case — the seed itself contains entries outside schema_columns.
    The backfill must apply the same schema-validity check before filling
    in, so we don't relax the schema_columns contract via the seed path."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_llm_response("gs_009", [])
    )

    seed = [
        "catalog.schema.stale_table.gone",  # not in schema
        "catalog.schema.orders.revenue",  # in schema
        "  ",  # whitespace, ignored by lookup helper
    ]

    results = diagnose_failing_qids(
        failing_qids=[_qid_card("gs_009", seed)],
        schema_columns=_SCHEMA,
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    # Only the schema-valid seed entry survives.
    assert results[0].blame_set == ("catalog.schema.orders.revenue",)
    kw = mock_marker.call_args[1]
    assert kw["blame_set_source"] == "seed_backfill"


def test_lookup_seed_for_qid_helper_directly() -> None:
    """Pin the helper contract: returns the seed for the matching qid,
    skips whitespace entries, returns () when qid is not found."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        _lookup_seed_for_qid,
    )

    cards = [
        {"qid": "a", "blame_set_seed": ["c.s.t.x", "  ", "c.s.t.y"]},
        {"qid": "b", "blame_set_seed": []},
        {"qid": "c"},  # missing key entirely
    ]
    assert _lookup_seed_for_qid(cards, "a") == ("c.s.t.x", "c.s.t.y")
    assert _lookup_seed_for_qid(cards, "b") == ()
    assert _lookup_seed_for_qid(cards, "c") == ()
    assert _lookup_seed_for_qid(cards, "missing") == ()
