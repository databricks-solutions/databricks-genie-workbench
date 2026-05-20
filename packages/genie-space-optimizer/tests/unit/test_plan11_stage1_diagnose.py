"""Plan 11 Stage 1 — diagnose_failing_qids() unit tests."""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.stages.plan11_types import PerQidDiagnosis


def _make_qid_input(qid: str) -> dict:
    return {
        "qid": qid,
        "question_text": "Top 10 orders by revenue?",
        "ground_truth_sql": "SELECT * FROM orders ORDER BY revenue DESC LIMIT 10",
        "generated_sql": "SELECT * FROM orders WHERE revenue = (SELECT MAX(revenue) FROM orders)",
        "judge_rationale": "Generated SQL finds max revenue, not top 10",
        "blame_set_seed": ["catalog.schema.orders.revenue"],
    }


def _make_success_response(qid: str, rca_label: str) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=True,
        parsed_output={
            "diagnoses": [
                {
                    "qid": qid,
                    "rca_kind_label": rca_label,
                    "observed_failure": "Query returned wrong rows",
                    "generated_sql_issue": "Used MAX() instead of ORDER BY + LIMIT",
                    "expected_sql_shape": "ORDER BY revenue DESC LIMIT 10",
                    "blame_set": ["catalog.schema.orders.revenue"],
                    "evidence_summary": "The generated SQL finds max value, not top-10 rows",
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


def _make_decline_response() -> LlmReasoningResponse:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    verdict = AbstainVerdict(
        reason=AbstainReason.MISSING_SCHEMA_CONTEXT,
        explanation="too thin",
        needed_evidence=(),
        suggested_next_step="retry next iteration",
    )
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text="",
        tokens_input=80,
        tokens_output=20,
        duration_ms=900,
        error=None,
    )


def _make_contract_failure_response() -> LlmReasoningResponse:
    """Pydantic validation passed but emit was empty — should fire contract_failure."""
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=True,
        parsed_output={"diagnoses": []},
        declined=None,
        raw_text="{}",
        tokens_input=50,
        tokens_output=10,
        duration_ms=500,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker")
def test_diagnose_happy_path(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_success_response("gs_009", "top-N collapsed")
    )

    results = diagnose_failing_qids(
        failing_qids=[_make_qid_input("gs_009")],
        schema_columns=["catalog.schema.orders.revenue"],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert len(results) == 1
    assert isinstance(results[0], PerQidDiagnosis)
    assert results[0].qid == "gs_009"
    assert results[0].rca_kind_label == "top-N collapsed"
    assert results[0].confidence == "high"
    mock_marker.assert_called_once()
    call_kwargs = mock_marker.call_args[1]
    assert call_kwargs["outcome"] == "diagnosed"


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker")
def test_diagnose_llm_decline_emits_declined_marker(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_decline_response())

    results = diagnose_failing_qids(
        failing_qids=[_make_qid_input("gs_009")],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )

    assert results == []
    mock_marker.assert_called_once()
    call_kwargs = mock_marker.call_args[1]
    assert call_kwargs["outcome"] == "declined"


@patch("genie_space_optimizer.optimization.stages.diagnose.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.diagnose.plan11_stage1_diagnosis_marker")
@patch("genie_space_optimizer.optimization.stages.diagnose.llm_contract_failure_marker")
def test_diagnose_empty_output_fires_contract_failure(
    mock_cf, mock_marker, MockLlmCall
):
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE1_DIAGNOSIS_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_contract_failure_response()
    )

    results = diagnose_failing_qids(
        failing_qids=[_make_qid_input("gs_009")],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )
    assert results == []
    mock_cf.assert_called_once()
    assert mock_marker.call_args[1]["outcome"] == "contract_failure"


def test_diagnose_empty_input_returns_empty():
    """No failing_qids → no LLM call → []."""
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
    results = diagnose_failing_qids(
        failing_qids=[],
        schema_columns=[],
        optimization_run_id="run_x",
        iteration=1,
        w=MagicMock(),
    )
    assert results == []
