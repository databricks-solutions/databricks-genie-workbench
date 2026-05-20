"""Plan 11 — repair_patch_with_llm() unit tests.

Covers: happy path on first attempt, exhaustion across attempts, and
LLM decline. Tests model the real ``LlmReasoningResponse`` shape (dict
``parsed_output``) rather than the MagicMock-attribute style.
"""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
    ValidationError,
    ValidationResult,
)


def _make_proposal(patch_type: PatchType = PatchType.ADD_EXAMPLE_SQL) -> RepairProposal:
    return RepairProposal(
        intent_id="intent_001",
        intent_name="test",
        intent_description="test",
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale="test",
        confidence="high",
        patch_body={"example_sql": "SELECT 1"},
        blame_set=(),
        repair_hypothesis="test",
        target_qids=("gs_009",),
    )


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="test",
        member_qids=("gs_009",),
        unifying_evidence="test",
        repair_hypothesis="test",
        primary_blame_set=(),
        confidence="high",
    )


def _schema_error() -> tuple[ValidationError, ...]:
    return (
        ValidationError(
            patch_id="intent_001",
            error_kind="genie_schema",
            error_detail="example_sql is required",
            failing_location="patch_body.example_sql",
        ),
    )


def _make_success_resp(patch_type: str = "add_example_sql") -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_repair.intent_001.attempt_1",
        skill_id="plan11_repair",
        succeeded=True,
        parsed_output={
            "intent_name": "test",
            "intent_description": "test",
            "repair_hypothesis": "test",
            "patch_type": patch_type,
            "rationale": "test",
            "confidence": "high",
            "patch_body": {
                "example_question": "Q?",
                "example_sql": "SELECT * FROM t LIMIT 10",
            },
            "blame_set": [],
            "target_qids": ["gs_009"],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=100,
        tokens_output=50,
        duration_ms=900,
        error=None,
    )


def _make_decline_resp() -> LlmReasoningResponse:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    verdict = AbstainVerdict(
        reason=AbstainReason.NO_APPLICABLE_PATCH_TYPE,
        explanation="cannot fix",
        needed_evidence=(),
        suggested_next_step="re-synthesize",
    )
    return LlmReasoningResponse(
        call_id="plan11_repair.intent_001.attempt_1",
        skill_id="plan11_repair",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text="",
        tokens_input=50,
        tokens_output=10,
        duration_ms=400,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.repair_loop.validate_patch")
@patch("genie_space_optimizer.optimization.stages.repair_loop.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.repair_loop.plan11_repair_loop_marker")
def test_repair_loop_converges_on_first_attempt(
    mock_marker, MockLlmCall, mock_validate
):
    from genie_space_optimizer.optimization.stages.repair_loop import (
        repair_patch_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_REPAIR_LOOP_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_success_resp())
    mock_validate.return_value = ValidationResult(
        patch_id="intent_001", is_valid=True, errors=()
    )

    result = repair_patch_with_llm(
        _make_proposal(),
        _schema_error(),
        _make_cluster(),
        w=MagicMock(),
        validate_kwargs={},
    )

    assert result is not None
    assert isinstance(result, RepairProposal)
    assert result.patch_type == PatchType.ADD_EXAMPLE_SQL
    assert mock_marker.call_count == 1
    assert mock_marker.call_args[1]["outcome"] == "repaired"


@patch("genie_space_optimizer.optimization.stages.repair_loop.validate_patch")
@patch("genie_space_optimizer.optimization.stages.repair_loop.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.repair_loop.plan11_repair_loop_marker")
@patch("genie_space_optimizer.optimization.stages.repair_loop.llm_contract_failure_marker")
def test_repair_loop_exhaustion_fires_contract_failure(
    mock_cf, mock_marker, MockLlmCall, mock_validate
):
    from genie_space_optimizer.optimization.stages.repair_loop import (
        repair_patch_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_REPAIR_LOOP_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    # LLM responds successfully on every attempt but validation always fails.
    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_success_resp())
    mock_validate.return_value = ValidationResult(
        patch_id="intent_001",
        is_valid=False,
        errors=_schema_error(),
    )

    result = repair_patch_with_llm(
        _make_proposal(),
        _schema_error(),
        _make_cluster(),
        w=MagicMock(),
        validate_kwargs={},
        max_attempts=2,
    )

    assert result is None
    mock_cf.assert_called_once()
    # With max_attempts=2: attempt 1 still_invalid, attempt 2 still_invalid,
    # attempt 3 exhausted. So 3 markers total.
    outcomes = [c[1]["outcome"] for c in mock_marker.call_args_list]
    assert outcomes == ["still_invalid", "still_invalid", "exhausted"]


@patch("genie_space_optimizer.optimization.stages.repair_loop.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.repair_loop.plan11_repair_loop_marker")
@patch("genie_space_optimizer.optimization.stages.repair_loop.llm_contract_failure_marker")
def test_repair_loop_llm_decline_fires_contract_failure(
    mock_cf, mock_marker, MockLlmCall
):
    from genie_space_optimizer.optimization.stages.repair_loop import (
        repair_patch_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_REPAIR_LOOP_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_decline_resp())

    result = repair_patch_with_llm(
        _make_proposal(),
        _schema_error(),
        _make_cluster(),
        w=MagicMock(),
    )

    assert result is None
    mock_cf.assert_called_once()
    assert mock_marker.call_count == 1
    assert mock_marker.call_args[1]["outcome"] == "llm_declined"


@patch("genie_space_optimizer.optimization.stages.repair_loop.validate_patch")
@patch("genie_space_optimizer.optimization.stages.repair_loop.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.repair_loop.plan11_repair_loop_marker")
def test_repair_loop_converges_on_second_attempt(
    mock_marker, MockLlmCall, mock_validate
):
    """LLM produces an invalid revision on attempt 1, but a valid one on
    attempt 2."""
    from genie_space_optimizer.optimization.stages.repair_loop import (
        repair_patch_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_REPAIR_LOOP_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_success_resp())
    # First validate fails, second passes.
    mock_validate.side_effect = [
        ValidationResult(patch_id="intent_001", is_valid=False, errors=_schema_error()),
        ValidationResult(patch_id="intent_001", is_valid=True, errors=()),
    ]

    result = repair_patch_with_llm(
        _make_proposal(),
        _schema_error(),
        _make_cluster(),
        w=MagicMock(),
        validate_kwargs={},
        max_attempts=2,
    )

    assert result is not None
    assert mock_marker.call_count == 2
    outcomes = [c[1]["outcome"] for c in mock_marker.call_args_list]
    assert outcomes == ["still_invalid", "repaired"]
