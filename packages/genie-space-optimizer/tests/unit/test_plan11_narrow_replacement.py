"""Plan 11 — narrow_replacement_with_llm() unit tests.

Covers: happy path on first attempt, LLM decline, and exhaustion.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster


def _make_proposal() -> RepairProposal:
    return RepairProposal(
        intent_id="intent_002",
        intent_name="broad filter",
        intent_description="desc",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.UPDATE_FILTER_CONDITION,
        rationale="fix",
        confidence="high",
        patch_body={"table": "orders", "sql_expression": "status = 'active'"},
        blame_set=(),
        repair_hypothesis="filter fix",
        target_qids=("gs_024",),
    )


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H002",
        semantic_theme="filter mismatch",
        member_qids=("gs_024",),
        unifying_evidence="wrong filter",
        repair_hypothesis="fix the filter",
        primary_blame_set=(),
        confidence="high",
    )


def _make_success_resp() -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_narrow.intent_002.attempt_1",
        skill_id="plan11_narrow",
        succeeded=True,
        parsed_output={
            "intent_name": "narrowed filter",
            "intent_description": "desc",
            "repair_hypothesis": "scoped fix",
            "patch_type": "add_example_sql",
            "rationale": "narrowed to specific question",
            "confidence": "medium",
            "patch_body": {
                "example_question": "active orders?",
                "example_sql": "SELECT * FROM orders WHERE status='active' LIMIT 10",
            },
            "blame_set": [],
            "target_qids": ["gs_024"],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=150,
        tokens_output=70,
        duration_ms=1100,
        error=None,
    )


def _make_decline_resp() -> LlmReasoningResponse:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    verdict = AbstainVerdict(
        reason=AbstainReason.UNSAFE_PATCH,
        explanation="cannot narrow further",
        needed_evidence=(),
        suggested_next_step="re-synthesize",
    )
    return LlmReasoningResponse(
        call_id="plan11_narrow.intent_002.attempt_1",
        skill_id="plan11_narrow",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text="",
        tokens_input=50,
        tokens_output=10,
        duration_ms=400,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.narrow_replacement.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.narrow_replacement.plan11_narrow_replacement_marker")
def test_narrow_happy_path(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_NARROW_REPLACEMENT_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_success_resp())

    result = narrow_replacement_with_llm(
        _make_proposal(),
        collateral_qids=("gs_003", "gs_007"),
        protected_sql={
            "gs_003": "SELECT * FROM orders",
            "gs_007": "SELECT count(*) FROM orders",
        },
        cluster=_make_cluster(),
        w=MagicMock(),
    )

    assert result is not None
    assert isinstance(result, RepairProposal)
    assert result.patch_type == PatchType.ADD_EXAMPLE_SQL  # narrowed from broad → surgical
    assert mock_marker.call_count == 1
    assert mock_marker.call_args[1]["outcome"] == "narrowed"


@patch("genie_space_optimizer.optimization.stages.narrow_replacement.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.narrow_replacement.plan11_narrow_replacement_marker")
@patch("genie_space_optimizer.optimization.stages.narrow_replacement.llm_contract_failure_marker")
def test_narrow_llm_decline_returns_none(mock_cf, mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_NARROW_REPLACEMENT_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_decline_resp())

    result = narrow_replacement_with_llm(
        _make_proposal(),
        collateral_qids=("gs_003",),
        protected_sql={"gs_003": "SELECT 1"},
        cluster=_make_cluster(),
        w=MagicMock(),
        max_attempts=1,
    )

    assert result is None
    mock_cf.assert_called_once()
    assert mock_marker.call_count == 1
    assert mock_marker.call_args[1]["outcome"] == "llm_declined"


@patch("genie_space_optimizer.optimization.stages.narrow_replacement.LlmReasoningCall")
@patch("genie_space_optimizer.optimization.stages.narrow_replacement.plan11_narrow_replacement_marker")
@patch("genie_space_optimizer.optimization.stages.narrow_replacement.llm_contract_failure_marker")
def test_narrow_exhaustion_after_max_attempts(mock_cf, mock_marker, MockLlmCall):
    """Force exhaustion by calling with attempt > max_attempts."""
    from genie_space_optimizer.optimization.stages.narrow_replacement import (
        narrow_replacement_with_llm,
    )
    mock_marker.return_value = "GSO_PLAN11_NARROW_REPLACEMENT_V1 {}"
    mock_cf.return_value = "GSO_LLM_CONTRACT_FAILURE_V1 {}"

    # Calling with attempt=3 and max_attempts=2 → immediate exhaustion path.
    result = narrow_replacement_with_llm(
        _make_proposal(),
        collateral_qids=("gs_003",),
        protected_sql={"gs_003": "SELECT 1"},
        cluster=_make_cluster(),
        w=MagicMock(),
        attempt=3,
        max_attempts=2,
    )

    assert result is None
    assert mock_marker.call_count == 1
    assert mock_marker.call_args[1]["outcome"] == "exhausted"
    mock_cf.assert_called_once()
