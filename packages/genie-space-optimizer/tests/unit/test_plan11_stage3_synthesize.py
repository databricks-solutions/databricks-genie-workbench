"""Plan 11 Stage 3 — run_plan11_synthesis_for_single_cluster() unit tests."""
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="top-N failures",
        member_qids=("gs_009",),
        unifying_evidence="RANK() doesn't bound row count",
        repair_hypothesis="Use ROW_NUMBER() and LIMIT 10",
        # Plan 12 — non-empty blame_set on the cluster so the Stage 3
        # survival contract (target_objects + blame_set required) passes
        # via synthesize.py's blame_set fallback + target_objects
        # derivation. The Plan 11 LLM doesn't emit target_objects today.
        primary_blame_set=("catalog.schema.orders.order_id",),
        confidence="high",
    )


def _make_success_response(patch_type: str = "add_example_sql") -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Add row-number example",
                    "intent_description": "Example showing ROW_NUMBER() + LIMIT pattern",
                    "repair_hypothesis": "Use ROW_NUMBER() and LIMIT 10",
                    "patch_type": patch_type,
                    "rationale": "Provides a concrete example of the correct pattern",
                    "confidence": "high",
                    "patch_body": {
                        "example_question": "Top 10 orders?",
                        "example_sql": "SELECT * FROM orders LIMIT 10",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=200,
        tokens_output=100,
        duration_ms=2100,
        error=None,
    )


def _make_decline_response() -> LlmReasoningResponse:
    from genie_space_optimizer.optimization.llm_abstain import (
        AbstainReason,
        AbstainVerdict,
    )
    verdict = AbstainVerdict(
        reason=AbstainReason.NO_APPLICABLE_PATCH_TYPE,
        explanation="cluster unclear",
        needed_evidence=(),
        suggested_next_step="re-cluster",
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=False,
        parsed_output=None,
        declined=verdict,
        raw_text="",
        tokens_input=100,
        tokens_output=20,
        duration_ms=800,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_synthesis_happy_path(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_success_response())

    result = run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001",
        w=MagicMock(),
    )

    assert result.skipped_reason == ""
    assert result.proposal is not None
    assert result.proposal["patch_type"] == "add_example_sql"
    mock_marker.assert_called_once()
    assert mock_marker.call_args[1]["outcome"] == "synthesized"
    assert mock_marker.call_args[1]["proposals_count"] == 1


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_synthesis_decline_returns_skipped(mock_marker, MockLlmCall):
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(return_value=_make_decline_response())

    result = run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001",
        w=MagicMock(),
    )

    assert result.proposal is None
    assert result.skipped_reason.startswith("exception:plan11_stage3_")
    assert mock_marker.call_args[1]["outcome"] == "declined"


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_synthesis_unknown_patch_type_drops_proposal(mock_marker, MockLlmCall):
    """LLM emitting an unknown patch_type → the proposal is dropped.
    Empty proposal list emits synth_none skipped_reason."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )
    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_success_response(patch_type="not_a_real_patch_type")
    )

    result = run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001",
        w=MagicMock(),
    )

    assert result.proposal is None
    assert result.skipped_reason == "synth_none"
    assert mock_marker.call_args[1]["outcome"] == "empty_synthesis"
    assert mock_marker.call_args[1]["proposals_count"] == 0
