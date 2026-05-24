"""Trial 13e — synthesize.py regression: the exact UPPER set the
workbench probe captured against the dc89 / 98ec bundles must now all
coerce successfully and produce non-zero proposals.

Pre-Trial-13e: 8/8 proposals dropped, ``empty_synthesis`` with
``all_candidates_unsafe`` and no signal naming the rejected raws.

Post-Trial-13e: 6/6 proposals accepted, ``outcome="synthesized"``,
``synthesis_rejected_patch_types`` empty.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="instruction-gap failures",
        member_qids=("gs_009", "gs_021", "gs_026"),
        unifying_evidence="missing per-customer revenue definition",
        repair_hypothesis="document KPI in instructions",
        primary_blame_set=("catalog.schema.orders.revenue",),
        confidence="high",
    )


def _proposal(patch_type: str, idx: int) -> dict:
    """Mirror the captured probe shape: every required field present so
    the only failure mode under test is patch_type casing."""
    return {
        "intent_name": f"intent_{idx}",
        "intent_description": "doc",
        "repair_hypothesis": "h",
        "patch_type": patch_type,
        "rationale": "r",
        "confidence": "medium",
        "patch_body": {
            "instruction_text": "## Notes\n- doc",
            "example_question": "q",
            "example_sql": "SELECT 1",
            "table": "catalog.schema.orders",
            "column": "revenue",
            "description": "d",
        },
        "blame_set": ["catalog.schema.orders.revenue"],
        "target_qids": ["gs_009"],
    }


def _upper_response() -> LlmReasoningResponse:
    """Exact UPPER set captured from the dc89/98ec probe runs:
    ``ADD_INSTRUCTION`` x3, ``ADD_EXAMPLE_SQL`` x2,
    ``ADD_COLUMN_DESCRIPTION`` x1.
    """
    raws = (
        ["ADD_INSTRUCTION"] * 3
        + ["ADD_EXAMPLE_SQL"] * 2
        + ["ADD_COLUMN_DESCRIPTION"] * 1
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [_proposal(pt, i) for i, pt in enumerate(raws)],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=400,
        tokens_output=200,
        duration_ms=2500,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_upper_snake_proposals_all_coerce_to_synthesized(
    mock_marker, MockLlmCall
) -> None:
    """The captured UPPER raws — every one of which would have been
    valid lower-case — now produce 6 surviving proposals and the marker
    reports ``outcome="synthesized"`` with no rejected raws."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(return_value=_upper_response())

    result = run_plan11_synthesis_for_single_cluster(
        cluster=_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_t13e",
        iteration=1,
        ag_id="ag_001",
        w=MagicMock(),
    )

    # 6 raws in, 6 surviving proposals out (no silent drops).
    assert mock_marker.called, "Stage 3 marker MUST be emitted"
    kwargs = mock_marker.call_args.kwargs
    assert kwargs["outcome"] == "synthesized"
    assert kwargs["proposals_count"] == 6
    # On the happy path we explicitly pass ``None`` (additive default).
    assert kwargs["synthesis_rejected_patch_types"] is None
    # The applied patch_types are the lower-case enum values.
    assert set(kwargs["patch_types"]) == {
        "add_instruction",
        "add_example_sql",
        "add_column_description",
    }
    # The returned envelope carries the first proposal (legacy contract).
    assert result.proposal is not None
