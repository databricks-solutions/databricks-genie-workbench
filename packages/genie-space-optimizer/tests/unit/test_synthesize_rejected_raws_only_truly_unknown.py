"""Trial 13e — synthesize.py mixed-vocabulary behavior.

When the LLM emits a mix of one valid proposal + one genuinely unknown
``patch_type`` (e.g. ``"add_nothing"``), the synthesis loop must:

* keep the valid proposal (``outcome="synthesized"``),
* track the unknown raw in ``rejected_patch_types_raw`` (but the marker
  receives ``None`` because the outcome is not ``empty_synthesis``),
* never raise — silent drop of the bad item is legitimate here because
  the survivor carries the cluster forward.

If we ever flip to ``outcome="empty_synthesis"`` because every proposal
hallucinated outside the enum, the marker contract guarantees the
rejected raws will be on the wire (covered by
``test_plan11_stage3_empty_synthesis_typed_reason.py``).
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
        semantic_theme="mixed-vocabulary failures",
        member_qids=("gs_009",),
        unifying_evidence="e",
        repair_hypothesis="h",
        primary_blame_set=("catalog.schema.orders.revenue",),
        confidence="medium",
    )


def _mixed_response() -> LlmReasoningResponse:
    """One valid proposal + one hallucinated value."""
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "good_one",
                    "intent_description": "ok",
                    "repair_hypothesis": "h",
                    "patch_type": "add_instruction",
                    "rationale": "r",
                    "confidence": "high",
                    "patch_body": {"instruction_text": "## Notes\n- doc"},
                    "blame_set": ["catalog.schema.orders.revenue"],
                    "target_qids": ["gs_009"],
                    # Trial 22 required-assets gate drops solo levers with
                    # no justification; this test pins the valid-survivor
                    # "synthesized" path, so the survivor must be grounded.
                    "single_lever_justification": "judge cited revenue gap",
                },
                {
                    "intent_name": "bad_one",
                    "intent_description": "hallucinated patch_type",
                    "repair_hypothesis": "h",
                    "patch_type": "add_nothing",  # not a PatchType member
                    "rationale": "r",
                    "confidence": "low",
                    "patch_body": {},
                    "blame_set": [],
                    "target_qids": [],
                },
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=200,
        tokens_output=100,
        duration_ms=1500,
        error=None,
    )


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_mixed_valid_and_unknown_yields_synthesized_with_survivor(
    mock_marker, MockLlmCall
) -> None:
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(return_value=_mixed_response())

    result = run_plan11_synthesis_for_single_cluster(
        cluster=_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_t13e",
        iteration=1,
        ag_id="ag_001",
        w=MagicMock(),
    )

    assert mock_marker.called
    kwargs = mock_marker.call_args.kwargs
    assert kwargs["outcome"] == "synthesized"
    assert kwargs["proposals_count"] == 1
    assert kwargs["patch_types"] == ["add_instruction"]
    # On the synthesized path the marker is passed ``None`` regardless
    # of whether the loop tracked any rejected raws — the canary only
    # surfaces on ``empty_synthesis``.
    assert kwargs["synthesis_rejected_patch_types"] is None
    assert result.proposal is not None


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_all_unknown_yields_empty_synthesis_with_rejected_map(
    mock_marker, MockLlmCall
) -> None:
    """If every emitted proposal hallucinates outside the enum, the
    marker must receive a populated rejected map so the canary fires."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=LlmReasoningResponse(
            call_id="plan11_stage3_synthesize.H001.iter_1",
            skill_id="plan11_synthesize",
            succeeded=True,
            parsed_output={
                "proposals": [
                    {
                        "intent_name": "bad",
                        "intent_description": "",
                        "repair_hypothesis": "",
                        "patch_type": "add_nothing",
                        "rationale": "",
                        "confidence": "low",
                        "patch_body": {},
                        "blame_set": [],
                        "target_qids": [],
                    },
                    {
                        "intent_name": "worse",
                        "intent_description": "",
                        "repair_hypothesis": "",
                        "patch_type": "do_the_thing",
                        "rationale": "",
                        "confidence": "low",
                        "patch_body": {},
                        "blame_set": [],
                        "target_qids": [],
                    },
                ],
            },
            declined=None,
            raw_text="{...}",
            tokens_input=100,
            tokens_output=50,
            duration_ms=900,
            error=None,
        )
    )

    run_plan11_synthesis_for_single_cluster(
        cluster=_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_t13e",
        iteration=1,
        ag_id="ag_001",
        w=MagicMock(),
    )

    assert mock_marker.called
    kwargs = mock_marker.call_args.kwargs
    assert kwargs["outcome"] == "empty_synthesis"
    assert kwargs["synthesis_empty_reason"] == "all_candidates_unsafe"
    rejected = kwargs["synthesis_rejected_patch_types"]
    assert isinstance(rejected, dict)
    assert rejected == {"add_nothing": 1, "do_the_thing": 1}
