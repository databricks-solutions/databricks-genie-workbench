"""Trial 30 W30.2(a) — member_qids carry-forward into target_qids_union.

Mirrors the cluster + LLM-response fixture from
``tests/unit/test_plan11_stage3_synthesize.py``. The cluster's
``member_qids`` include a rerouted QID (``gs_009``) that the surviving
proposal's ``target_qids`` omit (the W29.4 ``gs_009`` drop). With the
harvest-wire flag ON, the synthesized ``target_qids_union`` must still
carry ``gs_009``; with the flag OFF, the union is byte-stable legacy
(proposal ``target_qids`` only).

The ``target_qids_union`` is captured via the mocked
``plan11_stage3_synthesis_marker`` kwarg (same patch boundary the
existing Stage 3 synthesize tests use), which is emitted from the
synthesized branch *before* any post-marker survival-contract filtering.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningResponse
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster


def _make_cluster() -> FailureCluster:
    # gs_009 is a member but is NOT in the surviving proposal's
    # target_qids below — it is the rerouted QID the LLM omitted.
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="top-N failures",
        member_qids=("gs_009", "gs_010"),
        unifying_evidence="RANK() doesn't bound row count",
        repair_hypothesis="Use ROW_NUMBER() and LIMIT 10",
        primary_blame_set=("catalog.schema.orders.order_id",),
        confidence="high",
    )


def _make_success_response(
    patch_type: str = "add_example_sql",
) -> LlmReasoningResponse:
    # Surviving proposal targets gs_010 only — gs_009 is dropped.
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
                    "target_qids": ["gs_010"],
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


def _run_and_capture_union(mock_marker, MockLlmCall):
    """Run the single-cluster synthesis and return the
    ``target_qids_union`` passed to the (mocked) marker."""
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    mock_marker.return_value = "GSO_PLAN11_STAGE3_SYNTHESIS_V1 {}"
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_make_success_response()
    )

    run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001",
        w=MagicMock(),
    )

    # The synthesized branch emits outcome="synthesized".
    assert mock_marker.call_args[1]["outcome"] == "synthesized"
    return list(mock_marker.call_args[1]["target_qids_union"])


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_member_qid_carried_into_union(
    mock_marker, MockLlmCall, monkeypatch
):
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")

    union = _run_and_capture_union(mock_marker, MockLlmCall)

    # gs_010 from the proposal AND gs_009 from cluster.member_qids.
    assert "gs_010" in union
    assert "gs_009" in union


@patch("genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall")
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.plan11_stage3_synthesis_marker"
)
def test_flag_off_preserves_legacy_union(
    mock_marker, MockLlmCall, monkeypatch
):
    # Sub-flag OFF: legacy behaviour — only the proposal target_qids.
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "0")

    union = _run_and_capture_union(mock_marker, MockLlmCall)

    assert "gs_010" in union
    assert "gs_009" not in union
