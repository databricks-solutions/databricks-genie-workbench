"""Trial 17 step 2 — pin the plan-vs-proposal consistency wiring in
``stages.synthesize.run_plan11_synthesis_for_single_cluster``.

The Stage 3 LLM is asked to emit a ``selected_lever`` for every
proposal. The deterministic validator drops proposals whose
``(selected_lever, patch_type)`` pair is inconsistent with
``LEVER_TO_PATCH_TYPES``. Surviving proposals are stamped with the
Trial 17 fields so downstream gates can use them.

This test stubs the LLM call to avoid network and pins:

- Consistent proposals survive and carry ``selected_lever`` etc.
- Inconsistent proposals are dropped (counted in
  ``rejected_patch_types_raw``) and never reach
  ``RepairProposal.from_*`` constructors.
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _stub_reasoning_response(proposals: list[dict]):
    """Return a stub object shaped like ``LlmReasoningCall.invoke`` would."""

    class _Resp:
        succeeded = True
        declined = None
        parsed_output = {"proposals": proposals}
        tokens_input = 100
        tokens_output = 100

    return _Resp()


def _cluster(cluster_id: str = "H_X") -> FailureCluster:
    return FailureCluster(
        cluster_id=cluster_id,
        semantic_theme="top_n_cardinality_collapse",
        member_qids=("gs_009",),
        unifying_evidence="all members fail at top-N grammar",
        repair_hypothesis="introduce ORDER BY DESC LIMIT N example sql",
        primary_blame_set=("main.demo.t.col",),
        confidence="high",
    )


def _invoke_with_proposals(proposals: list[dict]):
    """Drive ``run_plan11_synthesis_for_single_cluster`` against a
    stubbed LLM reasoning response.
    """
    with mock_patch.object(
        syn.LlmReasoningCall,
        "invoke",
        return_value=_stub_reasoning_response(proposals),
    ):
        return syn.run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            member_qid_evidence=[
                {"qid": "gs_009", "blame_set": ["main.demo.t.col"]},
            ],
            optimization_run_id="r17",
            iteration=2,
            ag_id="AG_X",
            w=None,
        )


def test_trial17_consistent_proposal_survives_and_carries_lever():
    """A lever-5 + add_instruction proposal is consistent — survives."""
    result = _invoke_with_proposals(
        [
            {
                "intent_name": "top_n_pivot",
                "intent_description": "fix top-N grammar",
                "repair_hypothesis": "use ORDER BY DESC LIMIT",
                "patch_type": "add_instruction",
                "rationale": "empirical from gs_009",
                "confidence": "high",
                "patch_body": {
                    "instruction_text": "Use ORDER BY revenue DESC LIMIT N"
                },
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_009"],
                "selected_lever": "lever-5",
                "expected_behavioral_change": "ORDER BY DESC LIMIT instead of MAX()",
                "fallback_lever": "lever-6",
                "bundle_id": "",
            }
        ]
    )

    assert result.proposal is not None
    assert result.skipped_reason == ""
    # The first surviving proposal is reflected in ``result.proposal``
    # as a to_json() dict (legacy contract).
    payload = result.proposal
    assert payload["selected_lever"] == "lever-5"
    assert payload["fallback_lever"] == "lever-6"
    assert "ORDER BY" in payload["expected_behavioral_change"]


def test_trial17_inconsistent_proposal_is_dropped():
    """A lever-1 + add_instruction pair is inconsistent — must drop
    (add_instruction is lever-5 prose). The cluster result is the
    ``empty_synthesis`` shape because no surviving proposal remains.
    """
    result = _invoke_with_proposals(
        [
            {
                "intent_name": "wrong_lever",
                "intent_description": "wrong lever choice",
                "repair_hypothesis": "x",
                "patch_type": "add_instruction",
                "rationale": "x",
                "confidence": "low",
                "patch_body": {"instruction_text": "x"},
                "blame_set": [],
                "target_qids": ["gs_009"],
                "selected_lever": "lever-1",  # inconsistent: lever-1 is metadata
                "expected_behavioral_change": "",
                "fallback_lever": "",
                "bundle_id": "",
            }
        ]
    )
    assert result.proposal is None
    assert result.skipped_reason != ""


def test_trial17_legacy_proposal_without_selected_lever_survives():
    """Pre-Trial-17 prompts emit no ``selected_lever`` — the
    permissive validator returns None for empty levers, so the legacy
    path keeps working (back-compat)."""
    result = _invoke_with_proposals(
        [
            {
                "intent_name": "legacy",
                "intent_description": "legacy proposal without lever",
                "repair_hypothesis": "x",
                "patch_type": "add_instruction",
                "rationale": "x",
                "confidence": "low",
                "patch_body": {"instruction_text": "x"},
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_009"],
            }
        ]
    )
    assert result.proposal is not None
    assert result.proposal["selected_lever"] == ""


def test_trial17_mixed_batch_drops_only_inconsistent():
    """Two proposals, one consistent + one inconsistent — only the
    consistent one survives."""
    result = _invoke_with_proposals(
        [
            {
                "intent_name": "good",
                "intent_description": "good",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "high",
                "patch_body": {
                    "table": "main.demo.t",
                    "column": "col",
                    "description": "desc",
                },
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_009"],
                "selected_lever": "lever-1",
            },
            {
                "intent_name": "bad",
                "intent_description": "bad",
                "repair_hypothesis": "x",
                "patch_type": "add_instruction",
                "rationale": "x",
                "confidence": "low",
                "patch_body": {"instruction_text": "x"},
                "blame_set": [],
                "target_qids": ["gs_009"],
                "selected_lever": "lever-1",  # inconsistent
            },
        ]
    )
    assert result.proposal is not None
    payload = result.proposal
    assert payload["patch_type"] == PatchType.ADD_COLUMN_DESCRIPTION.value
    assert payload["selected_lever"] == "lever-1"
