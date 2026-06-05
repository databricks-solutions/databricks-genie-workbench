"""Trial 24 replay-readiness — generalized instruction grounding.

Follow-on B's FB2 only grounds a solo corrective ``add_instruction``'s
justification when the cluster ``rca_kind`` is in the Trial 24 forced-kit
map (``extra_defensive_filter`` / ``top_n_cardinality_collapse``). The
live e943 fix therefore covers only those two RCAs.

``GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING`` widens the FB2 fallback
(``single_lever_justification`` -> ``expected_behavioral_change`` ->
``rationale``) to ANY ``INSTRUCTION_TEXT`` proposal regardless of
``rca_kind`` so a grounded solo corrective instruction lands across a
broader multi-RCA replay.

This test exercises the SYNTHESIS-side grounding directly: a lone
``add_instruction`` on a NON-allowlist RCA (``ambiguous_terminology`` —
in neither ``KIT_FOR_RCA`` nor ``_TRIAL24_KIT_FOR_RCA``, so it is NOT
forced into a kit and a single lever is admissible) with an EMPTY
``single_lever_justification`` but a populated
``expected_behavioral_change``:

  * general grounding ON  -> grounded from ``expected_behavioral_change``
    -> survives ``_check_required_assets``.
  * general grounding OFF -> ungrounded -> drops
    ``unjustified_single_lever`` (the e943 death mode).
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _stub_reasoning_response(proposals: list[dict]):
    class _Resp:
        succeeded = True
        declined = None
        parsed_output = {"proposals": proposals}
        tokens_input = 100
        tokens_output = 100

    return _Resp()


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H_T24_GEN",
        semantic_theme="ambiguous_terminology",
        member_qids=("gs_013",),
        unifying_evidence="users mean net revenue but 'revenue' is ambiguous",
        repair_hypothesis="instruct planner to treat revenue as net revenue",
        primary_blame_set=("main.demo.t.col",),
        confidence="high",
        # NON-allowlist RCA: not in KIT_FOR_RCA nor _TRIAL24_KIT_FOR_RCA,
        # so no kit is forced and a single lever is admissible. The only
        # gate that can drop the lone instruction is required_assets.
        root_cause="ambiguous_terminology",
    )


_SCHEMA = {
    "data_sources": {
        "tables": [
            {
                "identifier": "main.demo.t",
                "column_configs": [{"column_name": "col"}],
            },
        ],
        "metric_views": [],
    },
}


def _solo_instruction_proposal() -> dict:
    # Lone corrective add_instruction (lever-5). EMPTY
    # single_lever_justification (the e943 shape) but a populated
    # expected_behavioral_change that the general grounding falls back to.
    return {
        "intent_name": "define revenue as net revenue",
        "intent_description": "disambiguate 'revenue'",
        "repair_hypothesis": "treat revenue as net revenue",
        "patch_type": "add_instruction",
        "rationale": "judge cited ambiguous revenue terminology",
        "confidence": "high",
        "patch_body": {
            "instruction_text": "When users say 'revenue', use net revenue.",
        },
        "blame_set": ["main.demo.t.col"],
        "target_qids": ["gs_013"],
        "selected_lever": "lever-5",
        "single_lever_justification": "",
        "expected_behavioral_change": (
            "Planner resolves 'revenue' to net revenue instead of gross."
        ),
    }


def _invoke():
    with mock_patch.object(
        syn.LlmReasoningCall,
        "invoke",
        return_value=_stub_reasoning_response([_solo_instruction_proposal()]),
    ):
        return syn.run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice=_SCHEMA,
            history=[],
            member_qid_evidence=[
                {"qid": "gs_013", "blame_set": ["main.demo.t.col"]},
            ],
            optimization_run_id="r24gen",
            iteration=2,
            ag_id="AG_T24_GEN",
            w=None,
        )


def test_general_grounding_lands_solo_instruction_on_non_allowlist_rca(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.delenv(
        "GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING", raising=False
    )
    result = _invoke()
    assert result.proposal is not None, (
        "a grounded solo instruction on a non-allowlist RCA must land "
        f"with general grounding ON; skipped_reason={result.skipped_reason}, "
        f"drops={getattr(result, 'compiler_drop_summary', None)}"
    )


def test_general_grounding_off_drops_ungrounded_solo_instruction(
    monkeypatch,
) -> None:
    # Master on, general grounding explicitly OFF, and the RCA is not in
    # the narrow FB2 allowlist -> no grounding -> the lone instruction
    # drops as unjustified_single_lever (the e943 death mode).
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.setenv("GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING", "0")
    result = _invoke()
    assert result.proposal is None, (
        "with general grounding OFF and a non-allowlist RCA, the "
        "ungrounded lone instruction must drop"
    )
