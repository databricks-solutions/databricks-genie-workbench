"""Trial 23 W8 — pivot with a destination unit tests.

Trial 20 D3 refuses to repeat a sole-lever proposal whose family was
already kept_insufficient, but the refusal emptied the slate to
``stage3_returned_none``. W8 gives the refusal a destination: when the
drop would empty the slate, ONE replacement re-prompt demands a
multi-lever bundle and the result is re-normalized through the full
gate pipeline.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization import pivot_destination as pd


# ---- slate_emptied_by_sole_lever -----------------------------------

def test_emptied_true_when_all_dropped_with_rejected_family():
    assert pd.slate_emptied_by_sole_lever(
        proposals_before=1,
        proposals_after=0,
        rejected_families=["lever-5"],
    ) is True


def test_emptied_false_when_survivors_remain():
    assert pd.slate_emptied_by_sole_lever(
        proposals_before=2,
        proposals_after=1,
        rejected_families=["lever-5"],
    ) is False


def test_emptied_false_when_nothing_to_drop():
    assert pd.slate_emptied_by_sole_lever(
        proposals_before=0,
        proposals_after=0,
        rejected_families=["lever-5"],
    ) is False


def test_emptied_false_without_rejected_family():
    assert pd.slate_emptied_by_sole_lever(
        proposals_before=1,
        proposals_after=0,
        rejected_families=[],
    ) is False
    assert pd.slate_emptied_by_sole_lever(
        proposals_before=1,
        proposals_after=0,
        rejected_families=["", "  "],
    ) is False


# ---- build_pivot_directive -----------------------------------------

def test_directive_names_rejected_family_and_demands_bundle():
    d = pd.build_pivot_directive(
        rejected_families=["lever-5"],
        cluster_id="H001",
        root_cause="extra_defensive_filter",
    )
    assert "lever-5" in d
    assert "MULTI-LEVER BUNDLE" in d
    assert "bundle_id" in d
    assert "extra_defensive_filter" in d
    assert "H001" in d


def test_directive_deduplicates_and_sorts_families():
    d = pd.build_pivot_directive(
        rejected_families=["lever-6", "lever-5", "lever-5"],
        cluster_id="H002",
    )
    assert "lever-5, lever-6" in d


# ---- marker ---------------------------------------------------------

def test_marker_anti_success_only_on_emptied():
    landed = pd.pivot_destination_marker(
        optimization_run_id="run_x",
        iteration=2,
        cluster_id="H001",
        rejected_families=["lever-5"],
        outcome="pivot_landed",
        replacement_proposals=1,
    )
    emptied = pd.pivot_destination_marker(
        optimization_run_id="run_x",
        iteration=2,
        cluster_id="H001",
        rejected_families=["lever-5"],
        outcome="pivot_emptied_slate",
    )
    assert landed.startswith("GSO_TRIAL23_PIVOT_DESTINATION_V1 ")
    p_landed = json.loads(landed.split(" ", 1)[1])
    p_emptied = json.loads(emptied.split(" ", 1)[1])
    assert p_landed["anti_success"] is False
    assert p_landed["replacement_proposals"] == 1
    assert p_emptied["anti_success"] is True
    assert p_emptied["rejected_families"] == ["lever-5"]


# ---- synthesis wiring ----------------------------------------------

_INSUFFICIENT = ("lever-5:add_example_sql:soft_policy:unchanged",)


def _solo_lever5_response():
    """One solo lever-5 add_example_sql — the rejected sole lever."""
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Example",
                    "intent_description": "exemplar",
                    "repair_hypothesis": "show pattern",
                    "patch_type": "add_example_sql",
                    "rationale": "demonstrate",
                    "confidence": "high",
                    "selected_levers": ["lever-5"],
                    "patch_body": {
                        "example_question": "q?",
                        "example_sql": "SELECT 1",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=10,
        tokens_output=10,
        duration_ms=1,
        error=None,
    )


def _multi_lever_bundle_response():
    """A 2-proposal bundle sharing bundle_id with DIFFERENT families.

    Member families differ (lever-5 vs lever-6) so the bundle-invariant
    check keeps it (a same-family bundle would be dropped — that is
    W9's concern, not W8's).
    """
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Example",
                    "intent_description": "exemplar",
                    "repair_hypothesis": "show pattern",
                    "patch_type": "add_example_sql",
                    "rationale": "demonstrate",
                    "selected_levers": ["lever-5"],
                    "bundle_id": "b1",
                    "patch_body": {
                        "example_question": "q?",
                        "example_sql": "SELECT 1",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                },
                {
                    "intent_name": "Snippet",
                    "intent_description": "filter",
                    "repair_hypothesis": "scope the filter",
                    "patch_type": "add_sql_snippet_filter",
                    "rationale": "narrow",
                    "selected_levers": ["lever-6"],
                    "bundle_id": "b1",
                    "patch_body": {
                        "name": "f_scope",
                        "sql_expression": "order_id > 0",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                },
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=10,
        tokens_output=10,
        duration_ms=1,
        error=None,
    )


def _cluster():
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="theme",
        member_qids=("gs_009",),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=("catalog.schema.orders.order_id",),
        confidence="high",
        root_cause="",
    )


def _run(invoke):
    from unittest.mock import MagicMock, patch

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        return run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
            insufficient_repair_signatures=_INSUFFICIENT,
        )


def test_pivot_lands_replacement_bundle(capsys):
    from unittest.mock import MagicMock, patch

    invoke = MagicMock(
        side_effect=[
            _solo_lever5_response(),        # initial → D3 empties slate
            _multi_lever_bundle_response(),  # pivot re-prompt → survives
        ]
    )
    # Stamp the snippet member so the bundle survives C3 without a live
    # warehouse — W8 mechanics are the subject, not snippet validation.
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        SnippetValidatorVerdict,
    )
    _stamp = SnippetValidatorVerdict(
        outcome="stamped", abstain_reason=None, error_message=""
    )
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall, patch(
        "genie_space_optimizer.optimization.producer_snippet_validator."
        "validate_and_stamp_snippet_patch_body",
        return_value=_stamp,
    ):
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        result = run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
            insufficient_repair_signatures=_INSUFFICIENT,
        )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_PIVOT_DESTINATION_V1" in out
    assert '"outcome": "pivot_attempted"' in out
    assert '"outcome": "pivot_landed"' in out
    assert invoke.call_count == 2, (
        "W8: one initial + one pivot re-prompt (override re-entry makes "
        "no extra LLM call)"
    )
    assert result.proposal is not None


def test_pivot_empties_when_replacement_also_solo(capsys):
    from unittest.mock import MagicMock

    invoke = MagicMock(
        side_effect=[
            _solo_lever5_response(),  # initial → D3 empties slate
            _solo_lever5_response(),  # pivot → still solo → drops again
        ]
    )
    result = _run(invoke)
    out = capsys.readouterr().out
    assert '"outcome": "pivot_emptied_slate"' in out
    assert '"anti_success": true' in out
    assert result.proposal is None


def test_no_pivot_when_flag_off(capsys, monkeypatch):
    from unittest.mock import MagicMock

    monkeypatch.setenv("GSO_TRIAL23_PIVOT_DESTINATION", "0")
    invoke = MagicMock(return_value=_solo_lever5_response())
    _run(invoke)
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_PIVOT_DESTINATION_V1" not in out
    assert invoke.call_count == 1, "rollback: no pivot re-prompt when flag off"
