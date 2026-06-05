"""Trial 23 W4 — RCA-kind to mechanism routing.

W4's law is *correct at source*: an RCA kind that ``add_example_sql``
cannot fix must not be routed to ``add_example_sql``. The pure routing
module maps each such RCA kind to the mechanism(s) that DO fix it and
detects the "defaulted to example_sql" anti-pattern so the synthesizer
can emit the ``rca_mechanism_defaulted_to_example_sql`` anti-success
marker and feed the next iteration a forbidden signature.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    RCA_KIND_TO_FIXING_MECHANISMS,
    example_sql_is_insufficient_for,
    rca_mechanism_default_reason,
    rca_mechanism_defaulted_marker,
    recommended_mechanisms_for_rca,
)


# The three RCA kinds the Trial 23 plan names as inert under example_sql.
PLAN_NAMED_RCAS = (
    "extra_defensive_filter",
    "top_n_cardinality_collapse",
    "canonical_dimension_missed",
)


def test_plan_named_rcas_are_in_the_map_and_exclude_example_sql():
    for rca in PLAN_NAMED_RCAS:
        assert rca in RCA_KIND_TO_FIXING_MECHANISMS, rca
        fixing = RCA_KIND_TO_FIXING_MECHANISMS[rca]
        assert PatchMechanism.EXAMPLE_SQL not in fixing, (
            f"{rca}: example_sql must not be listed as a fixing mechanism"
        )
        assert fixing, f"{rca}: must name at least one fixing mechanism"


def test_example_sql_is_insufficient_for_named_rcas():
    for rca in PLAN_NAMED_RCAS:
        assert example_sql_is_insufficient_for(rca) is True
    # Unknown / unmapped RCA kinds have no example_sql contract.
    assert example_sql_is_insufficient_for("some_unknown_rca") is False
    assert example_sql_is_insufficient_for("") is False
    assert example_sql_is_insufficient_for(None) is False


def test_normalization_is_case_and_whitespace_insensitive():
    assert example_sql_is_insufficient_for("  Extra_Defensive_Filter ") is True


def test_reason_fires_when_only_example_sql_for_insufficient_rca():
    reason = rca_mechanism_default_reason(
        "extra_defensive_filter",
        {PatchMechanism.EXAMPLE_SQL},
    )
    assert reason == (
        "rca_mechanism_defaulted_to_example_sql:rca=extra_defensive_filter"
    )


def test_reason_silent_when_adequate_mechanism_present():
    # example_sql paired with a fixing mechanism (sql_snippet) is OK.
    reason = rca_mechanism_default_reason(
        "extra_defensive_filter",
        {PatchMechanism.EXAMPLE_SQL, PatchMechanism.SQL_SNIPPET},
    )
    assert reason == ""


def test_reason_silent_for_unmapped_rca_even_with_example_sql():
    reason = rca_mechanism_default_reason(
        "some_unknown_rca",
        {PatchMechanism.EXAMPLE_SQL},
    )
    assert reason == ""


def test_reason_silent_when_no_example_sql_in_set():
    # Inadequate-but-not-example_sql is a different concern (mechanism
    # coverage), not the W4 example_sql-default anti-pattern.
    reason = rca_mechanism_default_reason(
        "canonical_dimension_missed",
        {PatchMechanism.INSTRUCTION_TEXT},
    )
    assert reason == ""


def test_recommended_mechanisms_lists_fixing_mechanism_values_sorted():
    rec = recommended_mechanisms_for_rca("canonical_dimension_missed")
    assert rec == tuple(sorted(
        m.value for m in RCA_KIND_TO_FIXING_MECHANISMS[
            "canonical_dimension_missed"
        ]
    ))
    # Unmapped RCA → empty recommendation.
    assert recommended_mechanisms_for_rca("unknown") == ()


def _synthesis_response(patch_type: str):
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Add example",
                    "intent_description": "exemplar",
                    "repair_hypothesis": "show correct pattern",
                    "patch_type": patch_type,
                    "rationale": "demonstrate",
                    "confidence": "high",
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


def _cluster_with_rca(root_cause: str):
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
        root_cause=root_cause,
    )


def test_synthesis_emits_anti_marker_when_example_sql_only_for_insufficient_rca(
    capsys,
):
    from unittest.mock import MagicMock, patch

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response("add_example_sql")
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster_with_rca("extra_defensive_filter"),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1" in out, (
        "W4 wiring: example_sql-only slate for extra_defensive_filter must "
        "emit the anti-success marker"
    )


def test_synthesis_silent_for_rca_outside_w4_map(capsys):
    from unittest.mock import MagicMock, patch

    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response("add_example_sql")
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster_with_rca("some_other_rca"),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1" not in out
    ), "W4 wiring: RCAs outside the map must not emit the anti-marker"


def test_synthesis_silent_when_flag_off(capsys, monkeypatch):
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "0")
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response("add_example_sql")
        )
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster_with_rca("extra_defensive_filter"),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    out = capsys.readouterr().out
    assert (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1" not in out
    ), "W4 wiring: rollback (master flag off) must not emit the anti-marker"


def _synthesis_response_instruction():
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_1",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Add instruction",
                    "intent_description": "tell the planner",
                    "repair_hypothesis": "prose nudge",
                    "patch_type": "add_instruction",
                    "rationale": "explain",
                    "confidence": "high",
                    "patch_body": {
                        "instruction_text": "Rank by total, not by row.",
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


def _run_instruction_synthesis(rca_kind: str, monkeypatch):
    """Drive single-cluster synthesis with a lone add_instruction slate.

    The upstream Phase-2 KIT_FOR_RCA companion guard would drop a single-
    lever proposal for a kit-demanding RCA before the W4 binding runs;
    neutralising it here isolates the Track B / B1 instruction-route
    binding so the test pins THAT wiring, not the kit guard.
    """
    from unittest.mock import MagicMock, patch

    import genie_space_optimizer.optimization.stages.action_groups as _ags
    import genie_space_optimizer.optimization.stages.synthesize as _synth

    monkeypatch.setattr(
        _ags, "kit_for_rca_violation_reason", lambda *a, **k: ""
    )
    with patch.object(_synth, "LlmReasoningCall") as MockCall:
        MockCall.return_value.invoke = MagicMock(
            return_value=_synthesis_response_instruction()
        )
        _synth.run_plan11_synthesis_for_single_cluster(
            cluster=_cluster_with_rca(rca_kind),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )


def test_synthesis_emits_instruction_anti_marker_for_top_n_collapse(
    capsys, monkeypatch
):
    """Track B / B1: a lone add_instruction slate for a SQL-shape RCA
    (top_n_cardinality_collapse) must emit the instruction-text
    anti-success marker."""
    _run_instruction_synthesis("top_n_cardinality_collapse", monkeypatch)
    out = capsys.readouterr().out
    assert (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_INSTRUCTION_TEXT_V1" in out
    ), (
        "B1 wiring: lone add_instruction for top_n_cardinality_collapse "
        "must emit the instruction-text anti-success marker"
    )


def test_synthesis_emits_instruction_anti_marker_for_defensive_filter(
    capsys, monkeypatch
):
    """Track B / B1: extra_defensive_filter is a SQL-shape RCA. The e943
    live-llm-only run proved a lone add_instruction left the SQL shape
    unchanged and phantom-accepted, so a lone instruction slate must emit
    the instruction-text anti-success marker (admissible only paired with
    the structural sql_snippet companion)."""
    _run_instruction_synthesis("extra_defensive_filter", monkeypatch)
    out = capsys.readouterr().out
    assert (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_INSTRUCTION_TEXT_V1" in out
    ), (
        "B1 wiring: lone add_instruction for extra_defensive_filter must "
        "emit the instruction-text anti-success marker"
    )


def test_synthesis_silent_instruction_marker_for_unmapped_rca(
    capsys, monkeypatch
):
    """Track B / B1 negative: an RCA outside the SQL-shape map has no
    instruction-route contract, so the anti-marker must stay silent."""
    _run_instruction_synthesis("some_other_rca", monkeypatch)
    out = capsys.readouterr().out
    assert (
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_INSTRUCTION_TEXT_V1"
        not in out
    ), (
        "B1 wiring: RCAs outside the SQL-shape map must not emit the "
        "instruction-text anti-marker"
    )


def test_marker_payload_shape():
    marker = rca_mechanism_defaulted_marker(
        optimization_run_id="run1",
        iteration=3,
        cluster_id="cl1",
        rca_kind="top_n_cardinality_collapse",
        mechanisms={PatchMechanism.EXAMPLE_SQL},
    )
    assert marker.startswith(
        "GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1 "
    )
    payload = json.loads(marker.split(" ", 1)[1])
    assert payload["anti_success"] is True
    assert payload["rca_kind"] == "top_n_cardinality_collapse"
    assert payload["cluster_id"] == "cl1"
    assert payload["iteration"] == 3
    assert "example_sql" in payload["observed_mechanisms"]
    assert payload["recommended_mechanisms"] == list(
        recommended_mechanisms_for_rca("top_n_cardinality_collapse")
    )
