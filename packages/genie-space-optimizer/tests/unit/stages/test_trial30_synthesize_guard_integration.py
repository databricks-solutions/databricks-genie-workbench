"""Trial 30 W30.1b — guard WIRING into run_plan11_synthesis_for_single_cluster.

The guard's drop/keep logic is exhaustively unit-tested in
``test_trial30_enforced_switch_guard.py``. This module verifies the
*wiring*: that the real synthesis path invokes ``enforced_switch_survivors``
with the surviving proposal slate + the threaded ``inert_mechanism_history``
when ``GSO_TRIAL30_ENFORCE_GUARD`` is ON, applies its survivor set, and
does NOT invoke it when the guard sub-flag is OFF (byte-stable rollback).

We use a single complete-kit ``add_sql_snippet_filter`` proposal (declaring
both ``top_n_cardinality_collapse`` companions ``lever-6`` + ``lever-1`` so
it survives the kit_for_rca gate, with a valid ``sql_expression`` so it
survives the producer snippet validator) and spy on the guard boundary, so
the test is robust to the downstream binding/actuator stages that have their
own dedicated suites.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.enforced_mechanism_switch import (
    EnforcedSwitchOutcome,
)
from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H026",
        semantic_theme="top-N cardinality collapse",
        member_qids=("gs_026",),
        unifying_evidence="RANK() doesn't bound row count",
        repair_hypothesis="Use a structural snippet for the grain",
        primary_blame_set=("catalog.schema.orders.order_id",),
        confidence="high",
        root_cause="top_n_cardinality_collapse",
    )


def _kit_response() -> LlmReasoningResponse:
    # ``top_n_cardinality_collapse`` requires the COMPLETE kit
    # {lever-6, lever-1} (action_groups.KIT_FOR_RCA). The kit_for_rca gate
    # evaluates each proposal's OWN ``selected_levers`` against the kit, so
    # a single proposal must declare BOTH companions to survive to the
    # W30.1b guard. The proposal's structural mechanism is SQL_SNIPPET
    # (``add_sql_snippet_filter``); history rejects lever-6.
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H026.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Bound the grain with a filter",
                    "intent_description": "Structural snippet filter",
                    "repair_hypothesis": "fix the grain",
                    "patch_type": "add_sql_snippet_filter",
                    "selected_lever": "lever-6",
                    "selected_levers": ["lever-6", "lever-1"],
                    "rationale": "structural",
                    "confidence": "high",
                    "patch_body": {
                        "sql_expression": "QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts DESC) = 1",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_026"],
                },
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=200,
        tokens_output=100,
        duration_ms=2100,
        error=None,
    )


_HISTORY = (
    InertMechanismHistory(
        qid="gs_026",
        rca_kind="top_n_cardinality_collapse",
        rejected_mechanisms=("lever-6",),
    ),
)


def _isolate_downstream_filters(monkeypatch):
    """Disable the independent downstream binding/actuator stages that
    would otherwise drop the synthetic snippet on warehouse validation.
    Each has its own dedicated suite; here we exercise only the W30.1b
    guard wiring."""
    monkeypatch.setenv("GSO_TRIAL21_ACTUATOR", "0")
    monkeypatch.setenv("GSO_MECHANISM_COVERAGE_BINDING", "0")
    monkeypatch.setenv("GSO_RCA_MECHANISM_ROUTE_BINDING", "0")
    monkeypatch.setenv("GSO_INSTRUCTION_ROUTE_BINDING", "0")


@patch(
    "genie_space_optimizer.optimization.enforced_mechanism_switch."
    "enforced_switch_survivors"
)
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
)
def test_guard_invoked_with_history_when_enabled(
    MockLlmCall, mock_guard, monkeypatch
):
    _isolate_downstream_filters(monkeypatch)
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCE_GUARD", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    # Identity passthrough so the guard does not perturb the slate.
    mock_guard.side_effect = lambda views, history: EnforcedSwitchOutcome(
        survivors=list(views)
    )
    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_kit_response()
    )

    run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG_H026",
        w=MagicMock(),
        inert_mechanism_history=_HISTORY,
    )

    assert mock_guard.called, "guard must be invoked when ENFORCE_GUARD=1"
    _args, _kwargs = mock_guard.call_args
    # Positional: (views, history). The history must be the threaded one.
    passed_views, passed_history = _args
    assert passed_history == _HISTORY
    # The view exposes the canonicalized rca_kind and the proposal's qid.
    assert passed_views[0].qid == "gs_026"
    assert passed_views[0].rca_kind == "top_n_cardinality_collapse"


@patch(
    "genie_space_optimizer.optimization.enforced_mechanism_switch."
    "enforced_switch_survivors"
)
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
)
def test_guard_not_invoked_when_subflag_off(
    MockLlmCall, mock_guard, monkeypatch
):
    _isolate_downstream_filters(monkeypatch)
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCE_GUARD", "0")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_kit_response()
    )

    run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG_H026",
        w=MagicMock(),
        inert_mechanism_history=_HISTORY,
    )

    assert not mock_guard.called, "guard must NOT run when ENFORCE_GUARD=0"


@patch(
    "genie_space_optimizer.optimization.enforced_mechanism_switch."
    "enforced_switch_survivors"
)
@patch(
    "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
)
def test_guard_not_invoked_when_history_empty(
    MockLlmCall, mock_guard, monkeypatch
):
    _isolate_downstream_filters(monkeypatch)
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCE_GUARD", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")
    from genie_space_optimizer.optimization.stages.synthesize import (
        run_plan11_synthesis_for_single_cluster,
    )

    MockLlmCall.return_value.invoke = MagicMock(
        return_value=_kit_response()
    )

    run_plan11_synthesis_for_single_cluster(
        cluster=_make_cluster(),
        schema_slice={},
        history=[],
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG_H026",
        w=MagicMock(),
        inert_mechanism_history=(),
    )

    assert not mock_guard.called, "guard must NOT run with empty history"
