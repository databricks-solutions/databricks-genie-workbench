"""RCO-4b Phase A Task 3 — JSON round-trip for the new typed gate-stage
input/output dataclasses. Phase A defines all six; subsequent phases
consume them.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages.gate_types import (
    AsiExtractionInput,
    AsiExtractionOutcome,
    BaselineDriftDiagnosticInput,
    BaselineDriftDiagnosticOutcome,
    FullEvalAcceptanceInput,
    FullEvalAcceptanceOutcome,
    P0GateInput,
    P0GateOutcome,
    PropagationWaitInput,
    PropagationWaitOutcome,
    SliceGateInput,
    SliceGateOutcome,
)


def test_propagation_wait_input_roundtrips() -> None:
    inp = PropagationWaitInput(
        ag_id="AG_alpha",
        max_wait_seconds=30,
        poll_interval_seconds=2.0,
        applied_patches_count=3,
        patched_objects=("table_a", "table_b"),
        expected_instruction_snippets=("foo bar", "baz qux"),
        has_dictionary_changes=False,
    )
    s = json.dumps(inp.to_json())
    rt = PropagationWaitInput.from_json(json.loads(s))
    assert rt == inp


def test_propagation_wait_outcome_roundtrips() -> None:
    """Outcome carries ``audit_decision`` ('confirmed' or
    'waited_full_budget') plus an optional ``reason_code`` for the
    full-budget branch — matching the real harness ``_audit_emit`` shape
    at lines 12915-12946."""
    out = PropagationWaitOutcome(
        propagated=True,
        elapsed_seconds=4.0,
        max_wait_seconds=30,
        applied_patches_count=3,
        audit_decision="confirmed",
        reason_code=None,
    )
    s = json.dumps(out.to_json())
    rt = PropagationWaitOutcome.from_json(json.loads(s))
    assert rt == out


def test_propagation_wait_outcome_roundtrips_with_reason_code() -> None:
    out = PropagationWaitOutcome(
        propagated=False,
        elapsed_seconds=10.0,
        max_wait_seconds=10,
        applied_patches_count=1,
        audit_decision="waited_full_budget",
        reason_code="snippet_not_observed",
    )
    s = json.dumps(out.to_json())
    rt = PropagationWaitOutcome.from_json(json.loads(s))
    assert rt == out


def test_slice_gate_input_roundtrips() -> None:
    inp = SliceGateInput(
        ag_id="AG_alpha",
        run_id="run_X",
        iteration=3,
        all_benchmark_qids=("q1", "q2", "q3"),
        prev_failure_qids=("q1",),
        affected_question_ids=("q2",),
        baseline_passing_qids_known=True,
        slice_benchmark_count=2,
        full_benchmark_count=3,
        best_accuracy=80.0,
        noise_floor=2.0,
        legacy_gates_enabled=False,
        slice_gate_enabled=True,
    )
    s = json.dumps(inp.to_json())
    rt = SliceGateInput.from_json(json.loads(s))
    assert rt == inp


def test_slice_gate_outcome_roundtrips() -> None:
    out = SliceGateOutcome(
        should_run=False,
        skip_reason="legacy_gates_disabled",
        effective_tolerance=None,
        broadness_ratio=None,
        passed=None,
        rollback_reason=None,
        regression_judge=None,
    )
    s = json.dumps(out.to_json())
    rt = SliceGateOutcome.from_json(json.loads(s))
    assert rt == out


def test_p0_gate_input_roundtrips() -> None:
    inp = P0GateInput(
        ag_id="AG_alpha",
        run_id="run_X",
        iteration=3,
        p0_benchmark_count=5,
        legacy_gates_enabled=True,
    )
    s = json.dumps(inp.to_json())
    rt = P0GateInput.from_json(json.loads(s))
    assert rt == inp


def test_p0_gate_outcome_roundtrips() -> None:
    out = P0GateOutcome(
        should_run=True,
        skip_reason=None,
        passed=False,
        failure_count=2,
        rollback_reason="p0_gate: 2 failures",
    )
    s = json.dumps(out.to_json())
    rt = P0GateOutcome.from_json(json.loads(s))
    assert rt == out


def test_asi_extraction_input_roundtrips() -> None:
    """Phase D refined shape: ASI is an audit-forwarder, not a primitive
    extractor. Input carries the pre-stamped raw_audit dict from
    run_evaluation."""
    inp = AsiExtractionInput(
        ag_id="AG_alpha",
        iteration=3,
        raw_audit={"stage_letter": "C", "decision": "ok"},
    )
    s = json.dumps(inp.to_json())
    rt = AsiExtractionInput.from_json(json.loads(s))
    assert rt == inp


def test_asi_extraction_input_roundtrips_with_none_audit() -> None:
    inp = AsiExtractionInput(
        ag_id="AG_alpha",
        iteration=3,
        raw_audit=None,
    )
    s = json.dumps(inp.to_json())
    rt = AsiExtractionInput.from_json(json.loads(s))
    assert rt == inp


def test_asi_extraction_outcome_roundtrips() -> None:
    """Phase D refined shape: outcome carries the should_emit gate +
    the audit-row payload fields."""
    out = AsiExtractionOutcome(
        should_emit=True,
        stage_letter="C",
        gate_name="asi_extraction",
        decision="warn",
        reason_code="low_pre_arbiter_gain",
        metrics={"asi_indicator": 1.0},
    )
    s = json.dumps(out.to_json())
    rt = AsiExtractionOutcome.from_json(json.loads(s))
    assert rt == out


def test_asi_extraction_outcome_roundtrips_with_defaults() -> None:
    out = AsiExtractionOutcome(should_emit=False)
    s = json.dumps(out.to_json())
    rt = AsiExtractionOutcome.from_json(json.loads(s))
    assert rt == out


def test_baseline_drift_diagnostic_input_roundtrips() -> None:
    inp = BaselineDriftDiagnosticInput(
        ag_id="AG_alpha",
        iteration=3,
        prev_iter_pre_accept_baseline=80.0,
        current_post_arbiter_accuracy=72.0,
        diagnostic_threshold_pp=5.0,
    )
    s = json.dumps(inp.to_json())
    rt = BaselineDriftDiagnosticInput.from_json(json.loads(s))
    assert rt == inp


def test_baseline_drift_diagnostic_outcome_roundtrips() -> None:
    out = BaselineDriftDiagnosticOutcome(
        triggered=True,
        delta_pp=8.0,
        audit_metrics={"prev": 80.0, "current": 72.0},
        reason_code="suspected_stale_baseline",
        log_line="BASELINE DRIFT [ag-x]: iter 3 ...",
    )
    s = json.dumps(out.to_json())
    rt = BaselineDriftDiagnosticOutcome.from_json(json.loads(s))
    assert rt == out


def test_full_eval_acceptance_input_roundtrips() -> None:
    """Phase E refined shape: verdict-consolidation contract flattens
    upstream decisions into primitives the helper reads."""
    inp = FullEvalAcceptanceInput(
        ag_id="AG_alpha",
        iteration=3,
        strict_decision_accepted=True,
        strict_decision_reason_code="accepted",
        strict_decision_delta_pp=3.5,
        strict_decision_post_arbiter_candidate=72.0,
        strict_decision_post_arbiter_baseline=68.5,
        strict_decision_min_gain_pp=1.0,
        pre_arbiter_candidate=70.0,
        pre_arbiter_baseline=66.0,
        control_plane_reason_code="accepted",
        diagnostic_regression_judges=("judge_x",),
        regressions=({"judge": "j1", "drop": 1.0},),
    )
    s = json.dumps(inp.to_json())
    rt = FullEvalAcceptanceInput.from_json(json.loads(s))
    assert rt == inp


def test_full_eval_acceptance_outcome_roundtrips() -> None:
    out = FullEvalAcceptanceOutcome(
        accepted=True,
        branch="accept",
        reason_code="accepted",
        rollback_reason=None,
        regression_count=0,
        verdict_audit_metrics={"delta_pp": 3.5, "min_gain_pp": 1.0},
        rollback_audit_metrics=None,
        accept_audit_metrics={"post_arbiter_candidate": 72.0},
    )
    s = json.dumps(out.to_json())
    rt = FullEvalAcceptanceOutcome.from_json(json.loads(s))
    assert rt == out


def test_full_eval_acceptance_outcome_rollback_roundtrips() -> None:
    out = FullEvalAcceptanceOutcome(
        accepted=False,
        branch="rollback",
        reason_code="below_min_gain",
        rollback_reason="full_eval: j_main",
        regression_count=1,
        verdict_audit_metrics={"delta_pp": -0.5},
        rollback_audit_metrics={"regression_count": 1},
        accept_audit_metrics=None,
    )
    s = json.dumps(out.to_json())
    rt = FullEvalAcceptanceOutcome.from_json(json.loads(s))
    assert rt == out
