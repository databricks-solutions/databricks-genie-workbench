"""RCO-4b Phase B Task 4 — decide_slice_gate_post_eval.

Pure: given the slice drops computed by ``detect_regressions`` and
the effective tolerance, decide rollback vs pass.

Mirrors ``harness._run_gate_checks:13169-13222``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    SliceGateInput,
)


def _make_input(**overrides) -> SliceGateInput:
    base = dict(
        ag_id="ag-001",
        run_id="run-001",
        iteration=1,
        all_benchmark_qids=tuple(f"q-{i}" for i in range(50)),
        prev_failure_qids=(),
        affected_question_ids=("q-1", "q-2"),
        baseline_passing_qids_known=True,
        slice_benchmark_count=10,
        full_benchmark_count=50,
        best_accuracy=85.0,
        noise_floor=2.0,
        legacy_gates_enabled=True,
        slice_gate_enabled=True,
    )
    base.update(overrides)
    return SliceGateInput(**base)


def test_no_drops_passes() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_post_eval,
    )
    out = decide_slice_gate_post_eval(
        _make_input(),
        slice_drops=(),
        effective_tolerance=5.0,
    )
    assert out.passed is True
    assert out.rollback_reason is None
    assert out.regression_judge is None
    assert out.effective_tolerance == 5.0


def test_single_drop_rolls_back_with_first_judge() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_post_eval,
    )
    out = decide_slice_gate_post_eval(
        _make_input(),
        slice_drops=({"judge": "correctness", "drop": -3.5},),
        effective_tolerance=5.0,
    )
    assert out.passed is False
    assert out.rollback_reason == "slice_gate: correctness"
    assert out.regression_judge == "correctness"
    assert out.effective_tolerance == 5.0


def test_multiple_drops_use_first_judge() -> None:
    """Legacy code reads ``slice_drops[0]['judge']`` — the first drop
    wins for the rollback_reason string."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_post_eval,
    )
    out = decide_slice_gate_post_eval(
        _make_input(),
        slice_drops=(
            {"judge": "completeness", "drop": -4.0},
            {"judge": "correctness", "drop": -2.0},
        ),
        effective_tolerance=5.0,
    )
    assert out.passed is False
    assert out.rollback_reason == "slice_gate: completeness"
    assert out.regression_judge == "completeness"


def test_outcome_does_not_populate_pre_eval_fields() -> None:
    """The post-eval helper must NOT populate should_run / skip_reason /
    broadness_ratio. Those are the pre-eval helper's responsibility."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_post_eval,
    )
    out = decide_slice_gate_post_eval(
        _make_input(),
        slice_drops=(),
        effective_tolerance=5.0,
    )
    assert out.should_run is False  # default
    assert out.skip_reason is None
    assert out.broadness_ratio is None


def test_drop_without_judge_key_falls_back_to_empty_judge() -> None:
    """Defensive: a malformed slice_drop with no 'judge' key should
    not raise; the outcome's regression_judge will be empty string."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_slice_gate_post_eval,
    )
    out = decide_slice_gate_post_eval(
        _make_input(),
        slice_drops=({"drop": -3.0},),  # no "judge" key
        effective_tolerance=5.0,
    )
    assert out.passed is False
    assert out.regression_judge == ""
    assert out.rollback_reason == "slice_gate: "
