"""RCO-4b Phase C Task 3 — decide_p0_gate_post_eval.

Pure: given the failure count computed by ``run_evaluation``, decide
rollback vs pass.

Mirrors ``harness._run_gate_checks:13321-13343``.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    P0GateInput,
)


def _make_input(**overrides) -> P0GateInput:
    base = dict(
        ag_id="ag-001",
        run_id="run-001",
        iteration=1,
        p0_benchmark_count=5,
        legacy_gates_enabled=True,
    )
    base.update(overrides)
    return P0GateInput(**base)


def test_zero_failures_passes() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=0)
    assert out.passed is True
    assert out.failure_count == 0
    assert out.rollback_reason is None


def test_one_failure_rolls_back() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=1)
    assert out.passed is False
    assert out.failure_count == 1
    assert out.rollback_reason == "p0_gate: 1 failures"


def test_many_failures_rolls_back_with_full_count() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=17)
    assert out.passed is False
    assert out.failure_count == 17
    assert out.rollback_reason == "p0_gate: 17 failures"


def test_negative_failure_count_treated_as_zero() -> None:
    """Defensive: a corrupted count of -1 should not roll back.
    Mirrors the legacy ``if p0_failures:`` truthiness check (a falsey
    value — including 0 — passes; the legacy code never passes
    negative numbers, but the helper must be defensive)."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=-1)
    assert out.passed is True
    assert out.failure_count == 0
    assert out.rollback_reason is None


def test_outcome_does_not_populate_pre_eval_fields() -> None:
    """The post-eval helper must NOT populate should_run / skip_reason.
    Those are the pre-eval helper's responsibility."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=0)
    assert out.should_run is False  # dataclass default
    assert out.skip_reason is None


def test_rollback_reason_format_matches_legacy() -> None:
    """The exact string format ``"p0_gate: N failures"`` is observed
    by downstream rollback consumers (audit emission, return-dict
    construction in the harness). Pin the format."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_post_eval,
    )
    out = decide_p0_gate_post_eval(_make_input(), p0_failures_count=3)
    assert out.rollback_reason == "p0_gate: 3 failures"
