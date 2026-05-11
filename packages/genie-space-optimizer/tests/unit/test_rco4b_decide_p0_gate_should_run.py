"""RCO-4b Phase C Task 2 — decide_p0_gate_should_run tests.

The helper encapsulates the two pre-eval gating branches at
``harness._run_gate_checks:13276-13290``. Pure function — no
``run_evaluation`` calls, no Spark, no prints.
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


def test_legacy_disabled_skips_with_legacy_gates_disabled_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(legacy_gates_enabled=False)
    out = decide_p0_gate_should_run(inp)
    assert out.should_run is False
    assert out.skip_reason == "legacy_gates_disabled"


def test_legacy_disabled_short_circuits_before_empty_check() -> None:
    """When legacy gates are off, the helper must short-circuit before
    looking at p0_benchmark_count."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(
        legacy_gates_enabled=False,
        p0_benchmark_count=0,  # also empty
    )
    out = decide_p0_gate_should_run(inp)
    assert out.skip_reason == "legacy_gates_disabled"


def test_empty_p0_skips_with_p0_empty_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(p0_benchmark_count=0)
    out = decide_p0_gate_should_run(inp)
    assert out.should_run is False
    assert out.skip_reason == "p0_empty"


def test_non_empty_p0_with_legacy_enabled_runs() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(p0_benchmark_count=10)
    out = decide_p0_gate_should_run(inp)
    assert out.should_run is True
    assert out.skip_reason is None


def test_single_p0_benchmark_runs() -> None:
    """Boundary: 1 benchmark is enough to run."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(p0_benchmark_count=1)
    out = decide_p0_gate_should_run(inp)
    assert out.should_run is True


def test_negative_p0_benchmark_count_treated_as_empty() -> None:
    """Defensive: a corrupted count of -1 should be treated as empty,
    not as a runnable case."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input(p0_benchmark_count=-1)
    out = decide_p0_gate_should_run(inp)
    assert out.should_run is False
    assert out.skip_reason == "p0_empty"


def test_outcome_does_not_populate_post_eval_fields() -> None:
    """The pre-eval helper must NOT populate passed / failure_count /
    rollback_reason. Those are the post-eval helper's responsibility."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_p0_gate_should_run,
    )
    inp = _make_input()
    out = decide_p0_gate_should_run(inp)
    assert out.passed is None
    assert out.failure_count == 0  # dataclass default
    assert out.rollback_reason is None
