"""RCO-4b Phase C Task 7 — parity between pure helpers and a faithful
reimplementation of the legacy inline P0-gate decision logic.

We do NOT drive the harness; the harness path depends on Spark.
Instead, the test re-encodes the legacy decision tree from
``harness._run_gate_checks:13276-13343`` as a small in-test reference
and asserts the two pure helpers produce identical outcomes for a
parametrized matrix of scenarios.

If the legacy harness logic changes, this test must be updated in
the same commit — the parity assertion only proves the helpers match
the in-test reference, not the live harness.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_p0_gate_post_eval,
    decide_p0_gate_should_run,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    P0GateInput,
)


def _legacy_should_run(
    inp: P0GateInput,
) -> tuple[bool, str | None]:
    """Reimplements ``harness._run_gate_checks:13276-13290``.

    Legacy path:
      - ``if not ENABLE_LEGACY_SLICE_P0_GATES:`` → print banner; set
        p0_benchmarks = []. Equivalent to ``should_run=False,
        skip_reason="legacy_gates_disabled"``.
      - ``if p0_benchmarks:`` is False (because legacy disabled OR
        filter_benchmarks_by_scope returned []). Equivalent to
        ``should_run=False, skip_reason="p0_empty"`` when legacy is on.
      - Otherwise: ``should_run=True``.
    """
    if not inp.legacy_gates_enabled:
        return (False, "legacy_gates_disabled")
    if int(inp.p0_benchmark_count) <= 0:
        return (False, "p0_empty")
    return (True, None)


def _legacy_post_eval(p0_failures_count: int) -> tuple[bool, str | None, int]:
    """Reimplements ``harness._run_gate_checks:13321-13343``.

    Legacy code:
        p0_failures = p0_result.get("failures", [])
        if p0_failures:
            return {"passed": False,
                    "rollback_reason": f"p0_gate: {len(p0_failures)} failures",
                    ...}
        else:
            print(... PASS ...)
    """
    count = int(p0_failures_count)
    if count <= 0:
        return (True, None, 0)
    return (False, f"p0_gate: {count} failures", count)


PARITY_MATRIX = [
    # (description, legacy_on, p0_count, failures_count_or_None)
    ("legacy_off",                   False, 5,  None),
    ("legacy_off_zero_count",        False, 0,  None),
    ("legacy_on_empty_count",        True,  0,  None),
    ("legacy_on_single_passes",      True,  1,  0),
    ("legacy_on_single_fails",       True,  1,  1),
    ("legacy_on_many_passes",        True,  20, 0),
    ("legacy_on_few_fail",           True,  20, 2),
    ("legacy_on_all_fail",           True,  20, 20),
]


@pytest.mark.parametrize(
    "desc,legacy_on,p0_count,failures",
    PARITY_MATRIX,
    ids=[m[0] for m in PARITY_MATRIX],
)
def test_pure_helpers_match_legacy_reference(
    desc: str,
    legacy_on: bool,
    p0_count: int,
    failures: int | None,
) -> None:
    p0i = P0GateInput(
        ag_id="ag-parity",
        run_id="run-parity",
        iteration=1,
        p0_benchmark_count=p0_count,
        legacy_gates_enabled=legacy_on,
    )

    # Pre-eval parity.
    pure_pre = decide_p0_gate_should_run(p0i)
    legacy_should, legacy_skip = _legacy_should_run(p0i)
    assert pure_pre.should_run == legacy_should, f"{desc} should_run mismatch"
    assert pure_pre.skip_reason == legacy_skip, f"{desc} skip_reason mismatch"

    if not pure_pre.should_run:
        # When pre-eval skips, post-eval is not invoked by either path.
        assert failures is None, (
            f"{desc}: PARITY_MATRIX shape error — failures should be None "
            f"when pre-eval skips"
        )
        return

    # Post-eval parity (post is only invoked when pre-eval ran).
    assert failures is not None, f"{desc}: PARITY_MATRIX missing failures"
    pure_post = decide_p0_gate_post_eval(p0i, p0_failures_count=failures)
    legacy_passed, legacy_reason, legacy_count = _legacy_post_eval(failures)
    assert pure_post.passed == legacy_passed, f"{desc} passed mismatch"
    assert pure_post.rollback_reason == legacy_reason, f"{desc} reason mismatch"
    assert pure_post.failure_count == legacy_count, f"{desc} count mismatch"
