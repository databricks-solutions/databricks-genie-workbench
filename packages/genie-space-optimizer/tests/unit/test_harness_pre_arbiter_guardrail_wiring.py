"""Pin that the harness invokes the pre-arbiter regression guardrail."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_harness_calls_decide_pre_arbiter_regression_guardrail() -> None:
    """The acceptance block must include a pre-arbiter regression check.

    The acceptance gate lives in ``_run_gate_checks`` (called from
    ``_run_lever_loop``). The pre-arbiter regression guardrail must be
    invoked there, immediately after ``decide_control_plane_acceptance``,
    so a candidate that drops broad pre-arbiter accuracy without flipping
    any declared target qid is blocked at acceptance — the Q011 silent
    regression pattern.
    """
    src = inspect.getsource(harness._run_gate_checks)
    assert "decide_pre_arbiter_regression_guardrail" in src, (
        "harness must call decide_pre_arbiter_regression_guardrail next to "
        "decide_control_plane_acceptance to catch silent pre-arbiter regressions"
    )
    assert "pre_arbiter_regression_blocked" in src, (
        "harness must surface a pre_arbiter_regression_blocked audit reason "
        "so rejected candidates are debuggable"
    )
