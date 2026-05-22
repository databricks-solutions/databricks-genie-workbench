"""Plan v3 step §A — harness canary helper.

``maybe_run_state_machine_canary_iteration`` is the entry point the
harness calls once per iteration after ``iteration_counter += 1``.
Flag-gated; default OFF in code, default ON via setdefault in the
notebook.
"""
from __future__ import annotations

import os
from unittest.mock import patch


def _eval_rows_with_one_hard_failure():
    return [
        {
            "question_id": "q1",
            "feedback/result_correctness/value": "no",
            "question": "How many?",
            "ground_truth_sql": "SELECT COUNT(*) FROM t",
            "generated_sql": "SELECT 1",
            "judge_rationale": "wrong aggregate",
        },
    ]


def test_canary_disabled_returns_empty_tuple():
    """Flag off → helper returns () without invoking the state machine."""
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN_V3_STATE_MACHINE_ITERATION", None)
        result = maybe_run_state_machine_canary_iteration(
            eval_rows=_eval_rows_with_one_hard_failure(),
            iteration=1,
            run_id="r",
            workspace_client=None,
        )
    assert result == ()


def test_canary_runs_state_machine_when_flag_on(monkeypatch):
    """Flag on → helper builds initial states, runs the SM, returns final states."""
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")

    # Stub all the seams so the helper doesn't hit real LLM / applier
    # paths. The dispatch_input wrapper consumes eval_rows directly.
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.stages.diagnose.diagnose_failing_qids",
        lambda **kw: [],
    )

    result = maybe_run_state_machine_canary_iteration(
        eval_rows=_eval_rows_with_one_hard_failure(),
        iteration=1,
        run_id="r",
        workspace_client=None,
    )
    # One state per hard-failure QID.
    assert len(result) == 1


def test_canary_swallows_exceptions(monkeypatch):
    """The canary must never propagate exceptions to the harness loop."""
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")

    # Force a crash inside the SM by stubbing dispatch_input to raise.
    def boom(**kw):
        raise RuntimeError("test canary failure")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.optimizer"
        "._build_state_machine_initial_states",
        boom,
    )

    result = maybe_run_state_machine_canary_iteration(
        eval_rows=_eval_rows_with_one_hard_failure(),
        iteration=1,
        run_id="r",
        workspace_client=None,
    )
    # Caught, returned empty.
    assert result == ()


def test_canary_no_initial_states_returns_empty(monkeypatch):
    """Empty eval_rows → no hard-failure states → return ()."""
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    result = maybe_run_state_machine_canary_iteration(
        eval_rows=[],
        iteration=1,
        run_id="r",
    )
    assert result == ()
