"""Every exit path from maybe_run_state_machine_canary_iteration emits
a GSO_PLAN_V3_CANARY_V1 marker with a closed-vocabulary reason. The
prior 'silent return' exit paths (flag_off, empty initial_states) are
the holes that hid the current sidecar-not-authority state.
"""
import json


def test_canary_emits_flag_off_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "false")
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    maybe_run_state_machine_canary_iteration(
        eval_rows=[], iteration=1, run_id="r1",
    )
    out = capsys.readouterr().out
    assert "GSO_PLAN_V3_CANARY_V1" in out
    assert "reason=flag_off" in out


def test_canary_emits_dispatch_input_empty_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    maybe_run_state_machine_canary_iteration(
        eval_rows=[], iteration=1, run_id="r1",
    )
    out = capsys.readouterr().out
    assert "GSO_PLAN_V3_CANARY_V1" in out
    assert "reason=dispatch_input_empty" in out
