"""Cover the lever-loop exit-reason resolver helper."""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _resolve_lever_loop_exit_reason,
)
from genie_space_optimizer.optimization.rca_terminal import (
    RcaTerminalDecision,
    RcaTerminalStatus,
)


def test_default_when_loop_runs_to_completion():
    assert _resolve_lever_loop_exit_reason(None, None) == "lever_loop_completed"


def test_uses_resolver_status_value_when_plateau_break():
    decision = RcaTerminalDecision(
        status=RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES,
        should_continue=False,
        reason="no hard failures or open regression debt remain",
    )
    assert (
        _resolve_lever_loop_exit_reason(decision, None)
        == "plateau_plateau_no_open_failures"
    )


def test_uses_divergence_label_when_divergence_break():
    assert (
        _resolve_lever_loop_exit_reason(None, "divergence_consecutive_rollbacks")
        == "divergence_consecutive_rollbacks"
    )


def test_resolver_takes_precedence_over_divergence():
    decision = RcaTerminalDecision(
        status=RcaTerminalStatus.DIMINISHING_RETURNS_WITH_OPEN_DEBT,
        should_continue=False,
        reason="ignored",
    )
    assert (
        _resolve_lever_loop_exit_reason(decision, "divergence_xyz")
        == "plateau_diminishing_returns_with_open_debt"
    )
