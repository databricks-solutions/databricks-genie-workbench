from __future__ import annotations

from genie_space_optimizer.optimization.rca_terminal import (
    RcaTerminalDecision,
    RcaTerminalStatus,
    resolve_terminal_on_plateau,
)


def test_status_enum_includes_new_terminal_values() -> None:
    values = {member.value for member in RcaTerminalStatus}
    assert "unresolved_hard_failures_quarantined" in values
    assert "diminishing_returns_with_open_debt" in values
    assert "plateau_no_open_failures" in values


def test_resolver_returns_unresolved_hard_failures_quarantined_when_quarantined_and_hard() -> None:
    decision = resolve_terminal_on_plateau(
        quarantined_qids={"gs_013"},
        current_hard_qids={"gs_013"},
        regression_debt_qids=set(),
    )
    assert decision == RcaTerminalDecision(
        status=RcaTerminalStatus.UNRESOLVED_HARD_FAILURES_QUARANTINED,
        should_continue=False,
        reason="1 hard failure(s) remain in quarantine: ['gs_013']",
    )


def test_resolver_returns_diminishing_returns_with_open_debt_when_debt_remains() -> None:
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"gs_004"},
        regression_debt_qids={"gs_004"},
    )
    assert decision.status == RcaTerminalStatus.DIMINISHING_RETURNS_WITH_OPEN_DEBT
    assert decision.should_continue is False
    assert "regression debt" in decision.reason


def test_resolver_returns_plateau_no_open_failures_when_clean() -> None:
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids=set(),
        regression_debt_qids=set(),
    )
    assert decision.status == RcaTerminalStatus.PLATEAU_NO_OPEN_FAILURES
    assert decision.should_continue is False


def test_harness_plateau_printer_uses_resolve_terminal_on_plateau() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)
    plateau_idx = source.index("LEVER LOOP — TERMINATION: plateau")
    snippet = source[plateau_idx - 2000 : plateau_idx + 1500]
    assert "resolve_terminal_on_plateau(" in snippet
    assert "current_hard_qids=" in snippet
    assert "regression_debt_qids=" in snippet
    assert "quarantined_qids=" in snippet
    assert "(unknown)" not in snippet
