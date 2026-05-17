"""Phase 6.2 — the harness must break the lever loop when
``TerminalAction.next_step == "abort_run"``.

Tests the consumer logic directly by calling
``_consume_terminal_action``, the helper Task 6.2 adds to translate
a ``TerminalAction`` into a loop-control signal.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _consume_terminal_action,
)
from genie_space_optimizer.optimization.iteration_terminal_policy import (
    TerminalAction,
)


def test_abort_run_returns_break_signal(monkeypatch):
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "1")
    action = TerminalAction(
        next_step="abort_run",
        add_to_forbidden_set=True,
        reflection_payload={},
    )
    should_break, abort_reason = _consume_terminal_action(
        action=action,
        terminal_reason_value="invariant_violation",
        iteration=4,
        iteration_budget=5,
    )
    assert should_break is True
    assert abort_reason == "terminal_router_decision"


def test_retry_strategy_switch_does_not_break(monkeypatch):
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "1")
    action = TerminalAction(
        next_step="retry_strategy_switch",
        add_to_forbidden_set=True,
        reflection_payload={},
    )
    should_break, abort_reason = _consume_terminal_action(
        action=action,
        terminal_reason_value="no_structural_candidate",
        iteration=2,
        iteration_budget=5,
    )
    assert should_break is False
    assert abort_reason == ""


def test_skip_productive_does_not_break(monkeypatch):
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "1")
    action = TerminalAction(
        next_step="skip_productive",
        add_to_forbidden_set=True,
        reflection_payload={},
    )
    should_break, _ = _consume_terminal_action(
        action=action,
        terminal_reason_value="applyability_rejected",
        iteration=2,
        iteration_budget=5,
    )
    assert should_break is False


def test_abort_run_does_not_break_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "0")
    action = TerminalAction(
        next_step="abort_run",
        add_to_forbidden_set=True,
        reflection_payload={},
    )
    should_break, _ = _consume_terminal_action(
        action=action,
        terminal_reason_value="invariant_violation",
        iteration=4,
        iteration_budget=5,
    )
    assert should_break is False


def test_budget_boundary_abort_reason_is_named(monkeypatch):
    """The router collapses a retry to abort_run on the final
    iteration. Distinguish that 'budget exhausted' cause from a
    pure routing-table abort_run for the marker payload."""
    monkeypatch.setenv("GSO_ABORT_RUN_AUTHORITATIVE_ENABLED", "1")
    action = TerminalAction(
        next_step="abort_run",
        add_to_forbidden_set=True,
        reflection_payload={},
    )
    should_break, abort_reason = _consume_terminal_action(
        action=action,
        terminal_reason_value="no_structural_candidate",
        iteration=4,
        iteration_budget=5,
    )
    assert should_break is True
    assert abort_reason == "iteration_budget_exhausted"
