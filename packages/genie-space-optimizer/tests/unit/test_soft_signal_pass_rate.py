"""Phase 1 Addendum — tests for compute_soft_signal_pass_rate."""

from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_policy import (
    compute_soft_signal_pass_rate,
)


def _row(qid: str, *, soft_results: dict) -> dict:
    return {"qid": qid, "soft_signal_results": soft_results}


def test_pass_rate_is_one_when_all_soft_signals_pass() -> None:
    rows = [
        _row("gs_a", soft_results={"response_quality": "pass", "time_window": "pass"}),
        _row("gs_b", soft_results={"response_quality": "pass"}),
    ]
    assert compute_soft_signal_pass_rate(rows) == 1.0


def test_pass_rate_is_zero_when_all_fail() -> None:
    rows = [
        _row("gs_a", soft_results={"response_quality": "fail"}),
        _row("gs_b", soft_results={"response_quality": "fail"}),
    ]
    assert compute_soft_signal_pass_rate(rows) == 0.0


def test_pass_rate_aggregates_across_signals_and_qids() -> None:
    rows = [
        _row("gs_a", soft_results={"a": "pass", "b": "fail"}),  # 1/2
        _row("gs_b", soft_results={"a": "pass", "b": "pass"}),  # 2/2
    ]
    # Total: 3 of 4 = 0.75
    assert compute_soft_signal_pass_rate(rows) == 0.75


def test_pass_rate_returns_zero_for_empty_rows() -> None:
    assert compute_soft_signal_pass_rate([]) == 0.0


def test_pass_rate_skips_rows_without_soft_signal_results() -> None:
    rows = [
        {"qid": "gs_a"},  # no soft_signal_results key — skipped
        _row("gs_b", soft_results={"response_quality": "pass"}),
    ]
    assert compute_soft_signal_pass_rate(rows) == 1.0
