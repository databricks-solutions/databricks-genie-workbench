"""TDD coverage for the end-of-iteration scoreboard banner (Cycle9 T8).

`scoreboard.compute_scoreboard()` exists and is unit-tested but never
runs in a real loop. T8 wires it adjacent to the existing
`iteration_summary_marker` so the operator gets `dominant_signal` for
free at end-of-iteration.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T8.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _format_scoreboard_banner,
)


def test_scoreboard_banner_renders_dominant_signal():
    snapshot = {
        "iteration": 3,
        "passing_qids": ["q1", "q2"],
        "hard_failure_qids": ["q3"],
        "applied_patch_count": 2,
        "rolled_back_patch_count": 0,
        "trace_id_fallback_count": 0,
        "trace_id_total": 24,
    }
    banner = _format_scoreboard_banner(loop_snapshot=snapshot)
    assert "iteration_3" in banner.lower() or "iteration 3" in banner.lower()
    assert "dominant_signal" in banner.lower()


def test_scoreboard_banner_handles_empty_snapshot():
    banner = _format_scoreboard_banner(loop_snapshot={})
    assert banner.strip() != ""
