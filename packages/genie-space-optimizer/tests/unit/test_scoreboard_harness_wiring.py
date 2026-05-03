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


def test_format_scoreboard_banner_uses_build_scoreboard_when_trace_present():
    """Phase D Task 9: the banner reads real metrics from the trace, not
    the synthetic LoopSnapshot built from harness counts."""
    from genie_space_optimizer.optimization.harness import (
        _format_scoreboard_banner,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
        OptimizationTrace,
    )

    eval_rec = DecisionRecord(
        run_id="r", iteration=3,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )
    proposal_rec = DecisionRecord(
        run_id="r", iteration=3,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.PROPOSAL_EMITTED,
        proposal_id="P1",
    )
    applied_rec = DecisionRecord(
        run_id="r", iteration=3,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED, reason_code=ReasonCode.PATCH_APPLIED,
        proposal_id="P1",
    )
    resolved_rec = DecisionRecord(
        run_id="r", iteration=3,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.RESOLVED,
        reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
        question_id="q1",
    )
    trace = OptimizationTrace(
        decision_records=(eval_rec, proposal_rec, applied_rec, resolved_rec),
    )
    snap = {
        "iteration": 3,
        "passing_qids": ["q1"],
        "hard_failure_qids": [],
        "applied_patch_count": 1,
        "rolled_back_patch_count": 0,
        "trace": trace,
        "baseline_accuracy": 0.50,
        "candidate_accuracy": 0.62,
        "run_id": "r",
    }
    banner = _format_scoreboard_banner(loop_snapshot=snap)
    assert "END-OF-ITERATION SCOREBOARD" in banner
    assert "decision_trace_completeness_pct" in banner
    assert "rca_loop_closure_pct" in banner
    assert "dominant_signal" in banner


def test_format_scoreboard_banner_falls_back_when_no_trace_present():
    """When the harness has not populated ``loop_snapshot["trace"]``, the
    banner falls back to the legacy LoopSnapshot codepath so behaviour is
    unchanged for callers that have not yet wired the trace.
    """
    from genie_space_optimizer.optimization.harness import (
        _format_scoreboard_banner,
    )

    snap = {
        "iteration": 1,
        "passing_qids": ["q1"],
        "hard_failure_qids": [],
        "applied_patch_count": 0,
        "rolled_back_patch_count": 0,
    }
    banner = _format_scoreboard_banner(loop_snapshot=snap)
    assert "dominant_signal" in banner
