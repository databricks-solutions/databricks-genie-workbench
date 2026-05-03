"""Phase D Task 6: decision_trace_completeness_pct.

Covers:
- Returns 1.0 when every evaluated qid has a QID_RESOLUTION.
- Returns the partial fraction when some qids drop out before resolution.
- Returns 0.0 when there are no EVAL_CLASSIFIED records (no scope).
"""
from __future__ import annotations


def _trace(records):
    from genie_space_optimizer.optimization.rca_decision_trace import OptimizationTrace
    return OptimizationTrace(decision_records=tuple(records))


def _eval(qid, iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        question_id=qid,
    )


def _resolved(qid, iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.RESOLVED,
        reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
        question_id=qid,
    )


def test_returns_one_when_every_qid_has_qid_resolution():
    from genie_space_optimizer.optimization.scoreboard import (
        decision_trace_completeness_pct_from_trace,
    )

    trace = _trace([
        _eval("q1"), _eval("q2"),
        _resolved("q1"), _resolved("q2"),
    ])
    assert decision_trace_completeness_pct_from_trace(trace, iteration=1) == 1.0


def test_returns_partial_when_one_qid_drops_out():
    from genie_space_optimizer.optimization.scoreboard import (
        decision_trace_completeness_pct_from_trace,
    )

    trace = _trace([
        _eval("q1"), _eval("q2"),
        _resolved("q1"),  # q2 never reaches resolution
    ])
    assert decision_trace_completeness_pct_from_trace(trace, iteration=1) == 0.5


def test_returns_zero_when_no_eval_classified():
    from genie_space_optimizer.optimization.scoreboard import (
        decision_trace_completeness_pct_from_trace,
    )

    trace = _trace([])
    assert decision_trace_completeness_pct_from_trace(trace, iteration=1) == 0.0


def test_filters_by_iteration_independently():
    from genie_space_optimizer.optimization.scoreboard import (
        decision_trace_completeness_pct_from_trace,
    )

    trace = _trace([
        _eval("q1", iteration=1),
        _resolved("q1", iteration=1),
        _eval("q2", iteration=2),  # iter 2 has no resolution
    ])
    assert decision_trace_completeness_pct_from_trace(trace, iteration=1) == 1.0
    assert decision_trace_completeness_pct_from_trace(trace, iteration=2) == 0.0
