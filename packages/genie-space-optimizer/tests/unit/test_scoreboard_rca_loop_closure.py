"""Phase D Task 7: rca_loop_closure_pct.

Covers:
- 1.0 when every RCA_FORMED's target_qids reach QID_RESOLUTION.
- Partial when one RCA's target qid is missing a resolution.
- 1.0 (vacuous) when no RCA_FORMED records exist.
- Excludes RCA_FORMED records with empty target_qids from the denominator.
- Counts UNRESOLVED qid resolutions as closing the loop (the loop closed,
  even if the outcome was negative).
"""
from __future__ import annotations


def _trace(records):
    from genie_space_optimizer.optimization.rca_decision_trace import OptimizationTrace
    return OptimizationTrace(decision_records=tuple(records))


def _rca_formed(rca_id, target_qids, iteration=1, reason_code="rca_grounded"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode(reason_code),
        rca_id=rca_id,
        target_qids=tuple(target_qids),
    )


def _resolved(qid, iteration=1, outcome="resolved", reason_code="post_eval_fail_to_pass"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome(outcome),
        reason_code=ReasonCode(reason_code),
        question_id=qid,
    )


def test_one_when_every_rca_target_reaches_resolution():
    from genie_space_optimizer.optimization.scoreboard import (
        rca_loop_closure_pct_from_trace,
    )

    trace = _trace([
        _rca_formed("rca_1", ["q1", "q2"]),
        _resolved("q1"), _resolved("q2"),
    ])
    assert rca_loop_closure_pct_from_trace(trace, iteration=1) == 1.0


def test_partial_when_one_target_missing_resolution():
    from genie_space_optimizer.optimization.scoreboard import (
        rca_loop_closure_pct_from_trace,
    )

    trace = _trace([
        _rca_formed("rca_1", ["q1", "q2"]),
        _rca_formed("rca_2", ["q3"]),
        _resolved("q1"),  # q2 missing
        _resolved("q3"),
    ])
    assert rca_loop_closure_pct_from_trace(trace, iteration=1) == 0.5


def test_vacuous_one_when_no_rca_formed():
    from genie_space_optimizer.optimization.scoreboard import (
        rca_loop_closure_pct_from_trace,
    )

    trace = _trace([])
    assert rca_loop_closure_pct_from_trace(trace, iteration=1) == 1.0


def test_excludes_rca_with_empty_target_qids():
    from genie_space_optimizer.optimization.scoreboard import (
        rca_loop_closure_pct_from_trace,
    )

    trace = _trace([
        _rca_formed("rca_1", []),  # excluded
        _rca_formed("rca_2", ["q1"]),
        _resolved("q1"),
    ])
    assert rca_loop_closure_pct_from_trace(trace, iteration=1) == 1.0


def test_counts_unresolved_qid_resolution_as_closing_the_loop():
    from genie_space_optimizer.optimization.scoreboard import (
        rca_loop_closure_pct_from_trace,
    )

    trace = _trace([
        _rca_formed("rca_1", ["q1"]),
        _resolved(
            "q1", outcome="unresolved", reason_code="post_eval_hold_fail",
        ),
    ])
    assert rca_loop_closure_pct_from_trace(trace, iteration=1) == 1.0
