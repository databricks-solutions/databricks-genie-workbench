"""Phase D failure-bucketing T3: classify_unresolved_qid smoke test.

Sanity test that the classifier exists and returns a ClassificationResult.
Per-bucket coverage lives in Task 4's test file.
"""
from __future__ import annotations


def _make_trace(records, events=()):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        OptimizationTrace,
    )
    return OptimizationTrace(
        decision_records=tuple(records),
        journey_events=tuple(events),
    )


def test_classify_unresolved_qid_returns_classification_result_for_passing_qid():
    """A qid with a RESOLVED + POST_EVAL_FAIL_TO_PASS record returns
    ClassificationResult(bucket=None) — the sentinel for "qid is now passing"."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        ClassificationResult,
        classify_unresolved_qid,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord,
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    resolved_record = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.RESOLVED,
        reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
        question_id="q1",
    )
    trace = _make_trace([resolved_record])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert isinstance(result, ClassificationResult)
    assert result.bucket is None
    assert "passing" in result.reason.lower()


def test_classify_unresolved_qid_evidence_gap_for_qid_with_no_records():
    """Rung 1 — qid has zero records in the iteration → EVIDENCE_GAP."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket,
        classify_unresolved_qid,
    )

    trace = _make_trace([])  # empty trace
    result = classify_unresolved_qid(trace, "q_unknown", iteration=1)
    assert result.bucket is FailureBucket.EVIDENCE_GAP
    assert result.earliest_broken_link == "evidence_to_rca"
