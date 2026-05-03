"""Phase D failure-bucketing T5: operator transcript section augmentation.

Asserts:
- The "Unresolved QID Buckets" section header is followed by a
  histogram line summarizing bucket counts.
- Each per-qid line includes ``bucket=...`` and ``bucket_action=...``
  parts.
- A trace with no unresolved qids renders an empty body (existing
  behaviour preserved).
"""
from __future__ import annotations


def _make_trace_with_one_targeting_gap_qid():
    """Trace where qid q1 hits TARGETING_GAP (proposals exist, none target it)."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
        OptimizationTrace,
    )
    eval_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )
    cluster_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.CLUSTER_SELECTED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        cluster_id="H001",
        affected_qids=("q1",), target_qids=("q1",),
    )
    rca_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.RCA_GROUNDED,
        rca_id="rca_001", cluster_id="H001",
        affected_qids=("q1",), target_qids=("q1",),
    )
    ag_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.STRATEGIST_AG_EMITTED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.STRATEGIST_SELECTED,
        ag_id="AG_DECOMPOSED_H001",
        affected_qids=("q1",), target_qids=("q1",),
    )
    proposal_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.PROPOSAL_EMITTED,
        proposal_id="P1", ag_id="AG_DECOMPOSED_H001",
        target_qids=(),  # The TARGETING_GAP shape.
    )
    resolved_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.POST_EVAL_HOLD_FAIL,
        question_id="q1",
        next_action="continue triage",
    )
    return OptimizationTrace(decision_records=(
        eval_rec, cluster_rec, rca_rec, ag_rec, proposal_rec, resolved_rec,
    ))


def test_section_header_followed_by_bucket_histogram_line():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        render_operator_transcript,
    )

    trace = _make_trace_with_one_targeting_gap_qid()
    text = render_operator_transcript(trace=trace, iteration=1)
    lines = text.splitlines()
    section_idx = next(
        i for i, line in enumerate(lines)
        if "Unresolved QID Buckets" in line
    )
    histogram_line = lines[section_idx + 1]
    assert "buckets:" in histogram_line.lower()
    assert "TARGETING_GAP=1" in histogram_line


def test_per_qid_line_carries_bucket_annotation():
    """The qid line *inside the Unresolved QID Buckets section* must
    carry the bucket annotation. The same QID_RESOLUTION record also
    appears in Observed Results And Regressions (without annotation)
    by design — that section is the raw projection of the record type;
    the bucket label belongs to the Unresolved QID Buckets section."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        render_operator_transcript,
    )

    trace = _make_trace_with_one_targeting_gap_qid()
    text = render_operator_transcript(trace=trace, iteration=1)
    lines = text.splitlines()
    section_idx = next(
        i for i, line in enumerate(lines)
        if "Unresolved QID Buckets" in line
    )
    # Scan from the section header down to the next section header.
    section_body: list[str] = []
    for line in lines[section_idx + 1:]:
        # Section header lines are exactly two leading spaces
        # ("|  ") with non-space-padding next; body lines are
        # 4-space-indented ("|    "). Stop at the next section header.
        stripped = line.lstrip()
        if stripped.startswith("|  ") and not stripped.startswith("|   "):
            break
        section_body.append(line)
    qid_lines = [
        l for l in section_body
        if "qid=q1" in l and "outcome=unresolved" in l
    ]
    assert qid_lines, "expected at least one unresolved qid line in section"
    first_qid_line = qid_lines[0]
    assert "bucket=TARGETING_GAP" in first_qid_line
    assert "bucket_action=" in first_qid_line


def test_section_body_empty_when_no_unresolved_qids():
    """Existing behaviour: the section header always renders; the body
    is empty when no unresolved qids exist."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
        OptimizationTrace, render_operator_transcript,
    )

    eval_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )
    resolved_rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome.RESOLVED,
        reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
        question_id="q1",
    )
    trace = OptimizationTrace(decision_records=(eval_rec, resolved_rec))
    text = render_operator_transcript(trace=trace, iteration=1)
    lines = text.splitlines()
    section_idx = next(
        i for i, line in enumerate(lines)
        if "Unresolved QID Buckets" in line
    )
    # The next non-section line should be the next section header
    # ("Next Suggested Action") or the histogram line with
    # zero counts. Either is acceptable; a per-qid line is not.
    body_lines = []
    for line in lines[section_idx + 1:]:
        if line.strip().startswith("|  ") and not line.startswith("|   "):
            break  # next section
        if "qid=" in line and "outcome=unresolved" in line:
            body_lines.append(line)
    assert not body_lines, (
        f"expected empty body but found qid lines: {body_lines}"
    )
