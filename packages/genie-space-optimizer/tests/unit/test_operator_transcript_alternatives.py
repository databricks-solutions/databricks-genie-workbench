"""Phase D.5 Task 8 — operator transcript surfaces alternatives."""

from genie_space_optimizer.optimization.rca_decision_trace import (
    AlternativeOption,
    DecisionRecord,
    DecisionType,
    DecisionOutcome,
    OptimizationTrace,
    ReasonCode,
    RejectReason,
    render_operator_transcript,
)


def _trace_with(records):
    return OptimizationTrace(
        journey_events=(),
        decision_records=tuple(records),
    )


def test_transcript_shows_cluster_alternatives_under_chosen_cluster() -> None:
    rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.CLUSTER_SELECTED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.CLUSTERED,
        cluster_id="H001",
        rca_id="rca_h001",
        target_qids=("q1", "q2"),
        alternatives_considered=(
            AlternativeOption(
                option_id="C_005",
                kind="cluster",
                reject_reason=RejectReason.BELOW_HARD_THRESHOLD,
                reject_detail="qid count 1 < hard_threshold=2",
            ),
        ),
    )
    out = render_operator_transcript(trace=_trace_with([rec]), iteration=1)
    assert "RCA Cards With Evidence" in out
    assert "H001" in out
    # Alternatives line includes option_id + reason + detail.
    assert "alternatives:" in out
    assert "C_005" in out
    assert "below_hard_threshold" in out


def test_transcript_shows_ag_alternatives_under_chosen_ag() -> None:
    rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.STRATEGIST_AG_EMITTED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.STRATEGIST_SELECTED,
        ag_id="AG_001",
        target_qids=("q1",),
        alternatives_considered=(
            AlternativeOption(
                option_id="AG_002",
                kind="ag",
                score=0.42,
                reject_reason=RejectReason.LOWER_SCORE,
                reject_detail="lost by 0.18",
            ),
        ),
    )
    out = render_operator_transcript(trace=_trace_with([rec]), iteration=1)
    assert "AG Decisions And Rationale" in out
    assert "AG_001" in out
    assert "AG_002" in out
    assert "lower_score" in out


def test_transcript_shows_proposal_alternatives_under_each_proposal() -> None:
    rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.ACCEPTED,
        reason_code=ReasonCode.PROPOSAL_EMITTED,
        proposal_id="P_001",
        target_qids=("q1",),
        alternatives_considered=(
            AlternativeOption(
                option_id="P_007",
                kind="proposal",
                reject_reason=RejectReason.MALFORMED,
            ),
        ),
    )
    out = render_operator_transcript(trace=_trace_with([rec]), iteration=1)
    assert "Proposal Survival And Gate Drops" in out
    assert "P_001" in out
    assert "P_007" in out
    assert "malformed" in out


def test_transcript_omits_alternatives_line_when_none() -> None:
    rec = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.CLUSTER_SELECTED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.CLUSTERED,
        cluster_id="H001",
        rca_id="rca_h001",
        target_qids=("q1",),
    )
    out = render_operator_transcript(trace=_trace_with([rec]), iteration=1)
    assert "alternatives:" not in out
