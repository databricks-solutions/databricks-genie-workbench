"""Phase D Task 8: build_scoreboard aggregator.

Covers:
- Returns ScoreboardSnapshot with all 10 named metrics + dominant_signal.
- Dominant-signal priority order: PROPOSAL_GAP > RCA_GAP > GATE_OR_CAP_GAP
  > EVIDENCE_GAP > MODEL_CEILING > HEALTHY.
- Empty trace → PROPOSAL_GAP fallback (no eval rows).
- accuracy_delta picked up from explicit baseline/candidate inputs.
"""
from __future__ import annotations

import pytest


def _trace(records, events=()):
    from genie_space_optimizer.optimization.rca_decision_trace import OptimizationTrace
    return OptimizationTrace(
        decision_records=tuple(records), journey_events=tuple(events),
    )


def _eval(qid):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        question_id=qid,
    )


def _proposal(pid, qid="q1"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.PROPOSAL_EMITTED,
        proposal_id=pid, question_id=qid,
    )


def _applied(pid, qid="q1"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED, reason_code=ReasonCode.PATCH_APPLIED,
        proposal_id=pid, question_id=qid,
    )


def _resolved(qid, outcome="resolved", reason_code="post_eval_fail_to_pass"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome(outcome),
        reason_code=ReasonCode(reason_code),
        question_id=qid,
    )


def test_build_scoreboard_returns_typed_snapshot():
    from genie_space_optimizer.optimization.scoreboard import (
        ScoreboardSnapshot, build_scoreboard,
    )

    trace = _trace([_eval("q1"), _proposal("P1"), _applied("P1"), _resolved("q1")])
    snap = build_scoreboard(
        trace=trace, iteration=1,
        baseline_accuracy=0.50, candidate_accuracy=0.62, run_id="r",
    )
    assert isinstance(snap, ScoreboardSnapshot)
    assert snap.iteration == 1
    assert snap.run_id == "r"
    assert snap.accuracy_delta == pytest.approx(0.12)
    assert snap.causal_patch_survival_pct == 1.0
    assert snap.decision_trace_completeness_pct == 1.0
    assert snap.rca_loop_closure_pct == 1.0
    assert snap.dominant_signal == "HEALTHY"


def test_build_scoreboard_proposal_gap_when_no_proposals_generated():
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    trace = _trace([_eval("q1")])  # eval row but no proposals
    snap = build_scoreboard(
        trace=trace, iteration=1, baseline_accuracy=0, candidate_accuracy=0,
    )
    assert snap.dominant_signal == "PROPOSAL_GAP"


def test_build_scoreboard_rca_gap_when_loop_closure_low_and_survival_low():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    rca1 = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.RCA_GROUNDED,
        rca_id="rca_1", target_qids=("q1",),
    )
    rca2 = DecisionRecord(
        run_id="r", iteration=1,
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.RCA_GROUNDED,
        rca_id="rca_2", target_qids=("q2",),
    )
    trace = _trace([
        _eval("q1"), _eval("q2"), rca1, rca2, _proposal("P1", qid="q1"),
        _resolved("q1"),  # only q1 closes
    ])
    snap = build_scoreboard(
        trace=trace, iteration=1, baseline_accuracy=0, candidate_accuracy=0,
    )
    assert snap.rca_loop_closure_pct == 0.5
    assert snap.causal_patch_survival_pct == 0.0
    assert snap.dominant_signal == "RCA_GAP"


def test_build_scoreboard_gate_or_cap_gap_when_survival_low():
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    trace = _trace([
        _eval("q1"),
        _proposal("P1"), _proposal("P2"), _proposal("P3"),
        _applied("P1"),
        _resolved("q1"),
    ])
    snap = build_scoreboard(
        trace=trace, iteration=1, baseline_accuracy=0, candidate_accuracy=0,
    )
    assert snap.causal_patch_survival_pct == pytest.approx(1/3)
    assert snap.dominant_signal == "GATE_OR_CAP_GAP"


def test_build_scoreboard_evidence_gap_when_terminal_unactionable_present():
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    trace = _trace([
        _eval("q1"),
        _proposal("P1"), _applied("P1"),
        _resolved("q1", outcome="unresolved", reason_code="post_eval_hold_fail"),
    ])
    snap = build_scoreboard(
        trace=trace, iteration=1, baseline_accuracy=0.50, candidate_accuracy=0.55,
    )
    assert snap.terminal_unactionable_qids == 1
    assert snap.dominant_signal == "EVIDENCE_GAP"


def test_build_scoreboard_model_ceiling_when_no_accuracy_gain():
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    trace = _trace([
        _eval("q1"),
        _proposal("P1"), _applied("P1"),
        _resolved("q1"),
    ])
    snap = build_scoreboard(
        trace=trace, iteration=1, baseline_accuracy=0.60, candidate_accuracy=0.60,
    )
    assert snap.causal_patch_survival_pct == 1.0
    assert snap.terminal_unactionable_qids == 0
    assert snap.accuracy_delta == 0.0
    assert snap.dominant_signal == "MODEL_CEILING"
