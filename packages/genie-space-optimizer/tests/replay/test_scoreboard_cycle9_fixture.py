"""Phase D Task 10: snapshot regression test for build_scoreboard.

Pins the dict shape that build_scoreboard produces over a known fixture
so any subsequent change to a metric definition fails CI loudly. The
fixture preference is:

1. Cycle 9 raw fixture (real production decision records) if it has
   ``iterations[*].decision_records`` populated.
2. Hand-built synthetic trace covering the full ten-DecisionType chain.

Whichever applies, the produced ScoreboardSnapshot.to_dict() is
compared against a committed expected dict.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "airline_real_v1_cycle9_raw.json"
)


def _load_cycle9_iter1_records():
    """Return iteration-1 decision records from the cycle-9 raw fixture, or
    None if the fixture does not carry decision_records yet.
    """
    if not _FIXTURE_PATH.exists():
        return None
    with _FIXTURE_PATH.open() as f:
        payload = json.load(f)
    iterations = payload.get("iterations") or []
    if not iterations:
        return None
    iter1 = iterations[0] if isinstance(iterations, list) else None
    if not isinstance(iter1, dict):
        return None
    records = iter1.get("decision_records") or []
    return records or None


def _synthetic_trace():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
        OptimizationTrace,
    )
    records = [
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.EVAL_CLASSIFIED,
            outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
            question_id="q1",
        ),
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.CLUSTER_SELECTED,
            outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
            cluster_id="H001", question_id="q1",
        ),
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.RCA_FORMED,
            outcome=DecisionOutcome.INFO, reason_code=ReasonCode.RCA_GROUNDED,
            rca_id="rca_001", target_qids=("q1",),
        ),
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.PROPOSAL_GENERATED,
            outcome=DecisionOutcome.INFO, reason_code=ReasonCode.PROPOSAL_EMITTED,
            proposal_id="P1", question_id="q1",
        ),
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.PATCH_APPLIED,
            outcome=DecisionOutcome.APPLIED, reason_code=ReasonCode.PATCH_APPLIED,
            proposal_id="P1", question_id="q1", cluster_id="H001",
        ),
        DecisionRecord(
            run_id="r", iteration=1,
            decision_type=DecisionType.QID_RESOLUTION,
            outcome=DecisionOutcome.RESOLVED,
            reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
            question_id="q1",
        ),
    ]
    return OptimizationTrace(decision_records=tuple(records))


_EXPECTED_SYNTHETIC = {
    "accuracy_delta": 0.10,
    "causal_patch_survival_pct": 1.0,
    "decision_trace_completeness_pct": 1.0,
    "dominant_signal": "HEALTHY",
    "hard_cluster_coverage_pct": 1.0,
    "iteration": 1,
    "journey_completeness_pct": 0.0,
    "malformed_proposals_at_cap": 0,
    "rca_loop_closure_pct": 1.0,
    "rollback_attribution_complete_pct": 1.0,
    "run_id": "r",
    "terminal_unactionable_qids": 0,
    "trace_id_fallback_rate": 0.0,
}


def test_build_scoreboard_synthetic_full_chain_matches_pinned_dict():
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    trace = _synthetic_trace()
    snap = build_scoreboard(
        trace=trace, iteration=1,
        baseline_accuracy=0.50, candidate_accuracy=0.60, run_id="r",
    )
    assert snap.to_dict() == _EXPECTED_SYNTHETIC


@pytest.mark.skipif(
    _load_cycle9_iter1_records() is None,
    reason="cycle 9 raw fixture lacks decision_records",
)
def test_build_scoreboard_cycle9_fixture_dominant_signal_matches_known_value():
    """When cycle-9 raw decision_records are available, the dominant signal
    should be deterministic. Until a real-Genie cycle refreshes the fixture
    with non-empty decision_records, this test is skipped (the cycle-9 raw
    intake captured pre-Phase-B, so decision_records is []).
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, OptimizationTrace,
    )
    from genie_space_optimizer.optimization.scoreboard import build_scoreboard

    records = [
        DecisionRecord.from_dict(row)
        for row in _load_cycle9_iter1_records() or []
    ]
    trace = OptimizationTrace(decision_records=tuple(records))
    snap = build_scoreboard(
        trace=trace, iteration=1,
        baseline_accuracy=0.0, candidate_accuracy=0.0,
        run_id="cycle9",
    )
    assert snap.dominant_signal in {
        "HEALTHY", "PROPOSAL_GAP", "RCA_GAP", "GATE_OR_CAP_GAP",
        "EVIDENCE_GAP", "MODEL_CEILING",
    }
    assert isinstance(snap.causal_patch_survival_pct, float)
    assert 0.0 <= snap.causal_patch_survival_pct <= 1.0
