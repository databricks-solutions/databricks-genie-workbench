"""Trial 18 Step 2 — acceptance gate arbiter alignment.

These tests pin the canonical ``row_semantic_score`` wiring into the
gate predicates and the dispatch-input baseline. Before Trial 18 the
SM-lane gates only read the raw byte-match scalar; this misses the
arbiter-rescued semantic-correctness signal in 74% of d13938e7
production rows.

The fixtures replay the ``gs_013`` production failure mode:
  baseline raw byte-match = 0.0; arbiter=both_correct -> canonical 1.0.

After Trial 18, the gate compares canonical pre/post and the
collateral predicate detects arbiter-rescued regressions that the
byte-match was blind to.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.acceptance_gate import (
    acceptance_gate,
    _assess_collateral,
)
from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
    _row_score,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _trial18_flag_on(monkeypatch):
    """Default the Trial 18 flag ON for this test module — it gates
    the new behaviour these tests pin down.
    """
    monkeypatch.delenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", raising=False)
    yield


def _state_at_evaluated(
    *,
    qid: str = "gs_013",
    pre_score: float = 0.0,
    post_score: float = 1.0,
):
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord(
            "r", "row_is_hard_failure", pre_score, "SELECT 1", "x", 1,
        ),
    )
    for from_s, to_s, kw in (
        (FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED,
         {"diagnosed": DiagnosisRecord(
             "plan11_stage1", "k", "s", "f", "e", "high", "r")}),
        (FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED,
         {"clustered": ClusterMembershipRecord(
             "H1", "AG", (qid,), 6, "k")}),
        (FunnelStage.CLUSTERED, FunnelStage.PROPOSED,
         {"proposals": (ProposalAttempt(
             0, "i", "p", FunnelStage.APPLIED, "applied", "ok"),)}),
        (FunnelStage.PROPOSED, FunnelStage.NORMALIZED, {}),
        (FunnelStage.NORMALIZED, FunnelStage.APPLYABLE, {}),
        (FunnelStage.APPLYABLE, FunnelStage.APPLIED,
         {"applied": AppliedRecord(1, "c", 0, ("i",))}),
        (FunnelStage.APPLIED, FunnelStage.EVALUATED,
         {"evaluated": EvaluatedRecord(
             pre_score, post_score, "SELECT 1", "SELECT 2", "rp")}),
    ):
        s = s.advance(
            to_s,
            StageTransition(from_s, to_s, 1, "t", "validation_gate"),
            **kw,
        )
    return s


# ── Test 1: gs_013 production replay — gate must accept ────────────────


def test_acceptance_accepts_gs013_iter2_replay():
    """gs_013 iter-2 production row: arbiter=both_correct rescued
    the byte-match miss. Pre-Trial-18 the gate read 0.0/0.0 and
    rejected ``target_unchanged``. After Trial 18, ``EvaluatedRecord``
    carries the canonical post score (1.0) and the gate accepts.
    """
    s = _state_at_evaluated(pre_score=0.0, post_score=1.0)
    with patch(
        "genie_space_optimizer.optimization.state_machine.transformers."
        "acceptance_gate._assess_collateral",
        return_value=(),
    ):
        s2 = acceptance_gate.transform(
            s,
            TransformerContext(1, "r", ValidationContext(1, "r", {})),
        )
    assert s2.current_stage == FunnelStage.ACCEPTED
    assert s2.accepted is not None
    assert s2.accepted.decision == "accepted"


# ── Test 2: collateral predicate sees arbiter-rescued regressions ──────


def test_acceptance_detects_arbiter_rescued_collateral_regression():
    """Pre-Trial-18 the collateral predicate compared raw byte-match
    scalars. A non-target QID that was ``arbiter=both_correct`` pre
    (byte-match 0.0) and became ``ground_truth_correct`` post
    (byte-match 0.0) showed up as ``0.0 -> 0.0`` — no regression. The
    canonical accessor catches it: 1.0 -> 0.0.
    """
    s = _state_at_evaluated(qid="gs_013", pre_score=1.0, post_score=1.0)
    baseline_rows = (
        {
            "question_id": "gs_other",
            "_is_semantic_correct": True,
            "feedback/arbiter/value": "both_correct",
            "feedback/result_correctness/value": 0.0,
        },
    )
    post_rows = (
        {
            "question_id": "gs_other",
            "_is_semantic_correct": False,
            "feedback/arbiter/value": "ground_truth_correct",
            "feedback/result_correctness/value": 0.0,
        },
    )
    ctx = TransformerContext(
        iteration=1, run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        baseline_eval_rows=baseline_rows,
        post_apply_eval_rows=post_rows,
    )
    regressed = _assess_collateral(s, ctx)
    assert regressed == ("gs_other",), (
        "Trial 18 collateral predicate must use the canonical accessor "
        f"to detect arbiter-rescued regressions; got {regressed!r}"
    )


# ── Test 3: dispatch_input.HardQidSeenRecord.score uses canonical ──────


def test_pre_apply_score_uses_canonical_accessor():
    """Baseline ``HardQidSeenRecord.score`` must come from the
    canonical accessor so the gate compares like-with-like against
    the post-apply canonical score.
    """
    row = {
        "_is_semantic_correct": True,
        "feedback/result_correctness/value": 0.0,
        # Legacy ``score`` field still present (raw byte-match) — must
        # be overridden by canonical when Trial 18 flag is on.
        "score": 0.0,
    }
    assert _row_score(row) == 1.0


def test_pre_apply_score_falls_back_to_legacy_when_flag_off(monkeypatch):
    """``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` reverts to the legacy
    ``row["score"]`` lookup byte-for-byte. Critical for rollback
    parity.
    """
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "0")
    row = {
        "_is_semantic_correct": True,
        "feedback/result_correctness/value": 0.0,
        "score": 0.0,
    }
    assert _row_score(row) == 0.0


def test_pre_apply_score_falls_back_when_no_canonical_signal():
    """Rows without any canonical signal (legacy fixtures, synthetic
    tests) still return the ``row["score"]`` value so existing tests
    don't silently flip.
    """
    row = {"score": 0.42}
    assert _row_score(row) == 0.42
