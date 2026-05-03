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


# ─────────────────────────────────────────────────────────────────────
# Per-bucket happy-path fixtures (one fixture per rung).
# ─────────────────────────────────────────────────────────────────────


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


def _cluster(cluster_id, qids, iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.CLUSTER_SELECTED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.HARD_FAILURE,
        cluster_id=cluster_id,
        affected_qids=tuple(qids),
        target_qids=tuple(qids),
    )


def _rca(rca_id, cluster_id, qids, iteration=1, reason="rca_grounded"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    outcome = (
        DecisionOutcome.UNRESOLVED if reason == "rca_ungrounded"
        else DecisionOutcome.INFO
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.RCA_FORMED,
        outcome=outcome,
        reason_code=ReasonCode(reason),
        rca_id=rca_id, cluster_id=cluster_id,
        affected_qids=tuple(qids), target_qids=tuple(qids),
    )


def _ag(ag_id, qids, iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.STRATEGIST_AG_EMITTED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.STRATEGIST_SELECTED,
        ag_id=ag_id, affected_qids=tuple(qids), target_qids=tuple(qids),
    )


def _proposal(proposal_id, ag_id, target_qids=(), iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.PROPOSAL_GENERATED,
        outcome=DecisionOutcome.INFO, reason_code=ReasonCode.PROPOSAL_EMITTED,
        proposal_id=proposal_id, ag_id=ag_id,
        target_qids=tuple(target_qids),
    )


def _gate_drop(proposal_id, gate, iteration=1, reason="no_causal_target"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.GATE_DECISION,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode(reason),
        gate=gate, proposal_id=proposal_id,
    )


def _applied(proposal_id, ag_id, qid, iteration=1):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED, reason_code=ReasonCode.PATCH_APPLIED,
        proposal_id=proposal_id, ag_id=ag_id, question_id=qid,
    )


def _resolved(qid, iteration=1, outcome="unresolved", reason="post_eval_hold_fail"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.QID_RESOLUTION,
        outcome=DecisionOutcome(outcome),
        reason_code=ReasonCode(reason),
        question_id=qid,
    )


def _accepted(ag_id, iteration=1, outcome="rolled_back", reason="no_applied_patches"):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionRecord, DecisionType, DecisionOutcome, ReasonCode,
    )
    return DecisionRecord(
        run_id="r", iteration=iteration,
        decision_type=DecisionType.ACCEPTANCE_DECIDED,
        outcome=DecisionOutcome(outcome),
        reason_code=ReasonCode(reason),
        ag_id=ag_id,
    )


# ─────────────────────────────────────────────────────────────────────
# Rung-by-rung happy-path tests.
# ─────────────────────────────────────────────────────────────────────


def test_evidence_gap_when_qid_has_no_eval_classified():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([_resolved("q1")])  # only resolution, no eval row
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.EVIDENCE_GAP
    assert result.earliest_broken_link == "evidence_to_rca"


def test_rca_gap_when_cluster_has_no_grounded_rca():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("", "H001", ["q1"], reason="rca_ungrounded"),  # ungrounded
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.RCA_GAP
    assert result.earliest_broken_link == "rca_to_ag"


def test_proposal_gap_when_ag_has_no_proposals():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.PROPOSAL_GAP
    assert result.earliest_broken_link == "ag_to_proposal"


def test_targeting_gap_when_proposals_dont_target_qid():
    """Cycle-8 Bug 1 shape: AG_DECOMPOSED_H001 emitted proposals with
    target_qids: []."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_DECOMPOSED_H001", ["q1"]),
        _proposal("P1", "AG_DECOMPOSED_H001", target_qids=()),  # empty!
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.TARGETING_GAP
    assert result.earliest_broken_link == "proposal_to_target_qids"
    assert "target_qids" in result.reason


def test_gate_or_cap_gap_when_every_targeting_proposal_dropped():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=("q1",)),
        _gate_drop("P1", "blast_radius"),
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.GATE_OR_CAP_GAP
    assert result.earliest_broken_link == "target_qids_to_applied"


def test_apply_or_rollback_gap_when_iteration_rolled_back():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=("q1",)),
        _applied("P1", "AG_001", "q1"),
        _accepted("AG_001", outcome="rolled_back"),
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.APPLY_OR_ROLLBACK_GAP
    assert result.earliest_broken_link == "applied_to_observed"


def test_apply_or_rollback_gap_when_qid_pass_to_fail():
    """A qid that was passing and is now failing is a regression — the
    patch applied but introduced a new failure for this qid."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=("q1",)),
        _applied("P1", "AG_001", "q1"),
        _accepted("AG_001", outcome="accepted", reason="patch_applied"),
        _resolved("q1", reason="post_eval_pass_to_fail"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.APPLY_OR_ROLLBACK_GAP


def test_model_ceiling_when_patch_landed_and_held_but_qid_still_fails():
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=("q1",)),
        _applied("P1", "AG_001", "q1"),
        _accepted("AG_001", outcome="accepted", reason="patch_applied"),
        _resolved("q1", reason="post_eval_hold_fail"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.MODEL_CEILING
    assert result.earliest_broken_link == "observed_to_next_action"


# ─────────────────────────────────────────────────────────────────────
# Ladder-priority tests — multiple rungs match, earliest wins.
# ─────────────────────────────────────────────────────────────────────


def test_evidence_gap_wins_over_rca_gap():
    """A qid with both no-eval AND a present-but-ungrounded RCA on
    another cluster gets EVIDENCE_GAP (rung 1), not RCA_GAP (rung 2)."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _rca("", "H_other", ["q_other"], reason="rca_ungrounded"),
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.EVIDENCE_GAP


def test_rca_gap_wins_over_proposal_gap_for_same_qid():
    """A qid with both no-RCA AND no-AG returns RCA_GAP (rung 2) — the
    AG step is not relevant when the RCA is missing."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("", "H001", ["q1"], reason="rca_ungrounded"),
        # no AG, no proposal — also broken, but RCA_GAP wins.
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.RCA_GAP


def test_targeting_gap_wins_over_gate_or_cap_gap():
    """A qid with proposals that don't target it AND a gate drop on a
    sibling proposal returns TARGETING_GAP (rung 4), not GATE_OR_CAP_GAP
    (rung 5) — the targeting break is the earlier actionable signal."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=()),  # no target
        _gate_drop("P1", "blast_radius"),  # would be GATE_OR_CAP_GAP
        _resolved("q1"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.TARGETING_GAP


def test_apply_or_rollback_gap_wins_over_model_ceiling():
    """A qid with both rolled-back acceptance AND post_eval_hold_fail
    returns APPLY_OR_ROLLBACK_GAP (rung 6), not MODEL_CEILING (rung 7).

    The contract: if the iteration was rolled back, the patch never
    "really" landed — so MODEL_CEILING does not apply. This is why
    rung 7 sentinels on accepted iterations only.
    """
    from genie_space_optimizer.optimization.failure_bucketing import (
        FailureBucket, classify_unresolved_qid,
    )

    trace = _make_trace([
        _eval("q1"),
        _cluster("H001", ["q1"]),
        _rca("rca_001", "H001", ["q1"]),
        _ag("AG_001", ["q1"]),
        _proposal("P1", "AG_001", target_qids=("q1",)),
        _applied("P1", "AG_001", "q1"),
        _accepted("AG_001", outcome="rolled_back"),
        _resolved("q1", reason="post_eval_hold_fail"),
    ])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    assert result.bucket is FailureBucket.APPLY_OR_ROLLBACK_GAP


def test_evidence_record_ids_indices_into_trace():
    """The result's evidence_record_ids should be valid indices into
    trace.decision_records and refer to records that actually justified
    the bucket."""
    from genie_space_optimizer.optimization.failure_bucketing import (
        classify_unresolved_qid,
    )

    eval_rec = _eval("q1")
    cluster_rec = _cluster("H001", ["q1"])
    rca_rec = _rca("", "H001", ["q1"], reason="rca_ungrounded")
    resolution_rec = _resolved("q1")
    trace = _make_trace([eval_rec, cluster_rec, rca_rec, resolution_rec])
    result = classify_unresolved_qid(trace, "q1", iteration=1)
    for idx in result.evidence_record_ids:
        assert 0 <= idx < len(trace.decision_records)
    # At minimum the cluster record should be in the evidence (it's how
    # the classifier proved that the qid WAS clustered, which got it past
    # rung 1).
    cluster_idx = trace.decision_records.index(cluster_rec)
    assert cluster_idx in result.evidence_record_ids
