"""Phase B delta — Task 9.

Pins that ``render_operator_transcript`` projects every
``DecisionType`` into one of the nine named sections (no record
appears under a raw ``decision_type`` key, no record falls into a
catch-all bucket).

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 9.
"""
from __future__ import annotations


def _record(decision_type, qid: str, **kwargs):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        ReasonCode,
    )

    return DecisionRecord(
        iteration=1,
        decision_type=decision_type,
        outcome=kwargs.pop("outcome", DecisionOutcome.INFO),
        reason_code=kwargs.pop("reason_code", ReasonCode.NONE),
        question_id=qid,
        affected_qids=(qid,) if qid else (),
        evidence_refs=kwargs.pop("evidence_refs", ()),
        target_qids=kwargs.pop("target_qids", (qid,) if qid else ()),
        **kwargs,
    )


def test_every_decision_type_has_an_assigned_section() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        TYPE_TO_SECTION,
    )

    missing = [dt for dt in DecisionType if dt not in TYPE_TO_SECTION]
    assert missing == [], (
        f"DecisionType members without a transcript section: {missing}"
    )


def test_eval_classified_records_appear_under_hard_failures_section() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.EVAL_CLASSIFIED,
                qid="q1",
                reason_code=ReasonCode.HARD_FAILURE,
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    hard_idx = transcript.index("Hard Failures And QID State")
    rca_idx = transcript.index("RCA Cards With Evidence")
    assert hard_idx < transcript.index("q1") < rca_idx, (
        "EVAL_CLASSIFIED record should appear in the Hard Failures section"
    )


def test_rca_formed_appears_under_rca_cards_section() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        OptimizationTrace,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.RCA_FORMED,
                qid="q1",
                cluster_id="H001",
                rca_id="rca_h001",
                root_cause="missing_filter",
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    rca_idx = transcript.index("RCA Cards With Evidence")
    ag_idx = transcript.index("AG Decisions And Rationale")
    rca_h001_idx = transcript.index("rca_h001")
    assert rca_idx < rca_h001_idx < ag_idx


def test_strategist_ag_appears_under_ag_decisions_section() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        OptimizationTrace,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(DecisionType.STRATEGIST_AG_EMITTED, qid="", ag_id="AG1"),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    ag_idx = transcript.index("AG Decisions And Rationale")
    ps_idx = transcript.index("Proposal Survival And Gate Drops")
    ag1_idx = transcript.index("AG1")
    assert ag_idx < ag1_idx < ps_idx


def test_proposal_generated_appears_under_proposal_survival() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        OptimizationTrace,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.PROPOSAL_GENERATED,
                qid="q1",
                proposal_id="P001",
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    ps_idx = transcript.index("Proposal Survival And Gate Drops")
    ap_idx = transcript.index("Applied Patches And Acceptance")
    p001_idx = transcript.index("P001")
    assert ps_idx < p001_idx < ap_idx


def test_gate_decision_appears_under_proposal_survival_section() -> None:
    """GATE_DECISION (patch_cap, blast_radius) is part of proposal
    survival — it explains why proposals dropped."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.GATE_DECISION,
                qid="q1",
                outcome=DecisionOutcome.DROPPED,
                reason_code=ReasonCode.PATCH_CAP_DROPPED,
                gate="patch_cap",
                proposal_id="P001",
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    ps_idx = transcript.index("Proposal Survival And Gate Drops")
    ap_idx = transcript.index("Applied Patches And Acceptance")
    cap_idx = transcript.index("patch_cap")
    assert ps_idx < cap_idx < ap_idx


def test_patch_applied_and_acceptance_appear_under_applied_patches() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.PATCH_APPLIED,
                qid="q1",
                outcome=DecisionOutcome.APPLIED,
                reason_code=ReasonCode.PATCH_APPLIED,
                proposal_id="P001",
            ),
            _record(
                DecisionType.ACCEPTANCE_DECIDED,
                qid="q1",
                outcome=DecisionOutcome.ACCEPTED,
                reason_code=ReasonCode.PATCH_APPLIED,
                ag_id="AG1",
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    ap_idx = transcript.index("Applied Patches And Acceptance")
    obs_idx = transcript.index("Observed Results And Regressions")
    p001_idx = transcript.index("P001")
    ag1_idx = transcript.index("AG1")
    assert ap_idx < min(p001_idx, ag1_idx)
    assert max(p001_idx, ag1_idx) < obs_idx


def test_qid_resolution_appears_under_observed_results() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.QID_RESOLUTION,
                qid="q1",
                outcome=DecisionOutcome.RESOLVED,
                reason_code=ReasonCode.POST_EVAL_FAIL_TO_PASS,
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    obs_idx = transcript.index("Observed Results And Regressions")
    unr_idx = transcript.index("Unresolved QID Buckets")
    q1_idx = transcript.index("q1")
    assert obs_idx < q1_idx < unr_idx


def test_unresolved_qids_appear_under_unresolved_section() -> None:
    """QID_RESOLUTION with UNRESOLVED outcome is split between Observed
    Results (the transition row) and Unresolved QID Buckets (the open-
    bucket label)."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=(
            _record(
                DecisionType.QID_RESOLUTION,
                qid="q_open",
                outcome=DecisionOutcome.UNRESOLVED,
                reason_code=ReasonCode.POST_EVAL_HOLD_FAIL,
            ),
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    unr_idx = transcript.index("Unresolved QID Buckets")
    nxt_idx = transcript.index("Next Suggested Action")
    q_open_first = transcript.index("q_open")
    q_open_second = transcript.index("q_open", q_open_first + 1)
    # First mention is in Observed Results; second is in Unresolved
    # QID Buckets between unr_idx and nxt_idx.
    assert unr_idx < q_open_second < nxt_idx


def test_no_record_appears_below_next_suggested_action() -> None:
    """No record should fall through to a catch-all dump below the last
    section (the previous bottom-of-output ``decision_type`` block must
    be removed)."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        OptimizationTrace,
        ReasonCode,
        render_operator_transcript,
    )

    trace = OptimizationTrace(
        decision_records=tuple(
            _record(dt, qid=f"q_{i}", reason_code=ReasonCode.NONE)
            for i, dt in enumerate(DecisionType)
        ),
    )
    transcript = render_operator_transcript(trace=trace, iteration=1)
    next_idx = transcript.index("Next Suggested Action")
    tail = transcript[next_idx:]
    # No raw decision_type values should leak into the tail (other than
    # the section closing footer).
    for dt in DecisionType:
        assert dt.value not in tail, (
            f"DecisionType {dt.value} leaked into the tail of the transcript"
        )
