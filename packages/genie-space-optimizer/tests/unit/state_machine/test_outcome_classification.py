"""classify_run_outcome() maps trajectories to a typed OPTIMIZER_* outcome."""
from dataclasses import replace

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.outcome import (
    classify_run_outcome,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
    AppliedRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    build_trajectory,
)


def _t(frm: FunnelStage, to: FunnelStage, k="validation_gate"):
    return StageTransition(frm, to, 0, "t", k)


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1)


def _make_state_at(qid: str, stage: FunnelStage, **kw):
    """Advance a state from HARD_QID_SEEN up to the given stage, attaching kw at the final hop."""
    order = (
        FunnelStage.HARD_QID_SEEN,
        FunnelStage.DIAGNOSED,
        FunnelStage.CLUSTERED,
        FunnelStage.PROPOSED,
        FunnelStage.NORMALIZED,
        FunnelStage.APPLYABLE,
        FunnelStage.APPLIED,
        FunnelStage.EVALUATED,
        FunnelStage.ACCEPTED,
    )
    s = build_initial_state(qid=qid, iteration=1, seen=_seen())
    target_idx = order.index(stage)
    for i in range(target_idx):
        nxt = order[i + 1]
        extra = kw if nxt == stage else {}
        s = s.advance(nxt, _t(order[i], nxt), **extra)
    return s


def test_improved_when_accepted_with_positive_delta():
    s = _make_state_at(
        "gs_009", FunnelStage.ACCEPTED,
        accepted=AcceptanceDecisionRecord("accepted", "ok", True, ()),
    )
    s = replace(s, evaluated=EvaluatedRecord(
        pre_apply_score=0.0, post_apply_score=1.0,
        pre_apply_sql="", post_apply_sql="", eval_row_id_post="r",
    ))
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    outcome = classify_run_outcome((traj,))
    assert outcome == "OPTIMIZER_IMPROVED"


def test_tried_no_gain_when_applied_then_rolled_back():
    s = _make_state_at(
        "gs_009", FunnelStage.APPLIED,
        applied=AppliedRecord(1, "c", 0, ("i",)),
    )
    s = s.terminate(
        _t(FunnelStage.APPLIED, FunnelStage.TERMINATED),
        TerminalRecord("OPTIMIZER_TRIED_NO_GAIN", "rb", FunnelStage.APPLIED, "sig"),
    )
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    assert classify_run_outcome((traj,)) == "OPTIMIZER_TRIED_NO_GAIN"


def test_stalled_no_applied_when_deepest_below_applied():
    s = _make_state_at("gs_009", FunnelStage.APPLYABLE)
    s = s.terminate(
        _t(FunnelStage.APPLYABLE, FunnelStage.TERMINATED),
        TerminalRecord("OPTIMIZER_STALLED_NO_APPLIED_PATCHES", "x",
                       FunnelStage.APPLYABLE, "sig"),
    )
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    assert classify_run_outcome((traj,)) == "OPTIMIZER_STALLED_NO_APPLIED_PATCHES"


def test_no_candidates_when_deepest_below_proposed():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s = s.advance(FunnelStage.DIAGNOSED, _t(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, "llm"))
    s = s.terminate(
        _t(FunnelStage.DIAGNOSED, FunnelStage.TERMINATED),
        TerminalRecord("OPTIMIZER_NO_CANDIDATES", "x", FunnelStage.DIAGNOSED, ""),
    )
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    assert classify_run_outcome((traj,)) == "OPTIMIZER_NO_CANDIDATES"


def test_skipped_input_gap_when_no_trajectories_but_eval_had_hard_rows():
    assert classify_run_outcome((), hard_rows_in_eval=True) == "OPTIMIZER_SKIPPED_INPUT_GAP"


def test_invariant_violation_overrides_when_set():
    s = _make_state_at("gs_009", FunnelStage.PROPOSED)
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    # current_stage != ACCEPTED and != TERMINATED → SM1 invariant violation
    assert classify_run_outcome((traj,)) == "OPTIMIZER_INVARIANT_VIOLATION"


def test_blast_radius_rejected_proposed_is_settled_not_invariant_violation():
    # d139 postmortem: a blast-radius rejection cycles the QID back to
    # PROPOSED. When no narrower candidate materializes that iteration,
    # the QID legitimately ends in PROPOSED with a deliberate
    # blast_radius_rejected attempt. That is a settled no-candidate
    # outcome, NOT an SM1 invariant breakage.
    s = _make_state_at(
        "gs_009",
        FunnelStage.PROPOSED,
        proposals=(
            ProposalAttempt(
                attempt_index=0,
                intent_id="i1",
                patch_type="add_column_description",
                deepest_stage_in_attempt=FunnelStage.APPLYABLE,
                outcome="blast_radius_rejected",
                outcome_reason="reason=high_collateral_risk_flagged",
            ),
        ),
    )
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    outcome = classify_run_outcome((traj,))
    assert outcome != "OPTIMIZER_INVARIANT_VIOLATION"
    assert outcome == "OPTIMIZER_STALLED_NO_APPLIED_PATCHES"


def test_proposed_without_gate_rejection_still_invariant_violation():
    # A PROPOSED state with NO deliberate gate-rejection attempt is a
    # genuine stuck state and must still trip SM1.
    s = _make_state_at(
        "gs_009",
        FunnelStage.PROPOSED,
        proposals=(
            ProposalAttempt(
                attempt_index=0,
                intent_id="i1",
                patch_type="add_sql_snippet_filter",
                deepest_stage_in_attempt=FunnelStage.PROPOSED,
                outcome="proposed",
                outcome_reason="",
            ),
        ),
    )
    traj = build_trajectory(qid="gs_009", iterations=(s,))
    assert classify_run_outcome((traj,)) == "OPTIMIZER_INVARIANT_VIOLATION"
