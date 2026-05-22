"""QuestionStateInIteration.advance() returns a new state with the next stage populated."""
import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
    build_initial_state,
)


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord(
        eval_row_id="row_1",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="SELECT 1",
        expected_shape="ROW_NUMBER",
        iteration_first_seen=1,
    )


def test_initial_state_at_hard_qid_seen():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    assert s.current_stage == FunnelStage.HARD_QID_SEEN
    assert s.deepest_stage_reached == FunnelStage.HARD_QID_SEEN
    assert s.diagnosed is None
    assert len(s.transitions) == 0


def test_advance_attaches_diagnosis_record_and_transition():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    diag = DiagnosisRecord(
        source="plan11_stage1",
        rca_kind_label="plural_top_n_collapse",
        evidence_summary="...",
        observed_failure="...",
        expected_sql_shape="...",
        confidence="high",
        rca_card_id="rca_1",
    )
    s2 = s.advance(
        to_stage=FunnelStage.DIAGNOSED,
        transition=StageTransition(
            from_stage=FunnelStage.HARD_QID_SEEN,
            to_stage=FunnelStage.DIAGNOSED,
            at_ms=1000,
            transformer_name="plan11_stage1_diagnosis",
            transition_kind="llm",
        ),
        diagnosed=diag,
    )
    assert s2.current_stage == FunnelStage.DIAGNOSED
    assert s2.deepest_stage_reached == FunnelStage.DIAGNOSED
    assert s2.diagnosed == diag
    assert len(s2.transitions) == 1
    # Original state is unchanged (immutability).
    assert s.current_stage == FunnelStage.HARD_QID_SEEN


def test_advance_to_illegal_transition_raises():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    with pytest.raises(ValueError, match="illegal transition"):
        s.advance(
            to_stage=FunnelStage.APPLIED,
            transition=StageTransition(
                from_stage=FunnelStage.HARD_QID_SEEN,
                to_stage=FunnelStage.APPLIED,
                at_ms=1,
                transformer_name="bogus",
                transition_kind="validation_gate",
            ),
        )


def _t(frm: FunnelStage, to: FunnelStage) -> StageTransition:
    return StageTransition(
        from_stage=frm, to_stage=to, at_ms=0,
        transformer_name="t", transition_kind="validation_gate",
    )


def test_deepest_stage_monotonic_through_escalation_cycle():
    """When state cycles APPLYABLE -> PROPOSED for escalation, deepest stays APPLYABLE."""
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s = s.advance(FunnelStage.DIAGNOSED, _t(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED))
    s = s.advance(FunnelStage.CLUSTERED, _t(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED))
    s = s.advance(FunnelStage.PROPOSED, _t(FunnelStage.CLUSTERED, FunnelStage.PROPOSED))
    s = s.advance(FunnelStage.NORMALIZED, _t(FunnelStage.PROPOSED, FunnelStage.NORMALIZED))
    s = s.advance(FunnelStage.APPLYABLE, _t(FunnelStage.NORMALIZED, FunnelStage.APPLYABLE))
    # Escalation: APPLYABLE -> PROPOSED
    s = s.advance(FunnelStage.PROPOSED, _t(FunnelStage.APPLYABLE, FunnelStage.PROPOSED))
    assert s.current_stage == FunnelStage.PROPOSED
    assert s.deepest_stage_reached == FunnelStage.APPLYABLE
