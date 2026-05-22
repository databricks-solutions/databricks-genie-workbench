"""SM2: every StageTransition corresponds to exactly one transformer execution."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm2_single_transition_origin,
)
from genie_space_optimizer.optimization.state_machine.records import StageTransition
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage


def test_sm2_clean_with_unique_transitions():
    t1 = StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "a", "llm")
    t2 = StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "b", "batch")
    assert check_sm2_single_transition_origin(transitions=(t1, t2)) == []


def test_sm2_violation_on_duplicate_at_ms_same_transformer():
    t = StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "a", "llm")
    violations = check_sm2_single_transition_origin(transitions=(t, t))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM2"
