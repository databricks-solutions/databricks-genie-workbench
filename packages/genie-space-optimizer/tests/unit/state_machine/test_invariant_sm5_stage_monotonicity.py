"""SM5: deepest_stage_reached is monotonic across transitions; never decreases."""
from dataclasses import replace

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm5_stage_monotonicity,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)


def test_sm5_clean_on_normal_progression():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0, "", "", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    assert check_sm5_stage_monotonicity(states=(s,)) == []


def test_sm5_violation_if_deepest_synthesized_below_max_seen():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0, "", "", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    s_bad = replace(s, deepest_stage_reached=FunnelStage.HARD_QID_SEEN)
    violations = check_sm5_stage_monotonicity(states=(s_bad,))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM5"
