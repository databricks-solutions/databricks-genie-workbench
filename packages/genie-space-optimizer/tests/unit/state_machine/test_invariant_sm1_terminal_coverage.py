"""SM1: every QuestionStateInIteration at end-of-iteration is ACCEPTED or TERMINATED."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm1_terminal_coverage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)


def _seen() -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1)


def test_sm1_clean_when_all_states_accepted_or_terminated():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen()).terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        TerminalRecord("OPTIMIZER_NO_CANDIDATES", "x", FunnelStage.HARD_QID_SEEN, ""),
    )
    assert check_sm1_terminal_coverage(states=(s,)) == []


def test_sm1_violation_when_state_stuck_below_terminated():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    violations = check_sm1_terminal_coverage(states=(s,))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM1"
    assert "gs_009" in violations[0]["message"]
