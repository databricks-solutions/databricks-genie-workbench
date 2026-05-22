"""SM6: current_stage == TERMINATED iff terminal record is present."""
from dataclasses import replace

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm6_terminal_record_presence,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)


def test_sm6_clean_when_terminated_has_record():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0, "", "", 1),
    )
    s = s.terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        TerminalRecord("OPTIMIZER_NO_CANDIDATES", "x", FunnelStage.HARD_QID_SEEN, ""),
    )
    assert check_sm6_terminal_record_presence(states=(s,)) == []


def test_sm6_violation_when_terminated_but_no_record():
    s = build_initial_state(
        qid="gs_009", iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0, "", "", 1),
    )
    s = s.terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        TerminalRecord("OPTIMIZER_NO_CANDIDATES", "x", FunnelStage.HARD_QID_SEEN, ""),
    )
    s_bad = replace(s, terminal=None)
    violations = check_sm6_terminal_record_presence(states=(s_bad,))
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM6"
