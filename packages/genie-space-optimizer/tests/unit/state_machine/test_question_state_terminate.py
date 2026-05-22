"""QuestionStateInIteration.terminate() attaches a TerminalRecord."""
import pytest

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
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


def _terminal(stage: FunnelStage) -> TerminalRecord:
    return TerminalRecord(
        kind="OPTIMIZER_STALLED_SAFE_NOOP",
        reason="ladder exhausted",
        deepest_stage_reached=stage,
        forbidden_signature="sig",
    )


def test_terminate_from_proposed_attaches_terminal_record():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
    )
    s = s.advance(
        FunnelStage.PROPOSED,
        StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
    )
    s = s.terminate(
        StageTransition(FunnelStage.PROPOSED, FunnelStage.TERMINATED, 4, "term", "validation_gate"),
        _terminal(FunnelStage.PROPOSED),
    )
    assert s.current_stage == FunnelStage.TERMINATED
    assert s.terminal is not None
    assert s.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP"
    assert s.deepest_stage_reached == FunnelStage.PROPOSED  # not TERMINATED


def test_terminate_twice_raises():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    s = s.terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        _terminal(FunnelStage.HARD_QID_SEEN),
    )
    with pytest.raises(ValueError, match="already TERMINATED"):
        s.terminate(
            StageTransition(FunnelStage.TERMINATED, FunnelStage.TERMINATED, 2, "t", "validation_gate"),
            _terminal(FunnelStage.HARD_QID_SEEN),
        )


def test_terminate_with_wrong_target_raises():
    s = build_initial_state(qid="gs_009", iteration=1, seen=_seen())
    with pytest.raises(ValueError, match="to_stage == TERMINATED"):
        s.terminate(
            StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
            _terminal(FunnelStage.HARD_QID_SEEN),
        )
