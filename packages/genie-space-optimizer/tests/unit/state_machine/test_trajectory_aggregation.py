"""QuestionTrajectory aggregates state across iterations."""
from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    QuestionTrajectory,
    build_trajectory,
)


def _seen(it: int) -> HardQidSeenRecord:
    return HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", it)


def test_trajectory_first_seen_iteration():
    it1 = build_initial_state(qid="gs_009", iteration=1, seen=_seen(1))
    it2 = build_initial_state(qid="gs_009", iteration=2, seen=_seen(2))
    traj = build_trajectory(qid="gs_009", iterations=(it1, it2))
    assert traj.first_seen_iteration == 1


def test_trajectory_deepest_stage_ever_is_max_across_iterations():
    it1 = build_initial_state(qid="gs_009", iteration=1, seen=_seen(1))
    it1 = it1.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    )
    it2 = build_initial_state(qid="gs_009", iteration=2, seen=_seen(2))
    it2 = it2.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm"),
    ).advance(
        FunnelStage.CLUSTERED,
        StageTransition(FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch"),
    ).advance(
        FunnelStage.PROPOSED,
        StageTransition(FunnelStage.CLUSTERED, FunnelStage.PROPOSED, 3, "t", "llm"),
    )
    traj = build_trajectory(qid="gs_009", iterations=(it1, it2))
    assert traj.deepest_stage_ever == FunnelStage.PROPOSED


def test_trajectory_collects_terminal_reasons():
    it1 = build_initial_state(qid="gs_009", iteration=1, seen=_seen(1)).terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        TerminalRecord("OPTIMIZER_NO_CANDIDATES", "r1", FunnelStage.HARD_QID_SEEN, "sig"),
    )
    it2 = build_initial_state(qid="gs_009", iteration=2, seen=_seen(2)).terminate(
        StageTransition(FunnelStage.HARD_QID_SEEN, FunnelStage.TERMINATED, 1, "t", "validation_gate"),
        TerminalRecord("OPTIMIZER_STALLED_SAFE_NOOP", "r2", FunnelStage.HARD_QID_SEEN, "sig"),
    )
    traj = build_trajectory(qid="gs_009", iterations=(it1, it2))
    assert "r1" in traj.cumulative_terminal_reasons
    assert "r2" in traj.cumulative_terminal_reasons


def test_trajectory_roundtrip():
    it = build_initial_state(qid="gs_009", iteration=1, seen=_seen(1))
    traj = build_trajectory(qid="gs_009", iterations=(it,))
    assert QuestionTrajectory.from_json(traj.to_json()) == traj
