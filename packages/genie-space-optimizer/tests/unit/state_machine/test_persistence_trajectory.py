"""Persistence: write/read a QuestionTrajectory to/from JSON on disk."""
from pathlib import Path

from genie_space_optimizer.optimization.state_machine.persistence import (
    read_trajectory,
    trajectory_path,
    write_trajectory,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    build_trajectory,
)


def test_trajectory_path_format(tmp_path: Path):
    assert trajectory_path(run_root=tmp_path, qid="gs_009") == (
        tmp_path / "trajectories" / "trajectory_gs_009.json"
    )


def test_trajectory_write_then_read(tmp_path: Path):
    it = build_initial_state(
        qid="gs_009",
        iteration=1,
        seen=HardQidSeenRecord("row_1", "row_is_hard_failure", 0.0, "SELECT 1", "x", 1),
    )
    traj = build_trajectory(qid="gs_009", iterations=(it,))
    p = write_trajectory(run_root=tmp_path, trajectory=traj)
    assert p.exists()
    assert read_trajectory(p) == traj
