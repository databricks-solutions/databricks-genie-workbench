"""Tests for the PermissionError fallback added to
``state_machine.persistence.write_qstate`` /
``write_trajectory``. The fallback rebases under the
run-root resolver's PID-suffixed path so a shared-tmp
``PermissionError: [Errno 13]`` does not kill the lever loop."""
from __future__ import annotations

import os
from pathlib import Path

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.persistence import (
    write_qstate,
    write_trajectory,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    QuestionTrajectory,
    build_trajectory,
)


def _make_state(qid: str = "q1", iteration: int = 1) -> QuestionStateInIteration:
    seen = HardQidSeenRecord(
        eval_row_id="er_1",
        predicate="row_is_hard_failure",
        score=0.0,
        baseline_sql="SELECT 1",
        expected_shape="aggregate",
        iteration_first_seen=iteration,
    )
    return QuestionStateInIteration(
        qid=qid,
        iteration=iteration,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=seen,
        transitions=(
            StageTransition(
                from_stage=FunnelStage.HARD_QID_SEEN,
                to_stage=FunnelStage.HARD_QID_SEEN,
                at_ms=0,
                transformer_name="dispatch_input",
                transition_kind="ingest",
            ),
        ),
    )


def test_write_qstate_falls_back_when_run_root_not_writable(
    monkeypatch, tmp_path: Path,
) -> None:
    # Simulate the production failure: ``/tmp/gso/<run_id>`` exists
    # but is owned by another process / user and has no write perms.
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    blocked_root = tmp_path / "gso" / "blocked_run"
    blocked_root.mkdir(parents=True)
    os.chmod(blocked_root, 0o500)

    state = _make_state()
    try:
        # ``write_qstate`` must NOT raise PermissionError — the
        # internal fallback rebases under the PID-suffixed sibling
        # computed by the resolver.
        out_path = write_qstate(run_root=blocked_root, state=state)
        assert out_path.exists()
        # The fallback path must live under the PID-suffixed sibling,
        # NOT under the blocked root.
        assert blocked_root not in out_path.parents
        assert "__pid" in str(out_path)
    finally:
        os.chmod(blocked_root, 0o700)


def test_write_trajectory_falls_back_when_run_root_not_writable(
    monkeypatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.run_root_resolver.tempfile.gettempdir",
        lambda: str(tmp_path),
    )
    monkeypatch.delenv("GSO_PLAN_V3_RUN_ROOT", raising=False)
    monkeypatch.delenv("GSO_PHASE_H_BUNDLE_ROOT", raising=False)

    blocked_root = tmp_path / "gso" / "blocked_run_2"
    blocked_root.mkdir(parents=True)
    os.chmod(blocked_root, 0o500)

    traj = build_trajectory(qid="q1", iterations=(_make_state(),))
    try:
        out_path = write_trajectory(run_root=blocked_root, trajectory=traj)
        assert out_path.exists()
        assert blocked_root not in out_path.parents
    finally:
        os.chmod(blocked_root, 0o700)


def test_write_qstate_happy_path_unaffected(tmp_path: Path) -> None:
    # Writable run_root → no fallback; canonical path used.
    state = _make_state()
    out_path = write_qstate(run_root=tmp_path, state=state)
    assert out_path == tmp_path / "iteration_1" / "qstate_q1.json"
    assert out_path.exists()
