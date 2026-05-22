"""On-disk persistence for QuestionStateInIteration and QuestionTrajectory.

Layout (see design doc section 10):

    <run_root>/
      iteration_<n>/
        qstate_<qid>.json
        transitions.jsonl
      trajectories/
        trajectory_<qid>.json
      outcome.json
"""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.trajectory import (
    QuestionTrajectory,
)


def qstate_path(*, run_root: Path, iteration: int, qid: str) -> Path:
    return run_root / f"iteration_{iteration}" / f"qstate_{qid}.json"


def write_qstate(*, run_root: Path, state: QuestionStateInIteration) -> Path:
    p = qstate_path(run_root=run_root, iteration=state.iteration, qid=state.qid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state.to_json(), sort_keys=True, indent=2))
    return p


def read_qstate(path: Path) -> QuestionStateInIteration:
    payload = json.loads(path.read_text())
    return QuestionStateInIteration.from_json(payload)


def trajectory_path(*, run_root: Path, qid: str) -> Path:
    return run_root / "trajectories" / f"trajectory_{qid}.json"


def write_trajectory(*, run_root: Path, trajectory: QuestionTrajectory) -> Path:
    p = trajectory_path(run_root=run_root, qid=trajectory.qid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(trajectory.to_json(), sort_keys=True, indent=2))
    return p


def read_trajectory(path: Path) -> QuestionTrajectory:
    payload = json.loads(path.read_text())
    return QuestionTrajectory.from_json(payload)
