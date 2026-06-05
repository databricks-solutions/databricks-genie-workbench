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
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state.to_json(), sort_keys=True, indent=2))
    except PermissionError:
        # Production hardening — when ``run_root`` is inherited from a
        # prior process / user (e.g. ``/tmp/gso/<run_id>`` collision
        # on a shared Databricks Apps node), the per-iteration mkdir
        # would otherwise kill the entire lever loop with
        # ``PermissionError: [Errno 13]``. Rebase under a writable
        # per-PID fallback computed by
        # :func:`run_root_resolver.resolve_run_root` and try once more.
        # The fallback is run_id-scoped so it remains discoverable by
        # postmortems via the same run_id grep.
        from genie_space_optimizer.optimization.run_root_resolver import (
            resolve_run_root,
        )
        fallback_root = resolve_run_root(run_root.name)
        if fallback_root != run_root:
            p = qstate_path(
                run_root=fallback_root,
                iteration=state.iteration,
                qid=state.qid,
            )
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(state.to_json(), sort_keys=True, indent=2))
        else:
            raise
    return p


def read_qstate(path: Path) -> QuestionStateInIteration:
    payload = json.loads(path.read_text())
    return QuestionStateInIteration.from_json(payload)


def trajectory_path(*, run_root: Path, qid: str) -> Path:
    return run_root / "trajectories" / f"trajectory_{qid}.json"


def write_trajectory(*, run_root: Path, trajectory: QuestionTrajectory) -> Path:
    p = trajectory_path(run_root=run_root, qid=trajectory.qid)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(trajectory.to_json(), sort_keys=True, indent=2))
    except PermissionError:
        # Symmetric fallback with ``write_qstate`` above — see that
        # block's comment for the production rationale.
        from genie_space_optimizer.optimization.run_root_resolver import (
            resolve_run_root,
        )
        fallback_root = resolve_run_root(run_root.name)
        if fallback_root != run_root:
            p = trajectory_path(
                run_root=fallback_root, qid=trajectory.qid,
            )
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(trajectory.to_json(), sort_keys=True, indent=2),
            )
        else:
            raise
    return p


def read_trajectory(path: Path) -> QuestionTrajectory:
    payload = json.loads(path.read_text())
    return QuestionTrajectory.from_json(payload)
