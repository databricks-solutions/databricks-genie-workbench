"""Default ``MlflowRunner`` adapter used by the evidence bundle CLI.

Wraps Phase E.0 ``mlflow_audit.audit_optimization_run`` +
``MlflowClient.download_artifacts`` + ``mlflow_backfill.backfill_artifacts``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from mlflow.tracking import MlflowClient

from genie_space_optimizer.tools.mlflow_audit import audit_optimization_run


class DefaultMlflowRunner:
    def __init__(self) -> None:
        self._client = MlflowClient()

    def audit(self, *, optimization_run_id: str, experiment_id: str) -> Mapping[str, Any]:
        return audit_optimization_run(
            optimization_run_id=optimization_run_id,
            experiment_id=experiment_id,
            client=self._client,
        )

    def download_artifacts(
        self, *, run_id: str, artifact_path: str, dest: Path
    ) -> Sequence[Path]:
        dest.mkdir(parents=True, exist_ok=True)
        downloaded = self._client.download_artifacts(
            run_id=run_id, path=artifact_path, dst_path=str(dest)
        )
        path = Path(downloaded)
        if path.is_dir():
            return [p for p in path.rglob("*") if p.is_file()]
        return [path]

    def backfill(
        self,
        *,
        optimization_run_id: str,
        fixture_path: Path,
        anchor_run_id: str,
    ) -> Mapping[str, Any]:
        import json as _json

        from genie_space_optimizer.tools.mlflow_backfill import backfill_artifacts

        if not anchor_run_id:
            return {"uploaded": 0, "skipped_iterations": [], "target_run_id": ""}
        fixture = _json.loads(fixture_path.read_text())
        summary = backfill_artifacts(
            client=self._client,
            anchor_run_id=anchor_run_id,
            replay_fixture=fixture,
        )
        return {
            "uploaded": summary.uploaded,
            "skipped_iterations": summary.skipped_iterations,
            "target_run_id": summary.target_run_id,
        }
