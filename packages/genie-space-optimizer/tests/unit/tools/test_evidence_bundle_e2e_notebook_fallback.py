"""End-to-end: build_bundle pulls notebook_output.result when logs is empty."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from genie_space_optimizer.tools.evidence_bundle import build_bundle
from genie_space_optimizer.tools.evidence_layout import MissingPieceKind


class _StubDatabricksRunner:
    def __init__(
        self,
        *,
        run_payload: Mapping[str, Any],
        output_payload: Mapping[str, Any],
    ) -> None:
        self._run = run_payload
        self._output = output_payload

    def get_run(self, *, run_id: str, profile: str) -> Mapping[str, Any]:
        return self._run

    def get_run_output(self, *, run_id: str, profile: str) -> Mapping[str, Any]:
        return self._output


class _StubMlflowRunner:
    """Minimal MlflowRunner Protocol stub: empty audit + no downloads."""

    def audit(
        self, *, optimization_run_id: str, experiment_id: str
    ) -> Mapping[str, Any]:
        return {"sibling_runs": [], "missing_per_iteration": []}

    def download_artifacts(
        self, *, run_id: str, artifact_path: str, dest: Path
    ) -> Sequence[Path]:
        return []


_NOTEBOOK_STDOUT = (
    'GSO_RUN_MANIFEST_V1 {"optimization_run_id":"abc-123",'
    '"experiment_id":"123456","mlflow_experiment_id":"123456",'
    '"git_sha":"deadbeef"}\n'
)


def test_build_bundle_uses_notebook_output_when_logs_empty(tmp_path: Path) -> None:
    runner = _StubDatabricksRunner(
        run_payload={
            "run_id": 526124065145154,
            "tasks": [
                {"task_key": "lever_loop", "run_id": 852330621004424},
            ],
        },
        output_payload={
            "logs": "",
            "logs_truncated": False,
            "error": "",
            "notebook_output": {
                "result": _NOTEBOOK_STDOUT,
                "truncated": False,
            },
        },
    )
    mlflow_runner = _StubMlflowRunner()

    result = build_bundle(
        job_id="1036606061019898",
        run_id="526124065145154",
        profile="default",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow_runner,
    )

    stdout_path = result.paths.evidence_dir / "lever_loop_stdout.txt"
    assert stdout_path.exists(), "stdout file should be present"
    assert stdout_path.read_text() == _NOTEBOOK_STDOUT

    manifest = json.loads(result.paths.manifest.read_text())
    assert manifest["artifacts_pulled"]["stdout_source"] == "notebook_output.result"

    fallback_kinds = {p["kind"] for p in manifest["missing_pieces"]}
    assert MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT.value in fallback_kinds
    # opt-run id was resolved from the marker, NOT a placeholder
    assert manifest["resolved"]["optimization_run_id"] == "abc-123"


def test_build_bundle_logs_field_used_when_present(tmp_path: Path) -> None:
    runner = _StubDatabricksRunner(
        run_payload={
            "run_id": 1,
            "tasks": [{"task_key": "lever_loop", "run_id": 2}],
        },
        output_payload={
            "logs": _NOTEBOOK_STDOUT,
            "logs_truncated": False,
            "error": "",
            "notebook_output": {"result": "ignored", "truncated": False},
        },
    )
    mlflow_runner = _StubMlflowRunner()

    result = build_bundle(
        job_id="j",
        run_id="2",
        profile="default",
        output_root=tmp_path,
        databricks_runner=runner,
        mlflow_runner=mlflow_runner,
    )

    manifest = json.loads(result.paths.manifest.read_text())
    assert manifest["artifacts_pulled"]["stdout_source"] == "logs"
    fallback_kinds = {p["kind"] for p in manifest["missing_pieces"]}
    assert (
        MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT.value
        not in fallback_kinds
    )
