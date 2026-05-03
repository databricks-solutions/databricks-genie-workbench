"""End-to-end smoke: harness writes, bundle reads.

Logs decision-trail artifacts to a ``file://`` MLflow tracking URI for
one synthetic iteration, runs the bundle against fabricated stdout +
the live experiment, and asserts the manifest is gap-free.

Marked ``slow`` so the default unit suite does not import mlflow's full
machinery on every run.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.mark.slow
def test_one_iteration_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("mlflow")
    import mlflow

    tracking_uri = f"file://{tmp_path / 'mlruns'}"
    mlflow.set_tracking_uri(tracking_uri)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", tracking_uri)

    experiment_id = mlflow.create_experiment("gso-bundle-smoke")
    with mlflow.start_run(experiment_id=experiment_id, run_name="lever_loop") as run:
        mlflow.set_tag("genie.optimization_run_id", "opt-smoke")
        mlflow.set_tag("genie.run_type", "lever_loop")
        mlflow.log_dict(
            {"iteration": 1, "decisions": []},
            "phase_b/decision_trace/iter_01.json",
        )
        mlflow.log_text("transcript", "phase_b/operator_transcript/iter_01.txt")
        mlflow.log_dict(
            {"iteration": 1, "violations": []},
            "phase_a/journey_validation/iter_01.json",
        )
        anchor_run_id = run.info.run_id

    stdout = (
        "GSO_RUN_MANIFEST_V1 "
        + json.dumps(
            {
                "databricks_job_id": "j-smoke",
                "databricks_parent_run_id": "r-smoke",
                "event": "start",
                "lever_loop_task_run_id": "tr-smoke",
                "mlflow_experiment_id": experiment_id,
                "optimization_run_id": "opt-smoke",
                "space_id": "sp-1",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        + "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
        + '{"version":1,"iterations":[{"iteration":1,"qids":["q1"]}]}\n'
        + "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
    )

    db_runner = MagicMock()
    db_runner.get_run.return_value = {
        "run_id": "r-smoke",
        "tasks": [{"task_key": "lever_loop", "run_id": "tr-smoke"}],
    }
    db_runner.get_run_output.return_value = {"logs": stdout, "error": ""}

    from genie_space_optimizer.tools._mlflow_runner import DefaultMlflowRunner
    from genie_space_optimizer.tools.evidence_bundle import build_bundle
    from genie_space_optimizer.tools.evidence_layout import bundle_paths_for

    runner = DefaultMlflowRunner()
    result = build_bundle(
        job_id="j-smoke",
        run_id="r-smoke",
        profile="DEFAULT",
        output_root=tmp_path / "runid_analysis",
        databricks_runner=db_runner,
        mlflow_runner=runner,
    )
    paths = bundle_paths_for(
        root=tmp_path / "runid_analysis", optimization_run_id="opt-smoke"
    )
    assert paths.manifest.exists()
    assert result.manifest.exit_status == "complete"
    assert result.manifest.resolved["anchor_mlflow_run_id"] == anchor_run_id
    assert (
        paths.mlflow_dir
        / anchor_run_id
        / "phase_b"
        / "decision_trace"
        / "iter_01.json"
    ).exists()
