import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def fake_databricks_runner() -> MagicMock:
    runner = MagicMock()
    runner.get_run.return_value = {
        "run_id": "r-1",
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "tasks": [
            {"task_key": "lever_loop", "run_id": "tr-1", "state": {"result_state": "SUCCESS"}},
        ],
    }
    runner.get_run_output.return_value = {
        "logs": (
            "GSO_RUN_MANIFEST_V1 "
            '{"databricks_job_id":"j-1","databricks_parent_run_id":"r-1",'
            '"event":"start","lever_loop_task_run_id":"tr-1",'
            '"mlflow_experiment_id":"exp-1","optimization_run_id":"opt-abc",'
            '"space_id":"sp-1"}\n'
            "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
            '{"version":1,"iterations":[{"iteration":1,"qids":["q1"]}]}\n'
            "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
        ),
        "error": "",
    }
    return runner


@pytest.fixture
def fake_mlflow() -> MagicMock:
    mod = MagicMock()
    mod.audit.return_value = {"sibling_runs": [], "missing_per_iteration": []}
    mod.download_artifacts.return_value = []
    return mod


def test_build_bundle_creates_manifest_with_resolved_opt_run_id(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import BundleResult, build_bundle
    from genie_space_optimizer.tools.evidence_layout import (
        bundle_paths_for,
        manifest_from_dict,
    )

    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert isinstance(result, BundleResult)
    paths = bundle_paths_for(root=tmp_path, optimization_run_id="opt-abc")
    assert paths.manifest.exists()
    manifest = manifest_from_dict(json.loads(paths.manifest.read_text()))
    assert manifest.resolved["optimization_run_id"] == "opt-abc"
    assert manifest.resolved["lever_loop_task_run_id"] == "tr-1"
    assert manifest.inputs == {"job_id": "j-1", "run_id": "r-1", "profile": "p"}
    assert paths.job_run.exists()


def test_build_bundle_records_unresolved_opt_run_id_when_no_marker(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle
    from genie_space_optimizer.tools.evidence_layout import MissingPieceKind

    fake_databricks_runner.get_run_output.return_value = {"logs": "no markers", "error": ""}
    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert any(
        p.kind is MissingPieceKind.OPTIMIZATION_RUN_ID_UNRESOLVED
        for p in result.manifest.missing_pieces
    )
    assert result.manifest.exit_status == "incomplete"


def test_main_smoke(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import main as bundle_main

    with patch(
        "genie_space_optimizer.tools.evidence_bundle._default_databricks_runner",
        return_value=fake_databricks_runner,
    ), patch(
        "genie_space_optimizer.tools.evidence_bundle._default_mlflow_runner",
        return_value=fake_mlflow,
    ):
        rc = bundle_main(
            [
                "--job-id",
                "j-1",
                "--run-id",
                "r-1",
                "--profile",
                "p",
                "--output-dir",
                str(tmp_path),
            ]
        )
    assert rc == 0
