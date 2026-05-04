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


_STDOUT_WITH_FIXTURE = (
    "GSO_RUN_MANIFEST_V1 "
    '{"databricks_job_id":"j-1","databricks_parent_run_id":"r-1",'
    '"event":"start","lever_loop_task_run_id":"tr-1",'
    '"mlflow_experiment_id":"exp-1","optimization_run_id":"opt-abc",'
    '"space_id":"sp-1"}\n'
    "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
    '{"version":1,"iterations":[{"iteration":1,"qids":["q1"]}]}\n'
    "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
)


def test_build_bundle_extracts_replay_fixture(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    fake_databricks_runner.get_run_output.return_value = {
        "logs": _STDOUT_WITH_FIXTURE,
        "error": "[WARN] something noisy\n",
    }
    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert result.paths.replay_fixture.exists()
    fixture = json.loads(result.paths.replay_fixture.read_text())
    assert fixture["iterations"][0]["qids"] == ["q1"]
    stderr_path = result.paths.evidence_dir / "lever_loop_stderr.txt"
    assert stderr_path.exists()
    assert "something noisy" in stderr_path.read_text()


def test_build_bundle_records_replay_fixture_missing(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle
    from genie_space_optimizer.tools.evidence_layout import MissingPieceKind

    # Override the fixture-bearing default with a marker-only stdout.
    fake_databricks_runner.get_run_output.return_value = {
        "logs": (
            "GSO_RUN_MANIFEST_V1 "
            '{"databricks_job_id":"j-1","databricks_parent_run_id":"r-1",'
            '"event":"start","lever_loop_task_run_id":"tr-1",'
            '"mlflow_experiment_id":"exp-1","optimization_run_id":"opt-abc",'
            '"space_id":"sp-1"}\n'
        ),
        "error": "",
    }
    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert any(
        p.kind is MissingPieceKind.REPLAY_FIXTURE_NOT_IN_STDOUT
        for p in result.manifest.missing_pieces
    )


def test_build_bundle_invokes_mlflow_audit_and_downloads_artifacts(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    fake_mlflow.audit.return_value = {
        "anchor_run_id": "mr-1",
        "sibling_runs": [
            {
                "run_id": "mr-1",
                "run_type": "lever_loop",
                "artifact_paths": [
                    "phase_a/journey_validation/iter_01.json",
                    "phase_b/decision_trace/iter_01.json",
                    "phase_b/operator_transcript/iter_01.txt",
                ],
            },
            {"run_id": "mr-2", "run_type": "strategy", "artifact_paths": []},
        ],
        "missing_per_iteration": [],
    }

    def _download(*, run_id: str, artifact_path: str, dest: Path) -> list[Path]:
        target = dest / artifact_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}" if artifact_path.endswith(".json") else "transcript")
        return [target]

    fake_mlflow.download_artifacts.side_effect = _download

    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    paths = result.paths
    assert (paths.mlflow_dir / "mr-1" / "phase_b" / "decision_trace" / "iter_01.json").exists()
    assert paths.mlflow_audit_md.exists()
    assert paths.mlflow_audit_json.exists()
    assert result.manifest.resolved["anchor_mlflow_run_id"] == "mr-1"
    assert "mr-1" in result.manifest.resolved["sibling_mlflow_run_ids"]
    pulled = result.manifest.artifacts_pulled["mlflow_artifacts"]
    assert any(p["run_id"] == "mr-1" for p in pulled)


def test_trace_fetch_recommendations_accept_list_decision_trace(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.evidence_bundle import (
        _derive_trace_fetch_recommendations,
    )
    from genie_space_optimizer.tools.evidence_layout import TraceFetchReason

    trace_file = tmp_path / "run-1" / "phase_b" / "decision_trace" / "iter_3.json"
    trace_file.parent.mkdir(parents=True)
    trace_file.write_text(
        json.dumps(
            [
                {
                    "iteration": 3,
                    "outcome": "FAILED",
                    "reason_code": "UNKNOWN",
                    "evidence_refs": [{"trace_id": "trace-1"}],
                }
            ]
        )
    )

    recommendations = _derive_trace_fetch_recommendations(mlflow_dir=tmp_path)

    assert len(recommendations) == 1
    assert recommendations[0].reason is TraceFetchReason.UNRESOLVED_REASON_CODE
    assert recommendations[0].iteration == 3
    assert recommendations[0].trace_ids == ("trace-1",)


def test_build_bundle_records_phase_b_missing_on_anchor(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle
    from genie_space_optimizer.tools.evidence_layout import MissingPieceKind

    fake_mlflow.audit.return_value = {
        "anchor_run_id": "mr-1",
        "sibling_runs": [{"run_id": "mr-1", "run_type": "lever_loop", "artifact_paths": []}],
        "missing_per_iteration": [
            {"iteration": 4, "kind": "PHASE_B_DECISION_TRACE", "anchor_run_id": "mr-1"},
        ],
    }
    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert any(
        p.kind is MissingPieceKind.PHASE_B_ARTIFACT_MISSING_ON_ANCHOR
        and p.iteration == 4
        for p in result.manifest.missing_pieces
    )


def test_auto_backfill_invoked_when_decision_trail_missing(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    fake_databricks_runner.get_run_output.return_value = {
        "logs": _STDOUT_WITH_FIXTURE,
        "error": "",
    }
    audit_calls: list[dict] = []

    def _audit(**kwargs):
        audit_calls.append(kwargs)
        if len(audit_calls) == 1:
            return {
                "anchor_run_id": "mr-1",
                "sibling_runs": [
                    {"run_id": "mr-1", "run_type": "lever_loop", "artifact_paths": []}
                ],
                "missing_per_iteration": [
                    {
                        "iteration": 1,
                        "kind": "PHASE_B_DECISION_TRACE",
                        "anchor_run_id": "mr-1",
                    },
                ],
            }
        return {
            "anchor_run_id": "mr-1",
            "sibling_runs": [
                {
                    "run_id": "mr-1",
                    "run_type": "lever_loop",
                    "artifact_paths": ["phase_b/decision_trace/iter_01.json"],
                }
            ],
            "missing_per_iteration": [],
        }

    fake_mlflow.audit.side_effect = _audit
    fake_mlflow.download_artifacts.return_value = []
    fake_mlflow.backfill = MagicMock(
        return_value={"uploaded": ["phase_b/decision_trace/iter_01.json"]}
    )

    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
        auto_backfill=True,
    )
    fake_mlflow.backfill.assert_called_once()
    assert result.manifest.exit_status == "complete"


def test_trace_fetch_recommendations_derived_from_decision_trace(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    fake_databricks_runner.get_run_output.return_value = {
        "logs": _STDOUT_WITH_FIXTURE,
        "error": "",
    }
    fake_mlflow.audit.return_value = {
        "anchor_run_id": "mr-1",
        "sibling_runs": [
            {
                "run_id": "mr-1",
                "run_type": "lever_loop",
                "artifact_paths": ["phase_b/decision_trace/iter_01.json"],
            }
        ],
        "missing_per_iteration": [],
    }

    def _download(*, run_id: str, artifact_path: str, dest: Path) -> list[Path]:
        target = dest / artifact_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "iteration": 1,
                    "decisions": [
                        {
                            "decision_type": "PROPOSAL_GENERATED",
                            "outcome": "ABANDONED",
                            "reason_code": "UNKNOWN",
                            "evidence_refs": [{"trace_id": "tr-abc"}],
                        }
                    ],
                }
            )
        )
        return [target]

    fake_mlflow.download_artifacts.side_effect = _download

    result = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    recs = result.manifest.trace_fetch_recommendations
    assert any(
        r.reason.value == "UNRESOLVED_REASON_CODE" and "tr-abc" in r.trace_ids
        for r in recs
    )


def test_build_bundle_is_idempotent(
    tmp_path: Path,
    fake_databricks_runner: MagicMock,
    fake_mlflow: MagicMock,
) -> None:
    from genie_space_optimizer.tools.evidence_bundle import build_bundle

    first = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    fake_databricks_runner.reset_mock()
    fake_mlflow.reset_mock()
    # Re-prime the mocks so the second call (if it ever fully runs) returns
    # the same shapes. Idempotence should short-circuit and not exercise these.
    fake_databricks_runner.get_run.return_value = {
        "run_id": "r-1",
        "tasks": [{"task_key": "lever_loop", "run_id": "tr-1"}],
    }
    fake_databricks_runner.get_run_output.return_value = {
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
    second = build_bundle(
        job_id="j-1",
        run_id="r-1",
        profile="p",
        output_root=tmp_path,
        databricks_runner=fake_databricks_runner,
        mlflow_runner=fake_mlflow,
    )
    assert second.manifest.exit_status == first.manifest.exit_status
    fake_databricks_runner.get_run.assert_called_once()


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


def test_download_parent_bundle_succeeds_with_stub_client(monkeypatch, tmp_path) -> None:
    """Phase H Task 11: download_parent_bundle calls MlflowClient.
    download_artifacts and reports success when the call returns."""
    from pathlib import Path
    from genie_space_optimizer.tools.evidence_bundle import download_parent_bundle

    pulls: list[tuple[str, str, str]] = []

    class _FakeClient:
        def download_artifacts(self, run_id, path, dst_path):
            pulls.append((str(run_id), str(path), str(dst_path)))
            target = Path(dst_path) / "gso_postmortem_bundle"
            target.mkdir(parents=True, exist_ok=True)
            (target / "manifest.json").write_text(
                '{"schema_version":"v1","optimization_run_id":"opt-1"}'
            )
            return str(target)

    monkeypatch.setattr("mlflow.tracking.MlflowClient", lambda: _FakeClient())

    target = tmp_path / "evidence" / "gso_postmortem_bundle"
    success, missing = download_parent_bundle(
        parent_run_id="br-1",
        target_dir=target,
    )

    assert success is True
    assert missing == []
    assert ("br-1", "gso_postmortem_bundle", str(tmp_path / "evidence")) in pulls
    assert (target / "manifest.json").exists()


def test_download_parent_bundle_records_missing_piece_on_failure(
    monkeypatch, tmp_path,
) -> None:
    """If the MLflow download raises, download_parent_bundle returns
    (False, [MissingPiece]) so the caller can fall back without
    propagating the exception."""
    from genie_space_optimizer.tools.evidence_bundle import download_parent_bundle
    from genie_space_optimizer.tools.evidence_layout import MissingPieceKind

    class _FailingClient:
        def download_artifacts(self, *a, **k):
            raise RuntimeError("MLflow is down")

    monkeypatch.setattr("mlflow.tracking.MlflowClient", lambda: _FailingClient())

    target = tmp_path / "evidence" / "gso_postmortem_bundle"
    success, missing = download_parent_bundle(
        parent_run_id="br-1",
        target_dir=target,
    )

    assert success is False
    assert len(missing) == 1
    assert missing[0].kind == MissingPieceKind.MLFLOW_AUDIT_FAILED
    assert "parent bundle download failed" in missing[0].diagnosis
    assert "MLflow is down" in missing[0].diagnosis
