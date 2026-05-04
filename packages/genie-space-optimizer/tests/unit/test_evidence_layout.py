from pathlib import Path

import pytest


def test_bundle_paths_for_resolves_canonical_layout(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.evidence_layout import bundle_paths_for

    paths = bundle_paths_for(root=tmp_path, optimization_run_id="opt-abc")
    assert paths.root == tmp_path / "opt-abc"
    assert paths.evidence_dir == tmp_path / "opt-abc" / "evidence"
    assert paths.manifest == paths.evidence_dir / "manifest.json"
    assert paths.job_run == paths.evidence_dir / "job_run.json"
    assert paths.markers == paths.evidence_dir / "markers.json"
    assert paths.replay_fixture == paths.evidence_dir / "replay_fixture.json"
    assert paths.mlflow_audit_md == paths.evidence_dir / "mlflow_audit.md"
    assert paths.mlflow_audit_json == paths.evidence_dir / "mlflow_audit.json"
    assert paths.mlflow_dir == paths.evidence_dir / "mlflow"
    assert paths.traces_dir == paths.evidence_dir / "traces"
    assert paths.postmortem == paths.root / "postmortem.md"
    assert paths.intake == paths.root / "intake.md"


def test_bundle_paths_rejects_path_traversal() -> None:
    from genie_space_optimizer.tools.evidence_layout import bundle_paths_for

    with pytest.raises(ValueError, match="optimization_run_id"):
        bundle_paths_for(root=Path("/tmp"), optimization_run_id="../evil")


def test_manifest_round_trip(tmp_path: Path) -> None:
    from genie_space_optimizer.tools.evidence_layout import (
        Manifest,
        MissingPiece,
        MissingPieceKind,
        TraceFetchRecommendation,
        TraceFetchReason,
        manifest_from_dict,
        manifest_to_dict,
    )

    manifest = Manifest(
        schema_version=1,
        bundle_version=1,
        captured_at_utc="2026-05-04T12:34:56Z",
        inputs={"job_id": "j-1", "run_id": "r-1", "profile": "p"},
        resolved={
            "optimization_run_id": "opt-abc",
            "lever_loop_task_run_id": "tr-1",
            "mlflow_experiment_id": "exp-1",
            "anchor_mlflow_run_id": "mr-1",
            "sibling_mlflow_run_ids": ("mr-1", "mr-2"),
        },
        artifacts_pulled={
            "job_run": "evidence/job_run.json",
            "stdout": ("evidence/lever_loop_stdout.txt",),
            "stderr": ("evidence/lever_loop_stderr.txt",),
            "markers": "evidence/markers.json",
            "replay_fixture": "evidence/replay_fixture.json",
            "mlflow_audit_md": "evidence/mlflow_audit.md",
            "mlflow_audit_json": "evidence/mlflow_audit.json",
            "mlflow_artifacts": (
                {
                    "run_id": "mr-1",
                    "path": "evidence/mlflow/mr-1/phase_b/decision_trace/iter_01.json",
                    "size_bytes": 1234,
                },
            ),
            "traces": (),
        },
        missing_pieces=(
            MissingPiece(
                kind=MissingPieceKind.PHASE_B_ARTIFACT_MISSING_ON_ANCHOR,
                iteration=4,
                diagnosis="audit reports no phase_b/decision_trace/iter_04.json on any sibling",
                suggested_action="run mlflow_backfill --fixture <path>",
            ),
        ),
        trace_fetch_recommendations=(
            TraceFetchRecommendation(
                reason=TraceFetchReason.INCOMPLETE_DECISION_TRACE,
                iteration=4,
                trace_ids=("tr-abc",),
                detail="reason_code=UNKNOWN on 2 abandoned proposals",
            ),
        ),
        exit_status="complete_with_gaps",
    )

    encoded = manifest_to_dict(manifest)
    restored = manifest_from_dict(encoded)
    assert restored == manifest


def test_missing_piece_kind_is_closed_enum() -> None:
    from genie_space_optimizer.tools.evidence_layout import MissingPieceKind

    expected = {
        "STDOUT_TRUNCATED",
        "STDOUT_FALLBACK_NOTEBOOK_OUTPUT",
        "JOB_RUN_FETCH_FAILED",
        "MLFLOW_AUDIT_FAILED",
        "PHASE_A_ARTIFACT_MISSING_ON_ANCHOR",
        "PHASE_B_ARTIFACT_MISSING_ON_ANCHOR",
        "REPLAY_FIXTURE_NOT_IN_STDOUT",
        "OPTIMIZATION_RUN_ID_UNRESOLVED",
        "BACKFILL_FAILED",
    }
    assert {m.name for m in MissingPieceKind} == expected


def test_bundle_paths_includes_parent_bundle_paths(tmp_path) -> None:
    """Phase H Task 11: BundlePaths exposes the parent-bundle layout
    so evidence_bundle can materialize gso_postmortem_bundle/* under
    runid_analysis/<opt>/evidence/."""
    from genie_space_optimizer.tools.evidence_layout import bundle_paths_for
    paths = bundle_paths_for(root=tmp_path, optimization_run_id="opt-1")
    assert paths.parent_bundle_dir.name == "gso_postmortem_bundle"
    assert paths.parent_bundle_manifest.name == "manifest.json"
    assert paths.parent_bundle_artifact_index.name == "artifact_index.json"
    assert paths.parent_bundle_iterations_dir.name == "iterations"
    assert "gso_postmortem_bundle" in str(paths.parent_bundle_dir)
