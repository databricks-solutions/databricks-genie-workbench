"""Phase E.0 Task 5 — harness anchors decision-trail artifacts to a stable run.

Uses MLflow's file:// tracking store + a manual sibling-run setup to
verify the harness writes phase_a/ and phase_b/ artifacts to the
resolved anchor (the lever_loop sibling), not to whichever run is
currently active.
"""

from pathlib import Path


def test_artifacts_land_on_anchor_run_when_active_is_a_child(tmp_path: Path) -> None:
    import mlflow
    from mlflow.tracking import MlflowClient

    tracking_uri = f"file://{tmp_path}/mlruns"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("phase_e0_test")

    opt_run_id = "opt_test_1"

    parent = mlflow.start_run(run_name="lever_loop_parent")
    parent_run_id = parent.info.run_id
    mlflow.set_tags({
        "genie.optimization_run_id": opt_run_id,
        "genie.run_type": "lever_loop",
    })
    mlflow.end_run()

    child = mlflow.start_run(run_name="full_eval_child")
    child_run_id = child.info.run_id
    mlflow.set_tags({
        "genie.optimization_run_id": opt_run_id,
        "genie.run_type": "full_eval",
    })

    from genie_space_optimizer.optimization.harness import (
        _persist_phase_a_artifact_to_anchor,
    )

    result = _persist_phase_a_artifact_to_anchor(
        opt_run_id=opt_run_id,
        iteration=4,
        report_dict={"violations": [], "is_valid": True, "iteration": 4},
    )
    mlflow.end_run()

    client = MlflowClient()
    parent_artifacts = [
        a.path for a in client.list_artifacts(parent_run_id, "phase_a")
    ]
    child_artifacts_root = [a.path for a in client.list_artifacts(child_run_id)]
    assert result.success is True
    assert result.anchor_run_id == parent_run_id
    assert any("journey_validation" in p for p in parent_artifacts), (
        f"Expected phase_a/journey_validation/* on parent {parent_run_id}, "
        f"got {parent_artifacts}"
    )
    assert not any("phase_a" in p for p in child_artifacts_root), (
        f"phase_a/* must not land on child {child_run_id}, got {child_artifacts_root}"
    )


def test_persist_phase_a_returns_failure_when_no_sibling_found(tmp_path: Path) -> None:
    import mlflow

    tracking_uri = f"file://{tmp_path}/mlruns_b"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("phase_e0_test_b")

    from genie_space_optimizer.optimization.harness import (
        _persist_phase_a_artifact_to_anchor,
    )

    result = _persist_phase_a_artifact_to_anchor(
        opt_run_id="unknown_run_id",
        iteration=0,
        report_dict={"violations": [], "is_valid": True, "iteration": 0},
    )
    assert result.success is False
    assert result.anchor_run_id == ""
    assert result.exception_class == "NoSiblingRun"
