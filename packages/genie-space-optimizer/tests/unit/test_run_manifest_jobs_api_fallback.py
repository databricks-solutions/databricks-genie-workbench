"""Tier-3 (MLflow tags + Jobs API snapshot) tests for the pure
run_manifest stage.

The harness performs every impure operation (MLflow tag fetch,
``WorkspaceClient.jobs.get_run`` call, exception handling) and
hands the stage two JSON-safe inputs:

  * ``mlflow_run_tags: dict[str, str]``
  * ``jobs_run_snapshot: JobsRunSnapshot | None``

These tests build both directly and assert the stage's projection
+ resolution-path bookkeeping. No callable crosses the stage
boundary — that pattern was the architectural mistake the
user-spec correction caught.

Mirrors the deployed-runtime failure shape captured on both May-12
trial anchors (ccf1d60d, 31ecd96f): env vars empty, dbutils tags
empty, MLflow tags / Jobs snapshot supply the IDs.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.run_manifest import (
    DATABRICKS_ID_SENTINEL,
    JobsRunSnapshot,
    ResolutionPath,
    RunManifestInput,
    resolve_run_manifest,
)


def test_jobs_snapshot_fills_all_three_when_env_and_dbutils_empty() -> None:
    """Production failure shape: env empty, dbutils tags empty,
    harness supplies a fully-populated Jobs snapshot."""
    inp = RunManifestInput(
        env={},
        dbutils_available=True,
        dbutils_tags={},
        mlflow_run_tags={"mlflow.databricks.runID": "999888777666555"},
        jobs_run_snapshot=JobsRunSnapshot(
            job_id="918273645",
            parent_run_id="555444333222111",
            task_run_ids=("999888777666555", "111222333444555"),
        ),
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == "918273645"
    assert out.databricks_parent_run_id == "555444333222111"
    assert out.lever_loop_task_run_id == "999888777666555"
    assert out.resolution_path is ResolutionPath.JOBS_API
    assert out.fields_resolved == 3
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is True


def test_mlflow_tags_resolve_directly_when_snapshot_absent() -> None:
    """When the harness could not call ``jobs.get_run`` but the
    MLflow run carries auto-stamped Databricks tags, the stage
    fills the three IDs directly from those tags. ``jobs_api_*``
    diagnostics stay False — the harness did not attempt the call."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={
            "mlflow.databricks.jobID": "918273645",
            "mlflow.databricks.jobRunID": "555444333222111",
            "mlflow.databricks.runID": "999888777666555",
        },
        jobs_run_snapshot=None,
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == "918273645"
    assert out.databricks_parent_run_id == "555444333222111"
    assert out.lever_loop_task_run_id == "999888777666555"
    assert out.resolution_path is ResolutionPath.JOBS_API
    assert out.fields_resolved == 3
    assert out.jobs_api_attempted is False
    assert out.jobs_api_succeeded is False


def test_mlflow_tags_partial_then_snapshot_fills_remainder() -> None:
    """MLflow runID alone fills the task ID directly; Jobs snapshot
    supplies the job_id + parent_run_id."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={"mlflow.databricks.runID": "task-99"},
        jobs_run_snapshot=JobsRunSnapshot(
            job_id="job-7",
            parent_run_id="parent-5",
            task_run_ids=("task-99",),
        ),
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == "job-7"
    assert out.databricks_parent_run_id == "parent-5"
    assert out.lever_loop_task_run_id == "task-99"
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is True


def test_mixed_jobs_api_when_env_partial_and_snapshot_fills_remainder() -> None:
    """Env supplies job_id; Jobs snapshot supplies parent + task.
    Resolution path records the mix."""
    inp = RunManifestInput(
        env={"DATABRICKS_JOB_ID": "env-job-7"},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={},
        jobs_run_snapshot=JobsRunSnapshot(
            job_id="env-job-7",  # ignored — env already filled this
            parent_run_id="parent-from-jobs-api",
            task_run_ids=("task-from-jobs-api",),
        ),
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == "env-job-7"
    assert out.databricks_parent_run_id == "parent-from-jobs-api"
    assert out.lever_loop_task_run_id == "task-from-jobs-api"
    assert out.resolution_path is ResolutionPath.MIXED_JOBS_API
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is True


def test_attempted_but_empty_snapshot_yields_sentinel() -> None:
    """The harness called ``jobs.get_run`` but got back an empty
    Run (e.g. permissions error masquerading as empty). The stage
    records ``jobs_api_attempted=True`` / ``jobs_api_succeeded=False``
    and falls through to the sentinel for any field MLflow tags
    didn't already cover."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={},
        jobs_run_snapshot=JobsRunSnapshot(),  # all defaults, all empty
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == DATABRICKS_ID_SENTINEL
    assert out.databricks_parent_run_id == DATABRICKS_ID_SENTINEL
    assert out.lever_loop_task_run_id == DATABRICKS_ID_SENTINEL
    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is False


def test_no_evidence_at_all_yields_honest_sentinel_diagnostics() -> None:
    """No env, no dbutils, no MLflow tags, no snapshot — the
    resolver returns sentinels and the diagnostic flags say so
    honestly: ``dbutils_attempted=False``, ``jobs_api_attempted=False``.
    This is the legitimate "platform exposes no evidence" case."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={},
        jobs_run_snapshot=None,
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.databricks_job_id == DATABRICKS_ID_SENTINEL
    assert out.dbutils_attempted is False
    assert out.jobs_api_attempted is False
    assert out.jobs_api_succeeded is False


def test_snapshot_none_with_no_mlflow_tags_skips_tier3_cleanly() -> None:
    """The harness chose not to call ``jobs.get_run`` (no SDK or
    no seed). MLflow tags are also empty. Tier-3 contributes
    nothing; the stage falls back to whatever earlier tiers found."""
    inp = RunManifestInput(
        env={"DATABRICKS_JOB_ID": "env-j"},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={},
        jobs_run_snapshot=None,
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.databricks_job_id == "env-j"
    assert out.databricks_parent_run_id == DATABRICKS_ID_SENTINEL
    assert out.lever_loop_task_run_id == DATABRICKS_ID_SENTINEL
    # Env supplied one field, neither tier-2 nor tier-3 fired.
    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.jobs_api_attempted is False


def test_first_task_run_id_is_lever_loop_when_harness_orders() -> None:
    """The harness adapter is responsible for ordering
    ``task_run_ids`` so the lever-loop task is first. The stage
    trusts that ordering and uses ``task_run_ids[0]``."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        dbutils_tags={},
        mlflow_run_tags={},
        jobs_run_snapshot=JobsRunSnapshot(
            job_id="j",
            parent_run_id="p",
            task_run_ids=("first-task", "second-task"),
        ),
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.lever_loop_task_run_id == "first-task"
