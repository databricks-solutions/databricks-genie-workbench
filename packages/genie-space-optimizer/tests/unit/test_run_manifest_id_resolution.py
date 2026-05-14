"""Phase 0.1 — manifest threading: Jobs-API-resolved IDs are
NEVER blank when at least one resolver tier returned them.

The current bug: when env vars are unset (CI / interactive
notebook) AND dbutils tags are unavailable AND the Jobs run
snapshot resolver returns a value, the V1 manifest can still
emit ``databricks_job_id=""`` because the resolver-precedence
ladder doesn't include the snapshot tier as a fallback.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    assemble_run_manifest_v2_line,
)


def _payload(line: str) -> dict:
    """Extract the JSON payload from a GSO_RUN_MANIFEST_V2 line."""
    prefix, _, body = line.partition(" ")
    assert prefix == "GSO_RUN_MANIFEST_V2", f"unexpected prefix {prefix!r}"
    return json.loads(body)


def test_assemble_run_manifest_prefers_dbutils_over_env_over_snapshot():
    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt-1",
        space_id="space-1",
        event="started",
        env_resolved={
            "databricks_job_id": "env-job",
            "databricks_parent_run_id": "env-parent",
            "lever_loop_task_run_id": "env-task",
        },
        dbutils_resolved={
            "databricks_job_id": "dbu-job",
            "databricks_parent_run_id": "dbu-parent",
            "lever_loop_task_run_id": "dbu-task",
        },
        jobs_run_snapshot={
            "job_id": "snap-job",
            "parent_run_id": "snap-parent",
            "task_run_id": "snap-task",
        },
        mlflow_experiment_id="exp-1",
    )
    p = _payload(line)
    assert p["databricks_job_id"] == "dbu-job"
    assert p["lever_loop_task_run_id"] == "dbu-task"
    assert p["databricks_parent_run_id"] == "dbu-parent"


def test_assemble_run_manifest_falls_back_to_env_when_dbutils_blank():
    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt-1",
        space_id="space-1",
        event="started",
        env_resolved={
            "databricks_job_id": "env-job",
            "databricks_parent_run_id": "env-parent",
            "lever_loop_task_run_id": "env-task",
        },
        dbutils_resolved={
            "databricks_job_id": "",
            "databricks_parent_run_id": "",
            "lever_loop_task_run_id": "",
        },
        jobs_run_snapshot={
            "job_id": "snap-job",
            "parent_run_id": "snap-parent",
            "task_run_id": "snap-task",
        },
        mlflow_experiment_id="exp-1",
    )
    p = _payload(line)
    assert p["databricks_job_id"] == "env-job"
    assert p["lever_loop_task_run_id"] == "env-task"


def test_assemble_run_manifest_falls_back_to_jobs_snapshot_when_others_blank():
    """The Phase 0.1 fix: env blank + dbutils blank + snapshot
    populated MUST resolve to the snapshot values, not blank."""
    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt-1",
        space_id="space-1",
        event="started",
        env_resolved={
            "databricks_job_id": "",
            "databricks_parent_run_id": "",
            "lever_loop_task_run_id": "",
        },
        dbutils_resolved={
            "databricks_job_id": "",
            "databricks_parent_run_id": "",
            "lever_loop_task_run_id": "",
        },
        jobs_run_snapshot={
            "job_id": "snap-job",
            "parent_run_id": "snap-parent",
            "task_run_id": "snap-task",
        },
        mlflow_experiment_id="exp-1",
    )
    p = _payload(line)
    assert p["databricks_job_id"] == "snap-job"
    assert p["databricks_parent_run_id"] == "snap-parent"
    assert p["lever_loop_task_run_id"] == "snap-task"


def test_assemble_run_manifest_emits_unknown_sentinel_when_all_blank():
    """When EVERY tier is blank, the manifest emits the explicit
    'unknown' sentinel string rather than '' so postmortems can
    grep for it."""
    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt-1",
        space_id="space-1",
        event="started",
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        jobs_run_snapshot={"job_id": "", "parent_run_id": "", "task_run_id": ""},
        mlflow_experiment_id="",
    )
    p = _payload(line)
    assert p["databricks_job_id"] == "unknown"


def test_assemble_run_manifest_legacy_scalar_kwargs_still_work():
    """When the new tier kwargs are NOT provided, the existing
    scalar kwargs win and produce the legacy output unchanged."""
    line = assemble_run_manifest_v2_line(
        optimization_run_id="opt-1",
        databricks_job_id="legacy-job",
        databricks_parent_run_id="legacy-parent",
        lever_loop_task_run_id="legacy-task",
        mlflow_experiment_id="exp-1",
        space_id="space-1",
        event="started",
    )
    p = _payload(line)
    assert p["databricks_job_id"] == "legacy-job"
    assert p["lever_loop_task_run_id"] == "legacy-task"
    assert p["databricks_parent_run_id"] == "legacy-parent"
