"""Legacy helper Tier-3 wiring (P-B).

When ``stage_handlers_chunk_d_enabled()`` is False the harness uses
``_databricks_ids_from_env`` instead of the typed stage. Tier-3 must
fire on this path too — otherwise an emergency chunk-D rollback
re-introduces the ``unknown`` regression that P-B exists to fix.
"""
from __future__ import annotations

import os
from unittest import mock

from genie_space_optimizer.optimization.harness import (
    _databricks_ids_from_env,
)
from genie_space_optimizer.optimization.stages.run_manifest import (
    JobsRunSnapshot,
)


def test_legacy_helper_invokes_jobs_api_fallback_when_env_and_dbutils_blank(
    monkeypatch,
) -> None:
    """Mirrors the production failure shape: env empty, dbutils
    import fails, MLflow tag carries a runID, harness's
    ``_resolve_jobs_run_snapshot`` returns a full snapshot. The
    legacy helper must end up with three non-sentinel IDs."""
    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    def _fake_collect_tags(*, mlflow_run_id):
        return {
            "mlflow.databricks.jobID": "",
            "mlflow.databricks.jobRunID": "",
            "mlflow.databricks.runID": "777",
        }

    captured_kwargs = {}

    def _fake_resolve_snapshot(**kwargs):
        captured_kwargs.update(kwargs)
        return JobsRunSnapshot(
            job_id="11111",
            parent_run_id="22222",
            task_run_ids=("777",),
        )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._collect_mlflow_databricks_tags",
        _fake_collect_tags,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._resolve_jobs_run_snapshot",
        _fake_resolve_snapshot,
    )

    ids = _databricks_ids_from_env(mlflow_run_id="active-run-1")

    assert ids["databricks_job_id"] == "11111"
    assert ids["databricks_parent_run_id"] == "22222"
    assert ids["lever_loop_task_run_id"] == "777"
    # The harness helper must be called with the seed dictionaries
    # the legacy path computed — proves the wiring is correct.
    assert captured_kwargs["mlflow_run_tags"]["mlflow.databricks.runID"] == "777"
    assert "env_resolved" in captured_kwargs
    assert "dbutils_resolved" in captured_kwargs


def test_legacy_helper_default_args_remain_back_compatible(monkeypatch) -> None:
    """Existing call sites that omit ``mlflow_run_id`` (legacy
    code paths) must still work — the helper defaults to no MLflow
    seed, ``_resolve_jobs_run_snapshot`` returns ``None`` because
    no seed is available, and the result is the historical sentinel
    triple."""
    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._collect_mlflow_databricks_tags",
        lambda *, mlflow_run_id: {
            "mlflow.databricks.jobID": "",
            "mlflow.databricks.jobRunID": "",
            "mlflow.databricks.runID": "",
        },
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._resolve_jobs_run_snapshot",
        lambda **_: None,
    )

    ids = _databricks_ids_from_env()  # no kwargs — legacy call shape

    assert ids["databricks_job_id"] == "unknown"
    assert ids["databricks_parent_run_id"] == "unknown"
    assert ids["lever_loop_task_run_id"] == "unknown"


def test_legacy_helper_records_jobs_api_attempted_when_snapshot_is_empty(
    monkeypatch,
) -> None:
    """``_resolve_jobs_run_snapshot`` returns an EMPTY ``JobsRunSnapshot``
    (not None) when the harness called ``jobs.get_run`` and got
    nothing back. The legacy helper must propagate that into the
    emitted ``GSO_DATABRICKS_IDS_RESOLVED_V1`` marker as
    ``jobs_api_attempted=True, jobs_api_succeeded=False``."""
    import io
    import sys

    for var in (
        "DATABRICKS_JOB_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._collect_mlflow_databricks_tags",
        lambda *, mlflow_run_id: {
            "mlflow.databricks.jobID": "",
            "mlflow.databricks.jobRunID": "",
            "mlflow.databricks.runID": "12345",
        },
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._resolve_jobs_run_snapshot",
        lambda **_: JobsRunSnapshot(),  # empty — call attempted but returned nothing
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.config.databricks_ids_resolution_trace_enabled",
        lambda: True,
    )

    captured = io.StringIO()
    monkeypatch.setattr(sys, "stdout", captured)

    ids = _databricks_ids_from_env(mlflow_run_id="active-run-1")

    assert ids["databricks_job_id"] == "unknown"
    output = captured.getvalue()
    assert "GSO_DATABRICKS_IDS_RESOLVED_V1" in output
    assert '"jobs_api_attempted":true' in output
    assert '"jobs_api_succeeded":false' in output
