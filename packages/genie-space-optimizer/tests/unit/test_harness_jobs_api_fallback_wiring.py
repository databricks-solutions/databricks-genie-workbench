"""Harness adapter helpers for Tier-3 (P-B).

The pure stage at ``stages/run_manifest.py`` consumes plain JSON-
safe data: ``mlflow_run_tags: dict[str, str]`` and
``jobs_run_snapshot: JobsRunSnapshot | None``. The harness owns
*every* impure operation — MLflow tag fetch, seed selection,
``WorkspaceClient`` construction, ``jobs.get_run`` call, and all
exception handling — and hands the stage data only.

These tests pin the adapter shape:

  * ``_collect_mlflow_databricks_tags(mlflow_run_id)`` returns the
    three auto-stamped tags as a dict (empty strings for missing
    keys), and tolerates MLflow being unavailable.
  * ``_resolve_jobs_run_snapshot(...)`` returns ``None`` when the
    harness cannot or should not call ``jobs.get_run`` (no SDK,
    no seed, call raised). Returns a ``JobsRunSnapshot`` (possibly
    with all-empty fields) when the call was attempted, with the
    lever-loop task surfaced first in ``task_run_ids``.
"""
from __future__ import annotations

from unittest import mock

import pytest

from genie_space_optimizer.optimization.harness import (
    _collect_mlflow_databricks_tags,
    _resolve_jobs_run_snapshot,
)
from genie_space_optimizer.optimization.stages.run_manifest import (
    JobsRunSnapshot,
)


# --- _collect_mlflow_databricks_tags ---------------------------------


def test_collect_mlflow_tags_returns_three_keys() -> None:
    """Even when nothing resolves, the helper returns the three
    expected keys (empty strings) so the pure stage's
    ``mlflow_run_tags.get(...)`` calls are never KeyError-prone."""
    out = _collect_mlflow_databricks_tags(mlflow_run_id=None)
    assert set(out.keys()) == {
        "mlflow.databricks.jobID",
        "mlflow.databricks.jobRunID",
        "mlflow.databricks.runID",
    }
    for value in out.values():
        assert value == ""


def test_collect_mlflow_tags_reads_from_mlflow_get_run() -> None:
    fake_tags = {
        "mlflow.databricks.jobID": "job-123",
        "mlflow.databricks.jobRunID": "parent-456",
        "mlflow.databricks.runID": "task-789",
        "mlflow.user": "alice@example.com",
    }
    fake_run = mock.MagicMock()
    fake_run.data.tags = fake_tags

    with mock.patch(
        "genie_space_optimizer.optimization.harness.mlflow.get_run",
        return_value=fake_run,
    ):
        out = _collect_mlflow_databricks_tags(mlflow_run_id="abc")

    assert out["mlflow.databricks.jobID"] == "job-123"
    assert out["mlflow.databricks.jobRunID"] == "parent-456"
    assert out["mlflow.databricks.runID"] == "task-789"
    assert "mlflow.user" not in out


def test_collect_mlflow_tags_swallows_mlflow_errors() -> None:
    """Any MLflow exception (network, auth, missing run) returns
    the empty default. The harness downstream skips Tier-3 cleanly."""
    with mock.patch(
        "genie_space_optimizer.optimization.harness.mlflow.get_run",
        side_effect=RuntimeError("simulated MLflow outage"),
    ):
        out = _collect_mlflow_databricks_tags(mlflow_run_id="abc")

    assert all(v == "" for v in out.values())


# --- _resolve_jobs_run_snapshot --------------------------------------


def test_resolve_jobs_run_snapshot_returns_none_when_no_seed() -> None:
    """No MLflow tags, no env/dbutils partials → no seed → harness
    must NOT call ``jobs.get_run`` (avoids racy
    ``jobs.list_runs`` fallbacks). Returns ``None``."""
    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={},
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )
    assert snapshot is None


def test_resolve_jobs_run_snapshot_returns_none_when_sdk_unavailable(
    monkeypatch,
) -> None:
    """SDK construction raises (local pytest, no profile) → harness
    cannot call ``jobs.get_run`` → returns ``None``."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.make_workspace_client",
        mock.Mock(side_effect=RuntimeError("no databricks profile")),
    )
    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={"mlflow.databricks.runID": "12345"},
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )
    assert snapshot is None


def test_resolve_jobs_run_snapshot_uses_runid_seed_first(monkeypatch) -> None:
    """Seed precedence: ``mlflow.databricks.runID`` (most specific)
    first, then ``mlflow.databricks.jobRunID``, then any non-empty
    parent / task run ID from earlier tiers."""
    fake_lever = mock.MagicMock()
    fake_lever.task_key = "lever_loop"
    fake_lever.run_id = 999
    fake_other = mock.MagicMock()
    fake_other.task_key = "preflight"
    fake_other.run_id = 111

    fake_run = mock.MagicMock()
    fake_run.job_id = 42
    fake_run.run_id = 555
    fake_run.tasks = [fake_other, fake_lever]

    fake_ws = mock.MagicMock()
    fake_ws.jobs.get_run.return_value = fake_run

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.make_workspace_client",
        mock.Mock(return_value=fake_ws),
    )

    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={
            "mlflow.databricks.runID": "999",
            "mlflow.databricks.jobRunID": "555",
        },
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )

    assert isinstance(snapshot, JobsRunSnapshot)
    assert snapshot.job_id == "42"
    assert snapshot.parent_run_id == "555"
    # Lever-loop task surfaced first regardless of SDK's order.
    assert snapshot.task_run_ids[0] == "999"
    assert "111" in snapshot.task_run_ids
    fake_ws.jobs.get_run.assert_called_once_with(run_id=999)


def test_resolve_jobs_run_snapshot_falls_back_to_partial_seed(
    monkeypatch,
) -> None:
    """No MLflow tags, but env supplied a parent_run_id — harness
    uses that as the seed."""
    fake_run = mock.MagicMock()
    fake_run.job_id = 42
    fake_run.run_id = 100
    fake_run.tasks = []

    fake_ws = mock.MagicMock()
    fake_ws.jobs.get_run.return_value = fake_run

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.make_workspace_client",
        mock.Mock(return_value=fake_ws),
    )

    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={},
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "100", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )
    assert snapshot is not None
    fake_ws.jobs.get_run.assert_called_once_with(run_id=100)


def test_resolve_jobs_run_snapshot_returns_empty_snapshot_on_call_failure(
    monkeypatch,
) -> None:
    """``jobs.get_run`` raises (auth error, network) → harness
    returns an *empty* ``JobsRunSnapshot`` (NOT ``None``) so the
    stage records ``jobs_api_attempted=True`` /
    ``jobs_api_succeeded=False``. This is the diagnostic distinction
    between "we never tried" (None) and "we tried and got nothing"
    (empty snapshot)."""
    fake_ws = mock.MagicMock()
    fake_ws.jobs.get_run.side_effect = RuntimeError("simulated 403")

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.make_workspace_client",
        mock.Mock(return_value=fake_ws),
    )

    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={"mlflow.databricks.runID": "12345"},
        env_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )
    assert isinstance(snapshot, JobsRunSnapshot)
    assert snapshot.job_id == ""
    assert snapshot.parent_run_id == ""
    assert snapshot.task_run_ids == ()


def test_resolve_jobs_run_snapshot_skipped_when_all_fields_already_filled(
    monkeypatch,
) -> None:
    """Env + dbutils together resolved all three fields — no need
    to call Jobs API. Returns ``None`` so the stage records
    ``jobs_api_attempted=False`` (honest: we didn't need to try)."""
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness.make_workspace_client",
        mock.Mock(side_effect=AssertionError("must not be called")),
    )
    snapshot = _resolve_jobs_run_snapshot(
        mlflow_run_tags={"mlflow.databricks.runID": "999"},
        env_resolved={"databricks_job_id": "1", "databricks_parent_run_id": "2", "lever_loop_task_run_id": "3"},
        dbutils_resolved={"databricks_job_id": "", "databricks_parent_run_id": "", "lever_loop_task_run_id": ""},
    )
    assert snapshot is None
