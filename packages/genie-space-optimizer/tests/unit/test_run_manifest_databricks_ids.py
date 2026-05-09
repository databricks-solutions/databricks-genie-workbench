"""Cycle 14-V Task 6 — run manifest carries non-blank databricks IDs.

Resolves Open Q#10. The pre-T6 manifest emission emits blank
databricks_job_id / lever_loop_task_run_id / databricks_parent_run_id
when running outside a Databricks notebook context. Both anchors
(7Now run 338386531912450 F9, airline run 833709971504406 F8)
confirm this is cross-space.

Post-T6: ``_databricks_ids_from_env()`` returns three keys with
either the resolved ID or the literal sentinel ``"unknown"``;
NEVER blank/empty.
"""

from __future__ import annotations

import os
from unittest import mock

from genie_space_optimizer.optimization.harness import (
    _databricks_ids_from_env,
)


def test_environment_set_ids_populate_manifest() -> None:
    with mock.patch.dict(
        os.environ,
        {
            "DATABRICKS_JOB_ID": "job-42",
            "DATABRICKS_RUN_ID": "parent-123",
            "DATABRICKS_TASK_RUN_ID": "task-456",
        },
        clear=True,
    ):
        ids = _databricks_ids_from_env()
    assert ids == {
        "databricks_job_id": "job-42",
        "databricks_parent_run_id": "parent-123",
        "lever_loop_task_run_id": "task-456",
    }


def test_missing_env_falls_through_to_unknown_sentinel() -> None:
    """When dbutils is unavailable AND env is empty, the resolver
    must return the literal 'unknown' sentinel — NEVER blank."""
    with mock.patch.dict(os.environ, {}, clear=True):
        ids = _databricks_ids_from_env()
    # Three keys, all populated, none blank.
    assert set(ids.keys()) == {
        "databricks_job_id",
        "databricks_parent_run_id",
        "lever_loop_task_run_id",
    }
    for key, value in ids.items():
        assert value, f"{key} must not be blank; got {value!r}"
        # Outside Databricks (no dbutils): every value is the
        # sentinel.
        assert value == "unknown"


def test_partial_env_populates_present_ids_only() -> None:
    with mock.patch.dict(
        os.environ,
        {"DATABRICKS_JOB_ID": "job-7"},
        clear=True,
    ):
        ids = _databricks_ids_from_env()
    assert ids["databricks_job_id"] == "job-7"
    assert ids["databricks_parent_run_id"] == "unknown"
    assert ids["lever_loop_task_run_id"] == "unknown"


def test_databricks_job_run_id_alias_resolves_parent_run_id() -> None:
    """The legacy DATABRICKS_JOB_RUN_ID alias is honoured for
    backwards compatibility with the Cycle 12-T1 emission site."""
    with mock.patch.dict(
        os.environ,
        {"DATABRICKS_JOB_RUN_ID": "legacy-parent-77"},
        clear=True,
    ):
        ids = _databricks_ids_from_env()
    assert ids["databricks_parent_run_id"] == "legacy-parent-77"
