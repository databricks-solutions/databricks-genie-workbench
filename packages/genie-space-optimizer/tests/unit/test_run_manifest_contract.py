import os
from unittest import mock

import pytest

from genie_space_optimizer.optimization.stages.run_manifest import (
    DATABRICKS_ID_SENTINEL,
    ResolutionPath,
    RunManifestInput,
    RunManifestOutput,
    resolve_run_manifest,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_resolution_path_enum() -> None:
    assert ResolutionPath.ENV.value == "env"
    assert ResolutionPath.DBUTILS.value == "dbutils"
    assert ResolutionPath.MIXED.value == "mixed"
    assert ResolutionPath.SENTINEL.value == "sentinel"


def test_resolve_env_only_path() -> None:
    inp = RunManifestInput(
        env={
            "DATABRICKS_JOB_ID": "1234567890123456",
            "DATABRICKS_RUN_ID": "9876543210987654",
            "DATABRICKS_TASK_RUN_ID": "5555444433332222",
        },
        dbutils_available=False,
    )
    out = resolve_run_manifest(ctx=None, inp=inp)
    assert out.resolution_path is ResolutionPath.ENV
    assert out.databricks_job_id == "1234567890123456"
    assert out.databricks_parent_run_id == "9876543210987654"
    assert out.lever_loop_task_run_id == "5555444433332222"
    assert out.fields_resolved == 3
    assert out.fields_total == 3


def test_resolve_sentinel_when_all_blank() -> None:
    inp = RunManifestInput(env={}, dbutils_available=False)
    out = resolve_run_manifest(ctx=None, inp=inp)
    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.databricks_job_id == DATABRICKS_ID_SENTINEL
    assert out.databricks_parent_run_id == DATABRICKS_ID_SENTINEL
    assert out.lever_loop_task_run_id == DATABRICKS_ID_SENTINEL
    assert out.fields_resolved == 0
    assert out.fields_total == 3


def test_resolve_falls_back_to_jobrunid_when_run_id_missing() -> None:
    inp = RunManifestInput(
        env={
            "DATABRICKS_JOB_ID": "1",
            "DATABRICKS_JOB_RUN_ID": "2",
            "DATABRICKS_TASK_RUN_ID": "3",
        },
        dbutils_available=False,
    )
    out = resolve_run_manifest(ctx=None, inp=inp)
    assert out.databricks_parent_run_id == "2"


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(RunManifestInput, JsonRoundTrip)
    assert issubclass(RunManifestOutput, JsonRoundTrip)


def test_output_round_trips() -> None:
    out = RunManifestOutput(
        databricks_job_id="1234567890123456",
        databricks_parent_run_id="9876543210987654",
        lever_loop_task_run_id="5555444433332222",
        resolution_path=ResolutionPath.ENV,
        fields_resolved=3,
        fields_total=3,
    )
    payload = out.to_json()
    restored = RunManifestOutput.from_json(payload)
    assert restored == out


# ── P-B Tier-3 — ResolutionPath enum extensions ──────────────────────


def test_resolution_path_jobs_api_members_exist() -> None:
    """Tier-3 (Jobs API) resolution path adds two enum members:

    - ``JOBS_API`` — every field came from the Jobs API
      (env + dbutils both empty, MLflow tag seeded a Jobs API call).
    - ``MIXED_JOBS_API`` — at least one field came from the Jobs API
      and at least one came from env or dbutils.

    Pinned so any future enum churn breaks loudly here, not in
    downstream parsers.
    """
    assert ResolutionPath.JOBS_API.value == "jobs_api"
    assert ResolutionPath.MIXED_JOBS_API.value == "mixed_jobs_api"


# ── P-B Tier-3 — RunManifestInput field extensions ───────────────────


def test_input_carries_mlflow_tags_and_jobs_run_snapshot() -> None:
    """Tier-3 adds two new optional input fields:

    - ``mlflow_run_tags`` — auto-stamped MLflow tags for the active
      run (empty dict when MLflow is unavailable).
    - ``jobs_run_snapshot`` — pre-resolved Jobs API projection
      (``None`` when the harness did not call ``jobs.get_run``).

    Both are JSON-safe (``dict[str, str]`` and a frozen
    ``JsonRoundTrip`` dataclass) so ``RunManifestInput`` keeps
    round-tripping cleanly through the C15 chunk-D fixture replay.
    Defaults are empty / None so existing callers stay unchanged.
    """
    inp = RunManifestInput(env={}, dbutils_available=False)
    assert inp.mlflow_run_tags == {}
    assert inp.jobs_run_snapshot is None


def test_jobs_run_snapshot_dataclass_shape() -> None:
    """``JobsRunSnapshot`` is the JSON-safe boundary between the
    impure harness adapter (which calls ``WorkspaceClient.jobs.
    get_run``) and the pure stage. The stage only ever sees this
    plain dataclass — never an SDK type, never a callable."""
    from genie_space_optimizer.optimization.stages.run_manifest import (
        JobsRunSnapshot,
    )

    snap = JobsRunSnapshot(
        job_id="123",
        parent_run_id="456",
        task_run_ids=("789", "012"),
    )
    assert snap.job_id == "123"
    assert snap.parent_run_id == "456"
    assert snap.task_run_ids == ("789", "012")


def test_jobs_run_snapshot_round_trips_through_json() -> None:
    """Snapshot must survive ``to_json()`` / ``from_json()`` so it
    can be captured in chunk-D replay fixtures alongside the rest
    of ``RunManifestInput``."""
    from genie_space_optimizer.optimization.stages.run_manifest import (
        JobsRunSnapshot,
    )

    snap = JobsRunSnapshot(
        job_id="918273645",
        parent_run_id="555444333222111",
        task_run_ids=("999", "111"),
    )
    payload = snap.to_json()
    restored = JobsRunSnapshot.from_json(payload)
    assert restored == snap


def test_run_manifest_input_round_trips_with_jobs_run_snapshot() -> None:
    """The whole input round-trips — pinning that no callable
    leaked in (callables would break ``to_json()``)."""
    from genie_space_optimizer.optimization.stages.run_manifest import (
        JobsRunSnapshot,
    )

    inp = RunManifestInput(
        env={"DATABRICKS_JOB_ID": "1"},
        dbutils_available=True,
        dbutils_tags={"runId": "2"},
        mlflow_run_tags={"mlflow.databricks.runID": "3"},
        jobs_run_snapshot=JobsRunSnapshot(
            job_id="42",
            parent_run_id="100",
            task_run_ids=("3",),
        ),
    )
    payload = inp.to_json()
    # Plain JSON serialisation must succeed — proves no Callable
    # fields are present.
    import json
    json.dumps(payload)
    restored = RunManifestInput.from_json(payload)
    assert restored.jobs_run_snapshot is not None
    assert restored.jobs_run_snapshot.job_id == "42"
    assert restored.mlflow_run_tags == {"mlflow.databricks.runID": "3"}


# ── P-B Tier-3 — RunManifestOutput diagnostic fields ─────────────────


def test_output_carries_jobs_api_attempted_succeeded() -> None:
    """Tier-3 needs two new diagnostic output fields so the
    ``GSO_DATABRICKS_IDS_RESOLVED_V1`` marker can record whether
    the Jobs API path fired and whether it actually populated any
    ID. Defaults are False so legacy fixtures stay unchanged."""
    out = RunManifestOutput(
        databricks_job_id="1",
        databricks_parent_run_id="2",
        lever_loop_task_run_id="3",
        resolution_path=ResolutionPath.JOBS_API,
        fields_resolved=3,
        fields_total=3,
        jobs_api_attempted=True,
        jobs_api_succeeded=True,
    )
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is True
    payload = out.to_json()
    restored = RunManifestOutput.from_json(payload)
    assert restored.jobs_api_attempted is True
    assert restored.jobs_api_succeeded is True
    assert restored.resolution_path is ResolutionPath.JOBS_API


def test_output_jobs_api_fields_default_false() -> None:
    out = RunManifestOutput()
    assert out.jobs_api_attempted is False
    assert out.jobs_api_succeeded is False
