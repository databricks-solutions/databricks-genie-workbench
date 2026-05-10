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
