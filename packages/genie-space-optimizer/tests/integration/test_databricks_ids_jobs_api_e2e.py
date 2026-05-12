"""End-to-end Tier-3 integration test (P-B).

Reproduces the May-12 deployed-runtime failure shape from anchors
``ccf1d60d-d686-467b-bafa-1640131b4393`` and
``31ecd96f-5d56-4b5a-af8e-38e9e5c549af``:

  * ``DATABRICKS_*`` env vars unset
  * ``dbutils.notebook.entry_point`` reachable but ``tags()``
    returns no useful values (``dbutils_succeeded=false``)
  * MLflow active run carries the platform-stamped
    ``mlflow.databricks.runID`` tag
  * The Jobs API returns the authoritative Run snapshot with all
    three IDs

Asserts:

  1. The pure stage Tier-3 path produces three non-sentinel IDs.
  2. The ``GSO_DATABRICKS_IDS_RESOLVED_V1`` marker reports
     ``resolution_path=jobs_api`` and
     ``jobs_api_succeeded=true``.
  3. The ``GSO_RUN_MANIFEST_V1`` line carries the three IDs (no
     ``unknown``).
"""
from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    databricks_ids_resolved_marker,
    run_manifest_marker,
)
from genie_space_optimizer.optimization.stages.run_manifest import (
    JobsRunSnapshot,
    ResolutionPath,
    RunManifestInput,
    resolve_run_manifest,
)


def _extract_marker(stdout_text: str, name: str) -> dict:
    match = re.search(rf"{name}\s+(\{{.*\}})", stdout_text)
    assert match is not None, f"{name} not found in stdout: {stdout_text!r}"
    return json.loads(match.group(1))


def test_jobs_api_fallback_resolves_may_12_anchor_shape() -> None:
    """End-to-end: stage resolves all three IDs; markers carry
    them and the diagnostic flags.

    The harness in production calls
    ``_resolve_jobs_run_snapshot(...)`` and passes the resulting
    ``JobsRunSnapshot`` (or ``None``) directly into
    ``RunManifestInput``. This test simulates the harness's job
    by constructing the snapshot inline — no callable crosses the
    stage boundary."""
    snapshot = JobsRunSnapshot(
        job_id="918273645",
        parent_run_id="555444333222111",
        task_run_ids=("999888777666555", "111222333444555"),
    )

    inp = RunManifestInput(
        env={},  # env vars unset (May-12 anchor shape)
        dbutils_available=True,
        dbutils_tags={},  # dbutils reachable but empty (May-12 anchor)
        mlflow_run_tags={
            "mlflow.databricks.runID": "999888777666555",
        },
        jobs_run_snapshot=snapshot,
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    # 1. Pure-stage Tier-3 success.
    assert out.resolution_path is ResolutionPath.JOBS_API
    assert out.databricks_job_id == "918273645"
    assert out.databricks_parent_run_id == "555444333222111"
    assert out.lever_loop_task_run_id == "999888777666555"
    assert out.fields_resolved == 3
    assert out.dbutils_attempted is True
    assert out.dbutils_succeeded is False
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is True

    # 2. Diagnostic marker carries Tier-3 fields.
    diag_line = databricks_ids_resolved_marker(
        resolution_path=str(out.resolution_path),
        fields_resolved=out.fields_resolved,
        fields_total=out.fields_total,
        dbutils_attempted=out.dbutils_attempted,
        dbutils_succeeded=out.dbutils_succeeded,
        jobs_api_attempted=out.jobs_api_attempted,
        jobs_api_succeeded=out.jobs_api_succeeded,
    )
    diag_payload = _extract_marker(diag_line, "GSO_DATABRICKS_IDS_RESOLVED_V1")
    assert diag_payload["resolution_path"] == "jobs_api"
    assert diag_payload["jobs_api_attempted"] is True
    assert diag_payload["jobs_api_succeeded"] is True
    assert diag_payload["dbutils_succeeded"] is False

    # 3. Run-manifest marker carries the three IDs (no sentinel).
    manifest_line = run_manifest_marker(
        optimization_run_id="opt-e2e",
        databricks_job_id=out.databricks_job_id,
        databricks_parent_run_id=out.databricks_parent_run_id,
        lever_loop_task_run_id=out.lever_loop_task_run_id,
        mlflow_experiment_id="exp",
        space_id="space",
        event="start",
    )
    manifest_payload = _extract_marker(manifest_line, "GSO_RUN_MANIFEST_V1")
    assert manifest_payload["databricks_job_id"] == "918273645"
    assert manifest_payload["databricks_parent_run_id"] == "555444333222111"
    assert manifest_payload["lever_loop_task_run_id"] == "999888777666555"
    for value in (
        manifest_payload["databricks_job_id"],
        manifest_payload["databricks_parent_run_id"],
        manifest_payload["lever_loop_task_run_id"],
    ):
        assert value != "unknown", (
            "P-B invariant (when platform evidence is available): "
            "deployed manifest carries platform-resolved IDs, not "
            "the literal sentinel."
        )


def test_jobs_api_fallback_skipped_cleanly_when_no_platform_evidence() -> None:
    """When the harness has no SDK and no MLflow tags (local pytest,
    no profile), it passes ``jobs_run_snapshot=None`` and the stage
    returns sentinels — but the diagnostic flags are HONEST about
    what happened: ``jobs_api_attempted=False``,
    ``dbutils_succeeded=False``, ``fields_resolved=0``. Postmortems
    can tell "no platform evidence" apart from "evidence dropped"."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        mlflow_run_tags={},  # no MLflow either
        jobs_run_snapshot=None,  # harness did not attempt the call
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.databricks_job_id == "unknown"
    assert out.jobs_api_attempted is False
    assert out.jobs_api_succeeded is False


def test_jobs_api_call_attempted_but_returned_empty_emits_diagnostic() -> None:
    """The harness called ``jobs.get_run`` and got back a Run with
    no usable fields (e.g. permissions error). The stage records
    ``jobs_api_attempted=True, jobs_api_succeeded=False`` — exactly
    the postmortem signal for "evidence dropped" vs "no evidence"."""
    inp = RunManifestInput(
        env={},
        dbutils_available=False,
        mlflow_run_tags={},
        jobs_run_snapshot=JobsRunSnapshot(),  # call attempted, got nothing
    )

    out = resolve_run_manifest(ctx=None, inp=inp)

    assert out.resolution_path is ResolutionPath.SENTINEL
    assert out.databricks_job_id == "unknown"
    assert out.jobs_api_attempted is True
    assert out.jobs_api_succeeded is False
