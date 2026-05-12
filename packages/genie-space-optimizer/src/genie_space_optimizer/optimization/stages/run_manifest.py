"""Stage 11 (NEW for C15): Run manifest resolution.

Wraps ``harness._databricks_ids_from_env`` (and the C14-W T3
``GSO_DATABRICKS_IDS_RESOLVED_V1`` trace) under a typed input/output
contract.

Why this is a stage rather than a free helper:

  * The input — environment vars + dbutils availability — varies by
    runtime context (jobs vs interactive vs CI). Naming it explicitly
    lets us vendor one fixture per runtime context.
  * The output is the canonical experimental-setup record consumed by
    GSO_RUN_MANIFEST_V2. A contract-shaped output stops blank IDs
    from regressing in production (D-5).
  * The resolver was already being treated as a stage informally —
    C14-W T3's tracing marker logged the resolution path. This stage
    formalises it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


STAGE_KEY: str = "run_manifest"
DATABRICKS_ID_SENTINEL = "unknown"


class ResolutionPath(StrEnum):
    ENV = "env"
    DBUTILS = "dbutils"
    MIXED = "mixed"
    JOBS_API = "jobs_api"
    MIXED_JOBS_API = "mixed_jobs_api"
    SENTINEL = "sentinel"


@dataclass(frozen=True, slots=True)
class JobsRunSnapshot(JsonRoundTrip):
    """Tier-3 — JSON-safe projection of ``WorkspaceClient.jobs.get_run``.

    Wraps the subset of the SDK ``Run`` object the resolver actually
    uses, so the pure stage never depends on the SDK type and the
    whole ``RunManifestInput`` keeps round-tripping cleanly through
    JSON fixtures (which a ``Callable`` field would break).

    Field semantics match the Jobs API ``Run`` object:

    - ``job_id`` — parent job ID (``Run.job_id``).
    - ``parent_run_id`` — multitask parent run ID (``Run.run_id``).
    - ``task_run_ids`` — the ``run_id`` of every task in
      ``Run.tasks``. The harness adapter orders this tuple so the
      lever-loop task is first (``task_key == "lever_loop"``); the
      stage trusts that ordering and uses ``task_run_ids[0]`` as
      the resolved ``lever_loop_task_run_id``.
    """

    job_id: str = ""
    parent_run_id: str = ""
    task_run_ids: tuple[str, ...] = ()

    @classmethod
    def from_json(cls, payload: dict) -> "JobsRunSnapshot":
        return cls(
            job_id=str(payload.get("job_id", "")),
            parent_run_id=str(payload.get("parent_run_id", "")),
            task_run_ids=tuple(
                str(v) for v in (payload.get("task_run_ids") or [])
            ),
        )


@dataclass(frozen=True, slots=True)
class RunManifestInput(JsonRoundTrip):
    """Input to stages.run_manifest.execute.

    ``env`` carries the relevant subset of os.environ at run start.
    ``dbutils_available`` records whether the runtime can construct a
    DBUtils client; when True, the resolver attempts the dbutils
    fallback path. When False (CI / local), the resolver short-circuits
    to env-only / sentinel.

    Tier-3 (MLflow + Jobs evidence) fields:
      * ``mlflow_run_tags`` — auto-stamped Databricks tags from the
        active MLflow run (``mlflow.databricks.jobID`` /
        ``mlflow.databricks.jobRunID`` / ``mlflow.databricks.runID``).
        Pre-collected by the harness so the pure stage stays
        MLflow-free. Empty dict when MLflow is unavailable.
      * ``jobs_run_snapshot`` — pre-resolved JSON-safe projection
        of ``WorkspaceClient.jobs.get_run(...)``. The harness owns
        the SDK call (and its exception handling). ``None`` means
        "the harness did not call the Jobs API" (no SDK, no seed,
        or the call raised pre-call). A non-``None`` snapshot — even
        with all-empty fields — means "the harness called the Jobs
        API"; the stage uses that as the ``jobs_api_attempted``
        signal.
    """

    env: dict[str, str] = field(default_factory=dict)
    dbutils_available: bool = False
    # Optional injected callback for dbutils-tag resolution; production
    # uses the harness adapter that calls
    # ``dbutils.notebook.entry_point.getDbutils()...``. Tests pass a
    # plain dict.
    dbutils_tags: dict[str, str] = field(default_factory=dict)
    mlflow_run_tags: dict[str, str] = field(default_factory=dict)
    jobs_run_snapshot: JobsRunSnapshot | None = None

    @classmethod
    def from_json(cls, payload: dict) -> "RunManifestInput":
        snap_payload = payload.get("jobs_run_snapshot")
        snap: JobsRunSnapshot | None = None
        if isinstance(snap_payload, dict):
            snap = JobsRunSnapshot.from_json(snap_payload)
        return cls(
            env=dict(payload.get("env") or {}),
            dbutils_available=bool(payload.get("dbutils_available", False)),
            dbutils_tags=dict(payload.get("dbutils_tags") or {}),
            mlflow_run_tags=dict(payload.get("mlflow_run_tags") or {}),
            jobs_run_snapshot=snap,
        )


@dataclass(frozen=True, slots=True)
class RunManifestOutput(JsonRoundTrip):
    databricks_job_id: str = DATABRICKS_ID_SENTINEL
    databricks_parent_run_id: str = DATABRICKS_ID_SENTINEL
    lever_loop_task_run_id: str = DATABRICKS_ID_SENTINEL
    resolution_path: ResolutionPath = ResolutionPath.SENTINEL
    fields_resolved: int = 0
    fields_total: int = 3
    dbutils_attempted: bool = False
    dbutils_succeeded: bool = False
    jobs_api_attempted: bool = False
    jobs_api_succeeded: bool = False

    @classmethod
    def from_json(cls, payload: dict) -> "RunManifestOutput":
        return cls(
            databricks_job_id=str(payload.get("databricks_job_id", DATABRICKS_ID_SENTINEL)),
            databricks_parent_run_id=str(payload.get("databricks_parent_run_id", DATABRICKS_ID_SENTINEL)),
            lever_loop_task_run_id=str(payload.get("lever_loop_task_run_id", DATABRICKS_ID_SENTINEL)),
            resolution_path=ResolutionPath(payload.get("resolution_path", "sentinel")),
            fields_resolved=int(payload.get("fields_resolved", 0)),
            fields_total=int(payload.get("fields_total", 3)),
            dbutils_attempted=bool(payload.get("dbutils_attempted", False)),
            dbutils_succeeded=bool(payload.get("dbutils_succeeded", False)),
            jobs_api_attempted=bool(payload.get("jobs_api_attempted", False)),
            jobs_api_succeeded=bool(payload.get("jobs_api_succeeded", False)),
        )


def _from_env(env: dict[str, str]) -> dict[str, str]:
    return {
        "databricks_job_id": str(env.get("DATABRICKS_JOB_ID") or ""),
        "databricks_parent_run_id": str(
            env.get("DATABRICKS_RUN_ID") or env.get("DATABRICKS_JOB_RUN_ID") or ""
        ),
        "lever_loop_task_run_id": str(env.get("DATABRICKS_TASK_RUN_ID") or ""),
    }


def _from_dbutils_tags(tags: dict[str, str]) -> dict[str, str]:
    return {
        "databricks_job_id": str(tags.get("jobId") or ""),
        "databricks_parent_run_id": str(
            tags.get("multitaskParentRunId") or tags.get("jobRunId") or ""
        ),
        "lever_loop_task_run_id": str(tags.get("runId") or ""),
    }


def _from_mlflow_tags(tags: dict[str, str]) -> dict[str, str]:
    """Project MLflow auto-stamped Databricks tags directly onto
    the three resolved-ID slots.

    Tier 3a — fires whenever the platform stamped the tags on the
    active MLflow run, even if the harness could not call
    ``WorkspaceClient.jobs.get_run`` (no SDK / network).
    """
    return {
        "databricks_job_id": str(tags.get("mlflow.databricks.jobID") or ""),
        "databricks_parent_run_id": str(
            tags.get("mlflow.databricks.jobRunID") or ""
        ),
        "lever_loop_task_run_id": str(
            tags.get("mlflow.databricks.runID") or ""
        ),
    }


def _from_jobs_snapshot(snapshot: "JobsRunSnapshot") -> dict[str, str]:
    """Tier 3b — project the harness-resolved Jobs API snapshot.

    The harness adapter orders ``task_run_ids`` so the lever-loop
    task is first; the stage trusts that ordering and uses
    ``task_run_ids[0]``.
    """
    task_run_id = ""
    if snapshot.task_run_ids:
        task_run_id = str(snapshot.task_run_ids[0] or "")
    return {
        "databricks_job_id": str(snapshot.job_id or ""),
        "databricks_parent_run_id": str(snapshot.parent_run_id or ""),
        "lever_loop_task_run_id": task_run_id,
    }


def resolve_run_manifest(ctx, inp: RunManifestInput) -> RunManifestOutput:
    """Resolve Databricks IDs from env + optional dbutils tags +
    optional MLflow tags + optional pre-resolved Jobs API snapshot.

    Resolution order (first hit wins per field):

      1. env vars
      2. dbutils tags (if ``inp.dbutils_available``)
      3a. MLflow auto-stamped Databricks tags (direct projection)
      3b. Pre-resolved Jobs API snapshot (harness owns the SDK call;
          stage receives the JSON-safe ``JobsRunSnapshot``)
      4. sentinel (``"unknown"``)

    The stage performs no I/O — every input is plain data, so the
    whole resolution is JSON-replayable from a chunk-D fixture.
    """
    env_resolved = _from_env(inp.env or {})
    final = dict(env_resolved)

    dbutils_attempted = False
    dbutils_succeeded = False
    if not all(final.values()) and inp.dbutils_available:
        dbutils_attempted = True
        tags_resolved = _from_dbutils_tags(inp.dbutils_tags or {})
        for k, v in tags_resolved.items():
            if not final[k] and v:
                final[k] = v
                dbutils_succeeded = True

    mlflow_succeeded = False
    if not all(final.values()) and (inp.mlflow_run_tags or {}):
        mlflow_resolved = _from_mlflow_tags(inp.mlflow_run_tags or {})
        for k, v in mlflow_resolved.items():
            if not final[k] and v:
                final[k] = v
                mlflow_succeeded = True

    # ``jobs_api_attempted`` reflects a runtime fact only the
    # harness knows: did we call ``WorkspaceClient.jobs.get_run``?
    # The harness signals "yes" by passing a non-None snapshot
    # (possibly with all-empty fields when the call returned an
    # empty Run). ``None`` means "harness did not attempt the
    # call" — no SDK / no seed / call raised pre-call.
    jobs_api_attempted = inp.jobs_run_snapshot is not None
    jobs_api_succeeded = False
    if not all(final.values()) and jobs_api_attempted:
        api_resolved = _from_jobs_snapshot(inp.jobs_run_snapshot)
        for k, v in api_resolved.items():
            if not final[k] and v:
                final[k] = v
                jobs_api_succeeded = True

    final = {k: (v or DATABRICKS_ID_SENTINEL) for k, v in final.items()}
    fields_resolved = sum(
        1 for v in final.values() if v != DATABRICKS_ID_SENTINEL
    )

    tier3_succeeded = mlflow_succeeded or jobs_api_succeeded
    if all(env_resolved.values()):
        path = ResolutionPath.ENV
    elif tier3_succeeded and (
        any(env_resolved.values()) or dbutils_succeeded
    ):
        path = ResolutionPath.MIXED_JOBS_API
    elif tier3_succeeded:
        path = ResolutionPath.JOBS_API
    elif dbutils_succeeded and any(env_resolved.values()):
        path = ResolutionPath.MIXED
    elif dbutils_succeeded:
        path = ResolutionPath.DBUTILS
    else:
        path = ResolutionPath.SENTINEL

    return RunManifestOutput(
        databricks_job_id=final["databricks_job_id"],
        databricks_parent_run_id=final["databricks_parent_run_id"],
        lever_loop_task_run_id=final["lever_loop_task_run_id"],
        resolution_path=path,
        fields_resolved=fields_resolved,
        fields_total=3,
        dbutils_attempted=dbutils_attempted,
        dbutils_succeeded=dbutils_succeeded,
        jobs_api_attempted=jobs_api_attempted,
        jobs_api_succeeded=jobs_api_succeeded,
    )


INPUT_CLASS = RunManifestInput
OUTPUT_CLASS = RunManifestOutput
execute = resolve_run_manifest
