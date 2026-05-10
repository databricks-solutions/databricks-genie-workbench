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
    SENTINEL = "sentinel"


@dataclass(frozen=True, slots=True)
class RunManifestInput(JsonRoundTrip):
    """Input to stages.run_manifest.execute.

    ``env`` carries the relevant subset of os.environ at run start.
    ``dbutils_available`` records whether the runtime can construct a
    DBUtils client; when True, the resolver attempts the dbutils
    fallback path. When False (CI / local), the resolver short-circuits
    to env-only / sentinel.
    """

    env: dict[str, str] = field(default_factory=dict)
    dbutils_available: bool = False
    # Optional injected callback for dbutils-tag resolution; production
    # uses the harness adapter that calls
    # ``dbutils.notebook.entry_point.getDbutils()...``. Tests pass a
    # plain dict.
    dbutils_tags: dict[str, str] = field(default_factory=dict)


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


def resolve_run_manifest(ctx, inp: RunManifestInput) -> RunManifestOutput:
    """Resolve Databricks IDs from env + optional dbutils tags.

    Resolution order (first hit wins per field):
      1. env vars
      2. dbutils tags (if ``inp.dbutils_available``)
      3. sentinel (``"unknown"``)
    """
    env_resolved = _from_env(inp.env or {})

    dbutils_attempted = False
    dbutils_succeeded = False
    final = dict(env_resolved)
    if not all(final.values()) and inp.dbutils_available:
        dbutils_attempted = True
        tags_resolved = _from_dbutils_tags(inp.dbutils_tags or {})
        for k, v in tags_resolved.items():
            if not final[k] and v:
                final[k] = v
                dbutils_succeeded = True

    final = {k: (v or DATABRICKS_ID_SENTINEL) for k, v in final.items()}
    fields_resolved = sum(1 for v in final.values() if v != DATABRICKS_ID_SENTINEL)

    if all(env_resolved.values()):
        path = ResolutionPath.ENV
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
    )


INPUT_CLASS = RunManifestInput
OUTPUT_CLASS = RunManifestOutput
execute = resolve_run_manifest
