"""Databricks Job launcher for the optimization pipeline.

The optimization runner job is declared in ``databricks.yml`` and managed
by the Databricks bundle (Terraform).  The app receives the job ID via the
``GENIE_SPACE_OPTIMIZER_JOB_ID`` environment variable and triggers runs with
``jobs.run_now()``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobRunAs,
    JobSettings,
)

logger = logging.getLogger(__name__)

_PERSISTENT_JOB_NAME = "genie-space-optimizer-job"

_job_submit_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _build_idempotency_token(*, run_id: str, space_id: str, triggered_by: str) -> str:
    """Build a stable <=64-char idempotency token for run_now."""
    raw = f"{space_id}|{triggered_by}|{run_id}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"gso-{digest[:60]}"


# ---------------------------------------------------------------------------
# Main optimization job — bundle-managed, triggered via run_now()
# ---------------------------------------------------------------------------

def _resolve_job_id(ws: WorkspaceClient, job_id: int | None) -> int:
    """Return the configured job ID, falling back to a tag-filtered name lookup.

    The name lookup uses a substring match (which handles DABs dev-mode
    prefixes like ``[dev user] genie-space-optimizer-job``), then filters
    by the ``managed-by: databricks-bundle`` tag to avoid picking up stale
    runtime-created jobs with similar names.
    """
    if job_id:
        return job_id
    for j in ws.jobs.list(name=_PERSISTENT_JOB_NAME):
        tags = (j.settings.tags if j.settings else None) or {}
        if tags.get("managed-by") == "databricks-bundle" and j.job_id:
            logger.info("Resolved runner job by tag: id=%s", j.job_id)
            return j.job_id
    raise RuntimeError(
        "Runner job not found. Set GENIE_SPACE_OPTIMIZER_JOB_ID or rerun the Workbench deploy flow."
    )


def submit_optimization(
    ws: WorkspaceClient,
    *,
    job_id: int | None,
    run_id: str,
    space_id: str,
    domain: str,
    catalog: str,
    schema: str,
    apply_mode: str = "genie_config",
    levers: str = "[1,2,3,4,5]",
    triggered_by: str = "",
    deploy_target: str = "",
    warehouse_id: str = "",
    target_benchmark_count: str = "",
    llm_model: str = "",
    target_accuracy: str = "0.90",
    max_attempts: str = "3",
) -> tuple[str, int]:
    """Trigger a run on the bundle-managed optimization job.

    The job is declared in ``databricks.yml`` and its ID is provided by
    the ``GENIE_SPACE_OPTIMIZER_JOB_ID`` environment variable.  Falls back
    to looking up the job by name if the env var is not set.
    """
    resolved_job_id = _resolve_job_id(ws, job_id)
    with _job_submit_lock:
        waiter = ws.jobs.run_now(
            job_id=resolved_job_id,
            idempotency_token=_build_idempotency_token(
                run_id=run_id,
                space_id=space_id,
                triggered_by=triggered_by,
            ),
            # Only send parameters the 4-task job declares. Every key here must
            # be a job parameter on the runner (run_now rejects undeclared
            # keys), so this set MUST stay a subset of the declared params in
            # BOTH job definitions: the root bundle
            # (databricks.yml resources.jobs.gso-optimization-runner) and the
            # notebook installer (scripts/deploy_lib/gso_job.py) — which in turn
            # mirror the package bundle. `deploy_target` (deploy out of scope —
            # D7) and `target_benchmark_count` (a code constant, not a v2 job
            # param) are intentionally NOT sent; the kwargs remain for caller
            # signature compatibility. `benchmark_repair_max_tries` is declared
            # by the job but not overridden here, so it uses the job default.
            job_parameters={
                "run_id": run_id,
                "space_id": space_id,
                "domain": domain,
                "catalog": catalog,
                "schema": schema,
                "apply_mode": apply_mode,
                "levers": levers,
                # GSO v2 loop knobs. The job declares these in
                # databricks.yml; passing them here lets a user-chosen value
                # override the job default. target_accuracy is the 0–1 stop
                # target; max_attempts bounds the patch/eval loop.
                "target_accuracy": target_accuracy,
                "max_attempts": max_attempts,
                "triggered_by": triggered_by,
                "warehouse_id": warehouse_id,
                "llm_model": llm_model or os.getenv("LLM_MODEL", ""),
            },
        )

    job_run_id = str(waiter.run_id)
    logger.info(
        "Triggered optimization run %s on bundle-managed job %s (job run %s)",
        run_id,
        resolved_job_id,
        job_run_id,
    )
    return job_run_id, resolved_job_id


# ---------------------------------------------------------------------------
# Job URL and health checks
# ---------------------------------------------------------------------------

def get_job_url(ws: WorkspaceClient, job_id: int | None = None) -> str | None:
    """Resolve the URL to the persistent optimization job."""
    if job_id is None:
        try:
            for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME):
                tags = (job.settings.tags if job.settings else None) or {}
                if tags.get("managed-by") == "databricks-bundle" and job.job_id is not None:
                    job_id = int(job.job_id)
                    break
        except Exception:
            return None
    if job_id is None:
        return None
    host = (ws.config.host or "").rstrip("/")
    if not host:
        return None
    return f"{host}/jobs/{job_id}"


def check_job_health(ws: WorkspaceClient, sp_client_id: str, job_id: int | None = None) -> tuple[bool, str]:
    """Check if the optimization job exists and is accessible.

    For a bundle-managed job, ownership is set by the Workbench deploy flow and
    verified/repaired by ``_JobRunAsBootstrap`` at startup.  This check
    is lighter than the old orphan-detection logic.
    """
    try:
        if job_id is not None:
            detail = ws.jobs.get(job_id)
            run_as_name = detail.run_as_user_name or ""
            if sp_client_id and run_as_name and sp_client_id not in run_as_name:
                return False, (
                    f"Runner job {job_id} run_as is '{run_as_name}' but the "
                    f"current app SP is '{sp_client_id}'. Restart the app "
                    f"to auto-repair, or rerun the Workbench deploy flow."
                )
            return True, ""

        for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME):
            tags = (job.settings.tags if job.settings else None) or {}
            if tags.get("managed-by") == "databricks-bundle" and job.job_id is not None:
                return True, ""
        return False, (
            "Runner job not found. Rerun the Workbench deploy flow to create it, "
            "or check the GENIE_SPACE_OPTIMIZER_JOB_ID env var."
        )
    except Exception:
        return True, ""


# ---------------------------------------------------------------------------
# run_as self-healing (used by _JobRunAsBootstrap in app.py)
# ---------------------------------------------------------------------------

def ensure_job_run_as(ws: WorkspaceClient, job_id: int, sp_client_id: str) -> None:
    """Verify the bundle-managed job's ``run_as`` matches the current SP.

    If not, update it.  This provides self-healing after fresh deploys
    where the Workbench deploy flow may not have run yet.
    """
    try:
        detail = ws.jobs.get(job_id)
        run_as_name = detail.run_as_user_name or ""
        if sp_client_id and sp_client_id in run_as_name:
            logger.debug("Job %s run_as already set to SP %s", job_id, sp_client_id)
            return

        logger.info(
            "Updating job %s run_as from '%s' to SP '%s'",
            job_id, run_as_name, sp_client_id,
        )
        ws.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                run_as=JobRunAs(service_principal_name=sp_client_id),
            ),
        )
        logger.info("Job %s run_as updated to SP %s", job_id, sp_client_id)
    except Exception:
        logger.warning(
            "Could not verify/update run_as on job %s — "
            "rerun the Workbench deploy flow to set permissions manually",
            job_id,
            exc_info=True,
        )
