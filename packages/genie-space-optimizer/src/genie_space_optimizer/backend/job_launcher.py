"""Databricks Job launcher for optimization and deployment pipelines.

The main optimization runner job is declared in ``databricks.yml`` and managed
by the Databricks bundle (Terraform).  The app receives the job ID via the
``GENIE_SPACE_OPTIMIZER_JOB_ID`` environment variable and triggers runs with
``jobs.run_now()``.

Per-space deployment jobs are still created at runtime.
"""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import os
import threading
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.jobs import (
    JobEnvironment,
    JobParameterDefinition,
    JobRunAs,
    JobSettings,
    NotebookTask,
    QueueSettings,
    Source,
    Task,
    TaskDependency,
)
from databricks.sdk.service.workspace import ImportFormat, Language

logger = logging.getLogger(__name__)

_WS_BASE = "/Workspace/Shared/genie-space-optimizer"
_VOLUME_NAME = "app_artifacts"
_PERSISTENT_JOB_NAME = "genie-space-optimizer-job"

_NOTEBOOK_SOURCES = {
    "run_preflight": Path(__file__).resolve().parent.parent / "jobs" / "run_preflight.py",
    "run_baseline": Path(__file__).resolve().parent.parent / "jobs" / "run_baseline.py",
    "run_enrichment": Path(__file__).resolve().parent.parent / "jobs" / "run_enrichment.py",
    "run_lever_loop": Path(__file__).resolve().parent.parent / "jobs" / "run_lever_loop.py",
    "run_finalize": Path(__file__).resolve().parent.parent / "jobs" / "run_finalize.py",
    "run_deploy": Path(__file__).resolve().parent.parent / "jobs" / "run_deploy.py",
    "run_cross_env_deploy": Path(__file__).resolve().parent.parent / "jobs" / "run_cross_env_deploy.py",
    "run_deploy_approval": Path(__file__).resolve().parent.parent / "jobs" / "run_deploy_approval.py",
}
_WS_NOTEBOOKS = {name: f"{_WS_BASE}/{name}" for name in _NOTEBOOK_SOURCES}

_DEPLOY_JOB_ENVIRONMENT_CLIENT_VERSION = "4"

_cached_wheel_path: str | None = None
_cached_wheel_hash: str | None = None
_volume_ensured: bool = False
_legacy_cleaned: bool = False
_job_submit_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Shared utilities (used by deployment jobs AND submit_optimization)
# ---------------------------------------------------------------------------

def _volume_wheel_dir(catalog: str, schema: str) -> str:
    """Return the Volume-based directory path for wheel storage."""
    return f"/Volumes/{catalog}/{schema}/{_VOLUME_NAME}/dist"


def _ensure_volume(ws: WorkspaceClient, catalog: str, schema: str) -> None:
    """Create the managed Volume for app artifacts if it doesn't already exist."""
    global _volume_ensured
    if _volume_ensured:
        return
    try:
        ws.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=_VOLUME_NAME,
            volume_type=VolumeType.MANAGED,
        )
        logger.info("Created managed volume %s.%s.%s", catalog, schema, _VOLUME_NAME)
    except Exception as exc:
        if "already_exists" in str(exc).lower() or "already exists" in str(exc).lower():
            logger.debug("Volume %s.%s.%s already exists", catalog, schema, _VOLUME_NAME)
        else:
            raise RuntimeError(
                f"Could not create volume {catalog}.{schema}.{_VOLUME_NAME}: {exc}. "
                f"Create it manually with: CREATE VOLUME IF NOT EXISTS "
                f"{catalog}.{schema}.{_VOLUME_NAME}"
            ) from exc
    _volume_ensured = True


def _build_idempotency_token(*, run_id: str, space_id: str, triggered_by: str) -> str:
    """Build a stable <=64-char idempotency token for run_now."""
    raw = f"{space_id}|{triggered_by}|{run_id}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"gso-{digest[:60]}"


def _resolve_project_root() -> Path:
    """Find the source tree root that contains ``pyproject.toml``."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    app_source = Path("/app/python/source_code")
    if (app_source / "pyproject.toml").is_file():
        return app_source
    return Path.cwd()


def _find_wheel() -> Path:
    """Locate the newest project wheel across ALL search directories.

    Falls back to building from source in dev environments.
    """
    app_source = Path("/app/python/source_code")
    project_root = _resolve_project_root()

    search_dirs = [
        app_source,
        app_source / ".build",
        app_source / "dist",
        project_root / ".build",
        project_root / "dist",
        project_root,
    ]

    all_wheels: list[Path] = []
    for search_dir in search_dirs:
        if search_dir.is_dir():
            all_wheels.extend(search_dir.glob("genie_space_optimizer-*.whl"))

    if all_wheels:
        best = sorted(all_wheels, key=lambda p: p.name)[-1]
        logger.info(
            "Discovered wheel: %s (from %d candidate(s) across %d dirs)",
            best,
            len(all_wheels),
            len([d for d in search_dirs if d.is_dir()]),
        )
        return best

    import subprocess

    dist_dir = project_root / "dist"
    dist_dir.mkdir(exist_ok=True)
    last_error: Exception | None = None
    for cmd in [
        ["uv", "build", "--wheel", "-o", str(dist_dir)],
        ["python", "-m", "build", "--wheel", "-o", str(dist_dir)],
    ]:
        try:
            subprocess.run(cmd, cwd=str(project_root), check=True, capture_output=True)
            break
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            last_error = exc
            continue

    wheels = sorted(dist_dir.glob("genie_space_optimizer-*.whl"))
    if not wheels:
        searched = ", ".join(str(d) for d in search_dirs if d.is_dir())
        raise RuntimeError(
            "Could not find or build the genie_space_optimizer wheel. "
            f"Searched: {searched}. Build root: {project_root}. "
            f"Last build error: {last_error!r}"
        )
    return wheels[-1]


def _cleanup_stale_wheels(ws: WorkspaceClient, dist_dir: str, keep_path: str) -> None:
    """Remove old hash-bucketed wheel directories from the Volume dist directory."""
    keep_dir = keep_path.rsplit("/", 1)[0] if "/" in keep_path else keep_path
    try:
        for entry in ws.files.list_directory_contents(dist_dir):
            path = entry.path or ""
            if entry.is_directory and path and path != keep_dir:
                try:
                    ws.files.delete_directory(path)
                    logger.info("Deleted stale wheel artifact: %s", path)
                except Exception:
                    logger.debug("Could not delete stale artifact: %s", path, exc_info=True)
    except Exception:
        logger.debug("Could not list %s for cleanup", dist_dir, exc_info=True)


def _find_volume_wheel(ws: WorkspaceClient, catalog: str, schema: str) -> str | None:
    """Look for an existing wheel in the UC Volume dist directory.

    Used when running inside a Databricks Job cluster where the local
    filesystem doesn't have the wheel file.  Returns the Volume path
    to the newest wheel, or ``None`` if nothing is found.
    """
    dist_dir = _volume_wheel_dir(catalog, schema)
    try:
        best_path: str | None = None
        for entry in ws.files.list_directory_contents(dist_dir):
            path = entry.path or ""
            if path.endswith(".whl") and "genie_space_optimizer" in path:
                if best_path is None or path > best_path:
                    best_path = path
            elif entry.is_directory:
                try:
                    for sub in ws.files.list_directory_contents(path):
                        sub_path = sub.path or ""
                        if sub_path.endswith(".whl") and "genie_space_optimizer" in sub_path:
                            if best_path is None or sub_path > best_path:
                                best_path = sub_path
                except Exception:
                    pass
        if best_path:
            if not best_path.startswith("/"):
                best_path = f"/{best_path}"
            logger.info("Found existing wheel in Volume: %s", best_path)
            return best_path
    except Exception:
        logger.debug("Could not search Volume for existing wheel", exc_info=True)
    return None


def _cleanup_legacy_workspace_wheels(ws: WorkspaceClient) -> None:
    """One-time removal of the old workspace-based wheel directory."""
    global _legacy_cleaned
    if _legacy_cleaned:
        return
    _legacy_cleaned = True
    legacy_dir = f"{_WS_BASE}/dist"
    try:
        ws.workspace.delete(legacy_dir, recursive=True)
        logger.info("Cleaned up legacy workspace wheel directory: %s", legacy_dir)
    except Exception:
        logger.debug("No legacy workspace wheel directory to clean up", exc_info=True)


def _resolve_sp_client_id(ws: WorkspaceClient) -> str:
    return ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")


def _ensure_job_visibility(ws: WorkspaceClient, job_id: int) -> None:
    """Best-effort permission update so users can view job runs in the UI."""
    try:
        ws.permissions.update(
            "jobs",
            str(job_id),
            access_control_list=[
                AccessControlRequest(
                    group_name="users",
                    permission_level=PermissionLevel.CAN_VIEW,
                )
            ],
        )
    except Exception as exc:
        logger.warning(
            "Could not set CAN_VIEW permissions on job %s: %s",
            job_id,
            exc,
        )


def _ensure_artifacts(
    ws: WorkspaceClient, catalog: str, schema: str,
) -> tuple[str, str]:
    """Upload wheel to a UC Volume and runner notebooks to the workspace.

    Used by per-space deployment jobs which are still runtime-created.
    Returns ``(vol_wheel_path, wheel_hash)``.
    """
    global _cached_wheel_path, _cached_wheel_hash

    _ensure_volume(ws, catalog, schema)

    wheel_local = _find_wheel()
    wheel_bytes = wheel_local.read_bytes()
    wheel_hash = hashlib.md5(wheel_bytes).hexdigest()
    wheel_filename = wheel_local.name
    dist_dir = _volume_wheel_dir(catalog, schema)
    vol_wheel_path = f"{dist_dir}/{wheel_hash[:8]}/{wheel_filename}"

    logger.info(
        "Wheel resolved: %s (local=%s, size=%d, hash=%s)",
        wheel_filename,
        wheel_local,
        len(wheel_bytes),
        wheel_hash[:8],
    )

    if _cached_wheel_hash == wheel_hash and _cached_wheel_path == vol_wheel_path:
        logger.debug(
            "Wheel unchanged (hash=%s), skipping upload", wheel_hash[:8]
        )
        return vol_wheel_path, wheel_hash

    ws.files.upload(vol_wheel_path, io.BytesIO(wheel_bytes), overwrite=True)
    logger.info(
        "Uploaded wheel → %s (%d bytes, hash=%s)",
        vol_wheel_path,
        len(wheel_bytes),
        wheel_hash[:8],
    )

    _cleanup_stale_wheels(ws, dist_dir=dist_dir, keep_path=vol_wheel_path)
    _cleanup_legacy_workspace_wheels(ws)

    ws.workspace.mkdirs(_WS_BASE)
    for name, src_path in _NOTEBOOK_SOURCES.items():
        notebook_src = src_path.read_text(encoding="utf-8")
        ws_path = _WS_NOTEBOOKS[name]
        ws.workspace.import_(
            ws_path,
            content=base64.b64encode(notebook_src.encode("utf-8")).decode("ascii"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded notebook → %s", ws_path)

    _cached_wheel_path = vol_wheel_path
    _cached_wheel_hash = wheel_hash
    return vol_wheel_path, wheel_hash


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
        "Runner job not found. Set GENIE_SPACE_OPTIMIZER_JOB_ID or run deploy.sh."
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
    max_iterations: str = "5",
    triggered_by: str = "",
    experiment_name: str = "",
    deploy_target: str = "",
    warehouse_id: str = "",
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
            job_parameters={
                "run_id": run_id,
                "space_id": space_id,
                "domain": domain,
                "catalog": catalog,
                "schema": schema,
                "apply_mode": apply_mode,
                "levers": levers,
                "max_iterations": max_iterations,
                "triggered_by": triggered_by,
                "experiment_name": experiment_name,
                "deploy_target": deploy_target,
                "warehouse_id": warehouse_id,
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

    For a bundle-managed job, ownership is set by ``deploy.sh`` and
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
                    f"to auto-repair, or re-run deploy.sh."
                )
            return True, ""

        for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME):
            tags = (job.settings.tags if job.settings else None) or {}
            if tags.get("managed-by") == "databricks-bundle" and job.job_id is not None:
                return True, ""
        return False, (
            "Runner job not found. Run deploy.sh to create it, "
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
    where ``deploy.sh`` may not have been run yet.
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
            "run deploy.sh to set permissions manually",
            job_id,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Per-space deployment jobs (still runtime-created)
# ---------------------------------------------------------------------------

def ensure_deployment_job(
    ws: WorkspaceClient,
    space_id: str,
    catalog: str,
    schema: str,
) -> int:
    """Find or create a per-space deployment job and return its job_id.

    The job runs ``run_cross_env_deploy`` notebook which reads model_name,
    model_version, target_workspace_url, and target_space_id as parameters.
    """
    from genie_space_optimizer.common.config import (
        DEPLOYMENT_JOB_NAME_TEMPLATE,
        format_mlflow_template,
    )

    job_name = format_mlflow_template(
        DEPLOYMENT_JOB_NAME_TEMPLATE, space_id=space_id,
    )

    for job in ws.jobs.list(name=job_name, limit=1):
        if job.job_id is not None:
            logger.info("Found existing deployment job %s (id=%s)", job_name, job.job_id)
            return int(job.job_id)

    vol_wheel = _find_volume_wheel(ws, catalog, schema)
    if vol_wheel:
        ws_wheel_path = vol_wheel
    else:
        ws_wheel_path, _ = _ensure_artifacts(ws, catalog=catalog, schema=schema)
    sp_client_id = _resolve_sp_client_id(ws)
    run_as = JobRunAs(service_principal_name=sp_client_id) if sp_client_id else None

    settings = JobSettings(
        name=job_name,
        run_as=run_as,
        description=(
            f"Cross-environment deployment job for Genie Space {space_id}. "
            "Triggered after a new UC model version is registered."
        ),
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
        parameters=[
            JobParameterDefinition(name="model_name", default=""),
            JobParameterDefinition(name="model_version", default=""),
            JobParameterDefinition(name="target_workspace_url", default=""),
            JobParameterDefinition(name="target_space_id", default=space_id),
        ],
        tasks=[
            Task(
                task_key="Approval_Check",
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_deploy_approval"],
                    source=Source.WORKSPACE,
                    base_parameters={
                        "model_name": "{{job.parameters.model_name}}",
                        "model_version": "{{job.parameters.model_version}}",
                        "approval_tag_name": "{{task.name}}",
                    },
                ),
                environment_key="default",
                timeout_seconds=86400,
                max_retries=0,
            ),
            Task(
                task_key="cross_env_deploy",
                depends_on=[TaskDependency(task_key="Approval_Check")],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_cross_env_deploy"],
                    source=Source.WORKSPACE,
                    base_parameters={
                        "model_name": "{{job.parameters.model_name}}",
                        "model_version": "{{job.parameters.model_version}}",
                        "target_workspace_url": "{{job.parameters.target_workspace_url}}",
                        "target_space_id": "{{job.parameters.target_space_id}}",
                    },
                ),
                environment_key="default",
                timeout_seconds=3600,
                max_retries=0,
            ),
        ],
        environments=[
            JobEnvironment(
                environment_key="default",
                spec=Environment(
                    client=_DEPLOY_JOB_ENVIRONMENT_CLIENT_VERSION,
                    dependencies=[
                        ws_wheel_path,
                        "mlflow[databricks]>=3.4.0",
                        "databricks-sdk>=0.40.0",
                    ],
                ),
            ),
        ],
        tags={
            "app": "genie-space-optimizer",
            "managed-by": "backend-job-launcher",
            "pattern": "deployment-job",
            "space_id": space_id,
        },
    )

    created = ws.jobs.create(
        name=settings.name,
        description=settings.description,
        max_concurrent_runs=settings.max_concurrent_runs,
        queue=settings.queue,
        parameters=settings.parameters,
        tasks=settings.tasks,
        environments=settings.environments,
        tags=settings.tags,
        run_as=settings.run_as,
    )
    if created.job_id is None:
        raise RuntimeError(f"Deployment job creation for space {space_id} returned no job_id")

    job_id = int(created.job_id)
    _ensure_job_visibility(ws, job_id)
    logger.info("Created deployment job %s (id=%s) for space %s", job_name, job_id, space_id)
    return job_id
