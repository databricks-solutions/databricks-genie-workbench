#!/usr/bin/env python3
"""Shared provisioning module for Genie Workbench.

Consolidates post-`apps create` / post-bundle-deploy provisioning into one
callable: ``provision_workbench(**config) -> ProvisionResult``. Two callers:

- CLI path — ``scripts/install.sh`` and ``scripts/deploy.sh`` invoke this via
  ``uv run python -m scripts.setup_workbench`` after the app and optimization
  job already exist (bundle creates the job first; the shell passes
  ``--gso-job-id`` so this module's ``_ensure_gso_job`` step no-ops).
- Non-CLI path — ``scripts/notebooks/setup_workbench.py`` imports
  ``provision_workbench`` and calls it from a Databricks notebook on
  serverless compute. The notebook creates the GSO job itself via
  ``WorkspaceClient.jobs.create`` using ``GSO_JOB_DAG`` from this module
  before calling ``provision_workbench``.

Pure SDK (no ``databricks`` CLI subprocess) so the module works inside
notebooks where the CLI is not installed.

Responsibilities:
1. Resolve the app's service principal
2. UC: ensure schema, volume, GSO Delta tables; enable CDF; grant SP
3. Lakebase: ensure project, role, database permissions
4. Resolve Lakebase database resource path (for postgres app resource)
5. Apps PATCH: set user_api_scopes and resources (sql-warehouse, postgres)
6. GSO job permissions (owner + SP CAN_MANAGE + group CAN_VIEW)
7. Workspace bundle-directory permissions (SP CAN_MANAGE)
8. Patch app.yaml in the workspace folder (substitutes 6 placeholders)
9. Optional: grant SP CAN_EDIT on every Genie Space the deployer can edit

Not in scope (shell still handles):
- Preflight checks, frontend build, file sync, ``databricks bundle deploy``,
  ``_metadata.py`` upload, ``apps deploy`` invocation, deployment verification.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _SCRIPT_DIR.parent
_GSO_SRC = _PROJECT_DIR / "packages" / "genie-space-optimizer" / "src"
if _GSO_SRC.is_dir() and str(_GSO_SRC) not in sys.path:
    sys.path.insert(0, str(_GSO_SRC))

# Reuse Lakebase plumbing as-is — already pure SDK, single source of truth.
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
from setup_lakebase import (  # noqa: E402
    _ensure_project as _ensure_lakebase_project,
    _ensure_role as _ensure_lakebase_role,
    _grant_permissions as _grant_lakebase_permissions,
)


# ── Constants ──────────────────────────────────────────────────────────────

SP_CATALOG_PRIVILEGES = ["USE_CATALOG"]
SP_SCHEMA_PRIVILEGES = [
    "USE_SCHEMA",
    "SELECT",
    "MODIFY",
    "CREATE_TABLE",
    "CREATE_FUNCTION",
    "CREATE_MODEL",
    "CREATE_VOLUME",
    "EXECUTE",
    "MANAGE",
]
SP_VOLUME_PRIVILEGES = ["READ_VOLUME", "WRITE_VOLUME"]

APP_USER_API_SCOPES = [
    "sql",
    "dashboards.genie",
    "serving.serving-endpoints",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
    "files.files",
]

APP_YAML_PLACEHOLDERS = (
    "__WAREHOUSE_ID__",
    "__GSO_CATALOG__",
    "__GSO_JOB_ID__",
    "__LAKEBASE_INSTANCE__",
    "__LLM_MODEL__",
    "__MLFLOW_EXPERIMENT_ID__",
)

GSO_JOB_NAME = "gso-optimization-job"

# Inline 6-task DAG — mirrors databricks.yml. The notebook path creates the
# job via WorkspaceClient.jobs.create using this spec; the CLI path keeps
# using the bundle and this literal stays unused there. Accepted drift —
# see `docs/non-cli-install.md` for unification tracking.
def _gso_job_dag(workspace_folder: str, wheel_path: str) -> dict[str, Any]:
    """Build the 6-task optimization DAG rooted at a given workspace folder.

    workspace_folder: absolute /Workspace/... path where the repo lives
    wheel_path: absolute /Workspace/... or /Volumes/... path to the GSO wheel
    """
    notebook_base = (
        f"{workspace_folder.rstrip('/')}"
        f"/packages/genie-space-optimizer/src/genie_space_optimizer/jobs"
    )
    parameters = [
        {"name": name, "default": default} for name, default in [
            ("run_id", ""), ("space_id", ""), ("domain", "default"),
            ("catalog", ""), ("schema", ""), ("apply_mode", "genie_config"),
            ("levers", "[1,2,3,4,5,6]"), ("max_iterations", "5"),
            ("triggered_by", ""), ("experiment_name", ""),
            ("deploy_target", ""), ("warehouse_id", ""),
        ]
    ]
    tasks = []
    prior = None
    for stage in ("preflight", "baseline_eval", "enrichment",
                  "lever_loop", "finalize"):
        task: dict[str, Any] = {
            "task_key": stage,
            "notebook_task": {"notebook_path": f"{notebook_base}/run_{stage}"},
            "environment_key": "default",
            "timeout_seconds": 7200,
            "max_retries": 0,
        }
        if prior:
            task["depends_on"] = [{"task_key": prior}]
        if stage == "preflight":
            task["notebook_task"]["base_parameters"] = {
                p["name"]: f"{{{{job.parameters.{p['name']}}}}}"
                for p in parameters
                if p["name"] != "triggered_by"
            }
        tasks.append(task)
        prior = stage
    # Final deploy gate (no-op condition task)
    tasks.append({
        "task_key": "deploy",
        "depends_on": [{"task_key": "finalize"}],
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "deploy",
            "right": "disabled",
        },
    })
    return {
        "name": GSO_JOB_NAME,
        "description": (
            "Persistent DAG optimization runner managed by Genie Workbench "
            "(preflight -> baseline_eval -> enrichment -> lever_loop -> "
            "finalize -> deploy). SP executes with granted privileges on "
            "user schemas."
        ),
        "max_concurrent_runs": 20,
        "queue": {"enabled": True},
        "tags": {
            "app": "genie-workbench",
            "managed-by": "setup_workbench",
            "pattern": "persistent-dag",
        },
        "parameters": parameters,
        "tasks": tasks,
        "environments": [{
            "environment_key": "default",
            "spec": {
                "environment_version": "4",
                "dependencies": [wheel_path],
            },
        }],
    }


# ── Result dataclass ───────────────────────────────────────────────────────

@dataclass
class ProvisionResult:
    sp_client_id: str
    sp_display_name: str
    gso_job_id: str
    gso_catalog: str
    gso_schema: str
    lakebase_database: str
    lakebase_branch: str
    genie_spaces_granted: int
    app_yaml_workspace_path: str
    patched_placeholders: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Public entry point ─────────────────────────────────────────────────────

def provision_workbench(
    *,
    app_name: str,
    catalog: str,
    warehouse_id: str,
    llm_model: str = "databricks-claude-sonnet-4-6",
    lakebase_project: str = "",
    mlflow_experiment_id: str = "",
    workspace_folder: str = "",
    gso_job_id: str = "",
    gso_schema: str = "genie_space_optimizer",
    grant_genie_spaces: bool = True,
    profile: str | None = None,
    deployer_email: str | None = None,
) -> ProvisionResult:
    """Run the full workbench provisioning sequence. Idempotent."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    if not deployer_email:
        deployer_email = _resolve_deployer_email(w)

    _log(f"Starting provisioning for app '{app_name}' (deployer={deployer_email or '<unknown>'})")

    # 1. App SP
    sp_client_id, sp_display_name = _resolve_app_sp(w, app_name)
    _log(f"App SP: {sp_display_name or sp_client_id} ({sp_client_id})")

    # 2. UC schema + tables + volume + grants
    _ensure_uc(
        w, catalog=catalog, schema=gso_schema,
        warehouse_id=warehouse_id, principal=sp_client_id,
    )

    # 3. Lakebase project + role + grants (if project configured)
    lakebase_db = ""
    lakebase_branch = ""
    if lakebase_project:
        _ensure_lakebase_project(w, lakebase_project)
        _ensure_lakebase_role(w, lakebase_project, sp_client_id)
        ok = _grant_lakebase_permissions(w, lakebase_project, sp_client_id, "primary")
        if not ok:
            _warn("Lakebase grants incomplete — app may fall back to in-memory storage.")
        lakebase_db, lakebase_branch = _resolve_lakebase_db(w, lakebase_project)

    # 4. GSO job — find by name; non-CLI caller can pass gso_job_id from notebook-side create
    resolved_job_id = gso_job_id or _find_gso_job(w)
    if not resolved_job_id:
        _warn(
            "GSO optimization job not found. CLI path: ensure bundle deploy ran. "
            "Notebook path: create via WorkspaceClient.jobs.create(_gso_job_dag(...)) "
            "and pass gso_job_id=."
        )

    # 5. App PATCH: scopes + resources
    _patch_app(
        w, app_name=app_name,
        warehouse_id=warehouse_id,
        lakebase_db=lakebase_db,
        lakebase_branch=lakebase_branch,
    )

    # 6. GSO job permissions
    if resolved_job_id and deployer_email:
        _set_job_permissions(
            w, job_id=resolved_job_id,
            owner_user=deployer_email, sp=sp_client_id,
        )

    # 7. Bundle workspace directory permissions (CLI path only — bundle writes here)
    if deployer_email:
        _grant_bundle_dir(w, deployer_email=deployer_email, sp=sp_client_id)

    # 8. Patch app.yaml on workspace
    app_yaml_path = ""
    substitutions = {
        "__WAREHOUSE_ID__": warehouse_id,
        "__GSO_CATALOG__": catalog,
        "__GSO_JOB_ID__": resolved_job_id,
        "__LAKEBASE_INSTANCE__": lakebase_project,
        "__LLM_MODEL__": llm_model,
        "__MLFLOW_EXPERIMENT_ID__": mlflow_experiment_id,
    }
    if workspace_folder:
        app_yaml_path = _patch_workspace_app_yaml(w, workspace_folder, substitutions)

    # 9. Genie Space SP grants (optional)
    granted = 0
    if grant_genie_spaces and sp_client_id:
        granted = _grant_genie_spaces(w, sp_client_id)

    _log("Provisioning complete.")
    return ProvisionResult(
        sp_client_id=sp_client_id,
        sp_display_name=sp_display_name or sp_client_id,
        gso_job_id=resolved_job_id,
        gso_catalog=catalog,
        gso_schema=gso_schema,
        lakebase_database=lakebase_db,
        lakebase_branch=lakebase_branch,
        genie_spaces_granted=granted,
        app_yaml_workspace_path=app_yaml_path,
        patched_placeholders=substitutions,
    )


# ── Helpers ────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    print(f"[setup_workbench] {msg}")


def _warn(msg: str) -> None:
    print(f"[setup_workbench] WARN: {msg}", file=sys.stderr)


def _resolve_deployer_email(w) -> str:
    try:
        me = w.current_user.me()
        if me.user_name:
            return me.user_name
        if me.emails:
            return me.emails[0].value or ""
    except Exception:
        pass
    return ""


def _resolve_app_sp(w, app_name: str) -> tuple[str, str]:
    """Return (sp_client_id, sp_display_name). Raises if app or SP missing."""
    from databricks.sdk.errors import NotFound
    try:
        app = w.apps.get(name=app_name)
    except NotFound as e:
        raise RuntimeError(
            f"App '{app_name}' does not exist. Create it first via "
            f"`databricks apps create` or the Apps UI."
        ) from e
    sp_id = (
        getattr(app, "service_principal_client_id", None)
        or getattr(app, "service_principal_name", None)
        or ""
    )
    if not sp_id:
        raise RuntimeError(
            f"App '{app_name}' has no service principal yet. "
            f"Wait for it to finish provisioning and re-run."
        )
    # Resolve display name (best-effort — some SP list calls require special scopes)
    display = ""
    try:
        for sp in w.service_principals.list():
            if (sp.application_id or "") == sp_id:
                display = sp.display_name or ""
                break
    except Exception:
        pass
    return sp_id, display


# ── Step 2: UC schema, tables, volume, grants ──────────────────────────────

def _ensure_uc(w, *, catalog: str, schema: str, warehouse_id: str, principal: str) -> None:
    schema_fqn = f"{catalog}.{schema}"
    volume_fqn = f"{schema_fqn}.app_artifacts"

    _sql(
        w, warehouse_id,
        f"CREATE SCHEMA IF NOT EXISTS {schema_fqn} "
        f"COMMENT 'Genie Space Optimizer state tables, prompts, and benchmarks'",
    )
    _log(f"Schema ensured: {schema_fqn}")

    _sql(w, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {volume_fqn}")
    _log(f"Volume ensured: {volume_fqn}")

    # Tables — DDL lives in the GSO package so optimization code is authoritative
    try:
        from genie_space_optimizer.optimization.ddl import _ALL_DDL
    except ImportError as e:
        raise RuntimeError(
            f"Could not import GSO DDL (genie_space_optimizer package not on path): {e}. "
            f"Ensure packages/genie-space-optimizer/src is importable."
        ) from e

    failed: list[str] = []
    for table, ddl_template in _ALL_DDL.items():
        stmt = ddl_template.replace("{catalog}", catalog).replace("{schema}", schema)
        try:
            _sql(w, warehouse_id, stmt)
            _log(f"Table ensured: {schema_fqn}.{table}")
        except Exception as e:
            _warn(f"Table creation failed for {table}: {e}")
            failed.append(table)
            continue
        # CDF is non-fatal — some environments reject TBLPROPERTIES on CREATE-already-exists
        try:
            _sql(
                w, warehouse_id,
                f"ALTER TABLE {schema_fqn}.{table} "
                f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",
            )
        except Exception:
            _warn(f"CDF enablement failed for {table} (non-fatal)")

    if failed:
        raise RuntimeError(
            f"Failed to create {len(failed)} table(s): {', '.join(failed)}. "
            f"Check warehouse access and permissions on {schema_fqn}."
        )

    # Grants
    _grant_uc(w, "CATALOG", catalog, principal, SP_CATALOG_PRIVILEGES)
    _grant_uc(w, "SCHEMA", schema_fqn, principal, SP_SCHEMA_PRIVILEGES)
    _grant_uc(w, "VOLUME", volume_fqn, principal, SP_VOLUME_PRIVILEGES)


def _sql(w, warehouse_id: str, statement: str, *, wait_timeout: str = "50s") -> None:
    """Execute a SQL statement synchronously, polling until terminal."""
    from databricks.sdk.service.sql import StatementState
    terminal = {
        StatementState.SUCCEEDED, StatementState.FAILED,
        StatementState.CANCELED, StatementState.CLOSED,
    }
    r = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=wait_timeout,
    )
    state = r.status.state if r.status else None
    deadline = time.time() + 300  # 5 min hard cap
    while state not in terminal and time.time() < deadline:
        time.sleep(5)
        r = w.statement_execution.get_statement(statement_id=r.statement_id)
        state = r.status.state if r.status else None
    if state != StatementState.SUCCEEDED:
        err = ""
        if r.status and r.status.error:
            err = r.status.error.message or ""
        raise RuntimeError(f"SQL failed ({state}): {err or '<no message>'}\n  stmt: {statement[:200]}")


def _grant_uc(w, securable_type: str, full_name: str, principal: str, privileges: list[str]) -> None:
    """Idempotent grant. Applies privileges individually so partial success works
    (e.g. SELECT grants succeed even if MANAGE requires metastore admin)."""
    from databricks.sdk.service.catalog import (
        PermissionsChange, Privilege, SecurableType,
    )
    secr = SecurableType[securable_type.upper()]

    applied: list[str] = []
    needs_admin: list[tuple[str, str]] = []  # (privilege, error)
    missing_securable = False

    for p in privileges:
        try:
            w.grants.update(
                securable_type=secr,
                full_name=full_name,
                changes=[PermissionsChange(principal=principal, add=[Privilege[p]])],
            )
            applied.append(p)
        except Exception as e:
            msg = str(e).lower()
            if "does not exist" in msg or "resource_does_not_exist" in msg:
                missing_securable = True
                break
            # Bucket permission errors — otherwise re-raise so surprises surface
            if any(s in msg for s in ("permission_denied", "forbidden", "manage", "not authorized")):
                needs_admin.append((p, str(e).strip().splitlines()[0]))
            else:
                raise

    if applied:
        _log(f"Granted {','.join(applied)} on {securable_type} {full_name} to {principal}")
    if missing_securable:
        _warn(f"{securable_type} '{full_name}' does not exist — skipping grants")
        return
    if needs_admin:
        _warn(
            f"Could not grant {len(needs_admin)}/{len(privileges)} privilege(s) on "
            f"{securable_type} '{full_name}'. Ask an owner/admin to run:"
        )
        for p, err in needs_admin:
            print(
                f"    GRANT {p} ON {securable_type.upper()} `{full_name}` TO `{principal}`  -- {err}",
                file=sys.stderr,
            )
        print(
            "    (The app may still work if the SP has access via group inheritance; "
            "Auto-Optimize needs these privileges to write optimizer state.)",
            file=sys.stderr,
        )


# ── Step 3: Lakebase database resolution (project/role/grants are in setup_lakebase) ──

def _resolve_lakebase_db(w, project_name: str) -> tuple[str, str]:
    """Return (database_resource_name, branch_resource_path). Empty strings if not found."""
    try:
        dbs = w.api_client.do(
            "GET",
            f"/api/2.0/postgres/projects/{project_name}/branches/production/databases",
        )
    except Exception as e:
        _warn(f"Could not resolve Lakebase database: {e}")
        return "", ""
    databases = (dbs or {}).get("databases") or []
    if not databases:
        _warn(f"Project '{project_name}' has no databases — postgres resource won't be auto-wired")
        return "", ""
    db_resource = databases[0].get("name", "") or ""
    # resource name looks like: projects/<proj>/branches/production/databases/<id>
    branch = "/".join(db_resource.split("/")[:4]) if db_resource else ""
    _log(f"Lakebase database: {db_resource}")
    return db_resource, branch


# ── Step 5: Apps PATCH ─────────────────────────────────────────────────────

def _patch_app(
    w, *, app_name: str, warehouse_id: str,
    lakebase_db: str, lakebase_branch: str,
) -> None:
    """Set user_api_scopes and resources. Preserves user-managed resources."""
    try:
        current = w.api_client.do("GET", f"/api/2.0/apps/{app_name}") or {}
    except Exception as e:
        raise RuntimeError(f"Could not read existing app config for '{app_name}': {e}") from e

    existing = current.get("resources") or []
    # The PATCH replaces resources wholesale — preserve anything non-empty or
    # explicitly referenced by app.yaml (sql-warehouse, postgres).
    managed = {"sql-warehouse", "postgres"}
    by_name: dict[str, dict[str, Any]] = {}
    for r in existing:
        name = r.get("name")
        if not name:
            continue
        has_config = any(k for k in r if k != "name")
        if has_config or name in managed:
            by_name[name] = r

    # Always set sql-warehouse with the target warehouse
    by_name["sql-warehouse"] = {
        "name": "sql-warehouse",
        "sql_warehouse": {"id": warehouse_id, "permission": "CAN_USE"},
    }
    # Wire postgres resource only when we have a fully-qualified DB path
    if lakebase_db:
        by_name["postgres"] = {
            "name": "postgres",
            "postgres": {
                "branch": lakebase_branch,
                "database": lakebase_db,
                "permission": "CAN_CONNECT_AND_CREATE",
            },
        }

    payload = {
        "user_api_scopes": APP_USER_API_SCOPES,
        "resources": list(by_name.values()),
    }
    try:
        w.api_client.do("PATCH", f"/api/2.0/apps/{app_name}", body=payload)
        _log(f"App scopes + resources configured (warehouse={warehouse_id}, postgres={'yes' if lakebase_db else 'no'})")
    except Exception as e:
        _warn(f"Could not configure app scopes/resources: {e}")


# ── Step 6: Job permissions ────────────────────────────────────────────────

def _find_gso_job(w) -> str:
    """Find the GSO job by name. Empty string if not found."""
    try:
        jobs = w.jobs.list(name=GSO_JOB_NAME)
        for j in jobs:
            if j.settings and j.settings.name == GSO_JOB_NAME:
                return str(j.job_id)
    except Exception as e:
        _warn(f"Could not list jobs to find '{GSO_JOB_NAME}': {e}")
    return ""


def _set_job_permissions(w, *, job_id: str, owner_user: str, sp: str) -> None:
    """Set owner + SP CAN_MANAGE + users group CAN_VIEW on the GSO job."""
    acl = [
        {"user_name": owner_user, "permission_level": "IS_OWNER"},
        {"group_name": "users", "permission_level": "CAN_VIEW"},
        {"service_principal_name": sp, "permission_level": "CAN_MANAGE"},
    ]
    try:
        w.api_client.do(
            "PUT", f"/api/2.0/permissions/jobs/{job_id}",
            body={"access_control_list": acl},
        )
        _log(f"Job permissions set on {job_id} (owner={owner_user}, SP=CAN_MANAGE, users=CAN_VIEW)")
    except Exception as e:
        _warn(f"Could not set job permissions on {job_id}: {e}")


def _grant_bundle_dir(w, *, deployer_email: str, sp: str) -> None:
    """Grant SP CAN_MANAGE on the bundle workspace directory.

    Bundle deploys write notebooks under
    ``/Workspace/Users/<deployer>/.bundle/genie-workbench/app`` which is
    private by default. The SP needs read access to execute those notebooks.
    No-op on non-CLI path (bundle never ran, directory doesn't exist).
    """
    bundle_root = f"/Workspace/Users/{deployer_email}/.bundle/genie-workbench/app"
    try:
        status = w.workspace.get_status(path=bundle_root)
    except Exception:
        _log(f"Bundle directory {bundle_root} not present — skipping (non-CLI path)")
        return
    obj_id = getattr(status, "object_id", None)
    if not obj_id:
        _warn(f"Could not resolve object_id for {bundle_root}")
        return
    try:
        w.api_client.do(
            "PATCH", f"/api/2.0/permissions/directories/{obj_id}",
            body={
                "access_control_list": [
                    {"service_principal_name": sp, "permission_level": "CAN_MANAGE"}
                ]
            },
        )
        _log(f"SP granted CAN_MANAGE on bundle directory {bundle_root}")
    except Exception as e:
        _warn(f"Could not grant SP on bundle directory: {e}")


# ── Step 8: app.yaml patching on workspace ─────────────────────────────────

def _patch_workspace_app_yaml(
    w, workspace_folder: str, substitutions: dict[str, str],
) -> str:
    """Read local app.yaml, substitute placeholders, upload to workspace_folder/app.yaml.

    Returns the workspace path of the uploaded file. The substitutions dict
    maps placeholder → value; missing values (empty strings) are left in
    place with a warning so the caller can see what wasn't filled.
    """
    from databricks.sdk.service.workspace import ImportFormat

    src = _PROJECT_DIR / "app.yaml"
    content = src.read_text()
    for placeholder, value in substitutions.items():
        # Substitute unconditionally — empty values are legitimate for
        # optional fields (e.g. MLFLOW_EXPERIMENT_ID disables tracing).
        content = content.replace(placeholder, value)

    # Anything still matching __FOO__ means a caller forgot to pass it.
    unresolved = [p for p in APP_YAML_PLACEHOLDERS if p in content]
    if unresolved:
        _warn(f"app.yaml has unresolved placeholders: {unresolved}")

    dst = f"{workspace_folder.rstrip('/')}/app.yaml"
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    w.workspace.import_(
        path=dst,
        format=ImportFormat.AUTO,
        overwrite=True,
        content=encoded,
    )
    _log(f"app.yaml written to {dst}")
    return dst


# ── Step 9: Genie Space SP grants ──────────────────────────────────────────

def _grant_genie_spaces(w, sp: str) -> int:
    """Grant SP CAN_EDIT on every Genie Space the current user can edit."""
    spaces: list[dict[str, Any]] = []
    try:
        result = w.api_client.do("GET", "/api/2.0/genie/spaces") or {}
        spaces = (
            result if isinstance(result, list)
            else (result.get("spaces") or result.get("genie_spaces") or [])
        )
    except Exception as e:
        _warn(f"Could not list Genie Spaces: {e}")
        return 0

    granted = 0
    for space in spaces:
        space_id = space.get("id") or space.get("space_id", "")
        space_name = space.get("title") or space.get("name") or space_id
        if not space_id:
            continue
        try:
            w.api_client.do(
                "PUT", f"/api/2.0/permissions/genie/{space_id}",
                body={
                    "access_control_list": [
                        {"service_principal_name": sp, "permission_level": "CAN_EDIT"}
                    ]
                },
            )
            _log(f"Granted CAN_EDIT to SP on Genie Space: {space_name} ({space_id})")
            granted += 1
        except Exception as e:
            _warn(f"Could not grant on {space_name}: {e}")
    return granted


# ── CLI entry point ────────────────────────────────────────────────────────

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="setup_workbench",
        description="Shared Genie Workbench provisioning (CLI + notebook entry point).",
    )
    p.add_argument("--app-name", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--llm-model", default="databricks-claude-sonnet-4-6")
    p.add_argument("--lakebase-project", default="")
    p.add_argument("--mlflow-experiment-id", default="")
    p.add_argument("--workspace-folder", default="",
                   help="Workspace path where app.yaml should be written")
    p.add_argument("--gso-job-id", default="",
                   help="Pre-resolved GSO job ID (CLI path passes this from bundle state)")
    p.add_argument("--gso-schema", default="genie_space_optimizer")
    p.add_argument("--profile", default=None)
    p.add_argument("--deployer-email", default=None)
    p.add_argument("--skip-genie-grants", action="store_true",
                   help="Do not grant SP access to user-editable Genie Spaces")
    p.add_argument("--result-json", default="",
                   help="If set, write the ProvisionResult as JSON to this path")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        result = provision_workbench(
            app_name=args.app_name,
            catalog=args.catalog,
            warehouse_id=args.warehouse_id,
            llm_model=args.llm_model,
            lakebase_project=args.lakebase_project,
            mlflow_experiment_id=args.mlflow_experiment_id,
            workspace_folder=args.workspace_folder,
            gso_job_id=args.gso_job_id,
            gso_schema=args.gso_schema,
            grant_genie_spaces=not args.skip_genie_grants,
            profile=args.profile,
            deployer_email=args.deployer_email,
        )
    except Exception as e:
        print(f"[setup_workbench] ERROR: {e}", file=sys.stderr)
        return 1

    if args.result_json:
        Path(args.result_json).write_text(json.dumps(result.to_dict(), indent=2))
    # Print a single-line JSON summary on stdout so shell callers can parse it
    print(f"SETUP_WORKBENCH_RESULT={json.dumps(result.to_dict())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
