"""Notebook installer orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .app_yaml import render_app_yaml
from .apps import (
    deploy_app_from_workspace,
    ensure_app,
    get_app,
    get_app_service_principal,
    patch_app_resources,
    require_successful_deployment,
)
from .config import InstallConfig, InstallResult, LakebaseInfo
from .genie_spaces import optionally_grant_genie_spaces
from .gso_job import ensure_gso_job
from .lakebase import ensure_lakebase
from .uc import ensure_uc_objects_and_grants
from .verify import verify_ai_gateway, verify_app_deployment
from .workspace_source import (
    prepare_workspace_source,
    read_workspace_text,
    source_path_for,
    workspace_api_path,
)
from backend.services.version_control.platform.bundles import preflight_deployment


def _default_status(message: str) -> None:
    print(f"[genie-workbench install] {message}", flush=True)


def llm_route_replacements(cfg: InstallConfig) -> dict[str, str]:
    return {
        "GENIE_LLM_ROUTE": cfg.llm_route,
        "GENIE_BYOK_ENABLED": "true" if cfg.byok_enabled else "false",
    }


def get_deployer_user(w) -> str:
    me = w.current_user.me()
    user_name = getattr(me, "user_name", None)
    if user_name:
        return user_name
    emails = getattr(me, "emails", None) or []
    if emails and getattr(emails[0], "value", None):
        return emails[0].value
    raise RuntimeError("Could not resolve current Databricks user")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def release_check_replacements(*, update_check: str, build_commit: str, deployed_at: str) -> dict[str, str]:
    """app.yaml values for the daily release check and build info (spec 5.2, 5.4)."""
    return {
        "UPDATE_CHECK_ENABLED": update_check,
        "GWB_BUILD_COMMIT": build_commit,
        "GWB_DEPLOYED_AT": deployed_at,
        "GWB_INSTALL_METHOD": "notebook",
    }


def resolve_update_check(choice: str, *, app_existed: bool, previous_value: str | None) -> str:
    """UPDATE_CHECK_ENABLED for a notebook install (spec 5.4).

    yes/no are explicit. auto turns the check on for a new app; for an existing
    app it keeps the previous decided value, or leaves it empty (ask admins in
    the app) so a rerun with default widgets never silently opts in.
    """
    if choice == "yes":
        return "true"
    if choice == "no":
        return "false"
    if not app_existed:
        return "true"
    return previous_value if previous_value in ("true", "false") else ""


def previous_update_check(app_yaml_text: str | None) -> str | None:
    """UPDATE_CHECK_ENABLED from a previously deployed app.yaml, if it was decided."""
    if not app_yaml_text:
        return None
    import yaml

    try:
        doc = yaml.safe_load(app_yaml_text) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(doc, dict):
        return None
    for entry in doc.get("env") or []:
        if isinstance(entry, dict) and entry.get("name") == "UPDATE_CHECK_ENABLED":
            value = str(entry.get("value") or "").strip().lower()
            return value if value in ("true", "false") else None
    return None


def describe_update_check(value: str) -> str:
    if value == "true":
        return "Daily release check: ON (runs in admins' browsers against api.github.com)."
    if value == "false":
        return "Daily release check: OFF."
    return "Daily release check: ASK (workspace admins will be asked in the app)."


def notebook_build_commit(w, repo_root: str | None) -> str:
    """Short HEAD commit of the Git folder the installer runs from, or "" if unknown."""
    if not repo_root:
        return ""
    try:
        info = w.workspace.get_status(workspace_api_path(repo_root))
        object_type = getattr(info.object_type, "value", info.object_type)
        if object_type != "REPO" or not info.object_id:
            return ""
        head = w.repos.get(info.object_id).head_commit_id or ""
    except Exception:  # noqa: BLE001 - build info is best effort
        return ""
    return head[:12]


def run_install(w, cfg: InstallConfig, status_fn=None) -> dict[str, Any]:
    status = status_fn or _default_status
    cfg = cfg.normalized()
    cfg.validate()
    preflight_deployment(Path(cfg.repo_root or ""))

    status("Resolving current Databricks user...")
    deployer_user = get_deployer_user(w)
    status(f"Using deployer: {deployer_user}")

    if cfg.llm_route == "gateway":
        status(f"Checking Unity AI Gateway with model {cfg.llm_model}...")
        verify_ai_gateway(w, cfg.llm_model)
        status("AI Gateway reachable.")
    else:
        status("LLM route: classic serving endpoints (AI Gateway check skipped).")

    status(f"Creating or reusing Databricks App '{cfg.app_name}'...")
    app_existed = get_app(w, cfg.app_name) is not None
    ensure_app(w, cfg)
    status("Waiting for app service principal...")
    sp = get_app_service_principal(w, cfg.app_name)
    app_sp_client_id = sp["client_id"]
    status(f"Resolved app service principal: {app_sp_client_id}")

    # Read the previous answer before the source folder (and its app.yaml) is regenerated.
    prior_app_yaml = (
        read_workspace_text(w, f"{source_path_for(cfg, deployer_user)}/app.yaml") if app_existed else None
    )
    update_check = resolve_update_check(
        cfg.update_check, app_existed=app_existed, previous_value=previous_update_check(prior_app_yaml)
    )
    status(describe_update_check(update_check))

    status("Generating curated workspace source folder...")
    source_path = prepare_workspace_source(w, cfg, deployer_user)
    status(f"Workspace source ready: {source_path}")

    status(f"Provisioning Unity Catalog objects in {cfg.catalog}.{cfg.gso_schema}...")
    uc_verification = ensure_uc_objects_and_grants(w, cfg, app_sp_client_id)
    status("Unity Catalog objects and grants processed.")

    lakebase: LakebaseInfo | None = None
    if cfg.lakebase_mode != "skip" and cfg.lakebase_instance:
        status(f"Provisioning Lakebase project '{cfg.lakebase_instance}'...")
        lakebase = ensure_lakebase(w, cfg, app_sp_client_id)
        status(
            "Lakebase processed "
            f"(database={lakebase.database_resource or 'unresolved'}, grants={lakebase.grants_applied})."
        )
    else:
        status("Skipping Lakebase provisioning.")

    status("Uploading GSO job notebooks, building wheel, and creating/updating job...")
    gso_job = ensure_gso_job(w, cfg, app_sp_client_id, deployer_user)
    status(f"GSO job ready: {gso_job.job_id}")
    if gso_job.run_as_bound:
        status("Job run_as set to the app service principal.")
    else:
        status(
            "Job created without run_as (installer lacks servicePrincipal.user on the "
            "app service principal); the app sets it at startup."
        )

    status("Rendering patched app.yaml into generated workspace source...")
    # Version Control observe surface: mirror deploy.sh's __VC_OBSERVE_*__ resolution.
    # WORKSPACE_ID/HOST come from this workspace; CATALOG/CONTROL_SCHEMA from the GSO
    # location (the VC control tables are colocated in the GSO schema); SP_PRINCIPAL_ID
    # is the app SP resolved above. If workspace-id/host cannot be resolved, substitute
    # EMPTY so the observe runtime stays fail-closed rather than blocking the deploy.
    try:
        vc_observe_workspace_id = str(w.get_workspace_id())
    except Exception:  # noqa: BLE001
        vc_observe_workspace_id = ""
    vc_observe_host = (getattr(w.config, "host", "") or "").rstrip("/")
    render_app_yaml(
        template_path=Path(cfg.repo_root or "") / "app.yaml",
        output_workspace_path=f"{source_path}/app.yaml",
        replacements={
            "WAREHOUSE_ID": cfg.warehouse_id,
            "GSO_CATALOG": cfg.catalog,
            "GSO_JOB_ID": str(gso_job.job_id),
            "LAKEBASE_INSTANCE": cfg.lakebase_instance or "",
            "LLM_MODEL": cfg.llm_model,
            **llm_route_replacements(cfg),
            "MLFLOW_EXPERIMENT_ID": cfg.mlflow_experiment_id or "",
            "VC_OBSERVE_WORKSPACE_ID": vc_observe_workspace_id,
            "VC_OBSERVE_HOST": vc_observe_host,
            "VC_OBSERVE_CATALOG": cfg.catalog,
            "VC_OBSERVE_CONTROL_SCHEMA": cfg.gso_schema,
            "VC_OBSERVE_SP_PRINCIPAL_ID": app_sp_client_id,
            **release_check_replacements(
                update_check=update_check,
                build_commit=notebook_build_commit(w, cfg.repo_root),
                deployed_at=utc_now_iso(),
            ),
        },
        workspace_client=w,
    )

    status("Configuring app scopes and resources...")
    resources_payload = patch_app_resources(w, cfg, lakebase)
    status("Triggering Databricks App deployment and waiting for success...")
    submitted_deployment = deploy_app_from_workspace(w, cfg.app_name, source_path)
    deployed_app = {"pending_deployment": submitted_deployment}
    deployment = require_successful_deployment(cfg.app_name, deployed_app)

    status("Processing optional Genie Agent grants...")
    genie_spaces_granted = optionally_grant_genie_spaces(w, cfg, app_sp_client_id)
    status(f"Genie Agent grants applied: {genie_spaces_granted}")

    status("Verifying deployment...")
    verification = verify_app_deployment(w, cfg.app_name, source_path)
    verification["uc_grants"] = uc_verification
    verification["app_resources"] = resources_payload

    result = InstallResult(
        app_name=cfg.app_name,
        app_url=verification.get("app_url"),
        source_path=source_path,
        service_principal_client_id=app_sp_client_id,
        gso_job=gso_job,
        lakebase=lakebase,
        genie_spaces_granted=genie_spaces_granted,
        deployment=deployment or {},
        verification=verification,
    )
    status("Install flow complete.")
    return result.to_dict()
