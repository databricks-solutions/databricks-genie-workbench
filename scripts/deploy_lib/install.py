"""Notebook installer orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .app_yaml import render_app_yaml
from .apps import deploy_app_from_workspace, ensure_app, get_app_service_principal, patch_app_resources, require_successful_deployment, wait_for_deployment
from .config import InstallConfig, InstallResult, LakebaseInfo
from .genie_spaces import optionally_grant_genie_spaces
from .gso_job import ensure_gso_job
from .lakebase import ensure_lakebase
from .uc import ensure_uc_objects_and_grants
from .verify import verify_app_deployment
from .workspace_source import prepare_workspace_source


def _default_status(message: str) -> None:
    print(f"[genie-workbench install] {message}", flush=True)


def get_deployer_user(w) -> str:
    me = w.current_user.me()
    user_name = getattr(me, "user_name", None)
    if user_name:
        return user_name
    emails = getattr(me, "emails", None) or []
    if emails and getattr(emails[0], "value", None):
        return emails[0].value
    raise RuntimeError("Could not resolve current Databricks user")


def run_install(w, cfg: InstallConfig, status_fn=None) -> dict[str, Any]:
    status = status_fn or _default_status
    cfg = cfg.normalized()
    cfg.validate()

    status("Resolving current Databricks user...")
    deployer_user = get_deployer_user(w)
    status(f"Using deployer: {deployer_user}")

    status(f"Creating or reusing Databricks App '{cfg.app_name}'...")
    ensure_app(w, cfg)
    status("Waiting for app service principal...")
    sp = get_app_service_principal(w, cfg.app_name)
    app_sp_client_id = sp["client_id"]
    status(f"Resolved app service principal: {app_sp_client_id}")

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

    status("Rendering patched app.yaml into generated workspace source...")
    render_app_yaml(
        template_path=Path(cfg.repo_root or "") / "app.yaml",
        output_workspace_path=f"{source_path}/app.yaml",
        replacements={
            "WAREHOUSE_ID": cfg.warehouse_id,
            "GSO_CATALOG": cfg.catalog,
            "GSO_JOB_ID": str(gso_job.job_id),
            "LAKEBASE_INSTANCE": cfg.lakebase_instance or "",
            "LLM_MODEL": cfg.llm_model,
            "MLFLOW_EXPERIMENT_ID": cfg.mlflow_experiment_id or "",
        },
        workspace_client=w,
    )

    status("Configuring app scopes and resources...")
    resources_payload = patch_app_resources(w, cfg, lakebase)
    status("Triggering Databricks App deployment...")
    deploy_app_from_workspace(w, cfg.app_name, source_path)
    status("Waiting for app deployment status...")
    deployed_app = wait_for_deployment(w, cfg.app_name, timeout_seconds=180, poll_seconds=10)
    deployment = require_successful_deployment(cfg.app_name, deployed_app)

    status("Processing optional Genie Space grants...")
    genie_spaces_granted = optionally_grant_genie_spaces(w, cfg, app_sp_client_id)
    status(f"Genie Space grants applied: {genie_spaces_granted}")

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
