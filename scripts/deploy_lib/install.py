"""Notebook installer orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .app_yaml import render_app_yaml
from .apps import deploy_app_from_workspace, ensure_app, get_app_service_principal, patch_app_resources, wait_for_deployment
from .config import InstallConfig, InstallResult, LakebaseInfo
from .genie_spaces import optionally_grant_genie_spaces
from .gso_job import ensure_gso_job
from .lakebase import ensure_lakebase
from .uc import ensure_uc_objects_and_grants
from .verify import verify_app_deployment
from .workspace_source import prepare_workspace_source


def get_deployer_user(w) -> str:
    me = w.current_user.me()
    user_name = getattr(me, "user_name", None)
    if user_name:
        return user_name
    emails = getattr(me, "emails", None) or []
    if emails and getattr(emails[0], "value", None):
        return emails[0].value
    raise RuntimeError("Could not resolve current Databricks user")


def run_install(w, cfg: InstallConfig) -> dict[str, Any]:
    cfg = cfg.normalized()
    cfg.validate()

    deployer_user = get_deployer_user(w)
    ensure_app(w, cfg)
    sp = get_app_service_principal(w, cfg.app_name)
    app_sp_client_id = sp["client_id"]

    source_path = prepare_workspace_source(w, cfg, deployer_user)
    uc_verification = ensure_uc_objects_and_grants(w, cfg, app_sp_client_id)

    lakebase: LakebaseInfo | None = None
    if cfg.lakebase_mode != "skip" and cfg.lakebase_instance:
        lakebase = ensure_lakebase(w, cfg, app_sp_client_id)

    gso_job = ensure_gso_job(w, cfg, app_sp_client_id, deployer_user)

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

    resources_payload = patch_app_resources(w, cfg, lakebase)
    deployment = deploy_app_from_workspace(w, cfg.app_name, source_path)
    wait_for_deployment(w, cfg.app_name, timeout_seconds=180, poll_seconds=10)

    genie_spaces_granted = optionally_grant_genie_spaces(w, cfg, app_sp_client_id)
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
    return result.to_dict()

