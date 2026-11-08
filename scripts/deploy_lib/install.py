"""Notebook installer orchestration."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .app_yaml import PLACEHOLDER_RE, render_app_yaml
from .apps import (
    deploy_app_from_workspace,
    ensure_app,
    get_app,
    get_app_service_principal,
    patch_app_resources,
    require_successful_deployment,
)
from .config import APP_NAME_ERROR, APP_NAME_RE, InstallConfig, InstallResult, LakebaseInfo
from .genie_spaces import optionally_grant_genie_spaces
from .gso_job import ensure_gso_job
from .lakebase import ensure_lakebase, find_soft_deleted_project
from .uc import ensure_uc_objects_and_grants
from .verify import verify_ai_gateway, verify_app_deployment
from .workspace_source import (
    prepare_workspace_source,
    read_workspace_text,
    source_path_for,
    workspace_api_path,
    write_workspace_file,
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
    app it keeps the previous decided value, or leaves it empty (ask users on
    the Admin tab) so a rerun with default widgets never silently opts in.
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
    value = app_env(app_yaml_text).get("UPDATE_CHECK_ENABLED", "").lower()
    return value if value in ("true", "false") else None


def prior_app_yaml_paths(app: dict[str, Any] | None, cfg: InstallConfig, deployer_user: str) -> list[str]:
    """Where the app's current app.yaml may live, most likely first.

    The deployment's own snapshot comes first: unlike the source folder, an
    interrupted installer run never rewrites it.
    """
    candidates: list[str] = []
    if app:
        active = app.get("active_deployment") or {}
        snapshot = (active.get("deployment_artifacts") or {}).get("source_code_path")
        for path in (snapshot, active.get("source_code_path"), app.get("default_source_code_path")):
            if path:
                candidates.append(str(path).rstrip("/"))
    candidates.append(source_path_for(cfg, deployer_user))
    return list(dict.fromkeys(candidates))


def prior_app_yaml_text(w, app: dict[str, Any] | None, cfg: InstallConfig, deployer_user: str) -> str | None:
    """The app.yaml of the app's current deployment, wherever it lives.

    Tries the active deployment's source path, then the app's default source path
    (CLI deploys), then this user's generated source folder, so a rerun by another
    user or after a CLI deploy still finds the app's settings.
    """
    for base in prior_app_yaml_paths(app, cfg, deployer_user):
        text = read_workspace_text(w, f"{base}/app.yaml")
        if text:
            return text
    return None


def describe_update_check(value: str) -> str:
    if value == "true":
        return "Daily release check: ON (runs in users' browsers on the Admin tab against api.github.com)."
    if value == "false":
        return "Daily release check: OFF."
    return "Daily release check: ASK (users will be asked on the Admin tab)."


RECORDED_LAKEBASE_DELETED = (
    "The Lakebase project '{project}' that app '{name}' uses is deleted (it can still be restored "
    "for 7 days). Restore it with `databricks postgres undelete-project projects/{project}`, then "
    "run the installer again."
)


def app_env(app_yaml_text: str | None) -> dict[str, str]:
    """Literal env values from a deployed app.yaml.

    Empty values, unrendered __X__ placeholders, and valueFrom entries are left out,
    so callers only ever see values an installer actually rendered.
    """
    if not app_yaml_text:
        return {}
    import yaml

    try:
        doc = yaml.safe_load(app_yaml_text) or {}
    except yaml.YAMLError:
        return {}
    if not isinstance(doc, dict):
        return {}
    env: dict[str, str] = {}
    for entry in doc.get("env") or []:
        if not isinstance(entry, dict) or not entry.get("name") or "value" not in entry:
            continue
        value = str(entry.get("value") or "").strip()
        if value and not PLACEHOLDER_RE.fullmatch(value):
            env[str(entry["name"])] = value
    return env


def reuse_recorded_lakebase(
    cfg: InstallConfig,
    app: dict[str, Any] | None,
    app_yaml_text: str | None,
) -> InstallConfig:
    """On a rerun, keep the Lakebase project the app already uses (spec 11.5, 11.9).

    lakebase_mode = create with a blank project name would otherwise target
    <app_name>-lakebase: a new, empty project whenever the app was set up with a
    different name (common for CLI installs or explicit names).
    """
    recorded = app_env(app_yaml_text).get("LAKEBASE_INSTANCE_NAME")
    if app is not None and cfg.lakebase_mode == "create" and not (cfg.lakebase_instance or "").strip() and recorded:
        return replace(cfg, lakebase_instance=recorded)
    return cfg


def _lakebase_project_state(w, project: str) -> str:
    """"missing", "deleted" (soft-deleted, still restorable), or "ok"."""
    from databricks.sdk.errors import NotFound

    try:
        found = w.postgres.get_project(name=f"projects/{project}")
    except NotFound:
        # A soft-deleted project can also 404 here; ensure_project finds it through
        # list_projects(show_deleted=True) and purges it, so check the same way.
        return "deleted" if find_soft_deleted_project(w, project) else "missing"
    return "deleted" if getattr(found, "delete_time", None) else "ok"


def require_reused_lakebase_not_deleted(w, cfg: InstallConfig) -> None:
    """A rerun that reuses the app's recorded project must never purge it.

    lakebase_mode = create purges a soft-deleted project of the same name before
    recreating it. The admin never typed this name, so stop and point at the
    restore command instead. A project that is fully gone is simply created again.
    """
    if cfg.lakebase_instance and _lakebase_project_state(w, cfg.lakebase_instance) == "deleted":
        raise RuntimeError(RECORDED_LAKEBASE_DELETED.format(project=cfg.lakebase_instance, name=cfg.app_name))


def notebook_build_commit(w, repo_root: str | None) -> str:
    """Short HEAD commit of the Git folder the installer runs from, or "" if unknown."""
    if not repo_root:
        return ""
    try:
        # A Git folder root reports object_type DIRECTORY; its object_id is the repo ID,
        # so ask the Repos API and treat any error (a plain folder) as "not recorded".
        info = w.workspace.get_status(workspace_api_path(repo_root))
        if not info.object_id:
            return ""
        head = w.repos.get(info.object_id).head_commit_id or ""
    except Exception:  # noqa: BLE001 - build info is best effort
        return ""
    return head[:12]


def run_install(w, cfg: InstallConfig, status_fn=None) -> dict[str, Any]:
    status = status_fn or _default_status
    app_name = cfg.app_name.strip()
    if not APP_NAME_RE.match(app_name):
        raise ValueError(APP_NAME_ERROR)
    # Fail fast on the values as typed, before any workspace call. The config is
    # validated again below once a rerun has picked up the app's Lakebase project.
    cfg.normalized().validate()
    preflight_deployment(Path(cfg.repo_root or ""))

    status("Resolving current Databricks user...")
    deployer_user = get_deployer_user(w)
    status(f"Using deployer: {deployer_user}")

    # Read the existing app and its current app.yaml before anything is created or
    # regenerated, so a rerun keeps the app's Lakebase project and release-check answer.
    existing_app = get_app(w, app_name)
    app_existed = existing_app is not None
    prior_app_yaml = prior_app_yaml_text(w, existing_app, cfg.normalized(), deployer_user) if app_existed else None
    typed_lakebase = (cfg.lakebase_instance or "").strip()
    cfg = reuse_recorded_lakebase(cfg, existing_app, prior_app_yaml)
    reused_lakebase = not typed_lakebase and bool(cfg.lakebase_instance)
    cfg = cfg.normalized()
    cfg.validate()
    if reused_lakebase:
        status(f"Keeping the Lakebase project this app already uses: {cfg.lakebase_instance}")
        require_reused_lakebase_not_deleted(w, cfg)

    if cfg.llm_route == "gateway":
        status(f"Checking Unity AI Gateway with model {cfg.llm_model}...")
        verify_ai_gateway(w, cfg.llm_model)
        status("AI Gateway reachable.")
    else:
        status("LLM route: classic serving endpoints (AI Gateway check skipped).")

    status(f"Creating or reusing Databricks App '{cfg.app_name}'...")
    ensure_app(w, cfg)
    status("Waiting for app service principal...")
    sp = get_app_service_principal(w, cfg.app_name)
    app_sp_client_id = sp["client_id"]
    status(f"Resolved app service principal: {app_sp_client_id}")

    update_check = resolve_update_check(
        cfg.release_check, app_existed=app_existed, previous_value=previous_update_check(prior_app_yaml)
    )
    status(describe_update_check(update_check))

    status("Generating curated workspace source folder...")
    source_path = prepare_workspace_source(w, cfg, deployer_user)
    if prior_app_yaml:
        # Regenerating the folder copied in the app.yaml template. Put the app's current
        # settings back until the new app.yaml is rendered, so an interrupted run does not
        # erase what the next upgrade (or rerun) reads.
        write_workspace_file(w, f"{source_path}/app.yaml", prior_app_yaml.encode("utf-8"))
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
