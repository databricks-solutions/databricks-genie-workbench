"""Databricks Apps API helpers for the notebook installer."""

from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any

from .config import InstallConfig, LakebaseInfo


APP_SCOPES = [
    "sql",
    "dashboards.genie",
    "serving.serving-endpoints",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
    "files.files",
]


def api_do(w, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    return w.api_client.do(method=method, path=path, body=body)


def get_app(w, app_name: str) -> dict[str, Any] | None:
    try:
        return api_do(w, "GET", f"/api/2.0/apps/{app_name}")
    except Exception as exc:
        msg = str(exc).lower()
        if "not_found" in msg or "not found" in msg or "does not exist" in msg:
            return None
        raise


def get_app_deployment(w, app_name: str, deployment_id: str) -> dict[str, Any] | None:
    try:
        return api_do(w, "GET", f"/api/2.0/apps/{app_name}/deployments/{deployment_id}")
    except Exception as exc:
        msg = str(exc).lower()
        if "not_found" in msg or "not found" in msg or "does not exist" in msg:
            return None
        raise


def ensure_app(w, cfg: InstallConfig) -> dict[str, Any]:
    existing = get_app(w, cfg.app_name)
    if existing:
        return existing
    return api_do(
        w,
        "POST",
        "/api/2.0/apps",
        {
            "name": cfg.app_name,
            "description": "Genie Workbench - Create, score, and optimize Genie Agents",
        },
    )


def _extract_service_principal(app: dict[str, Any]) -> dict[str, str] | None:
    client_id = (
        app.get("service_principal_client_id")
        or app.get("service_principal_name")
        or ""
    )
    client_id = str(client_id).strip()
    if client_id:
        return {
            "client_id": client_id,
            "display_name": str(app.get("service_principal_display_name") or client_id),
        }

    nested = app.get("service_principal")
    if isinstance(nested, dict):
        nested_id = (
            nested.get("client_id")
            or nested.get("application_id")
            or nested.get("service_principal_client_id")
            or nested.get("service_principal_name")
            or nested.get("id")
            or nested.get("name")
            or ""
        )
        nested_id = str(nested_id).strip()
        if nested_id:
            display_name = (
                nested.get("display_name")
                or nested.get("displayName")
                or nested.get("service_principal_display_name")
                or nested_id
            )
            return {"client_id": nested_id, "display_name": str(display_name)}
    return None


def get_app_service_principal(
    w,
    app_name: str,
    *,
    timeout_seconds: int = 300,
    poll_seconds: int = 5,
) -> dict[str, str]:
    deadline = time.time() + timeout_seconds
    last_app: dict[str, Any] | None = None

    while True:
        app = get_app(w, app_name)
        if not app:
            raise RuntimeError(f"Databricks App '{app_name}' does not exist")
        last_app = app

        sp = _extract_service_principal(app)
        if sp:
            return sp

        if time.time() >= deadline:
            break

        # App creation is asynchronous; the service principal may lag the
        # create response by minutes in fresh workspaces.
        time.sleep(poll_seconds)

    known_keys = sorted(last_app.keys()) if last_app else []
    client_id = (
        (last_app or {}).get("service_principal_client_id")
        or (last_app or {}).get("service_principal_name")
        or ""
    )
    raise RuntimeError(
        f"Could not resolve service principal for app '{app_name}' after "
        f"{timeout_seconds}s. Last app keys={known_keys}, "
        f"service_principal_client_id={client_id!r}."
    )


def start_app_if_needed(w, app_name: str) -> None:
    app = get_app(w, app_name) or {}
    state = ((app.get("compute_status") or {}).get("state") or "").upper()
    if state == "ACTIVE":
        return
    try:
        api_do(w, "POST", f"/api/2.0/apps/{app_name}/start", {})
    except Exception:
        pass


def _resource_has_config(resource: dict[str, Any]) -> bool:
    return any(key for key in resource if key != "name")


def patch_app_resources(
    w,
    cfg: InstallConfig,
    lakebase: LakebaseInfo | None,
) -> dict[str, Any]:
    app = get_app(w, cfg.app_name) or {}
    by_name: dict[str, dict[str, Any]] = {}

    for resource in app.get("resources") or []:
        name = resource.get("name")
        if not name:
            continue
        if _resource_has_config(resource) or name in {"sql-warehouse", "postgres"}:
            by_name[name] = resource

    by_name["sql-warehouse"] = {
        "name": "sql-warehouse",
        "sql_warehouse": {
            "id": cfg.warehouse_id,
            "permission": "CAN_USE",
        },
    }

    if lakebase and lakebase.database_resource:
        by_name["postgres"] = {
            "name": "postgres",
            "postgres": {
                "branch": lakebase.branch_resource,
                "database": lakebase.database_resource,
                "permission": "CAN_CONNECT_AND_CREATE",
            },
        }

    payload = {
        "user_api_scopes": APP_SCOPES,
        "resources": list(by_name.values()),
    }
    api_do(w, "PATCH", f"/api/2.0/apps/{cfg.app_name}", payload)
    return payload


def deploy_app_from_workspace(
    w,
    app_name: str,
    source_path: str,
    *,
    timeout_seconds: int = 1200,
) -> dict[str, Any]:
    start_app_if_needed(w, app_name)
    from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

    deployment = w.apps.deploy_and_wait(
        app_name,
        AppDeployment(source_code_path=source_path, mode=AppDeploymentMode.SNAPSHOT),
        timeout=timedelta(seconds=timeout_seconds),
    )
    if hasattr(deployment, "as_dict"):
        return deployment.as_dict()
    if isinstance(deployment, dict):
        return deployment
    return {}


DEPLOYMENT_SUCCESS_STATES = {"SUCCEEDED", "SUCCESS"}
DEPLOYMENT_PENDING_STATES = {"", "UNKNOWN", "IN_PROGRESS", "PENDING", "QUEUED", "RUNNING"}


def deployment_token(deployment: dict[str, Any] | None) -> str:
    if not isinstance(deployment, dict):
        return ""
    direct = deployment.get("deployment_id") or deployment.get("id") or ""
    if direct:
        return str(direct)
    nested = deployment.get("deployment")
    if isinstance(nested, dict):
        return str(nested.get("deployment_id") or nested.get("id") or "")
    return ""


def deployment_state(deployment: dict[str, Any] | None) -> str:
    if not isinstance(deployment, dict):
        return "UNKNOWN"
    return str(((deployment.get("status") or {}).get("state") or "UNKNOWN")).upper()


def deployment_fingerprint(deployment: dict[str, Any] | None) -> str:
    if not isinstance(deployment, dict) or not deployment:
        return ""
    token = deployment_token(deployment)
    if token:
        return f"id:{token}"
    return json.dumps(deployment, sort_keys=True, default=str)


def deployment_changed(
    deployment: dict[str, Any] | None,
    *,
    baseline_token: str | None = None,
    baseline_fingerprint: str | None = None,
) -> bool:
    token = deployment_token(deployment)
    if token:
        return token != (baseline_token or "")
    fingerprint = deployment_fingerprint(deployment)
    return bool(fingerprint and fingerprint != (baseline_fingerprint or ""))


def _selected_app(app: dict[str, Any], deployment: dict[str, Any]) -> dict[str, Any]:
    selected = dict(app)
    selected["pending_deployment"] = deployment
    selected.pop("active_deployment", None)
    return selected


def app_deployment(app: dict[str, Any]) -> dict[str, Any]:
    return app.get("pending_deployment") or app.get("active_deployment") or {}


def app_deployment_state(app: dict[str, Any]) -> str:
    return deployment_state(app_deployment(app))


def require_successful_deployment(app_name: str, app: dict[str, Any]) -> dict[str, Any]:
    deployment = app_deployment(app)
    state = app_deployment_state(app)
    if state in DEPLOYMENT_SUCCESS_STATES:
        return deployment
    raise RuntimeError(f"Databricks App '{app_name}' deployment did not succeed (state={state}).")


def wait_for_deployment(
    w,
    app_name: str,
    *,
    submitted_deployment: dict[str, Any] | None = None,
    baseline_active_token: str | None = None,
    baseline_active_fingerprint: str | None = None,
    timeout_seconds: int = 180,
    poll_seconds: int = 10,
) -> dict[str, Any]:
    submitted_token = deployment_token(submitted_deployment)
    wait_for_submitted = submitted_deployment is not None
    deadline = time.time() + timeout_seconds
    last_app: dict[str, Any] = {}
    observed_pending = False
    baseline_captured = baseline_active_token is not None or baseline_active_fingerprint is not None
    while time.time() < deadline:
        last_app = get_app(w, app_name) or {}
        pending = last_app.get("pending_deployment") or {}
        active = last_app.get("active_deployment") or {}
        if wait_for_submitted and not baseline_captured:
            baseline_active_token = deployment_token(active)
            baseline_active_fingerprint = deployment_fingerprint(active)
            baseline_captured = True

        if submitted_token:
            submitted = get_app_deployment(w, app_name, submitted_token)
            if submitted and deployment_state(submitted) not in DEPLOYMENT_PENDING_STATES:
                return _selected_app(last_app, submitted)
            for deployment in (pending, active):
                if deployment_token(deployment) != submitted_token:
                    continue
                if deployment_state(deployment) not in DEPLOYMENT_PENDING_STATES:
                    return _selected_app(last_app, deployment)
                break
            if pending:
                observed_pending = True
                if deployment_state(pending) not in DEPLOYMENT_PENDING_STATES:
                    return _selected_app(last_app, pending)
            elif active:
                if (
                    deployment_changed(
                        active,
                        baseline_token=baseline_active_token,
                        baseline_fingerprint=baseline_active_fingerprint,
                    )
                    and deployment_state(active) not in DEPLOYMENT_PENDING_STATES
                ):
                    return _selected_app(last_app, active)
        elif wait_for_submitted:
            if pending:
                observed_pending = True
                if deployment_state(pending) not in DEPLOYMENT_PENDING_STATES:
                    return _selected_app(last_app, pending)
            elif observed_pending and active:
                if (
                    deployment_changed(
                        active,
                        baseline_token=baseline_active_token,
                        baseline_fingerprint=baseline_active_fingerprint,
                    )
                    and deployment_state(active) not in DEPLOYMENT_PENDING_STATES
                ):
                    return _selected_app(last_app, active)
        else:
            state = app_deployment_state(last_app)
            if state not in DEPLOYMENT_PENDING_STATES:
                return last_app

        time.sleep(poll_seconds)
    if wait_for_submitted:
        pending = last_app.get("pending_deployment") or {}
        if pending:
            return _selected_app(last_app, pending)
        return _selected_app(last_app, {})
    return last_app
