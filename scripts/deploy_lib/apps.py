"""Databricks Apps API helpers for the notebook installer."""

from __future__ import annotations

import time
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
    "iam.access-control:read",
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
            "description": "Genie Workbench - Create, score, and optimize Genie Spaces",
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


def deploy_app_from_workspace(w, app_name: str, source_path: str) -> dict[str, Any]:
    start_app_if_needed(w, app_name)
    return api_do(
        w,
        "POST",
        f"/api/2.0/apps/{app_name}/deployments",
        {"source_code_path": source_path},
    )


def wait_for_deployment(
    w,
    app_name: str,
    *,
    timeout_seconds: int = 180,
    poll_seconds: int = 10,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_app: dict[str, Any] = {}
    while time.time() < deadline:
        last_app = get_app(w, app_name) or {}
        deployment = last_app.get("pending_deployment") or last_app.get("active_deployment") or {}
        state = ((deployment.get("status") or {}).get("state") or "UNKNOWN").upper()
        if state and state != "IN_PROGRESS":
            return last_app
        time.sleep(poll_seconds)
    return last_app
