"""Verification helpers for notebook installs."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote


APP_SOURCE_REQUIRED = [
    "backend/main.py",
    "backend/__init__.py",
    "pyproject.toml",
    "uv.lock",
    "package.json",
    "frontend/package.json",
    "app.yaml",
]


def _api_do(w, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    return w.api_client.do(method=method, path=path, body=body)


def workspace_path_exists(w, path: str) -> bool:
    try:
        _api_do(w, "GET", f"/api/2.0/workspace/get-status?path={quote(path, safe='')}")
        return True
    except Exception:
        return False


def verify_source_files(w, source_path: str) -> dict[str, Any]:
    missing = [rel for rel in APP_SOURCE_REQUIRED if not workspace_path_exists(w, f"{source_path}/{rel}")]
    return {
        "source_path": source_path,
        "required_files": APP_SOURCE_REQUIRED,
        "missing_files": missing,
        "ok": not missing,
    }


def verify_app_deployment(w, app_name: str, source_path: str) -> dict[str, Any]:
    app = _api_do(w, "GET", f"/api/2.0/apps/{app_name}")
    deployment = app.get("pending_deployment") or app.get("active_deployment") or {}
    return {
        "source": verify_source_files(w, source_path),
        "deployment_state": ((deployment.get("status") or {}).get("state") or "UNKNOWN"),
        "app_status": ((app.get("app_status") or {}).get("state") or "UNKNOWN"),
        "app_url": app.get("url"),
    }
