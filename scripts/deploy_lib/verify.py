"""Verification helpers for notebook installs."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .workspace_source import workspace_api_path


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


def verify_ai_gateway(w, llm_model: str) -> None:
    """Fail the install early when the gateway route can't serve the default model.

    Runs as the installing user: the app SP has no credentials the installer can
    use, and a gateway-wide failure (region, preview, model name) shows up for
    either identity.
    """
    from backend.services.llm_route import gateway_model_name

    model = gateway_model_name(llm_model)
    try:
        _api_do(w, "POST", "/ai-gateway/mlflow/v1/chat/completions", {
            "model": model,
            "max_tokens": 8,
            "messages": [{"role": "user", "content": "ping"}],
        })
    except Exception as exc:
        raise RuntimeError(
            f"Unity AI Gateway check failed for model '{model}': {exc}. "
            "The Workbench sends LLM calls through AI Gateway by default. Pick a "
            "model available on the gateway, or reinstall with llm_route=classic "
            "(GENIE_LLM_ROUTE=classic for deploy.sh) to use serving endpoints."
        ) from exc


def workspace_path_exists(w, path: str) -> bool:
    try:
        _api_do(
            w,
            "GET",
            f"/api/2.0/workspace/get-status?path={quote(workspace_api_path(path), safe='')}",
        )
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
