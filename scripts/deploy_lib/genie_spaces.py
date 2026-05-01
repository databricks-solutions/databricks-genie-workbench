"""Optional Genie Space permission grants for the app service principal."""

from __future__ import annotations

from typing import Any

from .config import InstallConfig


def _api_do(w, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    return w.api_client.do(method=method, path=path, body=body)


def list_visible_genie_spaces(w) -> list[dict[str, Any]]:
    data = _api_do(w, "GET", "/api/2.0/genie/spaces")
    if isinstance(data, list):
        return data
    return data.get("spaces") or data.get("genie_spaces") or []


def grant_can_manage_on_space(w, space_id: str, app_sp_client_id: str) -> None:
    _api_do(
        w,
        "PATCH",
        f"/api/2.0/permissions/dashboards.genie/{space_id}",
        {
            "access_control_list": [
                {
                    "service_principal_name": app_sp_client_id,
                    "permission_level": "CAN_MANAGE",
                }
            ]
        },
    )


def optionally_grant_genie_spaces(w, cfg: InstallConfig, app_sp_client_id: str) -> int:
    if not cfg.grant_genie_spaces:
        return 0
    granted = 0
    for space in list_visible_genie_spaces(w):
        space_id = space.get("id") or space.get("space_id")
        if not space_id:
            continue
        try:
            grant_can_manage_on_space(w, str(space_id), app_sp_client_id)
            granted += 1
        except Exception:
            continue
    return granted
