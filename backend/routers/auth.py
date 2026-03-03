"""Auth router - user identity and auth status endpoints."""

import logging
import os

from fastapi import APIRouter, Request

from backend.services.auth import get_workspace_client, is_running_on_databricks_apps

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/auth")


@router.get("/me")
async def get_current_user(request: Request) -> dict:
    """Return current user info from OBO headers or SDK.

    On Databricks Apps, reads X-Forwarded-User / X-Forwarded-Groups headers.
    Locally, fetches from the SDK's current_user.me().
    """
    # Databricks Apps injects user info via headers
    email = request.headers.get("X-Forwarded-User") or request.headers.get("X-Forwarded-Email")
    groups = request.headers.get("X-Forwarded-Groups", "")

    if email:
        is_admin = "admins" in groups.lower() or os.environ.get("DEV_ADMIN", "").lower() == "true"
        return {
            "email": email,
            "is_admin": is_admin,
            "groups": groups.split(",") if groups else [],
            "auth_source": "obo_headers",
        }

    # Local dev fallback
    dev_email = os.environ.get("DEV_USER_EMAIL")
    if dev_email:
        return {
            "email": dev_email,
            "is_admin": True,
            "groups": ["admins"],
            "auth_source": "dev_env",
        }

    # Try SDK
    try:
        client = get_workspace_client()
        user = client.current_user.me()
        return {
            "email": user.user_name or "",
            "display_name": user.display_name or "",
            "is_admin": False,
            "groups": [],
            "auth_source": "sdk",
        }
    except Exception as e:
        logger.warning(f"Could not get current user: {e}")
        return {
            "email": "unknown",
            "is_admin": False,
            "groups": [],
            "auth_source": "none",
        }


@router.get("/status")
async def auth_status() -> dict:
    """Health check with auth state."""
    try:
        client = get_workspace_client()
        return {
            "ok": True,
            "host": client.config.host,
            "auth_type": client.config.auth_type,
            "on_databricks_apps": is_running_on_databricks_apps(),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "on_databricks_apps": is_running_on_databricks_apps(),
        }
