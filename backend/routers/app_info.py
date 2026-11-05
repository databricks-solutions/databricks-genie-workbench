"""App version and the daily release-check setting (spec 5.4 to 5.6).

The app server never calls the internet. The release check runs in workspace
admins' browsers; this router only reports the installed version and stores
the admins' answer.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.app_version import RELEASES_REPO, get_build_info, install_time_update_check, release_url
from backend.models import AppVersionResponse, UpdateCheckRequest, UpdateCheckState
from backend.services import lakebase
from backend.watch._auth import require_admin

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin")

UPDATE_CHECK_KEY = "update_check"
LOCKED_DETAIL = (
    "The release check was set at install time. Change GENIE_UPDATE_CHECK in "
    ".env.deploy or the update_check notebook widget and redeploy."
)
SAVE_FAILED_DETAIL = "Couldn't save the release-check setting. Try again."


async def resolve_update_check() -> UpdateCheckState:
    """Install-time value, then the Lakebase answer, else ask. Read errors fail closed."""
    install = install_time_update_check()
    if install is not None:
        return UpdateCheckState(
            mode="on" if install else "off", source="install", locked=True,
            persisted=lakebase.is_available(),
        )
    try:
        stored = await lakebase.get_app_setting(UPDATE_CHECK_KEY)
    except Exception as exc:  # noqa: BLE001 - fail closed and never echo DB text
        logger.warning("Could not read the release-check setting: %s", type(exc).__name__)
        return UpdateCheckState(mode="off", source="error", locked=False, persisted=lakebase.is_available())
    persisted = lakebase.is_available()
    if lakebase.is_configured() and not persisted:
        # Configured but the pool never came up: the stored answer is unreadable.
        logger.warning("Lakebase is configured but unavailable; release check fails closed")
        return UpdateCheckState(mode="off", source="error", locked=False, persisted=False)
    if stored in ("on", "off"):
        return UpdateCheckState(mode=stored, source="app", locked=False, persisted=persisted)
    return UpdateCheckState(mode="ask", source="none", locked=False, persisted=persisted)


def _caller_email(request: Request) -> str | None:
    return (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-Preferred-Username")
        or None
    )


@router.get("/version")
async def get_app_version() -> AppVersionResponse:
    info = get_build_info()
    return AppVersionResponse(
        version=info.version,
        commit=info.commit,
        deployed_at=info.deployed_at,
        install_method=info.install_method,
        releases_repo=RELEASES_REPO,
        release_url=release_url(info.version),
        update_check=await resolve_update_check(),
    )


@router.put("/update-check", dependencies=[Depends(require_admin)])
async def put_update_check(body: UpdateCheckRequest, request: Request) -> UpdateCheckState:
    if install_time_update_check() is not None:
        raise HTTPException(status_code=409, detail=LOCKED_DETAIL)
    value = "on" if body.enabled else "off"
    try:
        persisted = await lakebase.set_app_setting(UPDATE_CHECK_KEY, value, _caller_email(request))
    except Exception as exc:  # noqa: BLE001 - never echo DB text
        logger.warning("Could not save the release-check setting: %s", type(exc).__name__)
        raise HTTPException(status_code=503, detail=SAVE_FAILED_DETAIL)
    if not persisted and lakebase.is_configured():
        # Configured Lakebase is down: do not fall back to a per-browser answer.
        raise HTTPException(status_code=503, detail=SAVE_FAILED_DETAIL)
    return UpdateCheckState(mode=value, source="app", locked=False, persisted=persisted)
