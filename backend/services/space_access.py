"""One access control plane for Genie Agents (MV-D109).

Every route that reads or changes a space's state calls
:func:`require_space_access` before any other work. Genie answers under the
caller's own token; the service principal never answers for a user.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import time

from fastapi import HTTPException
from genie_space_optimizer.common.genie_client import (
    SpaceAccessLevel,
    SpaceAccessUnavailable,
    check_space_access,
)

from backend.services.auth import require_obo_workspace_client

__all__ = ["SpaceAccessLevel", "clear_cache", "require_space_access"]

logger = logging.getLogger(__name__)

# Revocation takes effect within this window; denials are never cached.
_ALLOW_TTL_S = 30.0
_MAX_ENTRIES = 4096

_RANK = {SpaceAccessLevel.VIEW: 1, SpaceAccessLevel.EDIT: 2, SpaceAccessLevel.MANAGE: 3}
_LABEL = {
    SpaceAccessLevel.VIEW: "Can View",
    SpaceAccessLevel.EDIT: "Can Edit",
    SpaceAccessLevel.MANAGE: "Can Manage",
}

_allowed: dict[tuple[str, str], tuple[int, float]] = {}
_lock = threading.Lock()


def clear_cache() -> None:
    with _lock:
        _allowed.clear()


def _now() -> float:
    # Tests patch this, not time.monotonic, which the event loop also reads.
    return time.monotonic()


def _token_key(client) -> str | None:
    token = getattr(getattr(client, "config", None), "token", None)
    if not token:
        return None
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _cached_allows(key: tuple[str, str], level: SpaceAccessLevel) -> bool:
    now = _now()
    with _lock:
        entry = _allowed.get(key)
        if entry is None:
            return False
        rank, expires_at = entry
        if expires_at <= now:
            del _allowed[key]
            return False
        return rank >= _RANK[level]


def _remember(key: tuple[str, str], level: SpaceAccessLevel) -> None:
    now = _now()
    with _lock:
        if len(_allowed) >= _MAX_ENTRIES:
            for stale in [k for k, (_, expires_at) in _allowed.items() if expires_at <= now]:
                del _allowed[stale]
            if len(_allowed) >= _MAX_ENTRIES:
                _allowed.clear()
        _allowed[key] = (_RANK[level], now + _ALLOW_TTL_S)


def _refuse(status_code: int, code: str, level: SpaceAccessLevel, message: str,
            platform_message: str = "") -> HTTPException:
    return HTTPException(status_code=status_code, detail={
        "code": code,
        "required": level.value,
        "message": message,
        "platform_message": platform_message,
    })


async def require_space_access(space_id: str, level: SpaceAccessLevel) -> None:
    """Refuse unless the signed-in user holds *level* on the Genie Agent."""
    level = SpaceAccessLevel(level)
    try:
        client = require_obo_workspace_client()
    except RuntimeError as exc:
        raise _refuse(401, "authentication_required", level, str(exc)) from exc

    token_key = _token_key(client)
    key = (token_key, space_id) if token_key else None
    if key is not None and _cached_allows(key, level):
        return

    try:
        result = await asyncio.to_thread(check_space_access, client, space_id, level)
    except SpaceAccessUnavailable as exc:
        logger.warning("Space access check unavailable for %s (%s): %s", space_id, level.value, exc)
        raise _refuse(
            503, "space_access_unavailable", level,
            "Could not verify your access to this Genie Agent. Try again shortly.",
        ) from exc

    if result.allowed:
        if key is not None:
            _remember(key, level)
        return
    if result.status == 404:
        raise _refuse(404, "space_not_found", level,
                      "Genie Agent not found, or you cannot see it.", result.message)
    if result.status == 401:
        raise _refuse(401, "authentication_required", level,
                      "Your session could not be verified. Sign in again.", result.message)
    raise _refuse(403, "space_access_denied", level,
                  f"You need {_LABEL[level]} permission on this Genie Agent.", result.message)
