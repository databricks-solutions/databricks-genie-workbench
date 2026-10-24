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

from backend.services.auth import bounded_read_client, require_obo_workspace_client

__all__ = [
    "SpaceAccessLevel",
    "clear_cache",
    "ensure_space_access",
    "require_space_access",
    "resolve_space_access_level",
    "space_access_held",
]

logger = logging.getLogger(__name__)

# Revocation takes effect within this window; denials are never cached.
_ALLOW_TTL_S = 30.0
_MAX_ENTRIES = 4096

# A stuck Genie read must not pin a worker thread for the SDK's 300 s retry window.
_CHECK_HTTP_TIMEOUT_S = 20

# Genie's 403 text for the two refusals that are not about the space's grants (M0 check V3).
_SCOPE_MARKERS = ("required scope", "insufficient_scope")
_ENTITLEMENT_MARKER = "entitlement"

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


def ensure_space_access(space_id: str, level: SpaceAccessLevel) -> None:
    """Refuse unless the signed-in user holds *level* on the Genie Agent.

    Blocking. Sync handlers call it directly (Starlette's threadpool carries the OBO
    ContextVar); async handlers use :func:`require_space_access`.
    """
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
        result = check_space_access(
            bounded_read_client(client, http_timeout_seconds=_CHECK_HTTP_TIMEOUT_S),
            space_id, level,
        )
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
    raise _denied(level, result.message)


async def require_space_access(space_id: str, level: SpaceAccessLevel) -> None:
    """Refuse unless the signed-in user holds *level* on the Genie Agent."""
    await asyncio.to_thread(ensure_space_access, space_id, level)


def space_access_held(space_id: str, level: SpaceAccessLevel) -> bool:
    """Whether the signed-in user holds *level*; any refusal, answered or not, reads as False.

    For deciding what to show, never as a gate: it cannot tell a denial from an outage.
    """
    try:
        ensure_space_access(space_id, level)
    except HTTPException:
        return False
    return True


# EDIT refusals that still let the ladder ask VIEW (never upgrade — only downgrade).
_EDIT_LOWER_CODES = frozenset({
    "space_access_denied",
    "space_access_scope_missing",
    "space_access_entitlement_missing",
})


def resolve_space_access_level(space_id: str) -> SpaceAccessLevel | None:
    """The highest level Genie grants the signed-in user, or None below Can View.

    Blocking. On the EDIT rung, a plain denial / missing OAuth scope / missing
    entitlement lowers the answer to VIEW (or None); every other refusal is
    raised. On VIEW, only a plain denial yields None; every other refusal is
    raised unchanged. MANAGE is never asked: the app's user token cannot prove
    it (MV-D115, MV-D118), and nothing gates on it.
    """
    if _holds(space_id, SpaceAccessLevel.EDIT, lower_codes=_EDIT_LOWER_CODES):
        return SpaceAccessLevel.EDIT
    return (
        SpaceAccessLevel.VIEW
        if _holds(space_id, SpaceAccessLevel.VIEW, lower_codes=frozenset({"space_access_denied"}))
        else None
    )


def _holds(
    space_id: str,
    level: SpaceAccessLevel,
    *,
    lower_codes: frozenset[str],
) -> bool:
    """Return False when the refusal should lower the ladder answer; re-raise otherwise."""
    try:
        ensure_space_access(space_id, level)
    except HTTPException as refusal:
        code = refusal.detail.get("code") if isinstance(refusal.detail, dict) else None
        if code in lower_codes:
            return False
        raise
    return True


def _denied(level: SpaceAccessLevel, platform_message: str) -> HTTPException:
    lowered = (platform_message or "").lower()
    if any(marker in lowered for marker in _SCOPE_MARKERS):
        return _refuse(403, "space_access_scope_missing", level,
                       "This app's sign-in is missing the Genie permission scope. "
                       "Ask a workspace admin to check the app's user authorization.",
                       platform_message)
    if _ENTITLEMENT_MARKER in lowered:
        return _refuse(403, "space_access_entitlement_missing", level,
                       "Your account is missing a workspace entitlement Genie needs, such as "
                       "Databricks SQL access. Ask a workspace admin to grant it.",
                       platform_message)
    return _refuse(403, "space_access_denied", level,
                   f"You need {_LABEL[level]} permission on this Genie Agent.", platform_message)
