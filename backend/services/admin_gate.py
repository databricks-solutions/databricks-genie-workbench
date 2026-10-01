"""Workspace-admin gate shared by GenieWatch and Ontology (MV-D109 P1).

Moved from ``backend/watch/_auth.py`` (which re-exports it) so every admin-only
surface uses one predicate. The signal is the same one ``/api/auth/me`` uses to
drive the frontend ``isAdmin``:

1. off Apps only, ``X-Forwarded-Groups`` lists ``admins``; or a local dev mode
   applies (:func:`is_admin_request`, pure over headers + env);
2. otherwise the caller's OBO identity (``current_user.me()``) is in the
   ``admins`` group. Databricks Apps forwards ``X-Forwarded-User`` but NOT
   ``X-Forwarded-Groups``, so on a deployed app step 2 is the one that admits a
   real admin (commit e11655ad).

Step 2 uses the request's OBO client only — never the service principal — and
fails closed: no OBO context, or an SDK error, is "not admin". Results are
cached per access token for a short TTL so a page load's burst of ontology
calls costs one ``current_user.me()``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time

from fastapi import HTTPException, Request

from backend.services.auth import (
    is_running_on_databricks_apps,
    require_obo_workspace_client,
)

logger = logging.getLogger(__name__)

_CACHE_TTL_S = 300.0
_CACHE_MAX = 1024
_cache: dict[str, tuple[float, bool]] = {}
_cache_lock = threading.Lock()


def is_admin_request(request: Request) -> bool:
    """Whether the request headers alone prove workspace admin.

    Off Databricks Apps: ``X-Forwarded-Groups`` lists ``admins`` (exact group
    name). On Apps the header is ignored: the Apps proxy never sets it, so any
    value it carries was supplied by the caller. Falls back to the local dev
    modes (``DEV_ADMIN=true``, or ``DEV_USER_EMAIL`` set with no OBO headers)
    so non-Apps deployments behave the same as ``/api/auth/me``.
    """
    if not is_running_on_databricks_apps():
        header = request.headers.get("X-Forwarded-Groups") or ""
        if groups_include_admins([g.strip() for g in header.split(",")]):
            return True
    if os.environ.get("DEV_ADMIN", "").lower() == "true":
        return True
    has_obo_user = bool(
        request.headers.get("X-Forwarded-User")
        or request.headers.get("X-Forwarded-Email")
    )
    return not has_obo_user and bool(os.environ.get("DEV_USER_EMAIL"))


def obo_groups() -> list[str]:
    """The OBO caller's group display names (raises without an OBO context)."""
    me = require_obo_workspace_client().current_user.me()
    return [g.display for g in (me.groups or []) if g.display]


def groups_include_admins(groups: list[str]) -> bool:
    return any(g.lower() == "admins" for g in groups)


def _token_key(request: Request) -> str | None:
    token = request.headers.get("x-forwarded-access-token") or ""
    if not token:
        return None
    return hashlib.sha256(token.encode()).hexdigest()


def resolve_is_admin(request: Request) -> bool:
    """Headers first, then the OBO identity's groups (cached per token)."""
    if is_admin_request(request):
        return True
    key = _token_key(request)
    now = time.monotonic()
    if key is not None:
        with _cache_lock:
            hit = _cache.get(key)
        if hit is not None and hit[0] > now:
            return hit[1]
    try:
        admin = groups_include_admins(obo_groups())
    except Exception as e:  # no OBO context / SDK failure → fail closed, uncached
        logger.warning("Admin resolution via OBO SDK failed: %s", e)
        return False
    if key is not None:
        with _cache_lock:
            if len(_cache) >= _CACHE_MAX:
                _cache.clear()
            _cache[key] = (now + _CACHE_TTL_S, admin)
    return admin


def require_admin(request: Request) -> None:
    """FastAPI dependency: 403 unless the caller is a workspace admin."""
    if not resolve_is_admin(request):
        raise HTTPException(
            status_code=403,
            detail="This operation requires workspace admin access.",
        )
