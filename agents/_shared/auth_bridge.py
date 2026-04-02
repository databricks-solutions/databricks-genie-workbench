"""Bridge @app_agent UserContext into both monolith and AI Dev Kit auth systems.

During migration, agent tools receive `request.user_context` from @app_agent,
but domain logic (scanner, genie_client, etc.) calls `get_workspace_client()`
from the monolith's auth module. And `databricks-tools-core` functions use
their own separate ContextVars via `set_databricks_auth()`.

This module provides `obo_context()` — a single context manager that sets up
all three auth systems so existing domain logic works unchanged inside agents.

Source patterns:
    - backend/services/auth.py:25      (_obo_client ContextVar)
    - backend/services/auth.py:33-58   (set_obo_user_token)
    - databricks_tools_core/auth.py    (set_databricks_auth / clear_databricks_auth)
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config


# Monolith-compatible ContextVar (mirrors backend/services/auth.py:25)
_obo_client: ContextVar[Optional[WorkspaceClient]] = ContextVar(
    "_obo_client", default=None
)

# Singleton SP client (lazy-initialized)
_sp_client: Optional[WorkspaceClient] = None


@contextmanager
def obo_context(access_token: str, host: Optional[str] = None):
    """Set up OBO auth for monolith code and databricks-tools-core.

    Creates a per-request WorkspaceClient from the user's OBO token and
    stores it in both the monolith ContextVar and the AI Dev Kit ContextVars.

    Usage in any agent tool::

        @scorer.tool(description="Run IQ scan on a Genie Space")
        async def scan_space(space_id: str, request: AgentRequest) -> dict:
            with obo_context(request.user_context.access_token):
                # All of these now work:
                # - get_workspace_client() returns OBO client
                # - databricks-tools-core functions use OBO token
                result = scanner.calculate_score(space_id)

    For streaming generators, capture the token before yielding and
    re-enter obo_context() per-yield. This matches the pattern in
    backend/routers/create.py:125-198.

    Args:
        access_token: The user's OBO access token.
        host: Databricks workspace host. Defaults to DATABRICKS_HOST env var.

    Yields:
        WorkspaceClient configured with the user's OBO token.
    """
    resolved_host = host or os.environ.get("DATABRICKS_HOST", "")

    # 1. Create OBO WorkspaceClient (monolith pattern from auth.py:49-58)
    #    Must set auth_type="pat" and clear client_id/client_secret to prevent
    #    the SDK from using oauth-m2m from env vars on Databricks Apps.
    cfg = Config(
        host=resolved_host,
        token=access_token,
        auth_type="pat",
        client_id=None,
        client_secret=None,
    )
    client = WorkspaceClient(config=cfg)
    token = _obo_client.set(client)

    # 2. Set databricks-tools-core ContextVars (if available)
    has_tools_core = False
    try:
        from databricks_tools_core.auth import (
            set_databricks_auth,
            clear_databricks_auth,
        )

        set_databricks_auth(resolved_host, access_token)
        has_tools_core = True
    except ImportError:
        pass

    try:
        yield client
    finally:
        _obo_client.reset(token)
        if has_tools_core:
            clear_databricks_auth()


def get_workspace_client() -> WorkspaceClient:
    """Drop-in replacement for backend.services.auth.get_workspace_client().

    Returns the OBO client if inside an obo_context(), otherwise the default
    singleton (SP on Databricks Apps, CLI/PAT locally).

    Domain logic can import this instead of the monolith version during
    migration — the behavior is identical.
    """
    obo = _obo_client.get()
    if obo is not None:
        return obo
    return get_service_principal_client()


def get_service_principal_client() -> WorkspaceClient:
    """Get the service principal client (bypasses OBO).

    Used for app-level operations and as fallback when the user's OBO token
    lacks required scopes (e.g., Genie API before consent flow).
    """
    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client
