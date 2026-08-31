"""
Authentication utilities for Databricks Apps deployment.

On Databricks Apps, uses OBO (On Behalf Of) — each request creates a
WorkspaceClient with the user's forwarded token so all SDK calls (SQL,
UC, serving endpoints) execute under the user's identity and permissions.

Locally, falls back to PAT token or CLI profile (singleton client).
"""

import contextvars
import hashlib
import logging
import os
from contextvars import ContextVar
from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from backend._telemetry import PRODUCT_NAME, PRODUCT_VERSION

logger = logging.getLogger(__name__)

# Identity a foundation read runs under (MV-D50). "obo" (default) = the viewing
# admin; "sp" = the app service principal; "auto" = SP when the SP probe last
# succeeded (shared cross-user cache), else OBO.
ReadIdentity = Literal["obo", "sp", "auto"]

# Singleton client for local dev (or fallback when no user token is available)
_client: WorkspaceClient | None = None
_auth_logged = False

# Per-request OBO client stored in a context variable
_obo_client: ContextVar[WorkspaceClient | None] = ContextVar("_obo_client", default=None)


def is_running_on_databricks_apps() -> bool:
    """Check if running on Databricks Apps (vs local development)."""
    return os.environ.get("DATABRICKS_APP_PORT") is not None


def set_obo_user_token(token: str) -> None:
    """Set the user's OBO token for the current request context.

    Call this from middleware/dependencies with the user's Authorization
    header value. Creates a per-request WorkspaceClient that authenticates
    as the user.

    We must explicitly set ``auth_type="pat"`` because the Databricks Apps
    environment has DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET set,
    and the SDK would otherwise use oauth-m2m instead of the user's token.
    """
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        default = _get_default_client()
        host = default.config.host or ""

    cfg = Config(
        host=host,
        token=token,
        auth_type="pat",
        # Prevent the SDK from reading env vars that would override the token
        client_id=None,
        client_secret=None,  # gitleaks:allow
    )
    client = WorkspaceClient(config=cfg, product=PRODUCT_NAME, product_version=PRODUCT_VERSION)
    _obo_client.set(client)
    logger.debug("OBO client set for current request (host=%s, auth=%s)", host, cfg.auth_type)


def clear_obo_user_token() -> None:
    """Clear the per-request OBO client after the request completes."""
    _obo_client.set(None)


def _get_default_client() -> WorkspaceClient:
    """Get the default singleton client (SP on Apps, CLI/PAT locally)."""
    global _client, _auth_logged

    if _client is None:
        _client = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

        if not _auth_logged:
            logger.info("=== Databricks SDK Authentication ===")
            logger.info(f"  Host: {_client.config.host}")
            logger.info(f"  Auth type: {_client.config.auth_type}")
            logger.info(f"  Running on Databricks Apps: {is_running_on_databricks_apps()}")

            env_vars = [
                "DATABRICKS_HOST",
                "DATABRICKS_APP_PORT",
                "DATABRICKS_CLIENT_ID",
                "DATABRICKS_TOKEN",
            ]
            for var in env_vars:
                val = os.environ.get(var)
                if val:
                    if "TOKEN" in var or "SECRET" in var:
                        logger.info(f"  {var}: [SET]")
                    elif "CLIENT_ID" in var:
                        logger.info(f"  {var}: {val[:8]}...")
                    else:
                        logger.info(f"  {var}: {val}")

            _auth_logged = True

    return _client


def get_workspace_client() -> WorkspaceClient:
    """Get the WorkspaceClient for the current context.

    Returns the OBO (per-user) client if set, otherwise the default
    singleton. This ensures all SDK calls in the request path use the
    user's credentials when running on Databricks Apps.
    """
    obo = _obo_client.get()
    if obo is not None:
        return obo
    return _get_default_client()


def require_obo_workspace_client() -> WorkspaceClient:
    """Return only the request's user-authorized client.

    Unlike :func:`get_workspace_client`, this function never falls back to the
    app service principal. Use it for reads whose visibility is explicitly
    scoped to the current user's permissions.
    """
    obo = _obo_client.get()
    if obo is None:
        raise RuntimeError("This operation requires user authorization")
    return obo


def get_service_principal_client() -> WorkspaceClient:
    """Get the service principal client (bypasses OBO).

    Used for:
    - App-level operations (Lakebase persistence, background tasks)
    - Fallback when OBO token lacks required scopes (e.g., Genie API
      requires 'genie' scope which user authorization may not provide
      until the consent flow is triggered)
    """
    return _get_default_client()


def read_principal_id(client: WorkspaceClient, *, is_obo: bool) -> str:
    """A stable, cheap cache-partition key for a resolved read identity (MV-D50).

    Foundation-read caches key on this so an OBO (privilege-filtered) result is
    never served to another user, and an SP result never leaks into an OBO view.

    - OBO: a hash of the user's forwarded token — distinct users hash to distinct
      keys with no network round-trip; a rotated token simply misses the cache
      (never leaks another user's rows).
    - SP: the app client id (a fixed partition shared across all users, which is
      exactly what enables the SP shared cache).
    """
    if is_obo:
        token = (client.config.token or "").encode()
        return "obo:" + hashlib.sha256(token).hexdigest()[:16]
    return "sp:" + (client.config.client_id or "app")


def resolve_read_client(
    read_identity: ReadIdentity = "obo",
    *,
    sp_probe_ok: bool = False,
) -> tuple[WorkspaceClient, str]:
    """Resolve the WorkspaceClient for an ontology foundation read (MV-D50).

    A switch over ``read_identity`` — NOT a new auth path. Returns
    ``(client, principal_id)`` where ``principal_id`` partitions the per-identity
    cache (:func:`read_principal_id`).

    - ``"obo"`` (default): the viewing admin via :func:`require_obo_workspace_client`.
      No OBO context raises ``RuntimeError`` — the caller degrades the tier; it is
      **never** silently widened to the SP.
    - ``"sp"``: the app service principal (requires the banner's optional grants).
    - ``"auto"``: the SP when ``sp_probe_ok`` (shared cross-user cache), else the
      OBO viewer.
    """
    if read_identity == "sp" or (read_identity == "auto" and sp_probe_ok):
        client = get_service_principal_client()
        return client, read_principal_id(client, is_obo=False)
    # "obo" (default) or "auto" without a working SP probe → the viewer.
    # require_obo_workspace_client raises if there is no OBO context: the read
    # degrades, and the SP is never touched (no silent fallback).
    client = require_obo_workspace_client()
    return client, read_principal_id(client, is_obo=True)


def get_databricks_host() -> str:
    """Get the Databricks workspace host URL (without trailing slash)."""
    client = _get_default_client()
    host = client.config.host
    return host.rstrip("/") if host else ""


def get_llm_api_key() -> str:
    """Get the API key for LLM serving endpoints."""
    client = get_workspace_client()
    return client.config.token or os.environ.get("DATABRICKS_TOKEN", "")


def run_in_context(fn, *args, **kwargs):
    """Capture current contextvars and return a zero-arg callable that
    runs fn(*args, **kwargs) in that snapshot.

    Python <3.12 does not propagate contextvars into thread-pool threads.
    Use with loop.run_in_executor or ThreadPoolExecutor.submit:

        await loop.run_in_executor(None, run_in_context(handle_tool_call, n, a, cfg))
    """
    ctx = contextvars.copy_context()
    return lambda: ctx.run(fn, *args, **kwargs)
