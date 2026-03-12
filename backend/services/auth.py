"""
Authentication utilities for Databricks Apps deployment.

On Databricks Apps, uses OBO (On Behalf Of) — each request creates a
WorkspaceClient with the user's forwarded token so all SDK calls (SQL,
UC, serving endpoints) execute under the user's identity and permissions.

Locally, falls back to PAT token or CLI profile (singleton client).
"""

import contextvars
import logging
import os
from contextvars import ContextVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

logger = logging.getLogger(__name__)

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
    client = WorkspaceClient(config=cfg)
    _obo_client.set(client)
    logger.debug("OBO client set for current request (host=%s, auth=%s)", host, cfg.auth_type)


def clear_obo_user_token() -> None:
    """Clear the per-request OBO client after the request completes."""
    _obo_client.set(None)


def _get_default_client() -> WorkspaceClient:
    """Get the default singleton client (SP on Apps, CLI/PAT locally)."""
    global _client, _auth_logged

    if _client is None:
        _client = WorkspaceClient()

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


def get_service_principal_client() -> WorkspaceClient:
    """Get the service principal client (bypasses OBO).

    Used for:
    - App-level operations (Lakebase persistence, background tasks)
    - Fallback when OBO token lacks required scopes (e.g., Genie API
      requires 'genie' scope which user authorization may not provide
      until the consent flow is triggered)
    """
    return _get_default_client()


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
