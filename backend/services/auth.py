"""
Authentication utilities for Databricks Apps deployment.

Uses service principal authentication when running on Databricks Apps,
and falls back to PAT token or CLI authentication for local development.
"""

import logging
import os

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Singleton client — avoids re-reading ~/.databrickscfg on every call
_client: WorkspaceClient | None = None
_auth_logged = False


def is_running_on_databricks_apps() -> bool:
    """Check if running on Databricks Apps (vs local development)."""
    return os.environ.get("DATABRICKS_APP_PORT") is not None


def get_workspace_client() -> WorkspaceClient:
    """Get a cached Databricks WorkspaceClient with appropriate authentication.

    The client is created once and reused for the lifetime of the process.
    On Databricks Apps it uses the service principal; locally it uses
    PAT token or CLI profile.
    """
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


def get_databricks_host() -> str:
    """Get the Databricks workspace host URL (without trailing slash)."""
    client = get_workspace_client()
    host = client.config.host
    return host.rstrip("/") if host else ""


def get_llm_api_key() -> str:
    """Get the API key for LLM serving endpoints."""
    if is_running_on_databricks_apps():
        return get_workspace_client().config.token or ""
    return os.environ.get("DATABRICKS_TOKEN", "")
