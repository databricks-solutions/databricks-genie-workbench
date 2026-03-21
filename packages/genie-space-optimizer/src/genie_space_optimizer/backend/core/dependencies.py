from __future__ import annotations

from typing import TypeAlias
from ._defaults import ConfigDependency, ClientDependency, UserWorkspaceClientDependency
from ._headers import HeadersDependency
from .lakebase import LakebaseDependency
from .sql import SqlDependency


class Dependencies:
    """FastAPI dependency injection shorthand for route handler parameters."""

    Client: TypeAlias = ClientDependency
    """Databricks WorkspaceClient using app-level service principal credentials.
    Recommended usage: `ws: Dependencies.Client`"""

    UserClient: TypeAlias = UserWorkspaceClientDependency
    """WorkspaceClient authenticated on behalf of the current user via OBO token.
    Requires the X-Forwarded-Access-Token header.
    Recommended usage: `user_ws: Dependencies.UserClient`"""

    Config: TypeAlias = ConfigDependency
    """Application configuration loaded from environment variables.
    Recommended usage: `config: Dependencies.Config`"""

    Headers: TypeAlias = HeadersDependency
    """Databricks Apps HTTP headers for the current request.
    Recommended usage: `headers: Dependencies.Headers`"""
    Session: TypeAlias = LakebaseDependency
    """Lakebase session dependency.
    Recommended usage: `session: Dependencies.Session`"""
    Sql: TypeAlias = SqlDependency
    """SQL Warehouse query dependency.
    Recommended usage: `sql: Dependencies.Sql`"""


