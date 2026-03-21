from __future__ import annotations
import os
from typing import Annotated, AsyncGenerator, TypeAlias
from contextlib import asynccontextmanager

from databricks.sdk import WorkspaceClient
from fastapi import Depends, FastAPI, Request

from ._base import LifespanDependency
from ._config import AppConfig, logger
from ._headers import HeadersDependency


class _ConfigDependency(LifespanDependency):
    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.config = AppConfig()
        logger.info(f"Starting app with configuration:\n{app.state.config}")
        yield

    @staticmethod
    def __call__(request: Request) -> AppConfig:
        return request.app.state.config


class _WorkspaceClientDependency(LifespanDependency):
    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.workspace_client = WorkspaceClient()
        yield

    @staticmethod
    def __call__(request: Request) -> WorkspaceClient:
        return request.app.state.workspace_client


def _get_user_ws(
    headers: HeadersDependency,
) -> WorkspaceClient:
    """
    Returns a Databricks Workspace client with authentication behalf of user.
    If the request contains an X-Forwarded-Access-Token header, on behalf of user authentication is used.

    Example usage: `user_ws: Dependencies.UserClient`
    """

    if not headers.token:
        logger.warning(
            "OBO token not available — falling back to service principal credentials. "
            "User-scoped permissions will not apply."
        )
        return WorkspaceClient()

    host = os.environ.get("DATABRICKS_HOST", "")
    return WorkspaceClient(
        host=host,
        token=headers.token.get_secret_value(),
        auth_type="pat",
        client_id=None,
        client_secret=None,
    )


ConfigDependency: TypeAlias = Annotated[AppConfig, _ConfigDependency.depends()]

ClientDependency: TypeAlias = Annotated[
    WorkspaceClient, _WorkspaceClientDependency.depends()
]

UserWorkspaceClientDependency: TypeAlias = Annotated[
    WorkspaceClient, Depends(_get_user_ws)
]
