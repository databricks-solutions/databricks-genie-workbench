from __future__ import annotations

from typing import Annotated, TypeAlias
from uuid import UUID

from fastapi import Depends, Header
from pydantic import BaseModel, SecretStr


class DatabricksAppsHeaders(BaseModel):
    """Structured model for Databricks Apps HTTP headers.

    See: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/http-headers
    """

    host: str | None
    user_name: str | None
    user_id: str | None
    user_email: str | None
    request_id: UUID | None
    token: SecretStr | None


def get_databricks_headers(
    host: Annotated[str | None, Header(alias="X-Forwarded-Host")] = None,
    user_name: Annotated[
        str | None, Header(alias="X-Forwarded-Preferred-Username")
    ] = None,
    user_id: Annotated[str | None, Header(alias="X-Forwarded-User")] = None,
    user_email: Annotated[str | None, Header(alias="X-Forwarded-Email")] = None,
    request_id: Annotated[str | None, Header(alias="X-Request-Id")] = None,
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> DatabricksAppsHeaders:
    """Extract Databricks Apps headers from the incoming request."""
    return DatabricksAppsHeaders(
        host=host,
        user_name=user_name,
        user_id=user_id,
        user_email=user_email,
        request_id=UUID(request_id) if request_id else None,
        token=SecretStr(token) if token else None,
    )


HeadersDependency: TypeAlias = Annotated[
    DatabricksAppsHeaders, Depends(get_databricks_headers)
]
