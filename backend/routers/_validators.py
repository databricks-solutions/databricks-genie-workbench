"""Shared FastAPI path-parameter validators for routers."""

from __future__ import annotations

from typing import Annotated

from fastapi import Path

# run_id is always uuid4 — enforce strictly
RunId = Annotated[
    str,
    Path(
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    ),
]

# space_id is a Databricks Genie Space ID — alphanumeric + hyphens/underscores
SpaceId = Annotated[
    str,
    Path(pattern=r"^[0-9a-zA-Z_-]{1,128}$"),
]
