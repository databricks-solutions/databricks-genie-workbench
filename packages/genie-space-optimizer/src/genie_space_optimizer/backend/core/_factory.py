from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, FastAPI
from starlette.responses import JSONResponse

from ..._metadata import api_prefix, app_name, dist_dir
from ..utils import scrub_nan_inf
from ._base import LifespanDependency
from ._config import logger


class SafeJSONResponse(JSONResponse):
    """JSONResponse that replaces NaN/Inf with null before serialization."""

    def render(self, content: Any) -> bytes:
        return json.dumps(
            scrub_nan_inf(content),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")

# --- Lifespan ---


@asynccontextmanager
async def _chain_dep_lifespans(
    deps: list[LifespanDependency],
    app: FastAPI,
) -> AsyncIterator[None]:
    """Chain multiple dependency lifespans into a single nested context manager."""
    if not deps:
        yield
        return

    head, *tail = deps

    async with head.lifespan(app):
        async with _chain_dep_lifespans(tail, app):
            yield


# --- Factory ---


def create_app(
    *,
    routers: list[APIRouter] | None = None,
) -> FastAPI:
    """Create and configure a FastAPI application.

    Dependencies are discovered automatically from the Dependency registry.
    All concrete Dependency subclasses that have been imported are instantiated
    and their lifespans are chained in import order.

    Args:
        routers: List of APIRouter instances to include in the app.

    Returns:
        Configured FastAPI application instance.
    """
    all_deps: list[LifespanDependency] = []
    for dep in LifespanDependency._registry:
        try:
            all_deps.append(dep())
        except Exception as e:
            logger.error(f"Failed to instantiate dependency {dep.__name__}: {e}")
            raise e

    @asynccontextmanager
    async def _composed_lifespan(app: FastAPI):
        async with _chain_dep_lifespans(all_deps, app):
            yield

    app = FastAPI(
        title=app_name,
        lifespan=_composed_lifespan,
        default_response_class=SafeJSONResponse,
    )

    api_router: APIRouter = create_router()
    for dep in all_deps:
        for r in dep.get_routers():
            api_router.include_router(r)
    app.include_router(api_router)

    for router in routers or []:
        if router is not api_router:
            app.include_router(router)

    if dist_dir.exists():
        from ._static import CachedStaticFiles, add_not_found_handler

        app.mount("/", CachedStaticFiles(directory=dist_dir, html=True))
        add_not_found_handler(app)

    return app


# singleton APIRouter with the application's API prefix
@lru_cache(maxsize=1)
def create_router() -> APIRouter:
    """Return the singleton APIRouter with the application's API prefix."""
    return APIRouter(prefix=api_prefix)
