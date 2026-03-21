from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.staticfiles import NotModifiedResponse, StaticFiles
from starlette.types import Scope

from ..._metadata import api_prefix, dist_dir
from ._config import logger


class CachedStaticFiles(StaticFiles):
    """StaticFiles with proper Cache-Control headers for SPA deployments.

    - Hashed assets (Vite `assets/` dir): cached immutably (hash changes on every build).
    - Everything else (index.html, etc.): `no-cache` — always revalidate via ETag/304.
    """

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        request_headers = Headers(scope=scope)
        response = FileResponse(
            full_path, status_code=status_code, stat_result=stat_result
        )

        if "/assets/" in str(full_path):
            response.headers["cache-control"] = "public, max-age=31536000, immutable"
        else:
            response.headers["cache-control"] = "no-cache"

        if self.is_not_modified(response.headers, request_headers):
            return NotModifiedResponse(response.headers)
        return response


def add_not_found_handler(app: FastAPI) -> None:
    """Register a handler that serves the SPA index.html for non-API 404s."""

    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        logger.debug(
            f"HTTP exception handler called for request {request.url.path} with status code {exc.status_code}"
        )
        if exc.status_code == 404:
            path = request.url.path
            accept = request.headers.get("accept", "")

            is_api = path.startswith(api_prefix)
            is_get_page_nav = request.method == "GET" and "text/html" in accept

            # Heuristic: if the last path segment looks like a file (has a dot), don't SPA-fallback
            looks_like_asset = "." in path.split("/")[-1]

            if (not is_api) and is_get_page_nav and (not looks_like_asset):
                # Let the SPA router handle it
                return FileResponse(dist_dir / "index.html")
        # Default: return the original HTTP error (JSON 404 for API, etc.)
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    app.exception_handler(StarletteHTTPException)(http_exception_handler)
