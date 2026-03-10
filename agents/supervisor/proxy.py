"""Transparent proxy from supervisor to sub-agents.

Maps current /api/* paths to sub-agent URLs so the React SPA needs
zero changes. Handles JSON responses and SSE streaming.

The route table is ordered — more specific paths match before general
prefixes. Each entry maps a path prefix to an environment variable
containing the sub-agent's base URL (set via agents.yaml url_env_map).

Path → agent mapping derived from frontend/src/lib/api.ts (28 API calls).
"""

from __future__ import annotations

import os
import re
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse


# Ordered route table: (pattern, env_var_or_None)
# More specific patterns MUST come before general prefixes.
# None means the supervisor handles the route directly (no proxy).
ROUTE_TABLE: list[tuple[str, Optional[str]]] = [
    # Specific sub-paths that override their parent prefix
    ("/api/spaces/*/fix", "FIXER_URL"),      # fix agent (SSE)
    ("/api/genie/create", "CREATOR_URL"),     # create Genie Space via API

    # General prefixes
    ("/api/spaces",    "SCORER_URL"),          # list, scan, history, star
    ("/api/space",     "ANALYZER_URL"),        # fetch, parse
    ("/api/analyze",   "ANALYZER_URL"),        # section, all, stream (SSE)
    ("/api/genie",     "ANALYZER_URL"),        # query
    ("/api/sql",       "ANALYZER_URL"),        # execute
    ("/api/optimize",  "OPTIMIZER_URL"),       # stream optimization (SSE)
    ("/api/config",    "OPTIMIZER_URL"),       # merge
    ("/api/create",    "CREATOR_URL"),         # agent chat (SSE), discover, validate, create
    ("/api/checklist", "ANALYZER_URL"),        # static content
    ("/api/sections",  "ANALYZER_URL"),        # section list

    # Supervisor-owned (no proxy)
    ("/api/settings",  None),
    ("/api/auth",      None),
    ("/api/admin",     None),
]

# Pre-compile glob patterns (only "/api/spaces/*/fix" currently)
_COMPILED_ROUTES: list[tuple[re.Pattern, Optional[str]]] = []

for pattern, env_var in ROUTE_TABLE:
    if "*" in pattern:
        # Convert glob "*" to regex "[^/]+"
        regex = "^" + re.escape(pattern).replace(r"\*", "[^/]+")
        _COMPILED_ROUTES.append((re.compile(regex), env_var))
    else:
        # Simple prefix match
        _COMPILED_ROUTES.append((re.compile("^" + re.escape(pattern)), env_var))


def _resolve_upstream(path: str) -> Optional[str]:
    """Find the upstream agent URL for a given API path.

    Returns:
        Base URL string if the path should be proxied.
        None if the supervisor handles it directly.

    Raises:
        KeyError: If no route matches the path.
    """
    for compiled_pattern, env_var in _COMPILED_ROUTES:
        if compiled_pattern.match(path):
            if env_var is None:
                return None
            url = os.environ.get(env_var)
            if not url:
                return None
            return url.rstrip("/")

    raise KeyError(f"No route for {path}")


# Hop-by-hop headers that should not be forwarded
_HOP_HEADERS = frozenset({"host", "content-length", "transfer-encoding"})


def mount_proxy(app: FastAPI):
    """Mount the catch-all proxy route on a FastAPI app.

    This should be mounted AFTER any supervisor-owned routes
    (settings, auth, admin) so they take priority.
    """

    @app.api_route(
        "/api/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE"],
    )
    async def proxy(request: Request, path: str):
        full_path = f"/api/{path}"

        try:
            upstream_base = _resolve_upstream(full_path)
        except KeyError:
            return JSONResponse(
                status_code=404,
                content={"detail": f"No upstream agent for {full_path}"},
            )

        if upstream_base is None:
            # Supervisor-owned route that wasn't caught by an explicit handler.
            return JSONResponse(
                status_code=404,
                content={"detail": f"Not found: {full_path}"},
            )

        # Forward all headers except hop-by-hop
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _HOP_HEADERS
        }

        upstream_url = f"{upstream_base}{full_path}"
        if request.url.query:
            upstream_url += f"?{request.url.query}"

        body = await request.body()

        # First, make a non-streaming request to check the content type
        async with httpx.AsyncClient(timeout=300.0) as client:
            upstream_resp = await client.request(
                method=request.method,
                url=upstream_url,
                headers=headers,
                content=body,
                follow_redirects=True,
            )

        content_type = upstream_resp.headers.get("content-type", "")

        # SSE: re-issue as a streaming request and forward chunks
        if "text/event-stream" in content_type:

            async def stream():
                async with httpx.AsyncClient(timeout=300.0) as sc:
                    async with sc.stream(
                        method=request.method,
                        url=upstream_url,
                        headers=headers,
                        content=body,
                    ) as sr:
                        async for chunk in sr.aiter_bytes():
                            yield chunk

            return StreamingResponse(
                stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        # JSON: pass through with status code
        if content_type.startswith("application/json"):
            return JSONResponse(
                status_code=upstream_resp.status_code,
                content=upstream_resp.json(),
                headers={"X-Upstream-Agent": upstream_base},
            )

        # Other content types: pass through as-is
        return JSONResponse(
            status_code=upstream_resp.status_code,
            content={"raw": upstream_resp.text},
            headers={"X-Upstream-Agent": upstream_base},
        )
