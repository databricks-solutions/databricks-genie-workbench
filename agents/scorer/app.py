"""genie-scorer — IQ scoring agent for Genie Spaces.

Extracted from:
  - backend/routers/spaces.py  (scan, history, star, list endpoints)
  - backend/services/scanner.py (rule-based scoring engine)
  - backend/services/lakebase.py (score persistence)

This agent has NO LLM dependency — it's pure rule-based scoring.
Lowest-risk extraction target; validates the @app_agent pattern.

Integration patterns used:
  - Challenge 1 (OBO auth): obo_context() bridges @app_agent → monolith auth
  - Challenge 4 (SP fallback): genie_api_call() retries with SP on scope errors
  - Challenge 5 (Lakebase): init_pool(SCORER_DDL) for idempotent schema setup
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context
from agents._shared.sp_fallback import genie_api_call
from agents._shared.lakebase_client import init_pool, SCORER_DDL


@app_agent(
    name="genie-scorer",
    description=(
        "IQ scoring for Genie Spaces. Scans space configurations against a "
        "rule-based scoring rubric (foundation, data setup, SQL assets, "
        "optimization), persists scores to Lakebase, and tracks score history."
    ),
)
async def scorer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to the appropriate scoring tool."""
    # TODO: Parse intent from request.messages and dispatch to tools
    ...


# ── Lifecycle ────────────────────────────────────────────────────────────────


async def on_startup():
    """Initialize Lakebase pool with scorer-specific DDL."""
    await init_pool(SCORER_DDL)


# ── Tools ────────────────────────────────────────────────────────────────────
# Each tool maps to a current REST endpoint in backend/routers/spaces.py.
# Domain logic lives in scanner.py (moved as-is from backend/services/).


@scorer.tool(
    description=(
        "Run an IQ scan on a Genie Space. Fetches the space configuration, "
        "calculates a score (0-100) across four dimensions (foundation, data "
        "setup, SQL assets, optimization), and persists the result to Lakebase."
    ),
)
async def scan_space(space_id: str, request: AgentRequest) -> dict:
    """Source: backend/services/scanner.py::scan_space + backend/routers/spaces.py::trigger_scan

    Integration pattern:
        obo_context() sets up both monolith ContextVar and tools-core auth.
        genie_api_call() auto-retries with SP on scope errors.
        Domain logic (scanner.calculate_score) works unchanged.
    """
    with obo_context(request.user_context.access_token):
        # Fetch space config (with automatic SP fallback for scope errors)
        space_data = genie_api_call(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}",
            query={"include_serialized_space": "true"},
        )
        # TODO Phase 2: scanner.calculate_score(space_data)
        # TODO Phase 2: save_scan_result(space_id, score)
        raise NotImplementedError("Phase 2: move scanner.py + lakebase.py here")


@scorer.tool(
    description=(
        "Get score history for a Genie Space over the last N days. "
        "Returns a list of {score, maturity, scanned_at} entries."
    ),
)
async def get_history(space_id: str, days: int = 30) -> list[dict]:
    """Source: backend/services/lakebase.py::get_score_history"""
    raise NotImplementedError("Phase 2: move lakebase.get_score_history here")


@scorer.tool(
    description="Toggle the star (bookmark) status of a Genie Space.",
)
async def toggle_star(space_id: str, starred: bool) -> dict:
    """Source: backend/services/lakebase.py::star_space"""
    raise NotImplementedError("Phase 2: move lakebase.star_space here")


@scorer.tool(
    description=(
        "List all Genie Spaces the user has access to, enriched with IQ "
        "scores. Supports filtering by name, star status, and score range."
    ),
)
async def list_spaces(
    search: str | None = None,
    starred_only: bool = False,
    min_score: int | None = None,
    max_score: int | None = None,
) -> list[dict]:
    """Source: backend/routers/spaces.py::list_spaces

    Note (PR #6-#8): API response uses `space_id`/`title` fields (not `id`/`display_name`).
    Returns `space_url` per item (host + /genie/rooms/{space_id}).
    Uses SP fallback via get_service_principal_client() when OBO token lacks genie scope.
    """
    raise NotImplementedError("Phase 2: move list_spaces logic here")


@scorer.tool(
    description="Get detailed space metadata with latest scan result and star status.",
)
async def get_space_detail(space_id: str) -> dict:
    """Source: backend/routers/spaces.py::get_space_detail

    Note (PR #7): Includes SP fallback (_is_scope_error check) for Genie API calls.
    """
    raise NotImplementedError("Phase 2: move get_space_detail logic here")


# ── Standalone entry point ───────────────────────────────────────────────────
# For local development: uvicorn agents.scorer.app:app --port 8001

app = scorer.app
