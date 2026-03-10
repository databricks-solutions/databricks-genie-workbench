"""genie-scorer — IQ scoring agent for Genie Spaces.

Extracted from:
  - backend/routers/spaces.py  (scan, history, star, list endpoints)
  - backend/services/scanner.py (rule-based scoring engine)
  - backend/services/lakebase.py (score persistence)

This agent has NO LLM dependency — it's pure rule-based scoring.
Lowest-risk extraction target; validates the @app_agent pattern.
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent


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
async def scan_space(space_id: str) -> dict:
    """Source: backend/services/scanner.py::scan_space + backend/routers/spaces.py::trigger_scan"""
    # Domain logic (scanner.calculate_score) moves here as-is.
    # OBO auth: use request.user_context.access_token instead of ContextVar.
    # Lakebase persistence: use local lakebase.py copy.
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
    """Source: backend/routers/spaces.py::list_spaces"""
    raise NotImplementedError("Phase 2: move list_spaces logic here")


@scorer.tool(
    description="Get detailed space metadata with latest scan result and star status.",
)
async def get_space_detail(space_id: str) -> dict:
    """Source: backend/routers/spaces.py::get_space_detail"""
    raise NotImplementedError("Phase 2: move get_space_detail logic here")


# ── Standalone entry point ───────────────────────────────────────────────────
# For local development: uvicorn agents.scorer.app:app --port 8001

app = scorer.app
