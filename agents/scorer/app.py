"""genie-scorer — IQ scoring agent for Genie Spaces.

Wraps:
  - backend/services/scanner.py  (rule-based scoring engine)
  - backend/services/lakebase.py (score persistence, stars)
  - backend/routers/spaces.py    (list, detail endpoints)

This agent has NO LLM dependency — pure rule-based scoring.
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context
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
    ...


# ── Lifecycle ────────────────────────────────────────────────────────────────


async def on_startup():
    """Initialize Lakebase pool with scorer-specific DDL."""
    await init_pool(SCORER_DDL)


# ── Tools ────────────────────────────────────────────────────────────────────


@scorer.tool(
    description=(
        "Run an IQ scan on a Genie Space. Fetches the space configuration, "
        "calculates a score (0-15) across four dimensions, and persists the "
        "result to Lakebase."
    ),
)
async def scan_space(space_id: str, request: AgentRequest) -> dict:
    """Wraps backend/services/scanner.py::scan_space"""
    with obo_context(request.user_context.access_token):
        from backend.services.scanner import scan_space as _scan
        return await _scan(space_id)


@scorer.tool(
    description=(
        "Get score history for a Genie Space over the last N days. "
        "Returns a list of {score, maturity, scanned_at} entries."
    ),
)
async def get_history(space_id: str, days: int = 30) -> list[dict]:
    """Wraps backend/services/lakebase.py::get_score_history"""
    from backend.services.lakebase import get_score_history
    rows = await get_score_history(space_id, days=days)
    return [dict(r) for r in rows] if rows else []


@scorer.tool(
    description="Toggle the star (bookmark) status of a Genie Space.",
)
async def toggle_star(space_id: str, starred: bool) -> dict:
    """Wraps backend/services/lakebase.py::star_space"""
    from backend.services.lakebase import star_space
    await star_space(space_id, starred)
    return {"space_id": space_id, "starred": starred}


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
    request: AgentRequest = None,
) -> list[dict]:
    """Wraps backend/routers/spaces.py::list_spaces logic"""
    with obo_context(request.user_context.access_token):
        from backend.services.genie_client import list_genie_spaces
        from backend.services.lakebase import get_latest_score, get_starred_spaces
        from backend.services.auth import get_workspace_client

        raw_spaces = list_genie_spaces()
        client = get_workspace_client()
        host = (client.config.host or "").rstrip("/")
        starred_ids = set(await get_starred_spaces())

        items = []
        for space in raw_spaces:
            sid = space.get("space_id", "")
            title = space.get("display_name", space.get("title", ""))

            if search and search.lower() not in title.lower():
                continue
            if starred_only and sid not in starred_ids:
                continue

            score_data = await get_latest_score(sid)
            score = score_data.get("score") if score_data else None

            if min_score is not None and (score is None or score < min_score):
                continue
            if max_score is not None and (score is None or score > max_score):
                continue

            items.append({
                "space_id": sid,
                "title": title,
                "space_url": f"{host}/genie/rooms/{sid}" if host else None,
                "score": score,
                "maturity": score_data.get("maturity") if score_data else None,
                "starred": sid in starred_ids,
            })

        return items


@scorer.tool(
    description="Get detailed space metadata with latest scan result and star status.",
)
async def get_space_detail(space_id: str, request: AgentRequest) -> dict:
    """Wraps backend/routers/spaces.py::get_space_detail logic"""
    with obo_context(request.user_context.access_token):
        from backend.services.genie_client import get_genie_space
        from backend.services.lakebase import get_latest_score, is_space_starred

        space_info = get_genie_space(space_id)
        score_data = await get_latest_score(space_id)
        starred = await is_space_starred(space_id)

        return {
            "space_id": space_id,
            "title": space_info.get("display_name", ""),
            "score": score_data.get("score") if score_data else None,
            "maturity": score_data.get("maturity") if score_data else None,
            "starred": starred,
            "last_scanned": score_data.get("scanned_at") if score_data else None,
            "scan_result": score_data,
        }


# ── Standalone entry point ───────────────────────────────────────────────────

app = scorer.app
