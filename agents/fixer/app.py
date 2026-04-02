"""genie-fixer — AI fix agent for Genie Space configurations.

Wraps:
  - backend/services/fix_agent.py (FixAgent — LLM patch generation + application)

Streaming: Yes (SSE — thinking, patch, applying, complete/error events)
LLM: Yes (fix plan generation)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context


@app_agent(
    name="genie-fixer",
    description=(
        "AI fix agent that generates and applies targeted patches to Genie "
        "Space configurations based on IQ scan findings."
    ),
)
async def fixer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to fix tools."""
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@fixer.tool(
    description=(
        "Generate and apply fixes to a Genie Space based on IQ scan findings. "
        "Returns a stream of progress events: thinking, patch details, "
        "application status, and final result with before/after diff."
    ),
)
async def generate_fixes(
    space_id: str,
    findings: list[str],
    space_config: dict,
    request: AgentRequest = None,
) -> list[dict]:
    """Wraps backend/services/fix_agent.py::FixAgent.run

    Collects the streaming events into a list for agent protocol compatibility.
    For SSE streaming via the supervisor proxy, use the monolith endpoint.
    """
    with obo_context(request.user_context.access_token):
        from backend.services.fix_agent import get_fix_agent

        agent = get_fix_agent()
        events = []
        async for event in agent.run(
            space_id=space_id,
            findings=findings,
            space_config=space_config,
        ):
            events.append(event)

        return events


@fixer.tool(
    description=(
        "Apply a specific config patch to a Genie Space via the Databricks API. "
        "Takes a full updated config and writes it to the space."
    ),
)
async def apply_patch(
    space_id: str,
    updated_config: dict,
    request: AgentRequest = None,
) -> dict:
    """Wraps backend/services/fix_agent.py::_apply_config_to_databricks"""
    with obo_context(request.user_context.access_token):
        from backend.services.fix_agent import _apply_config_to_databricks
        await _apply_config_to_databricks(space_id, updated_config)
        return {"space_id": space_id, "status": "applied"}


# ── Standalone entry point ───────────────────────────────────────────────────

app = fixer.app
