"""genie-optimizer — optimization suggestions from benchmark feedback.

Wraps:
  - backend/services/optimizer.py (GenieSpaceOptimizer)
  - backend/routers/analysis.py   (optimize, merge, create endpoints)

Streaming: Yes (heartbeat SSE for long LLM calls)
LLM: Yes (suggestion generation)
"""

from __future__ import annotations

import asyncio

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context


@app_agent(
    name="genie-optimizer",
    description=(
        "Generates optimization suggestions for Genie Spaces based on "
        "benchmark labeling feedback. Merges suggestions into config and "
        "can create new optimized spaces."
    ),
)
async def optimizer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to optimization tools."""
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@optimizer.tool(
    description=(
        "Generate optimization suggestions based on benchmark labeling "
        "feedback. Uses LLM to analyze failure patterns and recommend "
        "config changes. May take 30-90 seconds."
    ),
)
async def generate_suggestions(
    space_data: dict,
    labeling_feedback: list[dict],
    request: AgentRequest = None,
) -> dict:
    """Wraps backend/services/optimizer.py::generate_optimizations"""
    with obo_context(request.user_context.access_token):
        from backend.services.auth import run_in_context
        from backend.services.optimizer import get_optimizer
        from backend.models import LabelingFeedbackItem

        feedback_items = [LabelingFeedbackItem(**f) for f in labeling_feedback]

        def _run():
            return get_optimizer().generate_optimizations(
                space_data=space_data,
                labeling_feedback=feedback_items,
            )

        result = await asyncio.get_running_loop().run_in_executor(
            None, run_in_context(_run),
        )
        return result.model_dump()


@optimizer.tool(
    description=(
        "Merge optimization suggestions into a space config. Fast operation "
        "that applies field-level changes without LLM calls."
    ),
)
async def merge_config(
    space_data: dict,
    suggestions: list[dict],
    request: AgentRequest = None,
) -> dict:
    """Wraps backend/services/optimizer.py::merge_config"""
    with obo_context(request.user_context.access_token):
        from backend.services.optimizer import get_optimizer
        from backend.models import OptimizationSuggestion

        suggestion_items = [OptimizationSuggestion(**s) for s in suggestions]
        result = get_optimizer().merge_config(
            space_data=space_data,
            suggestions=suggestion_items,
        )
        return result.model_dump()


@optimizer.tool(
    description=(
        "Create a new Genie Space with an optimized configuration. "
        "Requires GENIE_TARGET_DIRECTORY to be configured."
    ),
)
async def create_space(
    display_name: str,
    merged_config: dict,
    parent_path: str | None = None,
    request: AgentRequest = None,
) -> dict:
    """Wraps backend/genie_creator.py::create_genie_space"""
    with obo_context(request.user_context.access_token):
        from backend.genie_creator import create_genie_space as _create
        return _create(
            display_name=display_name,
            merged_config=merged_config,
            parent_path=parent_path,
        )


# ── Standalone entry point ───────────────────────────────────────────────────

app = optimizer.app
