"""Dynamic prompt assembly for the Create Genie agent.

Instead of a single monolithic system prompt (~500 lines), this package
assembles a scoped prompt per turn based on the agent's current workflow step.
Each turn includes:
  - Core identity + principles (~20 lines, always)
  - Full instructions for the CURRENT step (~40-80 lines)
  - Brief summaries of adjacent steps (previous + next, ~5 lines each)
  - Backtracking rules (~15 lines, always)
  - Tool/UI/autopilot rules (~60 lines, always)
  - Schema reference (always)

This reduces the per-turn prompt from ~500 to ~200-250 lines, improving
LLM adherence and lowering token costs.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from backend.prompts_create._core import CORE
from backend.prompts_create._requirements import (
    STEP as STEP_REQUIREMENTS,
    SUMMARY as SUMMARY_REQUIREMENTS,
)
from backend.prompts_create._data_sources import (
    STEP as STEP_DATA_SOURCES,
    SUMMARY as SUMMARY_DATA_SOURCES,
)
from backend.prompts_create._inspection import (
    STEP as STEP_INSPECTION,
    SUMMARY as SUMMARY_INSPECTION,
)
from backend.prompts_create._plan import (
    STEP as STEP_PLAN,
    SUMMARY as SUMMARY_PLAN,
)
from backend.prompts_create._generate import (
    STEP as STEP_CONFIG_CREATE,
    SUMMARY as SUMMARY_CONFIG_CREATE,
)
from backend.prompts_create._create import (
    STEP as STEP_POST_CREATION,
    SUMMARY as SUMMARY_POST_CREATION,
)
from backend.prompts_create._backtracking import BACKTRACKING
from backend.prompts_create._tools import TOOL_RULES

if TYPE_CHECKING:
    from backend.services.create_agent_session import AgentSession

# Ordered list of steps — order matters for adjacency.
STEP_ORDER = [
    "requirements",
    "data_sources",
    "inspection",
    "plan",
    "config_create",
    "post_creation",
]

STEP_PROMPTS = {
    "requirements": STEP_REQUIREMENTS,
    "data_sources": STEP_DATA_SOURCES,
    "inspection": STEP_INSPECTION,
    "plan": STEP_PLAN,
    "config_create": STEP_CONFIG_CREATE,
    "post_creation": STEP_POST_CREATION,
}

STEP_SUMMARIES = {
    "requirements": SUMMARY_REQUIREMENTS,
    "data_sources": SUMMARY_DATA_SOURCES,
    "inspection": SUMMARY_INSPECTION,
    "plan": SUMMARY_PLAN,
    "config_create": SUMMARY_CONFIG_CREATE,
    "post_creation": SUMMARY_POST_CREATION,
}


# ---------------------------------------------------------------------------
# Step detection
# ---------------------------------------------------------------------------

def _has_tool(history: list[dict], tool_name: str) -> bool:
    """Check if a tool was called in the conversation history."""
    for msg in history:
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                if tc.get("function", {}).get("name") == tool_name:
                    return True
    return False


def detect_step(session: AgentSession) -> str:
    """Infer which workflow step the agent is in from session state + tool history.

    Returns one of the STEP_ORDER values.
    """
    if session.space_id:
        return "post_creation"
    if session.space_config:
        return "config_create"
    if _has_tool(session.history, "present_plan") or _has_tool(session.history, "generate_plan"):
        return "plan"
    if _has_tool(session.history, "describe_table"):
        return "inspection"
    if _has_tool(session.history, "discover_tables"):
        return "data_sources"
    return "requirements"


# ---------------------------------------------------------------------------
# Adjacent-step summaries
# ---------------------------------------------------------------------------

def _get_adjacent_summaries(step: str) -> str:
    """Build brief summaries of the previous and next steps for context."""
    idx = STEP_ORDER.index(step)
    parts: list[str] = []

    if idx > 0:
        prev = STEP_ORDER[idx - 1]
        parts.append(f"**Previous** — {STEP_SUMMARIES[prev]}")

    if idx < len(STEP_ORDER) - 1:
        nxt = STEP_ORDER[idx + 1]
        parts.append(f"**Next** — {STEP_SUMMARIES[nxt]}")

    return "\n".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assemble_system_prompt(session: AgentSession, schema_reference: str) -> str:
    """Build a scoped system prompt for the current turn.

    The prompt includes:
    1. Core identity & principles
    2. Detailed instructions for the CURRENT step
    3. Brief summaries of adjacent steps (for backtracking awareness)
    4. Backtracking rules
    5. Tool/UI/autopilot rules
    6. Schema reference
    """
    step = detect_step(session)

    current_prompt = STEP_PROMPTS[step]
    adjacent = _get_adjacent_summaries(step)

    sections = [
        CORE,
        f"## Workflow\n\n{current_prompt}",
    ]

    if adjacent:
        sections.append(f"## Adjacent Steps (for context)\n{adjacent}")

    sections.extend([
        BACKTRACKING,
        TOOL_RULES,
        f"## Schema Reference\n{schema_reference}",
    ])

    return "\n\n".join(sections)
