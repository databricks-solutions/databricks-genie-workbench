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
  - (fix mode only) Current space config summary

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


def _has_tool_result(history: list[dict], tool_name: str) -> bool:
    """Check if a tool was called AND has a non-cancelled result in history."""
    call_ids_for_tool: set[str] = set()
    for msg in history:
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                if tc.get("function", {}).get("name") == tool_name:
                    call_ids_for_tool.add(tc["id"])

    if not call_ids_for_tool:
        return False

    for msg in history:
        if msg.get("role") == "tool" and msg.get("tool_call_id") in call_ids_for_tool:
            content = msg.get("content", "")
            if '"cancelled"' not in content:
                return True
    return False


def _inspection_complete(history: list[dict]) -> bool:
    """Check if describe_table ran AND at least one follow-up inspection tool
    (assess_data_quality or profile_table_usage) has a real result.

    This distinguishes "just started inspecting" from "inspection is done,
    ready for plan generation."
    """
    if not _has_tool_result(history, "describe_table"):
        return False
    return (
        _has_tool_result(history, "assess_data_quality")
        or _has_tool_result(history, "profile_table_usage")
    )


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
    if _inspection_complete(session.history):
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
# Space config summary (for fix mode)
# ---------------------------------------------------------------------------

def _summarize_space_config(config: dict) -> str:
    """Build a concise summary of an existing space config for the system prompt.

    Gives the agent full awareness of what's already in the space so it can
    jump straight to fixes without calling discovery/inspection tools.
    """
    lines: list[str] = []

    # Tables
    tables = config.get("data_sources", {}).get("tables") or []
    if tables:
        lines.append(f"**Tables ({len(tables)}):**")
        for t in tables:
            tid = t.get("table_identifier", "?")
            desc = t.get("description", "")
            col_count = len(t.get("columns") or [])
            desc_snippet = f" — {desc[:80]}..." if len(desc) > 80 else f" — {desc}" if desc else ""
            lines.append(f"- `{tid}` ({col_count} columns){desc_snippet}")

            # Columns with descriptions (only list those that have descriptions)
            cols_with_desc = [
                c for c in (t.get("columns") or [])
                if c.get("description")
            ]
            if cols_with_desc:
                for c in cols_with_desc[:10]:
                    lines.append(f"  - `{c.get('name', '?')}`: {c['description'][:60]}")
                if len(cols_with_desc) > 10:
                    lines.append(f"  - ... and {len(cols_with_desc) - 10} more with descriptions")

    # Joins
    joins = config.get("data_sources", {}).get("join_specs") or []
    if joins:
        lines.append(f"\n**Joins ({len(joins)}):**")
        for j in joins:
            lines.append(f"- {j.get('join_string', '?')[:120]}")

    # Instructions
    instructions = config.get("instructions", {})
    text_instr = instructions.get("text_instructions") or []
    if text_instr:
        lines.append(f"\n**Text instructions ({len(text_instr)}):**")
        for ti in text_instr[:5]:
            content = ti.get("content", str(ti)) if isinstance(ti, dict) else str(ti)
            lines.append(f"- {content[:100]}")
        if len(text_instr) > 5:
            lines.append(f"- ... and {len(text_instr) - 5} more")

    # Example SQLs
    example_sqls = instructions.get("example_question_sqls") or []
    if example_sqls:
        lines.append(f"\n**Example SQLs ({len(example_sqls)}):**")
        for eq in example_sqls[:5]:
            q = eq.get("question", "?") if isinstance(eq, dict) else str(eq)
            lines.append(f"- {q[:100]}")
        if len(example_sqls) > 5:
            lines.append(f"- ... and {len(example_sqls) - 5} more")

    # SQL snippets (measures, filters, expressions)
    snippets = instructions.get("sql_snippets") or {}
    for kind in ("measures", "filters", "expressions"):
        items = snippets.get(kind) or []
        if items:
            lines.append(f"\n**{kind.title()} ({len(items)}):**")
            for item in items[:5]:
                name = item.get("name", "?") if isinstance(item, dict) else str(item)
                lines.append(f"- {name}")
            if len(items) > 5:
                lines.append(f"- ... and {len(items) - 5} more")

    # Sample questions
    sample_qs = instructions.get("sample_questions") or []
    if sample_qs:
        lines.append(f"\n**Sample questions ({len(sample_qs)}):**")
        for sq in sample_qs[:5]:
            q = sq.get("question", str(sq)) if isinstance(sq, dict) else str(sq)
            lines.append(f"- {q[:100]}")
        if len(sample_qs) > 5:
            lines.append(f"- ... and {len(sample_qs) - 5} more")

    return "\n".join(lines)


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
    7. (fix mode) Current space config summary
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

    # In fix mode, inject the current space config so the agent doesn't
    # waste tool calls discovering what's already in the space.
    if session.space_id and session.space_config:
        summary = _summarize_space_config(session.space_config)
        if summary:
            sections.append(
                "## Current Space Config\n\n"
                "The space config is already loaded in your session. "
                "You do NOT need to call discover_tables, describe_table, or other "
                "inspection tools — use the summary below and jump straight to fixes.\n\n"
                f"{summary}"
            )

    return "\n\n".join(sections)
