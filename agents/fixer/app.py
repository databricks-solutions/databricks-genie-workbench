"""genie-fixer — AI fix agent for Genie Space configurations.

Extracted from:
  - backend/routers/spaces.py      (fix endpoint)
  - backend/services/fix_agent.py  (FixAgent, patch generation + application)
  - backend/prompts.py             (get_fix_agent_prompt)

Streaming: Yes (SSE with thinking → patch → applying → complete events)
LLM: Yes (fix plan generation)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent


@app_agent(
    name="genie-fixer",
    description=(
        "AI fix agent for Genie Spaces. Takes IQ scan findings and the "
        "current space configuration, uses an LLM to generate targeted "
        "config patches, and applies them via the Genie API."
    ),
)
async def fixer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to fix tools.

    The streaming fix workflow:
    1. thinking — "Analyzing findings..."
    2. patch — individual patches with field_path, old/new values, rationale
    3. applying — "Applying N fix(es)..."
    4. complete — summary with patches_applied count and diff

    Source: backend/services/fix_agent.py::FixAgent.run
    """
    # TODO: Phase 3 — move FixAgent.run here as streaming handler
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@fixer.tool(
    description=(
        "Generate a fix plan from IQ scan findings. Uses LLM to reason "
        "about findings, prioritize fixes, and produce specific config "
        "patch operations (field_path + new_value + rationale)."
    ),
)
async def generate_fixes(
    space_id: str,
    findings: list[str],
    space_config: dict,
) -> dict:
    """Source: backend/services/fix_agent.py::FixAgent.run (first half — plan generation)

    Returns:
      - patches: list of {field_path, new_value, rationale}
      - summary: human-readable summary of the fix plan
    """
    raise NotImplementedError("Phase 3: move fix_agent.py here")


@fixer.tool(
    description=(
        "Apply a list of config patches to a Genie Space via the "
        "Databricks API. Returns before/after diff."
    ),
)
async def apply_patches(
    space_id: str,
    patches: list[dict],
    space_config: dict,
) -> dict:
    """Source: backend/services/fix_agent.py::FixAgent.run (second half — patch application)

    Each patch has:
      - field_path: Dot-notation path (e.g., "instructions.text_instructions[0].content")
      - new_value: The value to set
      - rationale: Why this fix helps

    Returns:
      - patches_applied: int
      - summary: str
      - diff: {patches, original_config, updated_config}
    """
    raise NotImplementedError("Phase 3: move fix_agent.py patch application here")


# ── Standalone entry point ───────────────────────────────────────────────────

app = fixer.app
