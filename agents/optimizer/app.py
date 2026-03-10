"""genie-optimizer — Optimization suggestions from benchmark labeling feedback.

Extracted from:
  - backend/routers/analysis.py     (optimize, merge_config endpoints)
  - backend/services/optimizer.py   (GenieSpaceOptimizer)
  - backend/prompts.py              (get_optimization_prompt)

Streaming: Heartbeat SSE only (long-running LLM call with keepalives)
LLM: Yes (optimization suggestion generation)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent


@app_agent(
    name="genie-optimizer",
    description=(
        "Generates optimization suggestions for Genie Space configurations "
        "based on benchmark labeling feedback. Analyzes incorrect answers "
        "and suggests config changes (new instructions, SQL snippets, "
        "column descriptions) to improve accuracy."
    ),
)
async def optimizer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to optimization tools."""
    # TODO: Parse intent from request.messages and dispatch to tools
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@optimizer.tool(
    description=(
        "Generate optimization suggestions based on benchmark labeling feedback. "
        "Analyzes incorrect/correct Genie answers and suggests specific config "
        "changes to improve accuracy. Returns a list of suggestions with "
        "field paths, current values, and suggested replacements."
    ),
)
async def generate_suggestions(
    space_data: dict,
    labeling_feedback: list[dict],
) -> dict:
    """Source: backend/services/optimizer.py::GenieSpaceOptimizer.generate_optimizations

    Each feedback item has:
      - question_text: The benchmark question
      - is_correct: Whether Genie answered correctly
      - feedback_text: User's notes on what went wrong
    """
    raise NotImplementedError("Phase 5: move optimizer.py here")


@optimizer.tool(
    description=(
        "Merge optimization suggestions into a Genie Space configuration. "
        "Applies field-level changes without LLM calls — fast, deterministic."
    ),
)
async def merge_config(
    space_data: dict,
    suggestions: list[dict],
) -> dict:
    """Source: backend/services/optimizer.py::GenieSpaceOptimizer.merge_config

    Each suggestion has:
      - field_path: Dot-notation path (e.g., "instructions.text_instructions[0].content")
      - suggested_value: The new value to set
    """
    raise NotImplementedError("Phase 5: move optimizer.merge_config here")


@optimizer.tool(
    description=(
        "Label a benchmark question as correct or incorrect with feedback. "
        "Stores the labeling result for later optimization."
    ),
)
async def label_benchmark(
    question_text: str,
    is_correct: bool,
    feedback_text: str = "",
) -> dict:
    """New tool — currently labeling is handled purely in frontend state.

    This tool would persist labeling decisions for cross-session use.
    """
    raise NotImplementedError("Phase 5: implement labeling persistence")


# ── Standalone entry point ───────────────────────────────────────────────────

app = optimizer.app
