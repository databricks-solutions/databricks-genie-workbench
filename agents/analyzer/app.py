"""genie-analyzer — LLM-powered deep analysis of Genie Space configurations.

Extracted from:
  - backend/routers/analysis.py   (fetch, analyze, stream, query, SQL endpoints)
  - backend/services/analyzer.py  (GenieSpaceAnalyzer, section analysis, synthesis)
  - backend/synthesizer.py        (cross-section synthesis)
  - backend/services/genie_client.py (space fetching)

Streaming: Yes (SSE for multi-section analysis)
LLM: Yes (section analysis + synthesis)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent


@app_agent(
    name="genie-analyzer",
    description=(
        "Deep LLM-powered analysis of Genie Space configurations. Evaluates "
        "each section (tables, instructions, SQL snippets, etc.) against a "
        "checklist, synthesizes findings, and provides actionable recommendations."
    ),
)
async def analyzer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to analysis tools."""
    # TODO: Parse intent from request.messages and dispatch to tools
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@analyzer.tool(
    description=(
        "Fetch and parse a Genie Space by ID. Returns the space data "
        "and list of sections with their data."
    ),
)
async def fetch_space(genie_space_id: str) -> dict:
    """Source: backend/routers/analysis.py::fetch_space"""
    # Uses genie_client.get_serialized_space (moved as-is)
    raise NotImplementedError("Phase 4: move genie_client + analyzer here")


@analyzer.tool(
    description=(
        "Analyze a single section of a Genie Space configuration. "
        "Returns findings, score, and recommendations for that section."
    ),
)
async def analyze_section(
    section_name: str,
    section_data: dict | list | None,
    full_space: dict,
) -> dict:
    """Source: backend/services/analyzer.py::GenieSpaceAnalyzer.analyze_section"""
    raise NotImplementedError("Phase 4: move analyzer.py here")


@analyzer.tool(
    description=(
        "Analyze all sections with cross-sectional synthesis. Returns "
        "section analyses plus a synthesis result for full analysis."
    ),
)
async def analyze_all(
    sections: list[dict],
    full_space: dict,
) -> dict:
    """Source: backend/routers/analysis.py::analyze_all_sections"""
    raise NotImplementedError("Phase 4: move analyzer.py + synthesizer.py here")


@analyzer.tool(
    description=(
        "Query a Genie Space with a natural language question. "
        "Returns the generated SQL if successful."
    ),
)
async def query_genie(genie_space_id: str, question: str) -> dict:
    """Source: backend/services/genie_client.py::query_genie_for_sql"""
    raise NotImplementedError("Phase 4: move genie_client.query_genie_for_sql here")


@analyzer.tool(
    description=(
        "Execute a read-only SQL query on a Databricks SQL Warehouse. "
        "Returns tabular results limited to 1000 rows."
    ),
)
async def execute_sql(sql: str, warehouse_id: str | None = None) -> dict:
    """Source: backend/sql_executor.py::execute_sql

    Phase 8: Replace with databricks_tools_core.sql.execute_sql
    """
    raise NotImplementedError("Phase 4: move sql_executor.py (then Phase 8: replace with AI Dev Kit)")


@analyzer.tool(
    description="Parse pasted Genie Space JSON from the API response.",
)
async def parse_space_json(json_content: str) -> dict:
    """Source: backend/routers/analysis.py::parse_space_json"""
    raise NotImplementedError("Phase 4: move parse logic here")


# ── Standalone entry point ───────────────────────────────────────────────────

app = analyzer.app
