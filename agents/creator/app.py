"""genie-creator — Conversational wizard for building new Genie Spaces.

Extracted from:
  - backend/routers/create.py              (UC discovery, validation, agent chat, sessions)
  - backend/services/create_agent.py       (CreateGenieAgent tool-calling loop)
  - backend/services/create_agent_tools.py (16 tool definitions + implementations)
  - backend/services/create_agent_session.py (two-tier session persistence)
  - backend/services/uc_client.py          (UC browsing — replaced by AI Dev Kit)
  - backend/prompts_create/                (dynamic prompt assembly, 9 modules)
  - backend/references/                    (schema.md reference)
  - backend/genie_creator.py              (Genie API write operations)

This is the MOST COMPLEX agent (Phase 6 extraction). The tool-calling loop,
message compaction, session persistence, and dynamic prompting are all
irreplaceable domain logic that moves as-is.

What gets replaced:
  - 580 lines of JSON tool schemas → auto-generated from @creator.tool() signatures
  - 40-line handle_tool_call() dispatcher → auto-routing
  - uc_client.py (60 lines) → databricks_tools_core.unity_catalog
  - sql_executor.py (220 lines) → databricks_tools_core.sql

Streaming: Yes (SSE for agent chat)
LLM: Yes (tool-calling loop with Claude)
"""

from __future__ import annotations

from pydantic import BaseModel

from dbx_agent_app import AgentRequest, AgentResponse, app_agent


@app_agent(
    name="genie-creator",
    description=(
        "Conversational wizard for building new Genie Spaces. Guides users "
        "through requirements gathering, data source discovery, table "
        "inspection, plan presentation, config generation, and space creation."
    ),
)
async def creator(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to the creator workflow.

    The core tool-calling loop (CreateGenieAgent.chat) moves here as-is.
    It handles: step detection, LLM streaming, tool dispatch, message
    compaction, JSON repair, and session management.

    Source: backend/services/create_agent.py::CreateGenieAgent.chat
    """
    # TODO: Phase 6 — move CreateGenieAgent.chat here
    ...


# ── UC Discovery Tools ──────────────────────────────────────────────────────
# Phase 8: Replace implementations with databricks_tools_core


@creator.tool(description="List all Unity Catalog catalogs the user has access to.")
async def discover_catalogs() -> dict:
    """Source: backend/services/uc_client.py::list_catalogs

    Phase 8: from databricks_tools_core.unity_catalog import list_catalogs
    """
    raise NotImplementedError("Phase 6/8")


@creator.tool(description="List schemas within a catalog.")
async def discover_schemas(catalog: str) -> dict:
    """Source: backend/services/uc_client.py::list_schemas"""
    raise NotImplementedError("Phase 6/8")


@creator.tool(description="List tables within a catalog.schema.")
async def discover_tables(catalog: str, schema: str) -> dict:
    """Source: backend/services/uc_client.py::list_tables"""
    raise NotImplementedError("Phase 6/8")


# ── Table Inspection Tools ───────────────────────────────────────────────────


@creator.tool(
    description="Get detailed table metadata: columns, types, descriptions, row count, sample rows.",
)
async def describe_table(table: str) -> dict:
    """Source: backend/services/create_agent_tools.py::_describe_table (lines ~860-960)"""
    raise NotImplementedError("Phase 6")


@creator.tool(
    description=(
        "Profile selected columns: distinct values, null percentage, "
        "min/max, data type distribution."
    ),
)
async def profile_columns(table: str, columns: list[str] | None = None) -> dict:
    """Source: backend/services/create_agent_tools.py::_profile_columns"""
    raise NotImplementedError("Phase 6")


@creator.tool(
    description="Assess data quality: null rates, duplicate rates, freshness, anomalies.",
)
async def assess_data_quality(tables: list[str]) -> dict:
    """Source: backend/services/create_agent_tools.py::_assess_data_quality"""
    raise NotImplementedError("Phase 6")


@creator.tool(
    description="Profile table usage patterns: query frequency, common joins, active users.",
)
async def profile_table_usage(tables: list[str]) -> dict:
    """Source: backend/services/create_agent_tools.py::_profile_table_usage"""
    raise NotImplementedError("Phase 6")


@creator.tool(description="Execute a test SQL query and return results (read-only, max 5 rows).")
async def test_sql(sql: str) -> dict:
    """Source: backend/services/create_agent_tools.py::_test_sql

    Phase 8: Replace with databricks_tools_core.sql.execute_sql
    """
    raise NotImplementedError("Phase 6/8")


@creator.tool(description="List available SQL warehouses for the user.")
async def discover_warehouses() -> dict:
    """Source: backend/services/create_agent_tools.py::_discover_warehouses"""
    raise NotImplementedError("Phase 6")


# ── Config Generation Tools ──────────────────────────────────────────────────


@creator.tool(description="Get the Genie Space configuration JSON schema reference.")
async def get_config_schema() -> dict:
    """Source: backend/services/create_agent_tools.py::_get_config_schema"""
    raise NotImplementedError("Phase 6")


class TableConfig(BaseModel):
    """Pydantic model for deeply nested generate_config table arguments."""
    identifier: str
    description: str = ""
    column_configs: list[dict] = []


@creator.tool(
    description=(
        "Generate a complete Genie Space configuration from discovered "
        "tables, inspection data, and user requirements."
    ),
)
async def generate_config(
    tables: list[dict],
    sample_questions: list[str] | None = None,
    text_instructions: list[str] | None = None,
    example_sqls: list[dict] | None = None,
    join_specs: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    benchmarks: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Source: backend/services/create_agent_tools.py::_generate_config (~lines 245-650)

    This is the largest tool implementation. The LLM provides content;
    this tool handles all structural formatting (JSON schema compliance,
    column config normalization, instruction budget enforcement).
    """
    raise NotImplementedError("Phase 6")


@creator.tool(
    description="Present the space creation plan to the user for review before generating config.",
)
async def present_plan(
    tables: list[dict],
    sample_questions: list[str] | None = None,
    text_instructions: list[str] | None = None,
    example_sqls: list[dict] | None = None,
    join_specs: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    benchmarks: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Source: backend/services/create_agent_tools.py::_present_plan"""
    raise NotImplementedError("Phase 6")


@creator.tool(description="Validate a generated configuration against the Genie Space schema.")
async def validate_config(config: dict) -> dict:
    """Source: backend/services/create_agent_tools.py::_validate_config"""
    raise NotImplementedError("Phase 6")


@creator.tool(description="Apply incremental updates to an existing generated configuration.")
async def update_config(config: dict, updates: dict) -> dict:
    """Source: backend/services/create_agent_tools.py::_update_config"""
    raise NotImplementedError("Phase 6")


@creator.tool(
    description="Create a new Genie Space in the workspace with the generated configuration.",
)
async def create_space(
    display_name: str,
    config: dict,
    parent_path: str | None = None,
    warehouse_id: str | None = None,
) -> dict:
    """Source: backend/services/create_agent_tools.py::_create_space + backend/genie_creator.py"""
    raise NotImplementedError("Phase 6")


@creator.tool(
    description="Update an existing Genie Space with a modified configuration.",
)
async def update_space(space_id: str, config: dict) -> dict:
    """Source: backend/services/create_agent_tools.py::_update_space"""
    raise NotImplementedError("Phase 6")


# ── Standalone entry point ───────────────────────────────────────────────────

app = creator.app
