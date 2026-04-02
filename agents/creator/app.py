"""genie-creator — Conversational wizard for building new Genie Spaces.

Wraps:
  - backend/services/create_agent.py       (CreateGenieAgent tool-calling loop)
  - backend/services/create_agent_tools.py (16 tool implementations + dispatcher)
  - backend/services/create_agent_session.py (two-tier session persistence)
  - backend/services/uc_client.py          (UC browsing)
  - backend/prompts_create/                (dynamic prompt assembly)
  - backend/genie_creator.py              (Genie API write operations)

This is the most complex agent — 16 tools, session persistence, LLM
tool-calling loop with message compaction.

Streaming: Yes (SSE for agent chat)
LLM: Yes (tool-calling loop with Claude)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context
from agents.creator.schemas import GenerateConfigArgs


@app_agent(
    name="genie-creator",
    description=(
        "Conversational wizard for building new Genie Spaces. Guides users "
        "through requirements gathering, data source discovery, table "
        "inspection, plan presentation, config generation, and space creation."
    ),
)
async def creator(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to the creator workflow."""
    ...


# ── Helper ───────────────────────────────────────────────────────────────────

def _call_tool(name: str, arguments: dict, session_config: dict | None = None) -> dict:
    """Dispatch to backend/services/create_agent_tools.py::handle_tool_call."""
    from backend.services.create_agent_tools import handle_tool_call
    return handle_tool_call(name, arguments, session_config=session_config)


# ── UC Discovery Tools ──────────────────────────────────────────────────────


@creator.tool(description="List all Unity Catalog catalogs the user has access to.")
async def discover_catalogs(request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("discover_catalogs", {})


@creator.tool(description="List schemas within a catalog.")
async def discover_schemas(catalog: str, request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("discover_schemas", {"catalog": catalog})


@creator.tool(description="List tables within a catalog.schema.")
async def discover_tables(catalog: str, schema: str, request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("discover_tables", {"catalog": catalog, "schema": schema})


# ── Table Inspection Tools ──────────────────────────────────────────────────


@creator.tool(
    description="Get detailed table metadata: columns, types, descriptions, row count, sample rows.",
)
async def describe_table(table: str, request: AgentRequest) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("describe_table", {"table": table})


@creator.tool(
    description="Profile selected columns: distinct values, null percentage, min/max.",
)
async def profile_columns(table: str, columns: list[str] | None = None, request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("profile_columns", {"table": table, "columns": columns})


@creator.tool(
    description="Assess data quality: null rates, duplicate rates, freshness, anomalies.",
)
async def assess_data_quality(tables: list[str], request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("assess_data_quality", {"tables": tables})


@creator.tool(
    description="Profile table usage patterns: query frequency, common joins, active users.",
)
async def profile_table_usage(tables: list[str], request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("profile_table_usage", {"tables": tables})


@creator.tool(description="Execute a test SQL query and return results (read-only, max 5 rows).")
async def test_sql(sql: str, request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("test_sql", {"sql": sql})


@creator.tool(description="List available SQL warehouses for the user.")
async def discover_warehouses(request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("discover_warehouses", {})


# ── Config Generation Tools ─────────────────────────────────────────────────


@creator.tool(description="Get the Genie Space configuration JSON schema reference.")
async def get_config_schema(request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("get_config_schema", {})


@creator.tool(
    description=(
        "Generate a complete Genie Space configuration from discovered "
        "tables, inspection data, and user requirements."
    ),
    parameters=GenerateConfigArgs.model_json_schema(),
)
async def generate_config(request: AgentRequest = None, **kwargs) -> dict:
    args = GenerateConfigArgs(**kwargs)
    with obo_context(request.user_context.access_token):
        return _call_tool("generate_config", args.model_dump())


@creator.tool(
    description="Present the space creation plan to the user for review before generating config.",
    parameters=GenerateConfigArgs.model_json_schema(),
)
async def present_plan(request: AgentRequest = None, **kwargs) -> dict:
    args = GenerateConfigArgs(**kwargs)
    with obo_context(request.user_context.access_token):
        return _call_tool("present_plan", args.model_dump())


@creator.tool(description="Validate a generated configuration against the Genie Space schema.")
async def validate_config(config: dict, request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("validate_config", {"config": config})


@creator.tool(description="Apply incremental updates to an existing generated configuration.")
async def update_config(config: dict, updates: dict, request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("update_config", {"config": config, "updates": updates})


@creator.tool(
    description="Create a new Genie Space in the workspace with the generated configuration.",
)
async def create_space(
    display_name: str,
    config: dict,
    parent_path: str | None = None,
    warehouse_id: str | None = None,
    request: AgentRequest = None,
) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("create_space", {
            "display_name": display_name,
            "config": config,
            "parent_path": parent_path,
            "warehouse_id": warehouse_id,
        })


@creator.tool(
    description="Update an existing Genie Space with a modified configuration.",
)
async def update_space(space_id: str, config: dict, request: AgentRequest = None) -> dict:
    with obo_context(request.user_context.access_token):
        return _call_tool("update_space", {"space_id": space_id, "config": config})


# ── Standalone entry point ───────────────────────────────────────────────────

app = creator.app
