"""genie-analyzer — analysis, querying, and SQL execution agent.

Wraps:
  - backend/routers/analysis.py  (fetch, parse, query, SQL, benchmark compare)
  - backend/services/genie_client.py (space fetching, Genie queries)
  - backend/sql_executor.py (SQL warehouse execution)

Streaming: No (all request/response)
LLM: Yes (benchmark comparison uses LLM)
"""

from __future__ import annotations

from dbx_agent_app import AgentRequest, AgentResponse, app_agent

from agents._shared.auth_bridge import obo_context


@app_agent(
    name="genie-analyzer",
    description=(
        "Fetches and parses Genie Space configurations, queries Genie for SQL, "
        "executes SQL on warehouses, and compares benchmark results."
    ),
)
async def analyzer(request: AgentRequest) -> AgentResponse:
    """Route incoming agent requests to analysis tools."""
    ...


# ── Tools ────────────────────────────────────────────────────────────────────


@analyzer.tool(
    description=(
        "Fetch and parse a Genie Space by ID. Returns the space "
        "configuration data."
    ),
)
async def fetch_space(genie_space_id: str, request: AgentRequest) -> dict:
    """Wraps backend/services/genie_client.py::get_serialized_space"""
    with obo_context(request.user_context.access_token):
        from backend.services.genie_client import get_serialized_space
        space_data = get_serialized_space(genie_space_id)
        return {"genie_space_id": genie_space_id, "space_data": space_data}


@analyzer.tool(
    description="Parse pasted Genie Space JSON from the API response.",
)
async def parse_space_json(json_content: str) -> dict:
    """Wraps backend/routers/analysis.py::parse_space_json logic"""
    import json
    from datetime import datetime

    raw_response = json.loads(json_content)
    if "serialized_space" not in raw_response:
        raise ValueError("Missing 'serialized_space' field in JSON")

    serialized = raw_response["serialized_space"]
    space_data = json.loads(serialized) if isinstance(serialized, str) else serialized
    genie_space_id = f"pasted-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    return {"genie_space_id": genie_space_id, "space_data": space_data}


@analyzer.tool(
    description=(
        "Query a Genie Space with a natural language question. "
        "Returns the generated SQL if successful."
    ),
)
async def query_genie(genie_space_id: str, question: str, request: AgentRequest) -> dict:
    """Wraps backend/services/genie_client.py::query_genie_for_sql"""
    with obo_context(request.user_context.access_token):
        from backend.services.genie_client import query_genie_for_sql
        return query_genie_for_sql(
            genie_space_id=genie_space_id,
            question=question,
        )


@analyzer.tool(
    description=(
        "Execute a read-only SQL query on a Databricks SQL Warehouse. "
        "Returns tabular results limited to 1000 rows."
    ),
)
async def execute_sql(sql: str, warehouse_id: str | None = None, request: AgentRequest = None) -> dict:
    """Wraps backend/sql_executor.py::execute_sql"""
    with obo_context(request.user_context.access_token):
        from backend.sql_executor import execute_sql as _execute
        return _execute(sql=sql, warehouse_id=warehouse_id)


@analyzer.tool(
    description=(
        "Compare Genie SQL results against expected SQL results using "
        "LLM-based semantic comparison. Returns match type, confidence, "
        "and an auto-label suggestion."
    ),
)
async def compare_results(
    genie_result: dict,
    expected_result: dict,
    genie_sql: str | None = None,
    expected_sql: str | None = None,
    question: str | None = None,
    request: AgentRequest = None,
) -> dict:
    """Wraps backend/services/result_comparator.py::compare_results"""
    import asyncio
    with obo_context(request.user_context.access_token):
        from backend.services.auth import run_in_context
        from backend.services.result_comparator import compare_results as _compare

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            run_in_context(
                _compare,
                genie_result=genie_result,
                expected_result=expected_result,
                genie_sql=genie_sql,
                expected_sql=expected_sql,
                question=question,
            ),
        )
        return result.model_dump() if hasattr(result, "model_dump") else result


# ── Standalone entry point ───────────────────────────────────────────────────

app = analyzer.app
