"""Tool implementations for the Create Genie agent.

Each tool returns a dict that gets serialized as the tool result for the LLM.
Tools handle all mechanical formatting — the LLM provides content, tools handle structure.
"""

import copy
import json
import logging
import re
import secrets
from pathlib import Path
from typing import Any

from backend.services.auth import get_workspace_client, get_databricks_host
from backend.services.uc_client import list_catalogs, list_schemas, list_tables
from backend.sql_executor import execute_sql, get_sql_warehouse_id
from backend.genie_creator import create_genie_space

logger = logging.getLogger(__name__)

PII_PATTERNS = re.compile(
    r"(email|phone|ssn|social_security|credit_card|card_number|"
    r"address|zip_code|postal|passport|license_number|dob|date_of_birth|"
    r"birth_date|salary|wage|income|bank_account|routing_number|"
    r"password|secret|token|api_key)", re.IGNORECASE
)


# ── Tool definitions (OpenAI function-calling format) ────────────────────────

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "discover_catalogs",
            "description": "List all Unity Catalog catalogs the user has access to.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "discover_schemas",
            "description": "List schemas within a catalog.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "description": "Catalog name"},
                },
                "required": ["catalog"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "discover_tables",
            "description": "List tables within a catalog.schema. Returns table names, types, comments, and column counts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "description": "Catalog name"},
                    "schema": {"type": "string", "description": "Schema name"},
                },
                "required": ["catalog", "schema"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": "Get detailed column metadata for a table: column names, types, descriptions, and flags for potentially sensitive (PII) columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_identifier": {
                        "type": "string",
                        "description": "Fully qualified table name: catalog.schema.table",
                    },
                },
                "required": ["table_identifier"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "profile_columns",
            "description": "Profile key columns of a table: distinct values for string/category columns, date ranges for date columns. Helps write accurate SQL expressions and filters.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_identifier": {
                        "type": "string",
                        "description": "Fully qualified table name: catalog.schema.table",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column names to profile. If empty, profiles all string/date columns.",
                    },
                },
                "required": ["table_identifier"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "test_sql",
            "description": "Execute a SQL query to verify it runs successfully. Use this to test example SQL queries before including them in the config. Returns column names, first few rows, and row count.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SQL query to test"},
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "discover_warehouses",
            "description": "List eligible SQL warehouses (pro or serverless) for the Genie space.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_config_schema",
            "description": (
                "Retrieve the serialized_space JSON schema reference, validation rules, "
                "and tool usage examples. Call this if you're unsure how to structure "
                "generate_config or update_config arguments."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_config",
            "description": (
                "Build the complete serialized_space JSON from structured inputs. "
                "Use this for INITIAL space creation only. For post-creation changes, use update_config instead. "
                "Handles all mechanical formatting: generates IDs, sorts arrays, splits SQL into "
                "line-by-line arrays, formats join specs with backtick-quoting and --rt= annotations, "
                "wraps strings in arrays. The LLM provides the CONTENT; this tool handles the FORMAT. "
                "If unsure about parameter shapes, call get_config_schema first."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "description": "Tables to include in the space",
                        "items": {
                            "type": "object",
                            "properties": {
                                "identifier": {"type": "string", "description": "catalog.schema.table"},
                                "description": {"type": "string", "description": "Space-scoped table description"},
                                "column_configs": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "column_name": {"type": "string"},
                                            "description": {"type": "string"},
                                            "synonyms": {"type": "array", "items": {"type": "string"}},
                                            "exclude": {"type": "boolean"},
                                            "enable_matching": {"type": "boolean", "description": "Enable entity matching + format assistance"},
                                        },
                                        "required": ["column_name"],
                                    },
                                },
                            },
                            "required": ["identifier"],
                        },
                    },
                    "sample_questions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "3-5 sample questions for business users",
                    },
                    "text_instructions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Business rules and domain guidance lines",
                    },
                    "example_sqls": {
                        "type": "array",
                        "description": "Example question-SQL pairs",
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "sql": {"type": "string", "description": "The full SQL query as a single string"},
                                "usage_guidance": {"type": "string"},
                            },
                            "required": ["question", "sql"],
                        },
                    },
                    "measures": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "alias": {"type": "string"},
                                "display_name": {"type": "string"},
                                "sql": {"type": "string", "description": "Aggregate SQL expression, e.g. SUM(orders.amount)"},
                                "synonyms": {"type": "array", "items": {"type": "string"}},
                                "instruction": {"type": "string"},
                            },
                            "required": ["alias", "sql"],
                        },
                    },
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "display_name": {"type": "string"},
                                "sql": {"type": "string", "description": "Boolean condition WITHOUT the WHERE keyword"},
                                "synonyms": {"type": "array", "items": {"type": "string"}},
                                "instruction": {"type": "string"},
                            },
                            "required": ["display_name", "sql"],
                        },
                    },
                    "expressions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "alias": {"type": "string"},
                                "display_name": {"type": "string"},
                                "sql": {"type": "string", "description": "Dimension SQL expression, e.g. YEAR(orders.order_date)"},
                                "synonyms": {"type": "array", "items": {"type": "string"}},
                                "instruction": {"type": "string"},
                            },
                            "required": ["alias", "sql"],
                        },
                    },
                    "join_specs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "left_table": {"type": "string", "description": "Fully qualified left table"},
                                "left_alias": {"type": "string"},
                                "right_table": {"type": "string", "description": "Fully qualified right table"},
                                "right_alias": {"type": "string"},
                                "left_column": {"type": "string"},
                                "right_column": {"type": "string"},
                                "relationship": {
                                    "type": "string",
                                    "enum": ["MANY_TO_ONE", "ONE_TO_MANY", "ONE_TO_ONE", "MANY_TO_MANY"],
                                },
                                "instruction": {"type": "string"},
                            },
                            "required": ["left_table", "left_alias", "right_table", "right_alias", "left_column", "right_column", "relationship"],
                        },
                    },
                    "benchmarks": {
                        "type": "array",
                        "description": "Benchmark question-SQL pairs for scoring. If omitted, benchmarks are auto-generated from example_sqls.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "expected_sql": {"type": "string"},
                            },
                            "required": ["question", "expected_sql"],
                        },
                    },
                    "generate_benchmarks": {
                        "type": "boolean",
                        "description": "If true, auto-generate benchmarks from example_sqls. Defaults to true.",
                    },
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "present_plan",
            "description": (
                "Present a structured plan for the user to review BEFORE generating the config. "
                "The frontend renders this as collapsible sections mirroring the Genie Space UI tabs. "
                "Call this after the business logic checkpoint — the user must approve the plan before "
                "you call generate_config. Use the SAME parameter shapes as generate_config."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sample_questions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "3-5 sample questions for business users",
                    },
                    "text_instructions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Business rules and domain guidance lines",
                    },
                    "joins": {
                        "type": "array",
                        "description": "Table join specifications",
                        "items": {
                            "type": "object",
                            "properties": {
                                "left_table": {"type": "string"},
                                "right_table": {"type": "string"},
                                "left_column": {"type": "string"},
                                "right_column": {"type": "string"},
                                "relationship": {"type": "string"},
                            },
                            "required": ["left_table", "right_table", "left_column", "right_column"],
                        },
                    },
                    "measures": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "display_name": {"type": "string"},
                                "sql": {"type": "string"},
                            },
                            "required": ["display_name", "sql"],
                        },
                    },
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "display_name": {"type": "string"},
                                "sql": {"type": "string"},
                            },
                            "required": ["display_name", "sql"],
                        },
                    },
                    "expressions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "display_name": {"type": "string"},
                                "sql": {"type": "string"},
                            },
                            "required": ["display_name", "sql"],
                        },
                    },
                    "example_sqls": {
                        "type": "array",
                        "description": "Example question-SQL pairs",
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "sql": {"type": "string"},
                            },
                            "required": ["question", "sql"],
                        },
                    },
                },
                "required": ["sample_questions"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_config",
            "description": "Validate a serialized_space config against all API rules. Returns errors (will cause API rejection) and warnings (best-practice recommendations). If config is omitted, validates the last config produced by generate_config.",
            "parameters": {
                "type": "object",
                "properties": {
                    "config": {"type": "object", "description": "The serialized_space dict to validate (optional — defaults to last generated config)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_space",
            "description": "Create the Genie space via the Databricks API. Call this only after the user has approved the config.",
            "parameters": {
                "type": "object",
                "properties": {
                    "display_name": {"type": "string", "description": "Display name for the space"},
                    "config": {"type": "object", "description": "The validated serialized_space dict (optional — defaults to last generated config)"},
                    "parent_path": {"type": "string", "description": "Workspace folder path for the space (optional)"},
                },
                "required": ["display_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_space",
            "description": "Update an existing Genie space with a new configuration. Use this instead of create_space when the space has already been created and the user wants to modify it.",
            "parameters": {
                "type": "object",
                "properties": {
                    "space_id": {"type": "string", "description": "The ID of the existing Genie space to update"},
                    "config": {"type": "object", "description": "The validated serialized_space dict (optional — defaults to last generated config)"},
                },
                "required": ["space_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_config",
            "description": (
                "Patch the existing serialized_space config in-place. Use this INSTEAD of generate_config "
                "for post-creation modifications. It directly mutates the current config — no rebuild, no "
                "new IDs, instant. Supports multiple actions in one call."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "actions": {
                        "type": "array",
                        "description": "List of patch actions to apply sequentially",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": [
                                        "enable_prompt_matching",
                                        "disable_prompt_matching",
                                        "update_instructions",
                                        "update_sample_questions",
                                        "add_example_sql",
                                        "remove_example_sql",
                                        "add_table",
                                        "remove_table",
                                        "update_table_description",
                                        "update_column_config",
                                    ],
                                    "description": "The type of modification to apply",
                                },
                                "tables": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Table identifiers to target (for enable/disable_prompt_matching). Omit to target ALL tables.",
                                },
                                "columns": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Column names to target within the specified tables. Omit to target ALL columns.",
                                },
                                "instructions": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "New text instruction lines (for update_instructions — replaces existing)",
                                },
                                "sample_questions": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "New sample questions (for update_sample_questions — replaces existing)",
                                },
                                "table_identifier": {
                                    "type": "string",
                                    "description": "catalog.schema.table (for add_table, remove_table, update_table_description, update_column_config)",
                                },
                                "description": {
                                    "type": "string",
                                    "description": "Table or column description text",
                                },
                                "question": {"type": "string", "description": "Example question (for add/remove_example_sql)"},
                                "sql": {"type": "string", "description": "SQL query (for add_example_sql)"},
                                "usage_guidance": {"type": "string", "description": "When to use this SQL (for add_example_sql)"},
                                "column_name": {"type": "string", "description": "Column name (for update_column_config)"},
                                "synonyms": {"type": "array", "items": {"type": "string"}, "description": "Column synonyms (for update_column_config)"},
                                "exclude": {"type": "boolean", "description": "Exclude column (for update_column_config)"},
                            },
                            "required": ["action"],
                        },
                    },
                },
                "required": ["actions"],
            },
        },
    },
]


# ── Tool implementations ─────────────────────────────────────────────────────

def handle_tool_call(name: str, arguments: dict, session_config: dict | None = None) -> dict:
    """Dispatch a tool call to the appropriate handler.

    session_config is the last config produced by generate_config (from the session).
    Tools like validate_config and create_space can fall back to it when the LLM
    omits the config argument.
    """
    handlers = {
        "discover_catalogs": _discover_catalogs,
        "discover_schemas": _discover_schemas,
        "discover_tables": _discover_tables,
        "describe_table": _describe_table,
        "profile_columns": _profile_columns,
        "test_sql": _test_sql,
        "discover_warehouses": _discover_warehouses,
        "present_plan": _present_plan,
        "get_config_schema": _get_config_schema,
        "generate_config": _generate_config,
        "update_config": _update_config,
        "validate_config": _validate_config,
        "create_space": _create_space,
        "update_space": _update_space,
    }
    handler = handlers.get(name)
    if handler is None:
        return {"error": f"Unknown tool: {name}"}

    # Inject session config when the LLM omits it
    if name in ("validate_config", "create_space", "update_space", "update_config") and "config" not in arguments:
        if session_config:
            arguments["config"] = session_config
        elif name == "validate_config":
            return {"error": "No config to validate — call generate_config first"}
        elif name == "update_config":
            return {"error": "No config to update — call generate_config first"}

    try:
        return handler(**arguments)
    except TypeError as e:
        err_msg = str(e)
        logger.exception(f"Tool {name} failed with TypeError")
        if name == "generate_config" and "missing" in err_msg:
            return {
                "error": err_msg,
                "hint": (
                    "generate_config requires 'tables' and 'sample_questions'. "
                    "For post-creation changes, use update_config instead. "
                    "If unsure about parameter shapes, call get_config_schema."
                ),
            }
        return {"error": err_msg}
    except Exception as e:
        logger.exception(f"Tool {name} failed")
        return {"error": str(e)}


def _discover_catalogs() -> dict:
    catalogs = list_catalogs()
    return {
        "catalogs": catalogs,
        "count": len(catalogs),
        "ui_hint": {"type": "single_select", "id": "catalog_selection", "label": "Select a catalog"},
    }


def _discover_schemas(catalog: str) -> dict:
    schemas = list_schemas(catalog)
    return {
        "schemas": schemas,
        "count": len(schemas),
        "ui_hint": {"type": "single_select", "id": "schema_selection", "label": "Select a schema"},
    }


def _discover_tables(catalog: str, schema: str) -> dict:
    tables = list_tables(catalog, schema)
    return {
        "tables": tables,
        "count": len(tables),
        "ui_hint": {"type": "multi_select", "id": "table_selection", "label": "Select tables to include"},
    }


def _describe_table(table_identifier: str) -> dict:
    """Get column metadata via the SDK. Flags potential PII columns.
    Also fetches a handful of sample rows and a link to the UC explorer."""
    client = get_workspace_client()
    try:
        table_info = client.tables.get(table_identifier)
    except Exception as e:
        return {"error": f"Cannot access table {table_identifier}: {e}"}

    columns = []
    for col in (table_info.columns or []):
        col_name = col.name or ""
        is_pii = bool(PII_PATTERNS.search(col_name))
        columns.append({
            "name": col_name,
            "type": str(col.type_text or col.type_name or ""),
            "description": col.comment,
            "pii_hint": is_pii,
        })

    # Fetch sample rows (best-effort)
    sample_rows: list[dict] = []
    try:
        col_names = [c["name"] for c in columns[:20]]
        cols_sql = ", ".join(f"`{c}`" for c in col_names)
        result = execute_sql(f"SELECT {cols_sql} FROM {table_identifier} LIMIT 5")
        if not result.get("error") and result.get("data"):
            for row in result["data"][:5]:
                sample_rows.append(dict(zip(col_names, row)))
    except Exception:
        pass

    # Build UC explorer URL
    parts = table_identifier.split(".")
    uc_url = ""
    if len(parts) == 3:
        host = get_databricks_host()
        if host:
            uc_url = f"{host}/explore/data/{parts[0]}/{parts[1]}/{parts[2]}"

    return {
        "table": table_identifier,
        "comment": table_info.comment,
        "columns": columns,
        "column_count": len(columns),
        "pii_columns": [c["name"] for c in columns if c["pii_hint"]],
        "sample_rows": sample_rows,
        "uc_url": uc_url,
    }


def _profile_columns(table_identifier: str, columns: list[str] | None = None) -> dict:
    """Profile columns by querying distinct values and date ranges."""
    if not columns:
        desc_result = _describe_table(table_identifier)
        if "error" in desc_result:
            return desc_result
        string_types = {"string", "varchar", "char", "boolean"}
        date_types = {"date", "timestamp", "timestamp_ntz"}
        columns_to_profile = []
        for col in desc_result["columns"]:
            col_type = col["type"].lower().split("<")[0].split("(")[0].strip()
            if col_type in string_types or col_type in date_types:
                columns_to_profile.append(col["name"])
        columns = columns_to_profile[:15]

    profiles = {}
    for col_name in columns:
        try:
            result = execute_sql(
                f"SELECT DISTINCT `{col_name}` FROM {table_identifier} "
                f"WHERE `{col_name}` IS NOT NULL ORDER BY `{col_name}` LIMIT 21"
            )
            if result.get("error"):
                profiles[col_name] = {"error": result["error"]}
                continue

            values = [row[0] for row in result.get("data", [])]
            profiles[col_name] = {
                "distinct_values": values[:20],
                "has_more": len(values) > 20,
            }
        except Exception as e:
            profiles[col_name] = {"error": str(e)}

    # Build UC explorer URL
    parts = table_identifier.split(".")
    uc_url = ""
    if len(parts) == 3:
        host = get_databricks_host()
        if host:
            uc_url = f"{host}/explore/data/{parts[0]}/{parts[1]}/{parts[2]}"

    return {"table": table_identifier, "profiles": profiles, "uc_url": uc_url}


def _test_sql(sql: str) -> dict:
    result = execute_sql(sql, row_limit=5)
    if result.get("error"):
        return {"success": False, "sql": sql, "error": result["error"]}
    return {
        "success": True,
        "sql": sql,
        "columns": result.get("columns", []),
        "sample_rows": result.get("data", [])[:5],
        "row_count": result.get("row_count", 0),
    }


def _discover_warehouses() -> dict:
    client = get_workspace_client()
    try:
        warehouses = list(client.warehouses.list())
    except Exception as e:
        return {"error": f"Failed to list warehouses: {e}"}

    eligible = []
    for wh in warehouses:
        is_serverless = getattr(wh, "enable_serverless_compute", False)
        wh_type_str = str(getattr(wh, "warehouse_type", "")) if hasattr(wh, "warehouse_type") else ""
        is_pro = wh_type_str == "PRO"
        if is_serverless or is_pro:
            eligible.append({
                "id": wh.id,
                "name": wh.name,
                "type": "Serverless" if is_serverless else "Pro",
                "state": str(wh.state) if wh.state else "UNKNOWN",
                "size": str(wh.cluster_size) if wh.cluster_size else "N/A",
            })

    return {
        "warehouses": eligible,
        "count": len(eligible),
        "configured_warehouse_id": get_sql_warehouse_id(),
        "ui_hint": {"type": "single_select", "id": "warehouse_selection", "label": "Select a SQL warehouse"},
    }


# ── Config generation (the hard part) ────────────────────────────────────────

def _split_sql_to_lines(sql: str) -> list[str]:
    """Split a SQL string into line-by-line array elements with \\n terminators."""
    lines = sql.strip().split("\n")
    result = []
    for i, line in enumerate(lines):
        if i < len(lines) - 1:
            result.append(line + "\n")
        else:
            result.append(line)
    return result


def _get_config_schema() -> dict:
    """Return the serialized_space schema reference for the LLM."""
    schema_path = Path(__file__).parent.parent / "references" / "schema.md"
    try:
        content = schema_path.read_text()
    except FileNotFoundError:
        return {"error": "Schema reference file not found"}
    return {"schema_reference": content}


def _present_plan(
    sample_questions: list[str],
    text_instructions: list[str] | None = None,
    joins: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    example_sqls: list[dict] | None = None,
) -> dict:
    """Pass structured plan data through for frontend rendering."""
    sections: dict[str, Any] = {}

    sections["sample_questions"] = sample_questions or []
    sections["text_instructions"] = text_instructions or []
    sections["joins"] = joins or []
    sections["measures"] = measures or []
    sections["filters"] = filters or []
    sections["expressions"] = expressions or []
    sections["example_sqls"] = example_sqls or []

    total = sum(len(v) for v in sections.values() if isinstance(v, list))
    return {
        "sections": sections,
        "total_items": total,
        "ui_hint": {"type": "plan_review", "id": "plan_review", "label": "Review the plan"},
    }


def _generate_config(
    tables: list[dict],
    sample_questions: list[str] | None = None,
    text_instructions: list[str] | None = None,
    example_sqls: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    join_specs: list[dict] | None = None,
    benchmarks: list[dict] | None = None,
    generate_benchmarks: bool = True,
) -> dict:
    """Build a complete serialized_space config from structured inputs.

    Use this for INITIAL creation only. For post-creation modifications,
    use update_config instead.
    """
    if sample_questions is None:
        sample_questions = []

    config: dict[str, Any] = {"version": 2}

    # ── sample_questions ──
    sq_items = []
    for q in sample_questions:
        sq_items.append({"id": secrets.token_hex(16), "question": [q]})
    sq_items.sort(key=lambda x: x["id"])
    config["config"] = {"sample_questions": sq_items}

    # ── data_sources.tables ──
    ds_tables = []
    for tbl in tables:
        entry: dict[str, Any] = {"identifier": tbl["identifier"]}
        if tbl.get("description"):
            entry["description"] = [tbl["description"]]
        if tbl.get("column_configs"):
            cc_list = []
            for cc in tbl["column_configs"]:
                cc_entry: dict[str, Any] = {"column_name": cc["column_name"]}
                if cc.get("description"):
                    cc_entry["description"] = [cc["description"]]
                if cc.get("synonyms"):
                    cc_entry["synonyms"] = cc["synonyms"]
                if cc.get("exclude"):
                    cc_entry["exclude"] = True
                    cc_entry["enable_format_assistance"] = False
                    cc_entry["enable_entity_matching"] = False
                elif cc.get("enable_matching") is False:
                    cc_entry["enable_format_assistance"] = False
                    cc_entry["enable_entity_matching"] = False
                else:
                    cc_entry["enable_format_assistance"] = True
                    cc_entry["enable_entity_matching"] = True
                cc_list.append(cc_entry)
            cc_list.sort(key=lambda x: x["column_name"])
            entry["column_configs"] = cc_list
        ds_tables.append(entry)
    ds_tables.sort(key=lambda x: x["identifier"])
    config["data_sources"] = {"tables": ds_tables}

    # ── instructions ──
    instructions: dict[str, Any] = {}

    # text_instructions
    if text_instructions:
        content_lines = [line if line.endswith("\n") else line + "\n" for line in text_instructions]
        instructions["text_instructions"] = [{
            "id": secrets.token_hex(16),
            "content": content_lines,
        }]

    # example_question_sqls
    if example_sqls:
        eq_items = []
        for eq in example_sqls:
            entry = {
                "id": secrets.token_hex(16),
                "question": [eq["question"]],
                "sql": _split_sql_to_lines(eq["sql"]),
            }
            if eq.get("usage_guidance"):
                entry["usage_guidance"] = [eq["usage_guidance"]]
            eq_items.append(entry)
        eq_items.sort(key=lambda x: x["id"])
        instructions["example_question_sqls"] = eq_items

    # sql_snippets
    snippets: dict[str, list] = {}

    if measures:
        m_items = []
        for m in measures:
            entry = {"id": secrets.token_hex(16), "alias": m["alias"], "sql": [m["sql"]]}
            if m.get("display_name"):
                entry["display_name"] = m["display_name"]
            if m.get("synonyms"):
                entry["synonyms"] = m["synonyms"]
            if m.get("instruction"):
                entry["instruction"] = [m["instruction"]]
            m_items.append(entry)
        m_items.sort(key=lambda x: x["id"])
        snippets["measures"] = m_items

    if filters:
        f_items = []
        for f in filters:
            sql_str = f["sql"]
            if sql_str.strip().upper().startswith("WHERE "):
                sql_str = sql_str.strip()[6:]
            entry = {"id": secrets.token_hex(16), "display_name": f["display_name"], "sql": [sql_str]}
            if f.get("synonyms"):
                entry["synonyms"] = f["synonyms"]
            if f.get("instruction"):
                entry["instruction"] = [f["instruction"]]
            f_items.append(entry)
        f_items.sort(key=lambda x: x["id"])
        snippets["filters"] = f_items

    if expressions:
        e_items = []
        for e in expressions:
            entry = {"id": secrets.token_hex(16), "alias": e["alias"], "sql": [e["sql"]]}
            if e.get("display_name"):
                entry["display_name"] = e["display_name"]
            if e.get("synonyms"):
                entry["synonyms"] = e["synonyms"]
            if e.get("instruction"):
                entry["instruction"] = [e["instruction"]]
            e_items.append(entry)
        e_items.sort(key=lambda x: x["id"])
        snippets["expressions"] = e_items

    if snippets:
        instructions["sql_snippets"] = snippets

    # join_specs
    if join_specs:
        js_items = []
        for js in join_specs:
            la = js.get("left_alias", js["left_table"].split(".")[-1])
            ra = js.get("right_alias", js["right_table"].split(".")[-1])
            condition = f"`{la}`.`{js['left_column']}` = `{ra}`.`{js['right_column']}`"
            rt = f"--rt=FROM_RELATIONSHIP_TYPE_{js['relationship']}--"
            entry: dict[str, Any] = {
                "id": secrets.token_hex(16),
                "left": {"identifier": js["left_table"], "alias": la},
                "right": {"identifier": js["right_table"], "alias": ra},
                "sql": [condition, rt],
            }
            if js.get("instruction"):
                entry["instruction"] = [js["instruction"]]
            js_items.append(entry)
        js_items.sort(key=lambda x: x["id"])
        instructions["join_specs"] = js_items

    if instructions:
        config["instructions"] = instructions

    # ── benchmarks ──
    bench_items = []
    if benchmarks:
        for b in benchmarks:
            bench_items.append({
                "id": secrets.token_hex(16),
                "question": [b["question"]],
                "answer": [{"format": "SQL", "content": _split_sql_to_lines(b["expected_sql"])}],
            })
    elif generate_benchmarks and example_sqls:
        for eq in example_sqls:
            bench_items.append({
                "id": secrets.token_hex(16),
                "question": [eq["question"]],
                "answer": [{"format": "SQL", "content": _split_sql_to_lines(eq["sql"])}],
            })

    if bench_items:
        bench_items.sort(key=lambda x: x["id"])
        config["benchmarks"] = {"questions": bench_items}

    return {
        "config": config,
        "summary": {
            "tables": len(tables),
            "sample_questions": len(sample_questions),
            "example_sqls": len(example_sqls or []),
            "measures": len(measures or []),
            "filters": len(filters or []),
            "expressions": len(expressions or []),
            "join_specs": len(join_specs or []),
            "benchmarks": len(bench_items),
            "text_instructions": len(text_instructions or []),
        },
        "ui_hint": {"type": "config_preview", "id": "config_review", "label": "Review the generated configuration"},
    }


# ── Config patching ───────────────────────────────────────────────────────────


def _update_config(actions: list[dict], config: dict | None = None) -> dict:
    """Apply targeted patches to an existing serialized_space config."""
    if not config:
        return {"error": "No existing config to update — call generate_config first"}

    cfg = copy.deepcopy(config)
    applied: list[str] = []

    for act in actions:
        action = act["action"]

        if action in ("enable_prompt_matching", "disable_prompt_matching"):
            enable = action == "enable_prompt_matching"
            target_tables = act.get("tables")
            target_cols = act.get("columns")
            count = 0
            for tbl in cfg.get("data_sources", {}).get("tables", []):
                if target_tables and tbl["identifier"] not in target_tables:
                    continue
                ccs = tbl.get("column_configs", [])
                for cc in ccs:
                    if cc.get("exclude"):
                        continue
                    if target_cols and cc["column_name"] not in target_cols:
                        continue
                    cc["enable_format_assistance"] = enable
                    cc["enable_entity_matching"] = enable
                    count += 1
            applied.append(f"{'Enabled' if enable else 'Disabled'} prompt matching on {count} columns")

        elif action == "update_instructions":
            lines = act.get("instructions", [])
            content = [line if line.endswith("\n") else line + "\n" for line in lines]
            ti_list = cfg.setdefault("instructions", {}).get("text_instructions", [])
            if ti_list:
                ti_list[0]["content"] = content
            else:
                cfg["instructions"]["text_instructions"] = [{
                    "id": secrets.token_hex(16),
                    "content": content,
                }]
            applied.append(f"Updated text instructions ({len(lines)} lines)")

        elif action == "update_sample_questions":
            questions = act.get("sample_questions", [])
            sq_items = [{"id": secrets.token_hex(16), "question": [q]} for q in questions]
            sq_items.sort(key=lambda x: x["id"])
            cfg.setdefault("config", {})["sample_questions"] = sq_items
            applied.append(f"Updated sample questions ({len(questions)})")

        elif action == "add_example_sql":
            question = act.get("question", "")
            sql = act.get("sql", "")
            if not question or not sql:
                applied.append("Skipped add_example_sql — question and sql required")
                continue
            entry: dict[str, Any] = {
                "id": secrets.token_hex(16),
                "question": [question],
                "sql": _split_sql_to_lines(sql),
            }
            if act.get("usage_guidance"):
                entry["usage_guidance"] = [act["usage_guidance"]]
            eqs = cfg.setdefault("instructions", {}).setdefault("example_question_sqls", [])
            eqs.append(entry)
            eqs.sort(key=lambda x: x["id"])
            applied.append(f"Added example SQL: {question[:60]}")

        elif action == "remove_example_sql":
            question = act.get("question", "").lower()
            eqs = cfg.get("instructions", {}).get("example_question_sqls", [])
            before = len(eqs)
            cfg["instructions"]["example_question_sqls"] = [
                eq for eq in eqs if eq.get("question", [""])[0].lower() != question
            ]
            removed = before - len(cfg["instructions"]["example_question_sqls"])
            applied.append(f"Removed {removed} example SQL(s) matching '{question[:40]}'")

        elif action == "add_table":
            ident = act.get("table_identifier", "")
            if not ident:
                applied.append("Skipped add_table — table_identifier required")
                continue
            tables = cfg.setdefault("data_sources", {}).setdefault("tables", [])
            if any(t["identifier"] == ident for t in tables):
                applied.append(f"Table {ident} already present")
                continue
            new_tbl: dict[str, Any] = {"identifier": ident}
            if act.get("description"):
                new_tbl["description"] = [act["description"]]
            tables.append(new_tbl)
            tables.sort(key=lambda x: x["identifier"])
            applied.append(f"Added table {ident}")

        elif action == "remove_table":
            ident = act.get("table_identifier", "")
            tables = cfg.get("data_sources", {}).get("tables", [])
            before = len(tables)
            cfg["data_sources"]["tables"] = [t for t in tables if t["identifier"] != ident]
            removed = before - len(cfg["data_sources"]["tables"])
            applied.append(f"Removed {removed} table(s) matching {ident}")

        elif action == "update_table_description":
            ident = act.get("table_identifier", "")
            desc = act.get("description", "")
            for tbl in cfg.get("data_sources", {}).get("tables", []):
                if tbl["identifier"] == ident:
                    tbl["description"] = [desc]
                    applied.append(f"Updated description for {ident}")
                    break
            else:
                applied.append(f"Table {ident} not found")

        elif action == "update_column_config":
            ident = act.get("table_identifier", "")
            col_name = act.get("column_name", "")
            for tbl in cfg.get("data_sources", {}).get("tables", []):
                if tbl["identifier"] != ident:
                    continue
                ccs = tbl.setdefault("column_configs", [])
                cc_match = next((c for c in ccs if c["column_name"] == col_name), None)
                if not cc_match:
                    cc_match = {"column_name": col_name}
                    ccs.append(cc_match)
                    ccs.sort(key=lambda x: x["column_name"])
                if act.get("description") is not None:
                    cc_match["description"] = [act["description"]]
                if act.get("synonyms") is not None:
                    cc_match["synonyms"] = act["synonyms"]
                if act.get("exclude") is not None:
                    cc_match["exclude"] = act["exclude"]
                    if act["exclude"]:
                        cc_match["enable_format_assistance"] = False
                        cc_match["enable_entity_matching"] = False
                applied.append(f"Updated column {col_name} on {ident}")
                break
            else:
                applied.append(f"Table {ident} not found")

        else:
            applied.append(f"Unknown action: {action}")

    return {
        "config": cfg,
        "applied": applied,
        "action_count": len(applied),
    }


# ── Validation ────────────────────────────────────────────────────────────────

_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_TABLE_ID_PATTERN = re.compile(r"^[^.]+\.[^.]+\.[^.]+$")


def _validate_config(config: dict) -> dict:
    """Validate a serialized_space config. Returns errors and warnings."""
    errors = []
    warnings = []

    def error(path, msg):
        errors.append({"path": path, "message": msg})

    def warning(path, msg):
        warnings.append({"path": path, "message": msg})

    # version
    if config.get("version") is None:
        error("version", "Missing required 'version' field")

    # sample_questions
    sqs = config.get("config", {}).get("sample_questions", [])
    if not sqs:
        warning("config.sample_questions", "No sample questions defined")
    else:
        _check_sorted(sqs, lambda x: x.get("id", ""), "id", "config.sample_questions", error)
        for i, sq in enumerate(sqs):
            _check_id(f"config.sample_questions[{i}].id", sq.get("id"), error)

    # tables
    tables = config.get("data_sources", {}).get("tables", [])
    if not tables:
        error("data_sources.tables", "No tables defined")
    else:
        _check_sorted(tables, lambda x: x.get("identifier", ""), "identifier", "data_sources.tables", error)
        if len(tables) > 25:
            error("data_sources.tables", f"Maximum 25 tables allowed (found {len(tables)})")
        elif len(tables) > 5:
            warning("data_sources.tables", f"{len(tables)} tables — recommend ≤5 for accuracy")
        for i, tbl in enumerate(tables):
            ident = tbl.get("identifier", "")
            if not _TABLE_ID_PATTERN.match(ident):
                error(f"data_sources.tables[{i}].identifier", f"'{ident}' must be catalog.schema.table")

    # text_instructions
    ti = config.get("instructions", {}).get("text_instructions", [])
    if len(ti) > 1:
        error("instructions.text_instructions", f"Max 1 text instruction allowed, found {len(ti)}")

    # example_question_sqls
    eqs = config.get("instructions", {}).get("example_question_sqls", [])
    if eqs:
        _check_sorted(eqs, lambda x: x.get("id", ""), "id", "instructions.example_question_sqls", error)
        for i, eq in enumerate(eqs):
            _check_id(f"instructions.example_question_sqls[{i}].id", eq.get("id"), error)
            sql = eq.get("sql", [])
            if not sql:
                error(f"instructions.example_question_sqls[{i}].sql", "SQL must not be empty")

    # join_specs
    jss = config.get("instructions", {}).get("join_specs", [])
    if jss:
        _check_sorted(jss, lambda x: x.get("id", ""), "id", "instructions.join_specs", error)
        for i, js in enumerate(jss):
            sql = js.get("sql", [])
            if not any(isinstance(s, str) and s.startswith("--rt=") for s in sql):
                error(f"instructions.join_specs[{i}].sql", "Missing --rt= relationship type annotation")

    # sql_snippets
    snippets = config.get("instructions", {}).get("sql_snippets", {})
    for stype in ("filters", "expressions", "measures"):
        items = snippets.get(stype, [])
        if items:
            _check_sorted(items, lambda x: x.get("id", ""), "id", f"instructions.sql_snippets.{stype}", error)

    # instruction count
    total = len(eqs) + len(config.get("instructions", {}).get("sql_functions", [])) + (1 if ti else 0)
    if total > 100:
        error("instructions", f"Total instruction count {total} exceeds 100 limit")
    elif total > 80:
        warning("instructions", f"Instruction count {total}/100 — approaching limit")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "error_count": len(errors),
        "warning_count": len(warnings),
    }


def _check_id(path: str, id_val: Any, error_fn) -> None:
    if id_val is None:
        error_fn(path, "Missing ID")
    elif not isinstance(id_val, str) or not _ID_PATTERN.match(id_val):
        error_fn(path, f"Invalid ID format: {id_val}")


def _check_sorted(items: list, key_fn, key_name: str, path: str, error_fn) -> None:
    keys = [key_fn(item) for item in items]
    for i in range(1, len(keys)):
        if keys[i] < keys[i - 1]:
            error_fn(path, f"Array must be sorted by '{key_name}'")
            return


def _create_space(display_name: str, config: dict, parent_path: str | None = None) -> dict:
    """Create the Genie space via the API."""
    try:
        result = create_genie_space(
            display_name=display_name,
            merged_config=config,
            parent_path=parent_path,
        )
        return {
            "success": True,
            "space_id": result["genie_space_id"],
            "display_name": result["display_name"],
            "space_url": result["space_url"],
        }
    except (ValueError, PermissionError, TimeoutError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("create_space failed")
        return {"success": False, "error": str(e)}


def _update_space(space_id: str, config: dict) -> dict:
    """Update an existing Genie space with a new configuration."""
    try:
        from backend.services.auth import get_workspace_client, get_databricks_host
        from backend.genie_creator import _enforce_constraints, _clean_config

        constrained = _enforce_constraints(config)
        cleaned = _clean_config(constrained)
        serialized = json.dumps(cleaned)

        warehouse_id = get_sql_warehouse_id()

        body: dict[str, Any] = {"serialized_space": serialized}
        if warehouse_id:
            body["warehouse_id"] = warehouse_id

        client = get_workspace_client()
        client.api_client.do(
            method="PATCH",
            path=f"/api/2.0/genie/spaces/{space_id}",
            body=body,
        )
        host = get_databricks_host()
        return {
            "success": True,
            "space_id": space_id,
            "url": f"{host}/genie/rooms/{space_id}",
            "message": "Space updated successfully.",
        }
    except Exception as e:
        logger.exception("update_space failed")
        return {"success": False, "error": str(e)}
