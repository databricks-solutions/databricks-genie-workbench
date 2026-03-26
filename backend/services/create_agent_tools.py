"""Tool implementations for the Create Genie agent.

Each tool returns a dict that gets serialized as the tool result for the LLM.
Tools handle all mechanical formatting — the LLM provides content, tools handle structure.
"""

import copy
import json
import logging
import re
import secrets
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import mlflow
from mlflow.entities import SpanType

from backend.services.auth import get_workspace_client, get_databricks_host, run_in_context
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

ETL_PATTERNS = re.compile(
    r"(^_etl|^_load|^_ingest|^_pipeline|^_batch"
    r"|^_job_id$|^_run_id$|^_task_id$|^_execution_id$"
    r"|_created_at$|_updated_at$|_modified_at$|_inserted_at$|_loaded_at$"
    r"|^__[a-z]|^_dlt_|^_rescued_data$|^_metadata$"
    r"|^dwh_|^stg_|^src_|^etl_)", re.IGNORECASE
)

_STRING_TYPES = {"string", "varchar", "char"}
_DATE_TYPES = {"date", "timestamp", "timestamp_ntz"}
_NUMERIC_TYPES = {"int", "bigint", "smallint", "tinyint", "float", "double", "decimal"}
_BOOLEAN_TYPES = {"boolean"}

# Genie API accepts: STRING, INTEGER, DOUBLE, DECIMAL, DATE, BOOLEAN.
# Map common LLM-generated aliases to valid values.
_TYPE_HINT_MAP = {
    "NUMBER": "INTEGER", "INT": "INTEGER", "BIGINT": "INTEGER",
    "SMALLINT": "INTEGER", "TINYINT": "INTEGER",
    "FLOAT": "DOUBLE", "TIMESTAMP": "DATE",
}

# Cap concurrent SQL statements to avoid overwhelming the warehouse.
# 8 is appropriate for a Large warehouse shared across 3 app instances.
_SQL_CONCURRENCY = 8
_sql_semaphore = threading.Semaphore(_SQL_CONCURRENCY)


def _execute_sql_throttled(sql: str, **kwargs) -> dict:
    """Execute SQL through the shared concurrency limiter."""
    with _sql_semaphore:
        return execute_sql(sql, **kwargs)


def _base_col_type(type_text: str) -> str:
    """Normalize a column type to its base name (strip generics and precision)."""
    return type_text.lower().split("<")[0].split("(")[0].strip()


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
            "description": (
                "Get detailed column metadata for a table: column names, types, descriptions, "
                "and recommendations (ETL/metadata columns to exclude). "
                "Returns a recommendations summary with columns grouped by reason."
            ),
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
            "name": "assess_data_quality",
            "description": (
                "Assess data quality across one or more tables in parallel. "
                "For each table returns: null rates per column, empty/whitespace strings, "
                "boolean-as-string values (true/TRUE/True), inconsistent casing variants, "
                "constant columns (single value), and all-null columns. "
                "Use after describe_table to decide which columns to exclude or flag."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_identifiers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "One or more fully qualified table names (catalog.schema.table). Tables are assessed in parallel.",
                    },
                },
                "required": ["table_identifiers"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "profile_table_usage",
            "description": (
                "Discover how tables are used in the workspace: upstream/downstream lineage "
                "and recent query patterns from system tables. Best-effort — returns partial "
                "results if system tables are inaccessible. Use alongside assess_data_quality "
                "(both can be called in the same tool-call round)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_identifiers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fully qualified table names (catalog.schema.table).",
                    },
                },
                "required": ["table_identifiers"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "test_sql",
            "description": "Execute a SQL query to verify it runs successfully. Use this to test example SQL queries before including them in the config. Returns column names, first few rows, and row count. For parameterized SQL (using :param_name syntax), pass the parameters with default_value so the query can be tested with real values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "The SQL query to test"},
                    "parameters": {
                        "type": "array",
                        "description": "Parameters for parameterized SQL. Each parameter's default_value is substituted for :param_name before execution.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "Parameter name matching :name in the SQL"},
                                "default_value": {"type": "string", "description": "Value to substitute for testing"},
                            },
                            "required": ["name", "default_value"],
                        },
                    },
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
                        "description": "5 sample questions shown to users in the Genie Space UI as click-to-ask suggestions",
                    },
                    "text_instructions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Business rules injected into Genie's LLM prompt. Keep each instruction concise. "
                            "Focus on: terminology definitions ('revenue' means net after discounts), "
                            "default assumptions (default to current year when unspecified), "
                            "fiscal calendar rules, data quality warnings, and cross-table business logic. "
                            "Do NOT duplicate what's already in measures, filters, expressions, or joins. "
                            "Include any business rules the user explicitly stated."
                        ),
                    },
                    "example_sqls": {
                        "type": "array",
                        "minItems": 3,
                        "description": (
                            "At least 3 complex example question-SQL pairs that teach Genie how to write SQL "
                            "(few-shot learning). Keep each SQL query concise. "
                            "Make them non-trivial: multi-join, aggregations with "
                            "filters, date ranges, CASE expressions, etc."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "sql": {"type": "string", "description": "The full SQL query as a single string. Use :param_name for parameterized values."},
                                "usage_guidance": {"type": "string"},
                                "parameters": {
                                    "type": "array",
                                    "description": "Parameters for parameterized SQL (using :param_name in the query). Always include a default_value — it teaches Genie what valid values look like so it can extract the real value from user questions.",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string", "description": "Parameter name matching :name in the SQL"},
                                            "type_hint": {"type": "string", "enum": ["STRING", "NUMBER", "DATE", "BOOLEAN"]},
                                            "description": {"type": "string", "description": "What this parameter represents, with 2-3 REAL values from the data (e.g., 'The region. Values: North America, EMEA, APJ')"},
                                            "default_value": {"type": "string", "description": "A REAL value from the data (e.g., 'North America'). Genie runs the query with this value, so it must produce valid results."},
                                        },
                                        "required": ["name", "type_hint", "default_value"],
                                    },
                                },
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
                                "instruction": {"type": "string", "description": "When Genie should use this measure (e.g., 'Use for any revenue aggregation')"},
                                "comment": {"type": "string", "description": "Internal note about the measure definition or business context"},
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
                                "instruction": {"type": "string", "description": "When Genie should apply this filter (e.g., 'Apply when users ask about high-value orders')"},
                                "comment": {"type": "string", "description": "Internal note about threshold or business context"},
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
                                "instruction": {"type": "string", "description": "When Genie should use this expression (e.g., 'Use for year-based grouping')"},
                                "comment": {"type": "string", "description": "Internal note about this computed column"},
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
                                "instruction": {"type": "string", "description": "When Genie should use this join"},
                                "comment": {"type": "string", "description": "Description of the relationship (e.g., 'Join orders to customers on customer_id')"},
                            },
                            "required": ["left_table", "left_alias", "right_table", "right_alias", "left_column", "right_column", "relationship"],
                        },
                    },
                    "benchmarks": {
                        "type": "array",
                        "description": (
                            "10 benchmark question-SQL pairs for evaluating Genie accuracy. "
                            "Pass these from the plan's benchmarks section. Each must have question + expected_sql."
                        ),
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
                        "description": "If true and no benchmarks provided, auto-generate from example_sqls. Defaults to false.",
                    },
                    "metric_views": {
                        "type": "array",
                        "description": "Optional metric views to include. Only add if discover_tables found metric views in the schema.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "identifier": {"type": "string", "description": "catalog.schema.metric_view_name"},
                                "description": {"type": "string"},
                                "column_configs": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "column_name": {"type": "string"},
                                            "description": {"type": "string"},
                                            "enable_format_assistance": {"type": "boolean"},
                                        },
                                        "required": ["column_name"],
                                    },
                                },
                            },
                            "required": ["identifier"],
                        },
                    },
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_plan",
            "description": (
                "Generate the complete Genie Space plan using PARALLEL LLM calls (4x faster than "
                "building it manually). Extracts table context and inspection findings from session "
                "history automatically — you only need to pass user_requirements summarizing the "
                "user's goals and business context. Returns the plan as a present_plan result for "
                "user review. Use this INSTEAD of calling present_plan with manually constructed data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "user_requirements": {
                        "type": "string",
                        "description": (
                            "Summary of the user's goals, audience, business context, and any "
                            "specific rules they mentioned. Include terminology definitions, "
                            "fiscal calendars, default assumptions — anything the plan should reflect."
                        ),
                    },
                },
                "required": ["user_requirements"],
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
                "Prefer generate_plan (parallel, faster) unless you need to manually construct "
                "or revise specific plan sections. The user must approve the plan before "
                "you call generate_config. Parameters are IDENTICAL to generate_config so the plan "
                "is a 1:1 preview of what will be created."
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
                        "description": "5 sample questions shown to users in the Genie Space UI as click-to-ask suggestions",
                    },
                    "text_instructions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Business rules injected into Genie's LLM prompt. Keep each instruction concise. "
                            "Focus on: terminology definitions ('revenue' means net after discounts), "
                            "default assumptions (default to current year when unspecified), "
                            "fiscal calendar rules, data quality warnings, and cross-table business logic. "
                            "Do NOT duplicate what's already in measures, filters, expressions, or joins. "
                            "Include any business rules the user explicitly stated."
                        ),
                    },
                    "example_sqls": {
                        "type": "array",
                        "minItems": 3,
                        "description": (
                            "At least 3 complex example question-SQL pairs that teach Genie how to write SQL "
                            "(few-shot learning). Keep each SQL query concise. "
                            "Make them non-trivial: multi-join, aggregations with "
                            "filters, date ranges, CASE expressions, etc."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "sql": {"type": "string", "description": "The full SQL query as a single string. Use :param_name for parameterized values."},
                                "usage_guidance": {"type": "string"},
                                "parameters": {
                                    "type": "array",
                                    "description": "Parameters for parameterized SQL (using :param_name in the query).",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "type_hint": {"type": "string", "enum": ["STRING", "NUMBER", "DATE", "BOOLEAN"]},
                                            "description": {"type": "string"},
                                            "default_value": {"type": "string"},
                                        },
                                        "required": ["name", "type_hint", "default_value"],
                                    },
                                },
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
                                "comment": {"type": "string"},
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
                                "comment": {"type": "string"},
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
                                "comment": {"type": "string"},
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
                                "instruction": {"type": "string", "description": "When Genie should use this join"},
                                "comment": {"type": "string", "description": "Description of the relationship"},
                            },
                            "required": ["left_table", "left_alias", "right_table", "right_alias", "left_column", "right_column", "relationship"],
                        },
                    },
                    "benchmarks": {
                        "type": "array",
                        "description": (
                            "10 benchmark question-SQL pairs for evaluating Genie accuracy. "
                            "These are TEST questions — separate from sample_questions and example_sqls. "
                            "Each must have question + expected_sql."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string"},
                                "expected_sql": {"type": "string"},
                            },
                            "required": ["question", "expected_sql"],
                        },
                    },
                    "metric_views": {
                        "type": "array",
                        "description": "Optional metric views to include. Only add if discover_tables found metric views in the schema.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "identifier": {"type": "string", "description": "catalog.schema.metric_view_name"},
                                "description": {"type": "string"},
                                "column_configs": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "column_name": {"type": "string"},
                                            "description": {"type": "string"},
                                            "enable_format_assistance": {"type": "boolean"},
                                        },
                                        "required": ["column_name"],
                                    },
                                },
                            },
                            "required": ["identifier"],
                        },
                    },
                },
                "required": ["sample_questions", "example_sqls", "benchmarks"],
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
            "description": "Update an existing Genie space — config, display name, or both. Use this instead of create_space when the space has already been created. Supports renaming.",
            "parameters": {
                "type": "object",
                "properties": {
                    "space_id": {"type": "string", "description": "The ID of the existing Genie space to update"},
                    "config": {"type": "object", "description": "The validated serialized_space dict (optional — defaults to last generated config)"},
                    "display_name": {"type": "string", "description": "New display name for the space (optional — only if renaming)"},
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
                "new IDs, instant. Supports multiple actions in one call. "
                "For business rules, use add_instruction_line (appends) instead of update_instructions (replaces all)."
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
                                        "add_instruction_line",
                                        "remove_instruction_line",
                                        "update_sample_questions",
                                        "add_example_sql",
                                        "remove_example_sql",
                                        "add_table",
                                        "remove_table",
                                        "update_table_description",
                                        "update_column_config",
                                        "add_benchmark",
                                        "remove_benchmark",
                                        "update_benchmarks",
                                        "add_join",
                                        "remove_join",
                                        "add_measure",
                                        "remove_measure",
                                        "add_filter",
                                        "remove_filter",
                                        "add_expression",
                                        "remove_expression",
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
                                "instruction_line": {
                                    "type": "string",
                                    "description": "A single instruction line to add or remove (for add_instruction_line / remove_instruction_line). For business rules, domain definitions, or data quality warnings.",
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
                                "question": {"type": "string", "description": "Question text (for add/remove_example_sql, add/remove_benchmark)"},
                                "sql": {"type": "string", "description": "SQL query (for add_example_sql, add_measure, add_filter, add_expression)"},
                                "expected_sql": {"type": "string", "description": "Expected SQL answer (for add_benchmark)"},
                                "usage_guidance": {"type": "string", "description": "When to use this SQL (for add_example_sql)"},
                                "column_name": {"type": "string", "description": "Column name (for update_column_config)"},
                                "synonyms": {"type": "array", "items": {"type": "string"}, "description": "Synonyms (for update_column_config, add_measure, add_filter, add_expression)"},
                                "exclude": {"type": "boolean", "description": "Exclude column (for update_column_config)"},
                                "benchmarks": {
                                    "type": "array",
                                    "description": "Full replacement benchmark list (for update_benchmarks). Each item needs question + expected_sql.",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "question": {"type": "string"},
                                            "expected_sql": {"type": "string"},
                                        },
                                        "required": ["question", "expected_sql"],
                                    },
                                },
                                "left_table": {"type": "string", "description": "catalog.schema.table (for add_join, remove_join)"},
                                "right_table": {"type": "string", "description": "catalog.schema.table (for add_join, remove_join)"},
                                "left_column": {"type": "string", "description": "Join column on left table (for add_join)"},
                                "right_column": {"type": "string", "description": "Join column on right table (for add_join)"},
                                "left_alias": {"type": "string", "description": "Alias for left table (for add_join — defaults to table short name)"},
                                "right_alias": {"type": "string", "description": "Alias for right table (for add_join — defaults to table short name)"},
                                "relationship": {
                                    "type": "string",
                                    "enum": ["MANY_TO_ONE", "ONE_TO_MANY", "ONE_TO_ONE", "MANY_TO_MANY"],
                                    "description": "Join cardinality (for add_join, defaults to ONE_TO_MANY)",
                                },
                                "display_name": {"type": "string", "description": "Display name (for add_measure, add_filter, add_expression)"},
                                "alias": {"type": "string", "description": "SQL alias (for add_expression, remove_expression)"},
                                "instruction": {"type": "string", "description": "Usage instruction (for add_join, add_measure, add_filter, add_expression)"},
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
        "assess_data_quality": _assess_data_quality,
        "profile_table_usage": _profile_table_usage,
        "profile_columns": _profile_columns,
        "test_sql": _test_sql,
        "discover_warehouses": _discover_warehouses,
        "generate_plan": _generate_plan_fallback,
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
        hint = (
            "If unsure about parameter shapes, call get_config_schema. "
            f"Received arguments: {list(arguments.keys())}"
        )
        if name == "generate_config":
            hint = (
                "generate_config requires 'tables'. "
                "For post-creation changes, use update_config instead. " + hint
            )
        elif name == "present_plan":
            hint = (
                "present_plan accepts the same parameters as generate_config: "
                "tables, sample_questions, text_instructions, example_sqls, "
                "join_specs, measures, filters, expressions, benchmarks. " + hint
            )
        return {"error": err_msg, "hint": hint}
    except Exception as e:
        logger.exception(f"Tool {name} failed")
        return {"error": str(e)}


def _discover_catalogs() -> dict:
    catalogs = list_catalogs()
    return {
        "catalogs": catalogs,
        "count": len(catalogs),
        "ui_hint": {"type": "multi_select", "id": "catalog_selection", "label": "Select catalogs"},
    }


def _discover_schemas(catalog: str) -> dict:
    schemas = list_schemas(catalog)
    return {
        "schemas": schemas,
        "count": len(schemas),
        "ui_hint": {"type": "multi_select", "id": "schema_selection", "label": "Select schemas"},
    }


def _discover_tables(catalog: str, schema: str) -> dict:
    tables = list_tables(catalog, schema)

    metric_views: list[dict] = []
    try:
        result = execute_sql(
            f"SHOW METRIC VIEWS IN `{catalog}`.`{schema}`",
            row_limit=100,
        )
        if result.get("data") and result.get("columns"):
            name_idx = next(
                (i for i, c in enumerate(result["columns"]) if c["name"].lower() in ("viewname", "name", "view_name")),
                0,
            )
            for row in result["data"]:
                mv_name = row[name_idx] if name_idx < len(row) else None
                if mv_name:
                    metric_views.append({
                        "name": mv_name,
                        "full_name": f"{catalog}.{schema}.{mv_name}",
                        "type": "METRIC_VIEW",
                    })
    except Exception:
        pass

    response: dict[str, Any] = {
        "tables": tables,
        "count": len(tables),
        "ui_hint": {"type": "multi_select", "id": "table_selection", "label": "Select tables to include"},
    }

    if metric_views:
        response["metric_views"] = metric_views
        response["metric_view_count"] = len(metric_views)

    return response


@mlflow.trace(name="describe_table", span_type=SpanType.TOOL)
def _describe_table(table_identifier: str) -> dict:
    """Get column metadata via the SDK.

    Flags ETL/metadata columns with structured recommendations the
    agent and frontend can act on.
    """
    client = get_workspace_client()
    try:
        table_info = client.tables.get(table_identifier)
    except Exception as e:
        return {"error": f"Cannot access table {table_identifier}: {e}"}

    columns = []
    exclude_etl: list[str] = []

    for col in (table_info.columns or []):
        col_name = col.name or ""
        col_type = str(col.type_text or col.type_name or "")

        recommendations: list[dict] = []
        if ETL_PATTERNS.search(col_name):
            recommendations.append({"action": "exclude", "reason": "etl_metadata", "confidence": "high"})
            exclude_etl.append(col_name)

        entry: dict[str, Any] = {
            "name": col_name,
            "type": col_type,
            "description": col.comment,
        }
        if recommendations:
            entry["recommendations"] = recommendations
        columns.append(entry)

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

    table_type = str(table_info.table_type) if table_info.table_type else None

    result_dict: dict[str, Any] = {
        "table": table_identifier,
        "table_type": table_type,
        "comment": table_info.comment,
        "columns": columns,
        "column_count": len(columns),
        "sample_rows": sample_rows,
        "uc_url": uc_url,
    }

    if exclude_etl:
        result_dict["recommendations"] = {"exclude_etl": exclude_etl}

    return result_dict


@mlflow.trace(name="profile_columns", span_type=SpanType.TOOL)
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
        columns = columns_to_profile[:10]

    profiles = {}
    for col_name in columns:
        try:
            result = execute_sql(
                f"SELECT DISTINCT `{col_name}` FROM {table_identifier} "
                f"WHERE `{col_name}` IS NOT NULL ORDER BY `{col_name}` LIMIT 11"
            )
            if result.get("error"):
                profiles[col_name] = {"error": result["error"]}
                continue

            values = [row[0] for row in result.get("data", [])]
            profiles[col_name] = {
                "distinct_values": values[:10],
                "has_more": len(values) > 10,
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


# ── Data quality assessment ───────────────────────────────────────────────────

_BOOL_STRINGS = {"true", "false", "yes", "no", "y", "n"}
_NULL_STRINGS = {"null", "none", "na", "n/a", ""}
_MAX_QUALITY_COLS_PER_QUERY = 20


@mlflow.trace(name="assess_data_quality", span_type=SpanType.TOOL)
def _assess_data_quality(table_identifiers: list[str]) -> dict:
    """Assess data quality across one or more tables in parallel.

    Each table gets a single-pass SQL query for null/empty/constant metrics,
    then targeted queries for string columns to detect casing issues and
    boolean-as-string values. Tables are processed concurrently.
    """
    results: dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=min(len(table_identifiers), _SQL_CONCURRENCY)) as pool:
        futures = {
            pool.submit(run_in_context(_assess_single_table, tbl)): tbl
            for tbl in table_identifiers
        }
        for future in as_completed(futures):
            tbl = futures[future]
            try:
                results[tbl] = future.result()
            except Exception as e:
                logger.exception(f"assess_data_quality failed for {tbl}")
                results[tbl] = {"error": str(e)}

    global_summary = _build_global_summary(results)
    return {"tables": results, "summary": global_summary}


def _assess_single_table(table_identifier: str) -> dict:
    """Run quality checks on a single table."""
    # Get column metadata first
    desc = _describe_table(table_identifier)
    if "error" in desc:
        return {"error": desc["error"]}

    columns_meta = desc["columns"]
    total_rows = _get_row_count(table_identifier)
    if total_rows == 0:
        return {
            "total_rows": 0,
            "column_quality": {},
            "summary": {"good_columns": 0, "empty_table": True},
        }

    # Phase 1: null/empty/constant metrics — single-pass SQL per batch
    col_quality = _run_null_metrics(table_identifier, columns_meta, total_rows)

    # Phase 2: string-column deep checks (casing variants, bool-as-string)
    # Pass col_quality so high-cardinality columns are skipped automatically.
    string_cols = [
        c["name"] for c in columns_meta
        if _base_col_type(c["type"]) in _STRING_TYPES
    ]
    if string_cols:
        casing_issues = _run_casing_checks(table_identifier, string_cols, col_quality)
        for col_name, issues in casing_issues.items():
            if col_name in col_quality:
                col_quality[col_name].update(issues)

    # Build per-column recommendations
    exclude_recommend: list[str] = []
    review_recommend: list[str] = []
    for col_name, metrics in col_quality.items():
        recs = _column_quality_recommendations(col_name, metrics, total_rows)
        if recs:
            metrics["recommendations"] = recs
            for r in recs:
                if r["action"] == "exclude":
                    exclude_recommend.append(col_name)
                elif r["action"] == "flag":
                    review_recommend.append(col_name)

    # Summaries
    good = sum(1 for m in col_quality.values() if not m.get("recommendations"))
    sparse = sum(1 for m in col_quality.values()
                 if 0.5 < m.get("null_rate", 0) < 1.0)
    empty_cols = sum(1 for m in col_quality.values() if m.get("null_rate", 0) == 1.0)
    constant = sum(1 for m in col_quality.values() if m.get("distinct_count") == 1)

    return {
        "total_rows": total_rows,
        "column_quality": col_quality,
        "summary": {
            "good_columns": good,
            "sparse_columns": sparse,
            "empty_columns": empty_cols,
            "constant_columns": constant,
            "recommended_excludes": exclude_recommend,
            "recommended_review": review_recommend,
        },
    }


def _get_row_count(table_identifier: str) -> int:
    result = _execute_sql_throttled(f"SELECT COUNT(*) FROM {table_identifier}")
    if result.get("error") or not result.get("data"):
        return 0
    try:
        return int(result["data"][0][0])
    except (IndexError, TypeError, ValueError):
        return 0


def _run_null_metrics(
    table_identifier: str,
    columns_meta: list[dict],
    total_rows: int,
) -> dict[str, dict]:
    """Compute null rate, empty-string rate, and distinct count per column.

    Batches columns into groups to keep SQL manageable, runs batches in parallel.
    """
    col_quality: dict[str, dict] = {}
    batches = [
        columns_meta[i:i + _MAX_QUALITY_COLS_PER_QUERY]
        for i in range(0, len(columns_meta), _MAX_QUALITY_COLS_PER_QUERY)
    ]

    def run_batch(batch: list[dict]) -> dict[str, dict]:
        parts: list[str] = []
        for col in batch:
            cn = col["name"]
            base_type = _base_col_type(col["type"])
            parts.append(
                f"SUM(CASE WHEN `{cn}` IS NULL THEN 1 ELSE 0 END) AS `{cn}__nulls`"
            )
            parts.append(f"COUNT(DISTINCT `{cn}`) AS `{cn}__distinct`")
            if base_type in _STRING_TYPES:
                parts.append(
                    f"SUM(CASE WHEN TRIM(CAST(`{cn}` AS STRING)) = '' "
                    f"AND `{cn}` IS NOT NULL THEN 1 ELSE 0 END) AS `{cn}__empty`"
                )
        sql = f"SELECT {', '.join(parts)} FROM {table_identifier}"
        result = _execute_sql_throttled(sql)
        batch_quality: dict[str, dict] = {}
        if result.get("error") or not result.get("data"):
            for col in batch:
                batch_quality[col["name"]] = {"error": result.get("error", "no data")}
            return batch_quality
        row = result["data"][0]
        col_map = {c["name"]: i for i, c in enumerate(result["columns"])}
        for col in batch:
            cn = col["name"]
            base_type = _base_col_type(col["type"])
            nulls = _safe_int(row[col_map.get(f"{cn}__nulls", -1)])
            distinct = _safe_int(row[col_map.get(f"{cn}__distinct", -1)])
            metrics: dict[str, Any] = {
                "null_rate": round(nulls / total_rows, 4) if total_rows else 0,
                "null_count": nulls,
                "distinct_count": distinct,
            }
            if base_type in _STRING_TYPES:
                empty = _safe_int(row[col_map.get(f"{cn}__empty", -1)])
                metrics["empty_rate"] = round(empty / total_rows, 4) if total_rows else 0
                metrics["empty_count"] = empty
            batch_quality[cn] = metrics
        return batch_quality

    if len(batches) == 1:
        col_quality.update(run_batch(batches[0]))
    else:
        with ThreadPoolExecutor(max_workers=min(len(batches), _SQL_CONCURRENCY)) as pool:
            futures = {pool.submit(run_in_context(run_batch, b)): b for b in batches}
            for future in as_completed(futures):
                try:
                    col_quality.update(future.result())
                except Exception as e:
                    for col in futures[future]:
                        col_quality[col["name"]] = {"error": str(e)}

    return col_quality


_CASING_BATCH_SIZE = 4


def _run_casing_checks(
    table_identifier: str,
    string_cols: list[str],
    col_quality: dict[str, dict] | None = None,
) -> dict[str, dict]:
    """Detect boolean-as-string values and inconsistent casing in string columns.

    Optimised for latency: builds UNION ALL batches of up to _CASING_BATCH_SIZE
    columns, each combining bool+casing detection in a single scan.  Columns
    with high cardinality (>100 distinct values from phase-1 null metrics) are
    skipped because casing variance is noisy and the GROUP BY is expensive.

    Total SQL calls: ceil(eligible_cols / _CASING_BATCH_SIZE) per table.
    """
    eligible = _filter_casing_candidates(string_cols, col_quality)
    if not eligible:
        return {}

    batches = [
        eligible[i:i + _CASING_BATCH_SIZE]
        for i in range(0, len(eligible), _CASING_BATCH_SIZE)
    ]

    issues: dict[str, dict] = {}

    def run_batch(cols: list[str]) -> dict[str, dict]:
        sql = _build_casing_union_sql(table_identifier, cols)
        result = _execute_sql_throttled(sql)
        return _parse_casing_result(result)

    if len(batches) == 1:
        issues.update(run_batch(batches[0]))
    else:
        with ThreadPoolExecutor(max_workers=min(len(batches), _SQL_CONCURRENCY)) as pool:
            futures = {pool.submit(run_in_context(run_batch, b)): b for b in batches}
            for future in as_completed(futures):
                try:
                    issues.update(future.result())
                except Exception as e:
                    for c in futures[future]:
                        logger.warning(f"Casing check failed for {c}: {e}")

    return issues


def _filter_casing_candidates(
    string_cols: list[str],
    col_quality: dict[str, dict] | None,
) -> list[str]:
    """Pick string columns worth running casing checks on.

    Skips columns with >100 distinct values (high cardinality — expensive and
    noisy) or 0 distinct values (all null). Caps at 12 columns per table.
    """
    if col_quality is None:
        return string_cols[:12]

    eligible: list[str] = []
    for col in string_cols:
        metrics = col_quality.get(col, {})
        distinct = metrics.get("distinct_count", -1)
        if distinct == 0 or distinct > 100:
            continue
        eligible.append(col)
        if len(eligible) >= 12:
            break
    return eligible


def _build_casing_union_sql(table_identifier: str, cols: list[str]) -> str:
    """Build a UNION ALL query that checks bool+casing for multiple columns."""
    parts: list[str] = []
    for col in cols:
        parts.append(
            f"SELECT '{col}' AS col_name, "
            f"LOWER(CAST(`{col}` AS STRING)) AS normalized, "
            f"COLLECT_SET(CAST(`{col}` AS STRING)) AS variants, "
            f"COUNT(DISTINCT CAST(`{col}` AS STRING)) AS variant_count, "
            f"COUNT(*) AS cnt "
            f"FROM {table_identifier} "
            f"WHERE `{col}` IS NOT NULL "
            f"GROUP BY LOWER(CAST(`{col}` AS STRING)) "
            f"HAVING COUNT(DISTINCT CAST(`{col}` AS STRING)) > 1 "
            f"OR LOWER(CAST(`{col}` AS STRING)) IN "
            f"('true','false','yes','no','y','n')"
        )
    return " UNION ALL ".join(parts)


def _parse_casing_result(result: dict) -> dict[str, dict]:
    """Parse the UNION ALL casing result into per-column issues."""
    issues: dict[str, dict] = {}
    if result.get("error") or not result.get("data"):
        return issues

    for row in result["data"]:
        col_name = row[0]
        normalized = row[1]
        variants_raw = row[2]
        variant_count = _safe_int(row[3])
        cnt = _safe_int(row[4])

        if isinstance(variants_raw, str):
            try:
                variants_raw = json.loads(variants_raw)
            except (json.JSONDecodeError, TypeError):
                variants_raw = [variants_raw]
        if not isinstance(variants_raw, list):
            variants_raw = [str(variants_raw)]

        entry = {
            "normalized": normalized,
            "variants": variants_raw,
            "variant_count": variant_count,
            "count": cnt,
        }

        if col_name not in issues:
            issues[col_name] = {}

        if normalized in _BOOL_STRINGS:
            issues[col_name].setdefault("boolean_as_string", []).append(entry)
        if variant_count > 1:
            issues[col_name].setdefault("inconsistent_casing", []).append(entry)

    return issues


def _column_quality_recommendations(
    col_name: str,
    metrics: dict,
    total_rows: int,
) -> list[dict]:
    """Generate recommendations based on column quality metrics."""
    recs: list[dict] = []
    null_rate = metrics.get("null_rate", 0)
    distinct = metrics.get("distinct_count", -1)

    if null_rate == 1.0:
        recs.append({"action": "exclude", "reason": "all_null", "confidence": "high"})
    elif null_rate >= 0.8:
        recs.append({"action": "flag", "reason": "high_null_rate",
                      "detail": f"{null_rate:.0%} null", "confidence": "medium"})

    if distinct == 1 and null_rate < 1.0:
        recs.append({"action": "flag", "reason": "constant_value", "confidence": "medium"})

    if metrics.get("boolean_as_string"):
        has_multi_variant = any(
            len(bv.get("variants", [])) > 1
            for bv in metrics["boolean_as_string"]
        )
        if has_multi_variant:
            recs.append({
                "action": "flag",
                "reason": "inconsistent_boolean",
                "detail": "Multiple casings of boolean values (e.g. true/TRUE/True)",
                "confidence": "high",
            })
        else:
            recs.append({
                "action": "flag",
                "reason": "boolean_as_string",
                "detail": "Boolean values stored as strings",
                "confidence": "medium",
            })

    if metrics.get("inconsistent_casing"):
        non_bool = [
            cv for cv in metrics["inconsistent_casing"]
            if cv["normalized"] not in _BOOL_STRINGS
        ]
        if non_bool:
            recs.append({
                "action": "flag",
                "reason": "inconsistent_casing",
                "detail": f"{len(non_bool)} value(s) with mixed casing variants",
                "confidence": "high",
            })

    # Name-based checks (supplement describe_table's checks)
    if ETL_PATTERNS.search(col_name):
        recs.append({"action": "exclude", "reason": "etl_metadata", "confidence": "high"})

    return recs


def _build_global_summary(table_results: dict[str, dict]) -> dict:
    """Build a cross-table summary of quality findings."""
    total_tables = len(table_results)
    all_excludes: list[str] = []
    all_review: list[str] = []
    tables_with_issues = 0

    for tbl, res in table_results.items():
        if "error" in res:
            continue
        summary = res.get("summary", {})
        short_name = tbl.split(".")[-1]
        for col in summary.get("recommended_excludes", []):
            all_excludes.append(f"{short_name}.{col}")
        for col in summary.get("recommended_review", []):
            all_review.append(f"{short_name}.{col}")
        if summary.get("recommended_excludes") or summary.get("recommended_review"):
            tables_with_issues += 1

    return {
        "tables_assessed": total_tables,
        "tables_with_issues": tables_with_issues,
        "total_recommended_excludes": len(all_excludes),
        "total_recommended_review": len(all_review),
        "excludes": all_excludes[:20],
        "review": all_review[:20],
    }


def _safe_int(val: Any) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


# ── Table usage profiling (lineage + query history) ──────────────────────────

_LINEAGE_TIMEOUT_S = 8
_QUERY_HISTORY_TIMEOUT_S = 10
_USAGE_COL_PATTERN = re.compile(r"`(\w+)`|(?:^|\.)(\w+)(?:\s|,|\)|$)", re.MULTILINE)


@mlflow.trace(name="profile_table_usage", span_type=SpanType.TOOL)
def _profile_table_usage(table_identifiers: list[str]) -> dict:
    """Profile table lineage and recent query patterns from system tables.

    Best-effort: if system tables are inaccessible the tool returns
    ``system_tables_available: false`` and the agent can proceed without it.
    All SQL is executed through the shared semaphore.
    """
    results: dict[str, Any] = {}
    system_ok = True

    with ThreadPoolExecutor(max_workers=min(len(table_identifiers) + 1, _SQL_CONCURRENCY)) as pool:
        lineage_futures = {
            pool.submit(run_in_context(_fetch_lineage, tbl)): tbl for tbl in table_identifiers
        }
        history_future = pool.submit(run_in_context(_fetch_query_history, table_identifiers))

        for future in as_completed(lineage_futures):
            tbl = lineage_futures[future]
            try:
                lin = future.result()
                if lin.get("error"):
                    system_ok = False
                results.setdefault(tbl, {})["lineage"] = lin
            except Exception as e:
                system_ok = False
                results.setdefault(tbl, {})["lineage"] = {"error": str(e)}

        try:
            hist = history_future.result()
            if hist.get("error"):
                system_ok = False
        except Exception as e:
            system_ok = False
            hist = {"error": str(e)}

    for tbl in table_identifiers:
        tbl_lower = tbl.lower()
        tbl_short_lower = tbl.split(".")[-1].lower()
        tbl_hist = [
            q for q in hist.get("queries", [])
            if tbl_lower in q.get("query_preview", "").lower()
            or tbl_short_lower in q.get("query_preview", "").lower()
        ]
        results.setdefault(tbl, {})["recent_queries"] = tbl_hist[:10]

    summary = _build_usage_summary(results, table_identifiers)
    return {
        "system_tables_available": system_ok,
        "tables": results,
        "summary": summary,
    }


def _fetch_lineage(table_identifier: str) -> dict:
    """Fetch upstream and downstream tables from system.access.table_lineage."""
    sql = (
        f"SELECT source_table_full_name, target_table_full_name, "
        f"source_type, target_type "
        f"FROM system.access.table_lineage "
        f"WHERE (target_table_full_name = '{table_identifier}' "
        f"OR source_table_full_name = '{table_identifier}') "
        f"AND event_time >= date_sub(current_date(), 30) "
        f"LIMIT 50"
    )
    result = _execute_sql_throttled(sql)
    if result.get("error"):
        return {"error": result["error"]}

    upstream: list[str] = []
    downstream: list[str] = []
    for row in result.get("data", []):
        src, tgt = row[0], row[1]
        if tgt == table_identifier and src and src != table_identifier:
            if src not in upstream:
                upstream.append(src)
        if src == table_identifier and tgt and tgt != table_identifier:
            if tgt not in downstream:
                downstream.append(tgt)

    return {"upstream": upstream, "downstream": downstream}


def _fetch_query_history(table_identifiers: list[str]) -> dict:
    """Fetch recent queries referencing these tables from system.query.history."""
    if not table_identifiers:
        return {"queries": []}

    like_clauses = " OR ".join(
        f"LOWER(statement_text) LIKE '%{tbl.lower()}%'" for tbl in table_identifiers
    )
    sql = (
        f"SELECT executed_by, "
        f"SUBSTRING(statement_text, 1, 300) AS query_preview, "
        f"total_duration_ms, produced_rows "
        f"FROM system.query.history "
        f"WHERE start_time >= date_sub(current_date(), 7) "
        f"AND execution_status = 'FINISHED' "
        f"AND ({like_clauses}) "
        f"ORDER BY start_time DESC "
        f"LIMIT 50"
    )
    result = _execute_sql_throttled(sql)
    if result.get("error"):
        return {"error": result["error"]}

    queries: list[dict] = []
    for row in result.get("data", []):
        queries.append({
            "executed_by": row[0],
            "query_preview": row[1],
            "duration_ms": _safe_int(row[2]),
            "rows_produced": _safe_int(row[3]),
        })

    column_usage = _extract_column_patterns(queries)
    return {"queries": queries, "column_usage": column_usage}


def _extract_column_patterns(queries: list[dict]) -> dict[str, int]:
    """Extract frequently referenced column names from query snippets.

    Uses simple regex to find backtick-quoted or dot-qualified identifiers.
    Not perfect, but good enough to surface common patterns for the LLM.
    """
    counts: dict[str, int] = {}
    for q in queries:
        preview = q.get("query_preview", "")
        for m in _USAGE_COL_PATTERN.finditer(preview):
            col = m.group(1) or m.group(2)
            if col and len(col) > 1 and col.upper() not in {
                "SELECT", "FROM", "WHERE", "AND", "OR", "GROUP", "BY",
                "ORDER", "LIMIT", "JOIN", "ON", "AS", "IN", "NOT", "IS",
                "NULL", "CASE", "WHEN", "THEN", "ELSE", "END", "HAVING",
                "WITH", "UNION", "ALL", "DISTINCT", "COUNT", "SUM", "AVG",
                "MIN", "MAX", "LOWER", "UPPER", "TRIM", "CAST", "STRING",
                "INT", "BIGINT", "DATE", "TIMESTAMP", "TRUE", "FALSE",
                "BETWEEN", "LIKE", "EXISTS", "LEFT", "RIGHT", "INNER",
                "OUTER", "FULL", "CROSS", "ASC", "DESC", "OVER",
                "PARTITION", "ROW", "ROWS", "CURRENT", "PRECEDING",
                "FOLLOWING", "UNBOUNDED",
            }:
                counts[col] = counts.get(col, 0) + 1
    top = sorted(counts.items(), key=lambda x: -x[1])[:20]
    return dict(top)


def _build_usage_summary(
    table_results: dict[str, dict],
    table_identifiers: list[str],
) -> dict:
    """Build a human-friendly summary of lineage and usage findings."""
    total_upstream = 0
    total_downstream = 0
    total_queries = 0
    tables_with_lineage = 0

    for tbl in table_identifiers:
        lin = table_results.get(tbl, {}).get("lineage", {})
        up = len(lin.get("upstream", []))
        down = len(lin.get("downstream", []))
        total_upstream += up
        total_downstream += down
        if up or down:
            tables_with_lineage += 1
        total_queries += len(table_results.get(tbl, {}).get("recent_queries", []))

    return {
        "tables_with_lineage": tables_with_lineage,
        "total_upstream_sources": total_upstream,
        "total_downstream_consumers": total_downstream,
        "recent_query_count": total_queries,
    }


def _substitute_params(sql: str, parameters: list[dict] | None) -> str:
    """Replace :param_name placeholders with default values for testing."""
    if not parameters:
        return sql
    import re
    for param in parameters:
        name = param.get("name", "")
        value = param.get("default_value", "")
        if not name:
            continue
        type_hint = param.get("type_hint", "STRING").upper()
        if type_hint in ("NUMBER", "BOOLEAN", "INTEGER", "INT", "DECIMAL", "FLOAT", "DOUBLE", "BIGINT", "SMALLINT", "TINYINT"):
            literal = str(value)
        else:
            literal = f"'{value}'"
        sql = re.sub(rf":{re.escape(name)}\b", literal, sql)
    return sql


def _strip_leading_comments(sql: str) -> str:
    """Strip leading SQL line comments (--) that cause execution failures."""
    lines = sql.lstrip().splitlines()
    while lines and lines[0].lstrip().startswith("--"):
        lines.pop(0)
    return "\n".join(lines).strip()


def _test_sql(sql: str, parameters: list[dict] | None = None) -> dict:
    test_query = _substitute_params(sql, parameters)
    test_query = _strip_leading_comments(test_query)

    import re
    remaining = re.findall(r":([a-zA-Z_]\w*)\b", test_query)
    if remaining:
        return {
            "success": False,
            "sql": sql,
            "error": (
                f"Unbound SQL parameters: {', '.join(remaining)}. "
                "Pass 'parameters' with 'name' and 'default_value' for each :param so the query can be tested."
            ),
        }

    result = execute_sql(test_query, row_limit=5)
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


def _generate_plan_fallback(**kwargs) -> dict:
    """Fallback if generate_plan is called through normal dispatch.

    The real implementation lives in create_agent.py which has access to
    session history.  This returns an error nudging the agent to provide
    the context manually via present_plan.
    """
    return {
        "error": (
            "generate_plan requires session context (handled by the agent). "
            "If you see this, call present_plan with the plan data instead."
        ),
    }


def _present_plan(
    tables: list[dict] | None = None,
    sample_questions: list[str] | None = None,
    text_instructions: list[str] | None = None,
    example_sqls: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    join_specs: list[dict] | None = None,
    benchmarks: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> dict:
    """Pass structured plan data through for frontend rendering.

    Parameters are identical to generate_config so the plan is
    a 1:1 preview of the config that will be created.
    """
    sections: dict[str, Any] = {}

    sections["tables"] = tables or []
    sections["sample_questions"] = sample_questions or []
    sections["text_instructions"] = text_instructions or []
    sections["example_sqls"] = example_sqls or []
    sections["measures"] = measures or []
    sections["filters"] = filters or []
    sections["expressions"] = expressions or []
    sections["join_specs"] = join_specs or []
    sections["benchmarks"] = benchmarks or []
    if metric_views:
        sections["metric_views"] = metric_views

    total = sum(len(v) for v in sections.values() if isinstance(v, list))
    warnings = []
    bench_count = len(sections["benchmarks"])
    if bench_count < 10:
        warnings.append(
            f"Only {bench_count} benchmark questions provided — minimum is 10. "
            "Call present_plan again with at least 10 benchmarks."
        )
    example_count = len(sections["example_sqls"])
    if example_count < 3:
        warnings.append(
            f"Only {example_count} example SQL queries provided — minimum is 3. "
            "Call present_plan again with at least 3 complex example SQL pairs."
        )

    result: dict[str, Any] = {
        "sections": sections,
        "total_items": total,
        "ui_hint": {"type": "plan_review", "id": "plan_review", "label": "Review the plan"},
    }
    if warnings:
        result["warnings"] = warnings
    return result


@mlflow.trace(name="generate_config", span_type=SpanType.TOOL)
def _generate_config(
    tables: list[dict] | None = None,
    sample_questions: list[str] | None = None,
    text_instructions: list[str] | None = None,
    example_sqls: list[dict] | None = None,
    measures: list[dict] | None = None,
    filters: list[dict] | None = None,
    expressions: list[dict] | None = None,
    join_specs: list[dict] | None = None,
    benchmarks: list[dict] | None = None,
    generate_benchmarks: bool = False,
    metric_views: list[dict] | None = None,
) -> dict:
    """Build a complete serialized_space config from structured inputs.

    Use this for INITIAL creation only. For post-creation modifications,
    use update_config instead.
    """
    if not tables:
        return {
            "error": "tables is required and must contain at least one table",
            "hint": (
                "Pass tables as a list of objects with at least 'identifier' "
                "(catalog.schema.table). Review describe_table results for the "
                "identifiers you inspected earlier."
            ),
        }
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
    data_sources: dict[str, Any] = {"tables": ds_tables}

    if metric_views:
        mv_items = []
        for mv in metric_views:
            mv_entry: dict[str, Any] = {"identifier": mv["identifier"]}
            if mv.get("description"):
                mv_entry["description"] = [mv["description"]]
            if mv.get("column_configs"):
                cc_list = []
                for cc in mv["column_configs"]:
                    cc_entry: dict[str, Any] = {"column_name": cc["column_name"]}
                    if cc.get("description"):
                        cc_entry["description"] = [cc["description"]]
                    if cc.get("enable_format_assistance") is not None:
                        cc_entry["enable_format_assistance"] = cc["enable_format_assistance"]
                    cc_list.append(cc_entry)
                cc_list.sort(key=lambda x: x["column_name"])
                mv_entry["column_configs"] = cc_list
            mv_items.append(mv_entry)
        mv_items.sort(key=lambda x: x["identifier"])
        data_sources["metric_views"] = mv_items

    config["data_sources"] = data_sources

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
            if eq.get("parameters"):
                params = []
                for p in eq["parameters"]:
                    raw_hint = p.get("type_hint", "STRING").upper()
                    normalized_hint = _TYPE_HINT_MAP.get(raw_hint, raw_hint)
                    param_entry: dict[str, Any] = {
                        "name": p["name"],
                        "type_hint": normalized_hint,
                    }
                    if p.get("description"):
                        param_entry["description"] = [p["description"]]
                    if p.get("default_value"):
                        param_entry["default_value"] = {"values": [p["default_value"]]}
                    params.append(param_entry)
                params.sort(key=lambda x: x["name"])
                entry["parameters"] = params
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
            if m.get("comment"):
                entry["comment"] = [m["comment"]]
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
            if f.get("comment"):
                entry["comment"] = [f["comment"]]
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
            if e.get("comment"):
                entry["comment"] = [e["comment"]]
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
            rel_norm = js["relationship"].upper().replace("-", "_")
            rt = f"--rt=FROM_RELATIONSHIP_TYPE_{rel_norm}--"
            entry: dict[str, Any] = {
                "id": secrets.token_hex(16),
                "left": {"identifier": js["left_table"], "alias": la},
                "right": {"identifier": js["right_table"], "alias": ra},
                "sql": [condition, rt],
            }
            if js.get("instruction"):
                entry["instruction"] = [js["instruction"]]
            if js.get("comment"):
                entry["comment"] = [js["comment"]]
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

        elif action == "add_instruction_line":
            line = (act.get("instruction_line") or act.get("instruction") or "").strip()
            if not line:
                applied.append("Skipped add_instruction_line — instruction_line required")
                continue
            formatted = line if line.endswith("\n") else line + "\n"
            ti_list = cfg.setdefault("instructions", {}).get("text_instructions", [])
            if ti_list:
                existing = ti_list[0].get("content", [])
                if formatted not in existing and line + "\n" not in existing:
                    existing.append(formatted)
            else:
                cfg["instructions"]["text_instructions"] = [{
                    "id": secrets.token_hex(16),
                    "content": [formatted],
                }]
            applied.append(f"Added instruction line: {line[:80]}")

        elif action == "remove_instruction_line":
            line = (act.get("instruction_line") or act.get("instruction") or "").strip().lower()
            if not line:
                applied.append("Skipped remove_instruction_line — instruction_line required")
                continue
            ti_list = cfg.get("instructions", {}).get("text_instructions", [])
            if ti_list:
                before = len(ti_list[0].get("content", []))
                ti_list[0]["content"] = [
                    l for l in ti_list[0].get("content", [])
                    if line not in l.strip().lower()
                ]
                removed = before - len(ti_list[0]["content"])
                applied.append(f"Removed {removed} instruction line(s) matching '{line[:40]}'")
            else:
                applied.append("No text instructions to remove from")

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

        # ── Benchmark actions ─────────────────────────────────────────
        elif action == "add_benchmark":
            question = act.get("question", "")
            expected_sql = act.get("expected_sql", "")
            if not question or not expected_sql:
                applied.append("Skipped add_benchmark — question and expected_sql required")
                continue
            bq = cfg.setdefault("benchmarks", {}).setdefault("questions", [])
            bq.append({
                "id": secrets.token_hex(16),
                "question": [question],
                "answer": [{"format": "SQL", "content": _split_sql_to_lines(expected_sql)}],
            })
            bq.sort(key=lambda x: x["id"])
            applied.append(f"Added benchmark: {question[:60]}")

        elif action == "remove_benchmark":
            question = act.get("question", "").lower()
            bq = cfg.get("benchmarks", {}).get("questions", [])
            before = len(bq)
            cfg.setdefault("benchmarks", {})["questions"] = [
                b for b in bq if b.get("question", [""])[0].lower() != question
            ]
            removed = before - len(cfg["benchmarks"]["questions"])
            applied.append(f"Removed {removed} benchmark(s) matching '{question[:40]}'")

        elif action == "update_benchmarks":
            benchmarks_list = act.get("benchmarks", [])
            if not benchmarks_list:
                applied.append("Skipped update_benchmarks — benchmarks array required")
                continue
            bq_items = []
            for b in benchmarks_list:
                bq_items.append({
                    "id": secrets.token_hex(16),
                    "question": [b["question"]],
                    "answer": [{"format": "SQL", "content": _split_sql_to_lines(b["expected_sql"])}],
                })
            bq_items.sort(key=lambda x: x["id"])
            cfg["benchmarks"] = {"questions": bq_items}
            applied.append(f"Replaced all benchmarks ({len(bq_items)})")

        # ── Join actions ──────────────────────────────────────────────
        elif action == "add_join":
            lt = act.get("left_table", "")
            rt_table = act.get("right_table", "")
            lc = act.get("left_column", "")
            rc = act.get("right_column", "")
            rel = act.get("relationship", "ONE_TO_MANY")
            if not all([lt, rt_table, lc, rc]):
                applied.append("Skipped add_join — left_table, right_table, left_column, right_column required")
                continue
            la = act.get("left_alias", lt.split(".")[-1])
            ra = act.get("right_alias", rt_table.split(".")[-1])
            condition = f"`{la}`.`{lc}` = `{ra}`.`{rc}`"
            rel_norm = rel.upper().replace("-", "_")
            rt_tag = f"--rt=FROM_RELATIONSHIP_TYPE_{rel_norm}--"
            entry: dict[str, Any] = {
                "id": secrets.token_hex(16),
                "left": {"identifier": lt, "alias": la},
                "right": {"identifier": rt_table, "alias": ra},
                "sql": [condition, rt_tag],
            }
            if act.get("instruction"):
                entry["instruction"] = [act["instruction"]]
            js_list = cfg.setdefault("instructions", {}).setdefault("join_specs", [])
            js_list.append(entry)
            js_list.sort(key=lambda x: x["id"])
            applied.append(f"Added join: {la}.{lc} = {ra}.{rc} ({rel})")

        elif action == "remove_join":
            lt = act.get("left_table", "")
            rt_table = act.get("right_table", "")
            js_list = cfg.get("instructions", {}).get("join_specs", [])
            before = len(js_list)
            cfg.setdefault("instructions", {})["join_specs"] = [
                j for j in js_list
                if not (j.get("left", {}).get("identifier") == lt and j.get("right", {}).get("identifier") == rt_table)
            ]
            removed = before - len(cfg["instructions"]["join_specs"])
            applied.append(f"Removed {removed} join(s) between {lt} and {rt_table}")

        # ── SQL snippet actions (measures, filters, expressions) ──────
        elif action == "add_measure":
            dn = act.get("display_name", "")
            sql = act.get("sql", "")
            if not dn or not sql:
                applied.append("Skipped add_measure — display_name and sql required")
                continue
            entry_m: dict[str, Any] = {"id": secrets.token_hex(16), "display_name": dn, "sql": [sql]}
            if act.get("synonyms"):
                entry_m["synonyms"] = act["synonyms"]
            if act.get("instruction"):
                entry_m["instruction"] = [act["instruction"]]
            ms = cfg.setdefault("instructions", {}).setdefault("sql_snippets", {}).setdefault("measures", [])
            ms.append(entry_m)
            ms.sort(key=lambda x: x["id"])
            applied.append(f"Added measure: {dn}")

        elif action == "remove_measure":
            dn = act.get("display_name", "").lower()
            ms = cfg.get("instructions", {}).get("sql_snippets", {}).get("measures", [])
            before = len(ms)
            cfg.setdefault("instructions", {}).setdefault("sql_snippets", {})["measures"] = [
                m for m in ms if m.get("display_name", "").lower() != dn
            ]
            removed = before - len(cfg["instructions"]["sql_snippets"]["measures"])
            applied.append(f"Removed {removed} measure(s) matching '{dn}'")

        elif action == "add_filter":
            dn = act.get("display_name", "")
            sql = act.get("sql", "")
            if not dn or not sql:
                applied.append("Skipped add_filter — display_name and sql required")
                continue
            if sql.strip().upper().startswith("WHERE "):
                sql = sql.strip()[6:]
            entry_f: dict[str, Any] = {"id": secrets.token_hex(16), "display_name": dn, "sql": [sql]}
            if act.get("synonyms"):
                entry_f["synonyms"] = act["synonyms"]
            if act.get("instruction"):
                entry_f["instruction"] = [act["instruction"]]
            fs = cfg.setdefault("instructions", {}).setdefault("sql_snippets", {}).setdefault("filters", [])
            fs.append(entry_f)
            fs.sort(key=lambda x: x["id"])
            applied.append(f"Added filter: {dn}")

        elif action == "remove_filter":
            dn = act.get("display_name", "").lower()
            fs = cfg.get("instructions", {}).get("sql_snippets", {}).get("filters", [])
            before = len(fs)
            cfg.setdefault("instructions", {}).setdefault("sql_snippets", {})["filters"] = [
                f for f in fs if f.get("display_name", "").lower() != dn
            ]
            removed = before - len(cfg["instructions"]["sql_snippets"]["filters"])
            applied.append(f"Removed {removed} filter(s) matching '{dn}'")

        elif action == "add_expression":
            alias = act.get("alias", "")
            sql = act.get("sql", "")
            if not alias or not sql:
                applied.append("Skipped add_expression — alias and sql required")
                continue
            entry_e: dict[str, Any] = {"id": secrets.token_hex(16), "alias": alias, "sql": [sql]}
            if act.get("display_name"):
                entry_e["display_name"] = act["display_name"]
            if act.get("synonyms"):
                entry_e["synonyms"] = act["synonyms"]
            if act.get("instruction"):
                entry_e["instruction"] = [act["instruction"]]
            es = cfg.setdefault("instructions", {}).setdefault("sql_snippets", {}).setdefault("expressions", [])
            es.append(entry_e)
            es.sort(key=lambda x: x["id"])
            applied.append(f"Added expression: {alias}")

        elif action == "remove_expression":
            alias = act.get("alias", "").lower()
            es = cfg.get("instructions", {}).get("sql_snippets", {}).get("expressions", [])
            before = len(es)
            cfg.setdefault("instructions", {}).setdefault("sql_snippets", {})["expressions"] = [
                e for e in es if e.get("alias", "").lower() != alias
            ]
            removed = before - len(cfg["instructions"]["sql_snippets"]["expressions"])
            applied.append(f"Removed {removed} expression(s) matching '{alias}'")

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


_MAX_STRING_CHARS = 25_000        # per-string limit (API rejects above this)
_MAX_GUIDANCE_BYTES = 64 * 1024   # 64 KB combined comment+instruction+usage_guidance
_MAX_SERIALIZED_BYTES = 3_500_000 # 3.5 MB total serialized_space
_MAX_TABLES = 30
_MAX_INSTRUCTIONS = 100

_SIZE_CHECK_FIELDS = {"description", "content", "question", "sql", "instruction", "synonyms", "usage_guidance", "comment"}
_GUIDANCE_FIELDS = {"comment", "instruction", "usage_guidance"}


def _validate_config(config: dict | None = None) -> dict:
    """Validate a serialized_space config. Returns errors and warnings."""
    if not config:
        return {"error": "No config to validate — call generate_config first"}
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
        if len(tables) > _MAX_TABLES:
            error("data_sources.tables", f"Maximum {_MAX_TABLES} tables allowed (found {len(tables)})")
        elif len(tables) > 5:
            warning("data_sources.tables", f"{len(tables)} tables — recommend ≤5 for accuracy")
        col_keys: set[tuple[str, str]] = set()
        for i, tbl in enumerate(tables):
            ident = tbl.get("identifier", "")
            if not _TABLE_ID_PATTERN.match(ident):
                error(f"data_sources.tables[{i}].identifier", f"'{ident}' must be catalog.schema.table")
            ccs = tbl.get("column_configs", [])
            if ccs:
                _check_sorted(ccs, lambda x: x.get("column_name", ""), "column_name", f"data_sources.tables[{i}].column_configs", error)
            for j, cc in enumerate(ccs):
                key = (ident, cc.get("column_name", ""))
                if key in col_keys:
                    error(f"data_sources.tables[{i}].column_configs", f"Duplicate column config: {key}")
                col_keys.add(key)
                if cc.get("enable_entity_matching") and not cc.get("enable_format_assistance"):
                    error(
                        f"data_sources.tables[{i}].column_configs[{j}]",
                        f"enable_entity_matching requires enable_format_assistance to be true (column: {cc.get('column_name', '')})"
                    )

    # metric_views
    mvs = config.get("data_sources", {}).get("metric_views", [])
    if mvs:
        _check_sorted(mvs, lambda x: x.get("identifier", ""), "identifier", "data_sources.metric_views", error)
        for i, mv in enumerate(mvs):
            ccs = mv.get("column_configs", [])
            if ccs:
                _check_sorted(ccs, lambda x: x.get("column_name", ""), "column_name", f"data_sources.metric_views[{i}].column_configs", error)

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
            params = eq.get("parameters", [])
            if params:
                _check_sorted(params, lambda x: x.get("name", ""), "name", f"instructions.example_question_sqls[{i}].parameters", error)

    # sql_functions
    sfs = config.get("instructions", {}).get("sql_functions", [])
    if sfs:
        _check_sorted(sfs, lambda x: (x.get("id", ""), x.get("identifier", "")), "(id, identifier)", "instructions.sql_functions", error)

    # join_specs
    jss = config.get("instructions", {}).get("join_specs", [])
    if jss:
        _check_sorted(jss, lambda x: x.get("id", ""), "id", "instructions.join_specs", error)
        for i, js in enumerate(jss):
            sql = js.get("sql", [])
            if not sql or (isinstance(sql, list) and not any(isinstance(s, str) and s.strip() for s in sql)):
                error(f"instructions.join_specs[{i}].sql", "Join condition SQL must not be empty")
            elif not any(isinstance(s, str) and s.startswith("--rt=") for s in sql):
                error(f"instructions.join_specs[{i}].sql", "Missing --rt=FROM_RELATIONSHIP_TYPE_...-- annotation (API rejects without it)")

    # sql_snippets
    snippets = config.get("instructions", {}).get("sql_snippets", {})
    for stype in ("filters", "expressions", "measures"):
        items = snippets.get(stype, [])
        if items:
            _check_sorted(items, lambda x: x.get("id", ""), "id", f"instructions.sql_snippets.{stype}", error)
            for i, item in enumerate(items):
                sql_val = item.get("sql", [])
                if not sql_val or (isinstance(sql_val, list) and not any(isinstance(s, str) and s.strip() for s in sql_val)):
                    error(f"instructions.sql_snippets.{stype}[{i}].sql", "SQL must not be empty")

    # benchmarks
    bench_questions = config.get("benchmarks", {}).get("questions", [])
    if bench_questions:
        _check_sorted(bench_questions, lambda x: x.get("id", ""), "id", "benchmarks.questions", error)
        for i, bq in enumerate(bench_questions):
            _check_id(f"benchmarks.questions[{i}].id", bq.get("id"), error)
            answers = bq.get("answer", [])
            if len(answers) != 1:
                error(f"benchmarks.questions[{i}].answer", f"Must have exactly 1 answer (found {len(answers)})")
            elif answers[0].get("format") != "SQL":
                error(f"benchmarks.questions[{i}].answer[0].format", "Must be 'SQL'")

    # ── ID uniqueness ──────────────────────────────────────────────────────
    question_ids: list[str] = []
    for sq in sqs:
        if sq.get("id"):
            question_ids.append(sq["id"])
    for bq in bench_questions:
        if bq.get("id"):
            question_ids.append(bq["id"])
    if len(question_ids) != len(set(question_ids)):
        error("ids", "Duplicate IDs found across sample_questions and benchmarks.questions")

    instruction_ids: list[str] = []
    for item in ti:
        if item.get("id"):
            instruction_ids.append(item["id"])
    for item in eqs:
        if item.get("id"):
            instruction_ids.append(item["id"])
    for item in sfs:
        if item.get("id"):
            instruction_ids.append(item["id"])
    for item in jss:
        if item.get("id"):
            instruction_ids.append(item["id"])
    for stype in ("filters", "expressions", "measures"):
        for item in snippets.get(stype, []):
            if item.get("id"):
                instruction_ids.append(item["id"])
    if len(instruction_ids) != len(set(instruction_ids)):
        error("ids", "Duplicate IDs found across instruction elements")

    # instruction count (text_instructions block + SQL functions + example SQL queries)
    total = len(eqs) + len(sfs) + (1 if ti else 0)
    if total > _MAX_INSTRUCTIONS:
        error("instructions", f"Total instruction count {total} exceeds {_MAX_INSTRUCTIONS} limit")
    elif total > 80:
        warning("instructions", f"Instruction count {total}/{_MAX_INSTRUCTIONS} — approaching limit")

    # ── Size / length limits ──────────────────────────────────────────────
    oversized_strings = _check_string_sizes(config)
    for path, size in oversized_strings:
        error(path, f"String exceeds {_MAX_STRING_CHARS:,} char limit ({size:,} chars)")

    guidance_bytes = _measure_guidance_fields(config)
    if guidance_bytes > _MAX_GUIDANCE_BYTES:
        error(
            "instructions",
            f"Combined comment+instruction+usage_guidance is {guidance_bytes:,} bytes "
            f"(limit {_MAX_GUIDANCE_BYTES:,})"
        )
    elif guidance_bytes > _MAX_GUIDANCE_BYTES * 0.8:
        warning(
            "instructions",
            f"Combined guidance fields at {guidance_bytes:,}/{_MAX_GUIDANCE_BYTES:,} bytes — approaching limit"
        )

    serialized_size = len(json.dumps(config).encode("utf-8"))
    if serialized_size > _MAX_SERIALIZED_BYTES:
        error(
            "serialized_space",
            f"Serialized config is {serialized_size:,} bytes (limit {_MAX_SERIALIZED_BYTES:,})"
        )
    elif serialized_size > _MAX_SERIALIZED_BYTES * 0.8:
        warning(
            "serialized_space",
            f"Serialized config at {serialized_size:,}/{_MAX_SERIALIZED_BYTES:,} bytes — approaching limit"
        )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "error_count": len(errors),
        "warning_count": len(warnings),
    }


def _check_string_sizes(obj: Any, path: str = "", field_name: str = "") -> list[tuple[str, int]]:
    """Walk the config tree and find strings in size-checked fields that exceed the char limit."""
    violations: list[tuple[str, int]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            violations.extend(_check_string_sizes(v, f"{path}.{k}" if path else k, k))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            violations.extend(_check_string_sizes(item, f"{path}[{i}]", field_name))
    elif isinstance(obj, str) and field_name in _SIZE_CHECK_FIELDS:
        if len(obj) > _MAX_STRING_CHARS:
            violations.append((path, len(obj)))
    return violations


def _measure_guidance_fields(obj: Any, field_name: str = "") -> int:
    """Sum the byte size of all comment, instruction, and usage_guidance string values."""
    total = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            total += _measure_guidance_fields(v, k)
    elif isinstance(obj, list):
        for item in obj:
            total += _measure_guidance_fields(item, field_name)
    elif isinstance(obj, str) and field_name in _GUIDANCE_FIELDS:
        total += len(obj.encode("utf-8"))
    return total


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


@mlflow.trace(name="create_space", span_type=SpanType.TOOL)
def _create_space(display_name: str, config: dict | None = None, parent_path: str | None = None) -> dict:
    """Create the Genie space via the API.

    Path resolution is automatic: configured directory -> /Shared/.
    On permission errors the next candidate is tried transparently.
    """
    if not config:
        return {"success": False, "error": "No config provided — call generate_config first"}
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
            "parent_path": result.get("parent_path", ""),
        }
    except (ValueError, PermissionError, TimeoutError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("create_space failed")
        return {"success": False, "error": str(e)}


@mlflow.trace(name="update_space", span_type=SpanType.TOOL)
def _update_space(space_id: str, config: dict | None = None, display_name: str | None = None) -> dict:
    """Update an existing Genie space with a new configuration and/or name."""
    if not config and not display_name:
        return {"success": False, "error": "No config or display_name provided"}
    try:
        from backend.services.auth import get_workspace_client, get_databricks_host
        from backend.genie_creator import _enforce_constraints, _clean_config

        body: dict[str, Any] = {}

        if config:
            constrained = _enforce_constraints(config)
            cleaned = _clean_config(constrained)
            body["serialized_space"] = json.dumps(cleaned)

        if display_name:
            body["display_name"] = display_name

        warehouse_id = get_sql_warehouse_id()
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
