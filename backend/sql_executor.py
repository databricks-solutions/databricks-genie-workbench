"""
SQL execution utilities using Databricks Statement Execution API.
"""

import logging
import os
import re

from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)

# Maximum rows to return (prevent OOM on large results)
MAX_ROWS = 1000
# Wait timeout for synchronous execution
WAIT_TIMEOUT = "30s"

# Dangerous SQL patterns that should be blocked for read-only execution
_BLOCKED_SQL_PATTERNS = [
    r"\b(DROP|DELETE|TRUNCATE|UPDATE|INSERT|ALTER|CREATE|GRANT|REVOKE)\b",
    r"\b(EXEC|EXECUTE|CALL)\b",  # Stored procedures
    r";\s*\w",  # Statement chaining (multiple statements)
]


class SqlValidationError(Exception):
    """Raised when SQL validation fails."""

    pass


def validate_sql_read_only(sql: str) -> None:
    """Validate that SQL is a read-only SELECT query.

    Only allows SELECT statements and WITH clauses (CTEs).
    Blocks dangerous operations like DROP, DELETE, UPDATE, INSERT, etc.

    Args:
        sql: The SQL statement to validate

    Raises:
        SqlValidationError: If SQL contains dangerous patterns
    """
    sql_upper = sql.upper().strip()

    # Must start with SELECT or WITH (for CTEs)
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise SqlValidationError(
            "Only SELECT queries are allowed. Query must start with SELECT or WITH."
        )

    # Check for blocked patterns
    for pattern in _BLOCKED_SQL_PATTERNS:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            raise SqlValidationError(
                "Query contains disallowed SQL operation. "
                "Only read-only SELECT queries are permitted."
            )


def get_sql_warehouse_id() -> str | None:
    """Get the SQL Warehouse ID from environment."""
    return os.environ.get("SQL_WAREHOUSE_ID")


def execute_sql(
    sql: str,
    warehouse_id: str | None = None,
    row_limit: int = MAX_ROWS,
) -> dict:
    """Execute SQL on a Databricks SQL Warehouse.

    Uses the Statement Execution API which handles authentication
    automatically via the workspace client.

    Args:
        sql: SQL statement to execute
        warehouse_id: Optional warehouse ID (defaults to SQL_WAREHOUSE_ID env var)
        row_limit: Maximum rows to return

    Returns:
        dict with keys:
            - columns: list of {name, type_name}
            - data: list of rows (each row is a list of values)
            - row_count: number of rows returned
            - truncated: whether results were truncated
            - error: error message if failed
    """
    warehouse_id = warehouse_id or get_sql_warehouse_id()

    if not warehouse_id:
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "SQL_WAREHOUSE_ID not configured",
        }

    # Validate SQL is read-only before execution
    try:
        validate_sql_read_only(sql)
    except SqlValidationError as e:
        logger.warning(f"SQL validation failed: {e}")
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }

    client = get_workspace_client()

    try:
        logger.info(f"Executing SQL on warehouse {warehouse_id}")

        # Use the Statement Execution API
        # The SDK's execute_statement method handles polling automatically
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout=WAIT_TIMEOUT,
            row_limit=row_limit,
        )

        # Check execution status
        if response.status and response.status.state:
            state = response.status.state.value
            if state == "FAILED":
                error_msg = (
                    response.status.error.message
                    if response.status.error
                    else "Execution failed"
                )
                return {
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "truncated": False,
                    "error": error_msg,
                }

        # Extract schema (column metadata)
        columns = []
        if response.manifest and response.manifest.schema:
            columns = [
                {"name": col.name, "type_name": col.type_name}
                for col in response.manifest.schema.columns or []
            ]

        # Extract data
        data = []
        truncated = False
        if response.result and response.result.data_array:
            data = response.result.data_array
        if response.manifest:
            truncated = response.manifest.truncated or False

        return {
            "columns": columns,
            "data": data,
            "row_count": len(data),
            "truncated": truncated,
            "error": None,
        }

    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }
