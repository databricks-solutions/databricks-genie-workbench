"""OBO Unity Catalog metadata prefetch used by optimization triggers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from databricks.sdk.service.sql import Disposition, Format
from genie_space_optimizer.common.query_tags import gso_query_tags

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _sql_rows(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    statement: str,
) -> list[dict]:
    result = ws.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
        query_tags=gso_query_tags(purpose="optimization"),
    )
    rows = result.result.data_array if result.result and result.result.data_array else []
    columns = (
        result.manifest.schema.columns
        if result.manifest and result.manifest.schema and result.manifest.schema.columns
        else []
    )
    names = [column.name for column in columns]
    return [{key: value for key, value in zip(names, row)} for row in rows]


def fetch_uc_metadata_obo(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    catalog: str,
    schema_name: str,
    genie_table_refs: list[tuple[str, str, str]] | None = None,
) -> dict[str, list[dict] | None]:
    """Fetch UC metadata through the requesting user's SQL permissions."""
    if not warehouse_id:
        return {}
    if genie_table_refs:
        return _fetch_for_tables(ws, warehouse_id=warehouse_id, refs=genie_table_refs)

    safe_schema = schema_name.replace("'", "''")
    queries = {
        "uc_columns": (
            f"SELECT '{catalog}' AS catalog_name, '{safe_schema}' AS schema_name, "
            "c.table_name, c.column_name, c.data_type, c.comment, c.ordinal_position, t.table_type "
            f"FROM {catalog}.information_schema.columns c "
            f"LEFT JOIN {catalog}.information_schema.tables t "
            f"ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name "
            f"WHERE c.table_schema = '{safe_schema}'"
        ),
        "uc_tags": (
            f"SELECT catalog_name, schema_name, table_name, "
            f"CAST(NULL AS STRING) AS column_name, tag_name, tag_value "
            f"FROM {catalog}.information_schema.table_tags "
            f"WHERE schema_name = '{safe_schema}' UNION ALL "
            f"SELECT catalog_name, schema_name, table_name, column_name, "
            f"tag_name, tag_value FROM {catalog}.information_schema.column_tags "
            f"WHERE schema_name = '{safe_schema}'"
        ),
        "uc_routines": (
            "SELECT routine_name, routine_type, routine_definition, "
            "data_type AS return_type, routine_schema "
            f"FROM {catalog}.information_schema.routines "
            f"WHERE routine_schema = '{safe_schema}'"
        ),
    }
    return _execute_queries(ws, warehouse_id=warehouse_id, queries=queries)


def _fetch_for_tables(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    refs: list[tuple[str, str, str]],
) -> dict[str, list[dict] | None]:
    from genie_space_optimizer.common.uc_metadata import get_unique_schemas

    schema_groups: dict[tuple[str, str], list[str]] = {}
    for catalog, schema, table in refs:
        if catalog and schema and table:
            schema_groups.setdefault((catalog, schema), []).append(table)

    column_queries: list[str] = []
    tag_queries: list[str] = []
    for (catalog, schema), tables in schema_groups.items():
        safe_tables = ", ".join(
            f"'{table.replace(chr(39), chr(39) + chr(39))}'" for table in tables
        )
        column_queries.append(
            f"SELECT '{catalog}' AS catalog_name, '{schema}' AS schema_name, "
            "c.table_name, c.column_name, c.data_type, c.comment, c.ordinal_position, t.table_type "
            f"FROM {catalog}.information_schema.columns c "
            f"LEFT JOIN {catalog}.information_schema.tables t "
            f"ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name "
            f"WHERE c.table_schema = '{schema}' AND c.table_name IN ({safe_tables})"
        )
        tag_queries.extend([
            f"SELECT catalog_name, schema_name, table_name, "
            f"CAST(NULL AS STRING) AS column_name, tag_name, tag_value "
            f"FROM {catalog}.information_schema.table_tags "
            f"WHERE schema_name = '{schema}' AND table_name IN ({safe_tables})",
            f"SELECT catalog_name, schema_name, table_name, column_name, "
            f"tag_name, tag_value FROM {catalog}.information_schema.column_tags "
            f"WHERE schema_name = '{schema}' AND table_name IN ({safe_tables})",
        ])

    routine_queries = [
        "SELECT routine_name, routine_type, routine_definition, "
        "data_type AS return_type, routine_schema "
        f"FROM {catalog}.INFORMATION_SCHEMA.ROUTINES "
        f"WHERE routine_schema = '{schema}'"
        for catalog, schema in get_unique_schemas(refs)
    ]
    queries = {
        "uc_columns": " UNION ALL ".join(column_queries) or None,
        "uc_tags": " UNION ALL ".join(tag_queries) or None,
        "uc_routines": " UNION ALL ".join(routine_queries) or None,
    }
    return _execute_queries(ws, warehouse_id=warehouse_id, queries=queries)


def _execute_queries(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    queries: dict[str, str | None],
) -> dict[str, list[dict] | None]:
    result: dict[str, list[dict] | None] = {}
    for key, statement in queries.items():
        if not statement:
            result[key] = []
            continue
        try:
            result[key] = _sql_rows(ws, warehouse_id=warehouse_id, statement=statement)
        except Exception as exc:
            logger.warning("OBO metadata fetch failed for %s: %s", key, exc)
            result[key] = None
    logger.info(
        "OBO metadata prefetch: %s",
        {key: (len(value) if value is not None else "FAILED") for key, value in result.items()},
    )
    return result
