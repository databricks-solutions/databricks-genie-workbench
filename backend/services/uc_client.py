"""Unity Catalog browser for the Create Wizard."""
import logging
from backend.services.auth import get_workspace_client
from backend.sql_executor import execute_sql

logger = logging.getLogger(__name__)


def search_tables(
    keywords: list[str],
    catalogs: list[str] | None = None,
    max_results: int = 50,
) -> dict:
    """Search for tables across Unity Catalog using information_schema.

    Matches keywords against table names, column names, table comments,
    and column comments via OR + LIKE patterns. Returns matching tables
    with their matching columns and metadata.

    Args:
        keywords: Search terms (the LLM should generate synonyms, abbreviations, etc.)
        catalogs: Optional list of catalogs to scope the search. Empty/None = all accessible.
        max_results: Maximum tables to return.

    Returns:
        Dict with 'tables' list, 'search_terms_used', 'catalogs_searched', 'total_matches'.
    """
    if not keywords:
        return {"error": "No search keywords provided", "tables": []}

    # Build LIKE conditions for each keyword — match table names, column names,
    # comments, AND catalog/schema names for broader discovery
    like_conditions = []
    for kw in keywords:
        safe_kw = kw.replace("'", "''").replace("%", "\\%").replace("_", "\\_").lower()
        like_conditions.extend([
            f"lower(t.table_catalog) LIKE '%{safe_kw}%'",
            f"lower(t.table_schema) LIKE '%{safe_kw}%'",
            f"lower(t.table_name) LIKE '%{safe_kw}%'",
            f"lower(t.comment) LIKE '%{safe_kw}%'",
            f"lower(c.column_name) LIKE '%{safe_kw}%'",
            f"lower(c.comment) LIKE '%{safe_kw}%'",
        ])

    where_clause = " OR ".join(like_conditions)

    # Optional catalog filter
    catalog_filter = ""
    if catalogs:
        safe_catalogs = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in catalogs)
        catalog_filter = f"AND t.table_catalog IN ({safe_catalogs})"

    # Build keyword match expressions for the matching_columns and matched_keywords fields
    keyword_case_exprs = []
    for kw in keywords:
        safe_kw = kw.replace("'", "''").replace("%", "\\%").replace("_", "\\_").lower()
        keyword_case_exprs.append(
            f"CASE WHEN lower(c.column_name) LIKE '%{safe_kw}%' "
            f"OR lower(c.comment) LIKE '%{safe_kw}%' "
            f"THEN c.column_name END"
        )

    matching_cols_expr = "collect_set(COALESCE(" + ", ".join(
        f"CASE WHEN lower(c.column_name) LIKE '%{kw.replace(chr(39), chr(39)*2).lower()}%' "
        f"OR lower(c.comment) LIKE '%{kw.replace(chr(39), chr(39)*2).lower()}%' "
        f"THEN c.column_name END"
        for kw in keywords
    ) + "))"

    # Simpler approach: just collect columns that matched any keyword
    col_match_parts = []
    for kw in keywords:
        safe_kw = kw.replace("'", "''").replace("%", "\\%").replace("_", "\\_").lower()
        col_match_parts.append(
            f"lower(c.column_name) LIKE '%{safe_kw}%' OR lower(c.comment) LIKE '%{safe_kw}%'"
        )
    col_match_condition = " OR ".join(col_match_parts)

    sql = f"""
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.comment AS table_comment,
        t.table_type,
        t.last_altered,
        collect_set(
            CASE WHEN {col_match_condition} THEN c.column_name END
        ) AS matching_columns,
        count(DISTINCT c.column_name) AS total_columns
    FROM system.information_schema.tables t
    LEFT JOIN system.information_schema.columns c
        ON t.table_catalog = c.table_catalog
        AND t.table_schema = c.table_schema
        AND t.table_name = c.table_name
    WHERE ({where_clause})
    {catalog_filter}
    AND t.table_schema != 'information_schema'
    GROUP BY t.table_catalog, t.table_schema, t.table_name,
             t.comment, t.table_type, t.last_altered
    ORDER BY t.last_altered DESC NULLS LAST
    LIMIT {max_results}
    """

    try:
        sql = sql.strip()
        logger.info("search_tables: keywords=%s, catalogs=%s", keywords, catalogs)
        logger.debug("search_tables SQL:\n%s", sql)
        result = execute_sql(sql)
        if result.get("error"):
            logger.warning("search_tables query failed: %s", result["error"])
            return {"error": result["error"], "tables": []}

        tables = []
        for row in result.get("data", []):
            full_name = f"{row[0]}.{row[1]}.{row[2]}"
            # collect_set returns a string like '["col1","col2"]' from the API
            raw_cols = row[6] if len(row) > 6 else None
            if isinstance(raw_cols, str):
                import json as _json
                try:
                    matching_cols = [c for c in _json.loads(raw_cols) if c is not None]
                except (ValueError, TypeError):
                    matching_cols = []
            elif isinstance(raw_cols, list):
                matching_cols = [c for c in raw_cols if c is not None]
            else:
                matching_cols = []

            # Determine which keywords matched this table
            matched_kws = set()
            table_name_lower = (row[2] or "").lower()
            table_comment_lower = (row[3] or "").lower()
            for kw in keywords:
                kw_lower = kw.lower()
                if kw_lower in table_name_lower or kw_lower in table_comment_lower:
                    matched_kws.add(kw)
                for col in matching_cols:
                    if kw_lower in col.lower():
                        matched_kws.add(kw)

            tables.append({
                "full_name": full_name,
                "comment": row[3] or "",
                "table_type": row[4] or "",
                "total_columns": row[7] or 0,
                "matching_columns": matching_cols[:10],  # Cap to avoid bloat
                "matched_keywords": sorted(matched_kws),
            })

        catalogs_searched = sorted(set(t["full_name"].split(".")[0] for t in tables)) if tables else []

        # Build table list in the format the frontend expects for multi-select UI
        ui_tables = []
        for t in tables:
            desc_parts = []
            if t["comment"]:
                desc_parts.append(t["comment"])
            if t["matching_columns"]:
                desc_parts.append(f"Matching columns: {', '.join(t['matching_columns'][:5])}")
            if t["matched_keywords"]:
                desc_parts.append(f"Matched: {', '.join(t['matched_keywords'])}")

            ui_tables.append({
                "full_name": t["full_name"],
                "name": t["full_name"].split(".")[-1],
                "comment": " | ".join(desc_parts) if desc_parts else "",
            })

        return {
            "tables": ui_tables,
            "search_results": tables,  # Full results for LLM context
            "search_terms_used": keywords,
            "catalogs_searched": catalogs_searched,
            "total_matches": len(tables),
        }

    except Exception as e:
        logger.exception("search_tables failed")
        return {"error": str(e), "tables": []}


def list_catalogs() -> list[dict]:
    try:
        client = get_workspace_client()
        return [{"name": c.name, "comment": c.comment} for c in client.catalogs.list()]
    except Exception as e:
        logger.error(f"list_catalogs failed: {e}")
        return []


def list_schemas(catalog: str) -> list[dict]:
    try:
        client = get_workspace_client()
        return [{"name": s.name, "catalog_name": s.catalog_name, "comment": s.comment}
                for s in client.schemas.list(catalog_name=catalog)]
    except Exception as e:
        logger.error(f"list_schemas({catalog}) failed: {e}")
        return []


def list_tables(catalog: str, schema: str) -> list[dict]:
    try:
        client = get_workspace_client()
        return [
            {
                "name": t.name,
                "full_name": t.full_name,
                "catalog_name": t.catalog_name,
                "schema_name": t.schema_name,
                "comment": t.comment,
                "table_type": str(t.table_type) if t.table_type else None,
            }
            for t in client.tables.list(catalog_name=catalog, schema_name=schema)
        ]
    except Exception as e:
        logger.error(f"list_tables({catalog}.{schema}) failed: {e}")
        return []


def get_table_columns(catalog: str, schema: str, table: str) -> list[dict]:
    try:
        client = get_workspace_client()
        t = client.tables.get(f"{catalog}.{schema}.{table}")
        return [
            {
                "name": col.name,
                "type": str(col.type_text or col.type_name or ""),
                "comment": col.comment,
            }
            for col in (t.columns or [])
        ]
    except Exception as e:
        logger.error(f"get_table_columns({catalog}.{schema}.{table}) failed: {e}")
        return []
