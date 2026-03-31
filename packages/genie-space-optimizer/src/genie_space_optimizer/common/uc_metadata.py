"""
Unity Catalog introspection helpers.

Provides two ways to fetch metadata:

1. **REST API** (preferred) – uses ``WorkspaceClient`` directly, bypasses
   Spark Connect, and avoids ``system.information_schema`` permission issues.
2. **Spark SQL** (legacy fallback) – queries ``{catalog}.information_schema``
   via a Spark session.

Preflight should prefer the REST variants; the Spark functions are kept for
backwards-compatibility and edge cases where the SDK is unavailable.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def parse_table_identifier(identifier: str) -> tuple[str, str, str]:
    """Parse a Genie space table identifier into (catalog, schema, table).

    Identifiers come from the Genie Space API ``data_sources.tables[].identifier``
    and are typically fully-qualified: ``catalog.schema.table``.
    """
    parts = identifier.replace("`", "").split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0] if parts else ""


def extract_genie_space_table_refs(
    config: dict[str, Any],
) -> list[tuple[str, str, str]]:
    """Extract (catalog, schema, table) from a Genie Space config.

    Reads ``_tables``, ``_metric_views``, and ``_functions`` keys
    produced by ``fetch_space_config()``.
    """
    refs: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for key in ("_tables", "_metric_views", "_functions"):
        for identifier in config.get(key, []):
            if not identifier or identifier in seen:
                continue
            seen.add(identifier)
            refs.append(parse_table_identifier(identifier))
    return refs


def get_unique_schemas(
    refs: list[tuple[str, str, str]],
) -> list[tuple[str, str]]:
    """Deduplicate (catalog, schema) pairs from table references."""
    pairs: dict[tuple[str, str], None] = {}
    for cat, sch, _tbl in refs:
        if cat and sch:
            pairs[(cat, sch)] = None
    return list(pairs.keys())


# ---------------------------------------------------------------------------
# REST API helpers (preferred – no Spark, no information_schema permission)
# ---------------------------------------------------------------------------


def get_columns_for_tables_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch column metadata via the Unity Catalog REST API.

    Calls ``w.tables.get()`` per table and extracts column info from the
    ``TableInfo.columns`` list.  Returns dicts with keys:
    ``catalog_name``, ``schema_name``, ``table_name``,
    ``column_name``, ``data_type``, ``comment``.

    The ``catalog_name`` and ``schema_name`` fields enable FQN-based
    lookups downstream, avoiding short-name collisions in multi-catalog
    setups.
    """
    rows: list[dict] = []
    failed: list[str] = []
    for cat, sch, tbl in refs:
        if not (cat and sch and tbl):
            continue
        full_name = f"{cat}.{sch}.{tbl}"
        try:
            table_info = w.tables.get(full_name=full_name)
        except Exception as exc:
            failed.append(f"{full_name}: {type(exc).__name__}: {exc}")
            continue
        if not table_info.columns:
            continue
        for col in table_info.columns:
            rows.append({
                "catalog_name": cat,
                "schema_name": sch,
                "table_name": tbl,
                "column_name": getattr(col, "name", ""),
                "data_type": getattr(col, "type_text", ""),
                "comment": getattr(col, "comment", None) or "",
            })
    summary = (
        f"[UC_METADATA] REST get_columns_for_tables_rest: "
        f"{len(refs)} refs, {len(refs) - len(failed)} succeeded, {len(rows)} column rows"
    )
    print(summary, flush=True)
    if failed:
        for f in failed:
            print(f"[UC_METADATA]   FAILED: {f}", flush=True)
    return rows


def get_foreign_keys_for_tables_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch FK constraint metadata via the Unity Catalog REST API.

    Calls ``w.tables.get()`` per table and extracts ``ForeignKeyConstraint``
    entries from ``TableInfo.table_constraints``.  Returns dicts with keys:
    ``child_table`` (FQN), ``child_columns``, ``parent_table`` (FQN),
    ``parent_columns``, ``constraint_name``.

    Multi-column FKs are represented as a single dict with list-valued
    ``child_columns`` / ``parent_columns``.
    """
    rows: list[dict] = []
    failed: list[str] = []
    for cat, sch, tbl in refs:
        if not (cat and sch and tbl):
            continue
        full_name = f"{cat}.{sch}.{tbl}"
        try:
            table_info = w.tables.get(full_name=full_name)
        except Exception as exc:
            failed.append(f"{full_name}: {type(exc).__name__}: {exc}")
            continue
        constraints = getattr(table_info, "table_constraints", None) or []
        for tc in constraints:
            fk = getattr(tc, "foreign_key_constraint", None)
            if not fk:
                continue
            parent_table = getattr(fk, "parent_table", "") or ""
            child_cols = list(getattr(fk, "child_columns", []) or [])
            parent_cols = list(getattr(fk, "parent_columns", []) or [])
            if parent_table and child_cols and parent_cols:
                rows.append({
                    "child_table": full_name,
                    "child_columns": child_cols,
                    "parent_table": parent_table,
                    "parent_columns": parent_cols,
                    "constraint_name": getattr(fk, "name", "") or "",
                })
    summary = (
        f"[UC_METADATA] REST get_foreign_keys_for_tables_rest: "
        f"{len(refs)} refs, {len(refs) - len(failed)} succeeded, {len(rows)} FK rows"
    )
    print(summary, flush=True)
    if failed:
        for f in failed:
            print(f"[UC_METADATA]   FAILED: {f}", flush=True)
    return rows


def get_routines_for_schemas_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch function/routine metadata via the Unity Catalog REST API.

    Calls ``w.functions.list()`` per unique (catalog, schema) and returns
    dicts matching the Spark output: ``routine_name``, ``routine_type``,
    ``routine_definition``, ``return_type``, ``routine_schema``.
    """
    schemas = get_unique_schemas(refs)
    rows: list[dict] = []
    for cat, sch in schemas:
        try:
            for fn in w.functions.list(catalog_name=cat, schema_name=sch):
                rows.append({
                    "routine_name": getattr(fn, "name", ""),
                    "routine_type": getattr(fn, "routine_type", None)
                    or getattr(fn, "function_type", ""),
                    "routine_definition": getattr(fn, "routine_definition", "") or "",
                    "return_type": (
                        getattr(fn, "data_type", None)
                        or getattr(fn, "return_type", None)
                        or ""
                    ),
                    "routine_schema": sch,
                })
        except Exception as exc:
            print(f"[UC_METADATA] REST functions.list failed for {cat}.{sch}: {type(exc).__name__}: {exc}", flush=True)
    print(f"[UC_METADATA] REST get_routines_for_schemas_rest: {len(schemas)} schemas, {len(rows)} routines", flush=True)
    return rows


def get_tags_for_tables_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch table tags via the Entity Tag Assignments REST API.

    Calls ``w.entity_tag_assignments.list(entity_type="tables", ...)`` per
    table and returns dicts with keys matching the Spark
    ``information_schema.table_tags`` output: ``catalog_name``,
    ``schema_name``, ``table_name``, ``tag_name``, ``tag_value``.
    """
    rows: list[dict] = []
    failed: list[str] = []
    for cat, sch, tbl in refs:
        if not (cat and sch and tbl):
            continue
        full_name = f"{cat}.{sch}.{tbl}"
        try:
            for tag in w.entity_tag_assignments.list(
                entity_type="tables", entity_name=full_name
            ):
                rows.append({
                    "catalog_name": cat,
                    "schema_name": sch,
                    "table_name": tbl,
                    "tag_name": getattr(tag, "tag_key", ""),
                    "tag_value": getattr(tag, "tag_value", "") or "",
                })
        except Exception as exc:
            failed.append(f"{full_name}: {type(exc).__name__}: {exc}")
    summary = (
        f"[UC_METADATA] REST get_tags_for_tables_rest: "
        f"{len(refs)} refs, {len(refs) - len(failed)} succeeded, {len(rows)} tag rows"
    )
    print(summary, flush=True)
    if failed:
        for f in failed:
            print(f"[UC_METADATA]   FAILED: {f}", flush=True)
    return rows


# ---------------------------------------------------------------------------
# Spark SQL helpers (legacy fallback)
# ---------------------------------------------------------------------------


def get_columns_for_tables(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch column metadata only for the specific tables referenced by a Genie space.

    Unlike ``get_columns`` which returns all columns in a schema, this function
    queries ``information_schema`` scoped to the exact table names.
    """
    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        unions.append(
            f"SELECT '{cat}' AS catalog_name, '{sch}' AS schema_name, "
            f"table_name, column_name, data_type, comment "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name IN ({safe_tables})"
        )
    if not unions:
        return spark.sql(
            "SELECT CAST(NULL AS STRING) AS catalog_name, CAST(NULL AS STRING) AS schema_name, "
            "CAST(NULL AS STRING) AS table_name, CAST(NULL AS STRING) AS column_name, "
            "CAST(NULL AS STRING) AS data_type, CAST(NULL AS STRING) AS comment WHERE 1=0"
        )
    return spark.sql(" UNION ALL ".join(unions))


def get_tags_for_tables(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch tags only for the specific tables referenced by a Genie space."""
    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        unions.append(
            f"SELECT * FROM {cat}.information_schema.table_tags "
            f"WHERE schema_name = '{sch}' AND table_name IN ({safe_tables})"
        )
    if not unions:
        return spark.sql("SELECT CAST(NULL AS STRING) AS schema_name WHERE 1=0")
    return spark.sql(" UNION ALL ".join(unions))


def get_routines_for_schemas(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch routines for schemas that contain Genie space table references."""
    schemas = get_unique_schemas(refs)
    unions: list[str] = []
    for cat, sch in schemas:
        unions.append(
            f"SELECT routine_name, routine_type, routine_definition, "
            f"data_type AS return_type, routine_schema "
            f"FROM {cat}.INFORMATION_SCHEMA.ROUTINES "
            f"WHERE routine_schema = '{sch}'"
        )
    if not unions:
        return spark.sql("SELECT CAST(NULL AS STRING) AS routine_name WHERE 1=0")
    return spark.sql(" UNION ALL ".join(unions))


def get_foreign_keys_for_tables(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch FK constraints via Spark SQL against ``information_schema``.

    Queries ``key_column_usage`` joined with ``constraint_column_usage`` per
    unique (catalog, schema) pair derived from *refs*.  Multi-column FKs are
    grouped into a single dict.

    Returns the same schema as ``get_foreign_keys_for_tables_rest``.
    """
    schemas = get_unique_schemas(refs)
    ref_set = {f"{c}.{s}.{t}".lower() for c, s, t in refs if c and s and t}

    unions: list[str] = []
    for cat, sch in schemas:
        unions.append(
            f"""
            SELECT
                k.constraint_name,
                k.table_catalog AS child_catalog,
                k.table_schema  AS child_schema,
                k.table_name    AS child_table_name,
                k.column_name   AS child_column,
                c.table_catalog AS parent_catalog,
                c.table_schema  AS parent_schema,
                c.table_name    AS parent_table_name,
                c.column_name   AS parent_column
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY constraint_catalog, constraint_schema, constraint_name
                    ORDER BY ordinal_position
                ) AS rn
                FROM {cat}.information_schema.key_column_usage
                WHERE table_schema = '{sch}'
                  AND position_in_unique_constraint IS NOT NULL
            ) k
            JOIN (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY constraint_catalog, constraint_schema, constraint_name
                    ORDER BY column_name
                ) AS rn
                FROM {cat}.information_schema.constraint_column_usage
            ) c
              ON k.constraint_catalog = c.constraint_catalog
             AND k.constraint_schema  = c.constraint_schema
             AND k.constraint_name    = c.constraint_name
             AND k.rn                 = c.rn
            """
        )
    if not unions:
        return []
    try:
        df = spark.sql(" UNION ALL ".join(unions))
        raw_rows = df.collect()
    except Exception as exc:
        print(f"[UC_METADATA] Spark FK query failed: {exc}", flush=True)
        return []

    grouped: dict[str, dict] = {}
    for r in raw_rows:
        child_fqn = f"{r['child_catalog']}.{r['child_schema']}.{r['child_table_name']}"
        parent_fqn = f"{r['parent_catalog']}.{r['parent_schema']}.{r['parent_table_name']}"
        if child_fqn.lower() not in ref_set and parent_fqn.lower() not in ref_set:
            continue
        key = r["constraint_name"]
        if key not in grouped:
            grouped[key] = {
                "child_table": child_fqn,
                "child_columns": [],
                "parent_table": parent_fqn,
                "parent_columns": [],
                "constraint_name": key,
            }
        grouped[key]["child_columns"].append(r["child_column"])
        grouped[key]["parent_columns"].append(r["parent_column"])

    result = list(grouped.values())
    print(
        f"[UC_METADATA] Spark get_foreign_keys_for_tables: "
        f"{len(schemas)} schema(s), {len(raw_rows)} raw rows, {len(result)} FKs",
        flush=True,
    )
    return result


def get_columns(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch column metadata from ``information_schema.columns``.

    Returns columns: ``table_name``, ``column_name``, ``data_type``, ``comment``.
    """
    return spark.sql(
        f"SELECT table_name, column_name, data_type, comment "
        f"FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}'"
    )


def get_tags(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch table tags from ``information_schema.table_tags``.

    Returns all columns from the ``table_tags`` view filtered to the given schema.
    """
    return spark.sql(
        f"SELECT * FROM {catalog}.information_schema.table_tags "
        f"WHERE schema_name = '{schema}'"
    )


def get_routines(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch routine (UDF / TVF) metadata from ``information_schema.routines``.

    Returns columns: ``routine_name``, ``routine_type``,
    ``routine_definition``, ``return_type``, ``routine_schema``.
    """
    return spark.sql(
        f"SELECT routine_name, routine_type, routine_definition, "
        f"data_type AS return_type, routine_schema "
        f"FROM {catalog}.INFORMATION_SCHEMA.ROUTINES "
        f"WHERE routine_schema = '{schema}'"
    )


def get_table_descriptions(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch table-level comments/descriptions.

    Returns columns: ``table_name``, ``comment``.
    """
    return spark.sql(
        f"SELECT table_name, comment "
        f"FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}'"
    )


def get_metric_views(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Discover metric views in the schema.

    Metric views are VIEWs whose names conventionally start with ``mv_``.
    Returns columns: ``table_name``, ``view_definition``.
    """
    return spark.sql(
        f"SELECT table_name, view_definition "
        f"FROM {catalog}.information_schema.views "
        f"WHERE table_schema = '{schema}' AND table_name LIKE 'mv\\_%'"
    )


# ═══════════════════════════════════════════════════════════════════════
# TVF Schema Overlap Analysis
# ═══════════════════════════════════════════════════════════════════════


def describe_tvf_output_columns(
    spark: Any,
    tvf_identifier: str,
) -> list[str]:
    """Return the output column names of a TVF via ``DESCRIBE FUNCTION EXTENDED``.

    Falls back to parsing the ``routine_definition`` SQL if DESCRIBE does not
    produce a parseable return-type table.  Returns an empty list on failure.
    """
    try:
        rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {tvf_identifier}").collect()
    except Exception:
        logger.warning("DESCRIBE FUNCTION EXTENDED failed for %s", tvf_identifier, exc_info=True)
        return []

    columns: list[str] = []
    in_return_section = False
    for row in rows:
        line = str(row[0]) if row else ""
        lower = line.strip().lower()
        if "return" in lower and "type" in lower:
            in_return_section = True
            continue
        if in_return_section:
            if not line.strip() or line.startswith("--"):
                break
            parts = line.strip().split()
            if parts:
                col_name = parts[0].strip("`").strip('"').strip()
                if col_name and not col_name.startswith("("):
                    columns.append(col_name.lower())

    if not columns:
        try:
            cat, sch, func_name = parse_table_identifier(tvf_identifier)
            rdf = spark.sql(
                f"SELECT routine_definition FROM {cat}.information_schema.routines "
                f"WHERE routine_schema = '{sch}' AND routine_name = '{func_name}'"
            ).collect()
            if rdf:
                defn = str(rdf[0][0] or "")
                import re
                select_match = re.search(r"RETURNS\s+TABLE\s*\(([^)]+)\)", defn, re.IGNORECASE)
                if select_match:
                    for col_def in select_match.group(1).split(","):
                        col_name = col_def.strip().split()[0].strip("`").strip('"')
                        if col_name:
                            columns.append(col_name.lower())
        except Exception:
            logger.debug("Fallback column extraction failed for %s", tvf_identifier, exc_info=True)

    return columns


def check_tvf_schema_overlap(
    spark: Any,
    tvf_identifier: str,
    metadata_snapshot: dict,
) -> dict:
    """Check schema overlap between a TVF and other assets in the Genie Space.

    Returns::

        {
            "tvf_columns": ["col1", "col2"],
            "covered_columns": {"col1": "table_a", "col2": "mv_b"},
            "uncovered_columns": ["col3"],
            "coverage_ratio": 0.67,
            "full_coverage": False,
        }
    """
    tvf_cols = describe_tvf_output_columns(spark, tvf_identifier)
    if not tvf_cols:
        return {
            "tvf_columns": [],
            "covered_columns": {},
            "uncovered_columns": [],
            "coverage_ratio": 0.0,
            "full_coverage": False,
        }

    space_columns: set[str] = set()
    column_source: dict[str, str] = {}

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}

    for tbl in ds.get("tables", []):
        tbl_name = tbl.get("identifier", tbl.get("name", ""))
        for cc in tbl.get("column_configs", []):
            col = (cc.get("column_name") or cc.get("name") or "").lower()
            if col and col not in column_source:
                space_columns.add(col)
                column_source[col] = tbl_name

    for mv in ds.get("metric_views", []):
        mv_name = mv.get("identifier", mv.get("name", ""))
        for m in mv.get("measures", []):
            col = (m.get("name") or "").lower()
            if col and col not in column_source:
                space_columns.add(col)
                column_source[col] = mv_name
        for d in mv.get("dimensions", []):
            col = (d.get("name") or "").lower()
            if col and col not in column_source:
                space_columns.add(col)
                column_source[col] = mv_name

    covered: dict[str, str] = {}
    uncovered: list[str] = []
    for col in tvf_cols:
        if col in column_source:
            covered[col] = column_source[col]
        else:
            uncovered.append(col)

    ratio = len(covered) / len(tvf_cols) if tvf_cols else 0.0

    return {
        "tvf_columns": tvf_cols,
        "covered_columns": covered,
        "uncovered_columns": uncovered,
        "coverage_ratio": ratio,
        "full_coverage": len(uncovered) == 0 and len(tvf_cols) > 0,
    }
