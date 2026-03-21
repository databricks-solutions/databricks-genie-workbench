"""
Preflight logic — extracted from the harness to keep orchestration lean.

Fetches Genie Space config, UC metadata, loads or generates benchmarks,
validates SQL, registers judge prompts, and creates the initial MLflow
LoggedModel (iteration 0).
"""

from __future__ import annotations

import logging
import re
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import (
    EXPERIMENT_PATH_TEMPLATE,
    MAX_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
    format_mlflow_template,
)
from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.common.genie_schema import validate_serialized_space
from genie_space_optimizer.common.uc_metadata import (
    extract_genie_space_table_refs,
    get_columns,
    get_columns_for_tables,
    get_columns_for_tables_rest,
    get_foreign_keys_for_tables,
    get_foreign_keys_for_tables_rest,
    get_routines,
    get_routines_for_schemas,
    get_routines_for_schemas_rest,
    get_tags,
    get_tags_for_tables,
    get_tags_for_tables_rest,
)
from genie_space_optimizer.optimization.benchmarks import validate_benchmarks
from genie_space_optimizer.optimization.applier import _get_general_instructions
from genie_space_optimizer.optimization.evaluation import (
    _drop_benchmark_table,
    _flag_stale_temporal_benchmarks,
    create_evaluation_dataset,
    extract_genie_space_benchmarks,
    generate_benchmarks,
    load_benchmarks_from_dataset,
    register_benchmark_prompts,
    register_instruction_version,
)
from genie_space_optimizer.optimization.state import (
    load_run,
    load_runs_for_space,
    update_run_status as _update_run_status,
    write_stage,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ── Preflight print helpers ──────────────────────────────────────────
_PF_W = 60


def _pf_section(title: str) -> str:
    return f"\n{'=' * _PF_W}\n  {title}\n{'=' * _PF_W}"


def _pf_kv(key: str, value: object) -> str:
    return f"  {key + ':':<22s} {value}"


def _pf_bar() -> str:
    return "-" * _PF_W


def compute_asset_fingerprint(config: dict) -> str:
    """Compute a short hash over the sorted table/view/function refs in the Genie Space config.

    Used to detect when the Genie Space schema has changed (tables added or
    removed) between benchmark runs so stale benchmarks are regenerated.
    """
    import hashlib
    import json as _json

    refs: list[str] = []
    for t in config.get("_tables", []):
        if isinstance(t, str):
            refs.append(t)
        elif isinstance(t, dict):
            refs.append(t.get("identifier", t.get("name", "")))
    for mv in config.get("_metric_views", []):
        if isinstance(mv, str):
            refs.append(mv)
        elif isinstance(mv, dict):
            refs.append(mv.get("identifier", mv.get("name", "")))
    for fn in config.get("_functions", []):
        if isinstance(fn, str):
            refs.append(fn)
        elif isinstance(fn, dict):
            refs.append(fn.get("identifier", fn.get("name", "")))
    refs = sorted(set(r for r in refs if r))
    return hashlib.sha256(_json.dumps(refs).encode()).hexdigest()[:16]


def _collect_or_empty(fetch_fn: Any, label: str) -> tuple[list[dict], str | None]:
    """Best-effort metadata fetch; continue if catalog permissions are limited.

    Returns ``(rows, error_message)``.  *error_message* is ``None`` on
    success and a human-readable string when the query fails.
    """
    try:
        df = fetch_fn()
        rows = [r.asDict() for r in df.collect()]
        if not rows:
            logger.info(
                "UC metadata query for %s returned 0 rows (query succeeded, no data matched)", label,
            )
        return rows, None
    except Exception as exc:
        err_str = str(exc)
        is_permission = "INSUFFICIENT_PERMISSIONS" in err_str or "permission" in err_str.lower()
        level_fn = print if is_permission else logger.warning
        level_fn(
            f"[PREFLIGHT] {'PERMISSION DENIED' if is_permission else 'SKIPPED'} "
            f"for {label}: {type(exc).__name__}: {err_str[:300]}"
        )
        if is_permission:
            print(
                f"[PREFLIGHT]   This is non-fatal — {label} metadata will be empty. "
                "Tags are informational and not required for optimization."
            )
        return [], f"{type(exc).__name__}: {exc}"


def _resolve_experiment_path(*, space_id: str, domain: str) -> str:
    """Return a stable, app-owned experiment path for this Genie Space.

    Experiments live under ``/Shared/genie-space-optimizer/<space_id>/<domain>``
    so the SP can create them without OBO and each space gets its own experiment.
    """
    return format_mlflow_template(EXPERIMENT_PATH_TEMPLATE, space_id=space_id, domain=domain)


def _ensure_experiment_parent_dir(ws: WorkspaceClient, experiment_path: str) -> bool:
    """Ensure the workspace parent directory exists before creating experiment.

    Returns ``True`` if the directory was verified/created, ``False`` on failure.
    """
    if not experiment_path.startswith("/"):
        return True
    parent = str(PurePosixPath(experiment_path).parent)
    if not parent or parent == "/":
        return True
    try:
        ws.workspace.mkdirs(parent)
        return True
    except Exception as exc:
        logger.warning("Could not ensure experiment parent directory %s: %s", parent, exc)
        return False


def _collect_data_profile(
    spark: "SparkSession",
    tables: list[str],
    uc_columns: list[dict],
    *,
    max_tables: int = 0,
    sample_size: int = 0,
    low_cardinality_threshold: int = 0,
    metric_view_names: frozenset[str] = frozenset(),
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
) -> dict[str, dict]:
    """Profile actual data values for Genie Space tables.

    For each table (up to *max_tables*) runs a single TABLESAMPLE-bounded
    SQL query that collects per-column cardinality, distinct values
    for low-cardinality string columns, and min/max for numeric/date columns.

    When *w* and *warehouse_id* are provided the queries are routed through
    the SQL warehouse Statement Execution API; otherwise Spark SQL is used
    as a fallback.

    Returns ``{table_fqn: {"row_count": N, "columns": {col: {...}}}}``.
    Failures on individual tables are logged and skipped.
    """
    import json as _json

    from genie_space_optimizer.common.config import (
        LOW_CARDINALITY_THRESHOLD,
        MAX_PROFILE_TABLES,
        PROFILE_SAMPLE_SIZE,
    )
    from genie_space_optimizer.optimization.evaluation import _exec_sql

    max_tables = max_tables or MAX_PROFILE_TABLES
    sample_size = sample_size or PROFILE_SAMPLE_SIZE
    low_cardinality_threshold = low_cardinality_threshold or LOW_CARDINALITY_THRESHOLD

    _sql_kw: dict[str, Any] = dict(w=w, warehouse_id=warehouse_id, catalog=catalog, schema=schema)

    _NUMERIC_TYPES = frozenset({
        "int", "integer", "bigint", "smallint", "tinyint",
        "float", "double", "decimal", "numeric", "long", "short",
    })
    _DATE_TYPES = frozenset({"date", "timestamp", "timestamp_ntz"})
    _COMPLEX_TYPES = frozenset({"map", "array", "struct", "binary"})

    cols_by_table: dict[str, list[dict]] = {}
    for c in uc_columns:
        if not isinstance(c, dict):
            continue
        tbl = str(c.get("table_name") or "").strip()
        if tbl:
            cols_by_table.setdefault(tbl, []).append(c)

    def _parse_collect_set(raw_vals: Any) -> list[str]:
        """Handle COLLECT_SET result from warehouse (JSON string) or Spark (list)."""
        if raw_vals is None:
            return []
        if isinstance(raw_vals, str):
            try:
                raw_vals = _json.loads(raw_vals)
            except (ValueError, TypeError):
                return [raw_vals]
        return sorted(str(v) for v in raw_vals)

    profile: dict[str, dict] = {}
    for table_fqn in tables[:max_tables]:
        _leaf = table_fqn.split(".")[-1].strip("`").lower()
        if _leaf in metric_view_names or table_fqn.strip().lower() in metric_view_names:
            logger.info("Data profiling: %s is a metric view, skipping", table_fqn)
            continue

        tbl_key = table_fqn.strip().lower()
        tbl_cols = cols_by_table.get(tbl_key, [])
        if not tbl_cols:
            for k, v in cols_by_table.items():
                if k.endswith(table_fqn.split(".")[-1].strip("`").lower()):
                    tbl_cols = v
                    break
        if not tbl_cols:
            logger.debug("Data profiling: no columns known for %s, skipping", table_fqn)
            continue

        escaped_table = table_fqn.replace("`", "")
        parts = escaped_table.split(".")
        fq_table = ".".join(f"`{p.strip()}`" for p in parts)

        try:
            count_df = _exec_sql(
                f"SELECT COUNT(*) AS cnt FROM {fq_table}", spark, **_sql_kw,
            )
            row_count = int(count_df.iloc[0]["cnt"]) if not count_df.empty else 0
        except Exception:
            logger.debug("Data profiling: COUNT(*) failed for %s", table_fqn, exc_info=True)
            row_count = -1

        select_parts: list[str] = []
        col_meta: list[tuple[str, str]] = []
        for c in tbl_cols:
            col_name = str(c.get("column_name") or "").strip()
            dtype_raw = str(c.get("data_type") or "").strip().lower()
            dtype = dtype_raw.split("(")[0].strip()
            if not col_name:
                continue
            escaped_col = f"`{col_name}`"
            alias_card = f"`_card_{col_name}`"
            select_parts.append(f"COUNT(DISTINCT {escaped_col}) AS {alias_card}")

            if dtype in _NUMERIC_TYPES or dtype in _DATE_TYPES:
                select_parts.append(f"MIN({escaped_col}) AS `_min_{col_name}`")
                select_parts.append(f"MAX({escaped_col}) AS `_max_{col_name}`")

            col_meta.append((col_name, dtype))

        if not select_parts:
            continue

        select_clause = ", ".join(select_parts)
        query = (
            f"SELECT {select_clause} "
            f"FROM (SELECT * FROM {fq_table} TABLESAMPLE ({sample_size} ROWS))"
        )

        try:
            stats_df = _exec_sql(query, spark, **_sql_kw)
        except Exception:
            fallback_query = (
                f"SELECT {select_clause} "
                f"FROM (SELECT * FROM {fq_table} LIMIT {sample_size})"
            )
            try:
                stats_df = _exec_sql(fallback_query, spark, **_sql_kw)
            except Exception:
                logger.info("Data profiling: stats query failed for %s, skipping", table_fqn, exc_info=True)
                continue

        if stats_df.empty:
            continue
        row = stats_df.iloc[0]

        columns_profile: dict[str, dict] = {}
        low_card_cols: list[tuple[str, str]] = []

        for col_name, dtype in col_meta:
            card = row[f"_card_{col_name}"]
            col_info: dict[str, Any] = {"cardinality": int(card) if card is not None else 0}

            if dtype in _NUMERIC_TYPES or dtype in _DATE_TYPES:
                min_val = row[f"_min_{col_name}"]
                max_val = row[f"_max_{col_name}"]
                if min_val is not None:
                    col_info["min"] = str(min_val)
                if max_val is not None:
                    col_info["max"] = str(max_val)

            if (
                col_info["cardinality"] > 0
                and col_info["cardinality"] <= low_cardinality_threshold
                and dtype not in _NUMERIC_TYPES
                and dtype not in _DATE_TYPES
                and not any(dtype.startswith(ct) for ct in _COMPLEX_TYPES)
            ):
                low_card_cols.append((col_name, dtype))

            columns_profile[col_name] = col_info

        for col_name, _dtype in low_card_cols:
            escaped_col = f"`{col_name}`"
            dv_query = (
                f"SELECT COLLECT_SET({escaped_col}) AS vals "
                f"FROM (SELECT * FROM {fq_table} TABLESAMPLE ({sample_size} ROWS)) "
                f"WHERE {escaped_col} IS NOT NULL"
            )
            try:
                dv_df = _exec_sql(dv_query, spark, **_sql_kw)
                if not dv_df.empty and dv_df.iloc[0]["vals"] is not None:
                    parsed = _parse_collect_set(dv_df.iloc[0]["vals"])
                    if parsed:
                        columns_profile[col_name]["distinct_values"] = parsed
            except Exception:
                dv_fallback = (
                    f"SELECT COLLECT_SET({escaped_col}) AS vals "
                    f"FROM (SELECT * FROM {fq_table} LIMIT {sample_size}) "
                    f"WHERE {escaped_col} IS NOT NULL"
                )
                try:
                    dv_df = _exec_sql(dv_fallback, spark, **_sql_kw)
                    if not dv_df.empty and dv_df.iloc[0]["vals"] is not None:
                        parsed = _parse_collect_set(dv_df.iloc[0]["vals"])
                        if parsed:
                            columns_profile[col_name]["distinct_values"] = parsed
                except Exception:
                    logger.debug(
                        "Data profiling: COLLECT_SET failed for %s.%s",
                        table_fqn, col_name, exc_info=True,
                    )

        profile[escaped_table] = {
            "row_count": row_count,
            "columns": columns_profile,
        }
        logger.info(
            "Data profiling: %s — %d rows, %d columns profiled, %d low-cardinality",
            table_fqn, row_count, len(columns_profile), len(low_card_cols),
        )

    return profile


def _compute_join_overlaps(
    spark: "SparkSession",
    fk_constraints: list[dict],
    *,
    sample_size: int = 1000,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
) -> list[dict]:
    """Compute FK-to-PK overlap ratios for known foreign-key pairs.

    For each FK constraint, runs a sampled LEFT JOIN to measure how many
    FK values actually match a PK value on the other side. Returns a list
    of ``{left_table, right_table, fk_column, pk_column, overlap_ratio}``
    dicts.  Failures are logged and skipped.

    When *w* and *warehouse_id* are provided, queries are routed through
    the SQL warehouse; otherwise Spark SQL is used.
    """
    from genie_space_optimizer.optimization.evaluation import _exec_sql

    _sql_kw: dict[str, Any] = dict(w=w, warehouse_id=warehouse_id, catalog=catalog, schema=schema)

    results: list[dict] = []
    for fk in fk_constraints:
        if not isinstance(fk, dict):
            continue
        left_table = str(fk.get("table_name") or fk.get("child_table") or "").strip()
        right_table = str(fk.get("referenced_table") or fk.get("parent_table") or "").strip()
        fk_col = str(fk.get("column_name") or fk.get("child_column") or "").strip()
        pk_col = str(fk.get("referenced_column") or fk.get("parent_column") or "").strip()
        if not all([left_table, right_table, fk_col, pk_col]):
            continue

        def _fq(tbl: str) -> str:
            parts = tbl.replace("`", "").split(".")
            return ".".join(f"`{p.strip()}`" for p in parts)

        query = (
            f"SELECT "
            f"COUNT(DISTINCT a.`{fk_col}`) AS left_distinct, "
            f"COUNT(DISTINCT b.`{pk_col}`) AS right_distinct, "
            f"COUNT(DISTINCT CASE WHEN b.`{pk_col}` IS NOT NULL "
            f"THEN a.`{fk_col}` END) AS overlap "
            f"FROM (SELECT * FROM {_fq(left_table)} TABLESAMPLE ({sample_size} ROWS)) a "
            f"LEFT JOIN {_fq(right_table)} b "
            f"ON a.`{fk_col}` = b.`{pk_col}`"
        )
        try:
            result_df = _exec_sql(query, spark, **_sql_kw)
            if not result_df.empty:
                r = result_df.iloc[0]
                left_d = int(r["left_distinct"] or 0)
                overlap_count = int(r["overlap"] or 0)
                right_d = int(r["right_distinct"] or 0)
                overlap_ratio = (overlap_count / left_d) if left_d > 0 else 0.0
                results.append({
                    "left_table": left_table,
                    "right_table": right_table,
                    "fk_column": fk_col,
                    "pk_column": pk_col,
                    "overlap_ratio": round(overlap_ratio, 3),
                    "left_distinct": left_d,
                    "right_distinct": right_d,
                })
        except Exception:
            logger.debug(
                "Join overlap query failed for %s.%s -> %s.%s",
                left_table, fk_col, right_table, pk_col,
                exc_info=True,
            )

    return results


def _validate_core_access(
    w: "WorkspaceClient",
    spark: SparkSession,
    genie_table_refs: list,
) -> None:
    """Early-fail if the SP cannot read table metadata for referenced schemas.

    Uses the REST API (``w.tables.get``) to validate access — the same path
    the harness uses for UC metadata extraction.  This avoids Spark SQL
    ``information_schema`` queries which have a hidden dependency on the
    ``system`` catalog.
    """
    schema_sample: dict[tuple[str, str], str] = {}
    for ref in genie_table_refs:
        cat = ref[0] if isinstance(ref, (list, tuple)) else ""
        sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
        tbl = ref[2] if isinstance(ref, (list, tuple)) and len(ref) > 2 else ""
        if cat and sch and tbl:
            key = (cat, sch)
            if key not in schema_sample:
                schema_sample[key] = f"{cat}.{sch}.{tbl}"

    if not schema_sample:
        return

    try:
        who = spark.sql("SELECT current_user() AS user").collect()[0]["user"]
        logger.info("Spark runtime identity: %s", who)
    except Exception:
        logger.warning("Could not determine Spark runtime identity")

    failures: list[tuple[str, str, str]] = []

    for (cat, sch), sample_fqn in sorted(schema_sample.items()):
        try:
            table_info = w.tables.get(full_name=sample_fqn)
            if not table_info.columns:
                logger.warning(
                    "REST access check: %s returned no columns", sample_fqn,
                )
            else:
                logger.info(
                    "REST access check: %s.%s OK (%d columns on %s)",
                    cat, sch, len(table_info.columns), sample_fqn,
                )
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            logger.warning("REST access check failed for %s.%s via %s: %s", cat, sch, sample_fqn, err)
            failures.append((cat, sch, f"w.tables.get('{sample_fqn}') failed: {err}"))

    if failures:
        detail_lines = [f"  `{cat}`.`{sch}`: {reason}" for cat, sch, reason in failures]
        raise RuntimeError(
            "Cannot access Unity Catalog tables for required schemas.\n"
            + "\n".join(detail_lines)
            + "\n\nEnsure the service principal has: "
            "USE CATALOG on the catalog, plus USE SCHEMA + SELECT on each schema. "
            "Grant access from the Settings page."
        )


def _validate_write_access(spark: SparkSession, genie_table_refs: list) -> None:
    """Fail-fast if the runtime identity lacks MODIFY on schemas required for UC writes."""
    schemas: set[tuple[str, str]] = set()
    for ref in genie_table_refs:
        cat = ref[0] if isinstance(ref, (list, tuple)) else ""
        sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
        if cat and sch:
            schemas.add((cat, sch))

    if not schemas:
        return

    missing: list[str] = []
    for cat, sch in sorted(schemas):
        try:
            rows = spark.sql(
                f"SHOW GRANTS ON SCHEMA `{cat}`.`{sch}`"
            ).collect()
            has_modify = any(
                "MODIFY" in str(r.asDict().get("privilege", "")).upper()
                for r in rows
            )
            if not has_modify:
                missing.append(f"`{cat}`.`{sch}`")
        except Exception as exc:
            logger.warning("MODIFY check failed for %s.%s: %s", cat, sch, exc)
            missing.append(f"`{cat}`.`{sch}`")

    if missing:
        raise RuntimeError(
            f"UC write access (MODIFY) missing on: {', '.join(missing)}. "
            "The apply_mode requires MODIFY permission on these schemas. "
            "Grant write access from the Settings page, or choose 'Config only' mode."
        )


# ── Preflight Sub-Step Functions ────────────────────────────────────
# Each phase is individually callable from a notebook cell. The wrapper
# ``run_preflight()`` calls them in sequence for backward compatibility.


def preflight_fetch_config(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    apply_mode: str = "genie_config",
) -> dict:
    """Sub-step 1: Load Genie Space config from snapshot or API.

    Returns a context dict with keys: config, snapshot, genie_table_refs,
    domain, apply_mode, configured_cols.
    """
    domain = re.sub(r"[^a-z0-9_]+", "_", domain.lower()).strip("_") or "default"

    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    run_data = load_run(spark, run_id, catalog, schema) or {}
    snapshot = run_data.get("config_snapshot", {})
    config: dict
    if isinstance(snapshot, dict) and snapshot:
        config = snapshot
        logger.info("Using config snapshot from run row for %s", run_id)
    else:
        logger.warning(
            "No config snapshot found in run row for %s — fetching from API. "
            "This may fail on serverless if the runtime identity lacks Genie "
            "Space 'Can Edit' permission. The app backend should capture the "
            "snapshot at trigger time.",
            run_id,
        )
        config = fetch_space_config(w, space_id)
        logger.info("Fetched config for space %s", space_id)
        try:
            _update_run_status(
                spark, run_id, catalog, schema,
                config_snapshot=config,
            )
            logger.info("Back-filled config_snapshot into run row for %s", run_id)
        except Exception:
            logger.warning(
                "Could not back-fill config_snapshot for %s", run_id, exc_info=True,
            )

    _parsed = config.get("_parsed_space", config)
    _schema_ok, _schema_errors = validate_serialized_space(
        _parsed if isinstance(_parsed, dict) else config
    )
    if not _schema_ok:
        logger.warning(
            "Space config has structural issues for %s: %s", space_id, _schema_errors
        )

    _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
    _inv_tables = _ds.get("tables", [])
    _inv_mvs = _ds.get("metric_views", [])
    _inv_funcs = _ds.get("functions", [])
    _inv_instructions = _parsed.get("instructions", {}).get("text_instructions", [])
    _inv_instr_count = sum(
        len(ti.get("content", [])) if isinstance(ti.get("content"), list) else (1 if ti.get("content") else 0)
        for ti in _inv_instructions
    )
    _configured_cols = sum(len(t.get("column_configs", [])) for t in _inv_tables + _inv_mvs)

    genie_table_refs = extract_genie_space_table_refs(config)
    if genie_table_refs:
        logger.info(
            "Genie space references %d data assets across schemas: %s",
            len(genie_table_refs),
            sorted({f"{c}.{s}" for c, s, _ in genie_table_refs if c and s}),
        )

    _lines = [_pf_section("PREFLIGHT — GENIE SPACE CONFIGURATION")]
    _lines.append(_pf_kv("Space ID", space_id))
    _lines.append(_pf_kv("Source", "snapshot" if (isinstance(snapshot, dict) and snapshot) else "API"))
    _lines.append(_pf_kv("Tables", len(_inv_tables)))
    _lines.append(_pf_kv("Metric Views", len(_inv_mvs)))
    _lines.append(_pf_kv("Functions (TVF)", len(_inv_funcs)))
    _lines.append(_pf_kv("Instructions", _inv_instr_count))
    _lines.append(_pf_kv("Configured cols", f"{_configured_cols}  (column_configs with desc/FA/VD)"))
    _lines.append(_pf_bar())
    for t in _inv_tables[:10]:
        _tid = t.get("identifier", "?")
        _tcols = len(t.get("column_configs", []))
        _lines.append(f"    TABLE: {_tid} ({_tcols} configured cols)")
    for mv in _inv_mvs[:10]:
        _mid = mv.get("identifier", "?")
        _mcols = len(mv.get("column_configs", []))
        _lines.append(f"    METRIC VIEW: {_mid} ({_mcols} configured cols)")
    for fn in _inv_funcs[:10]:
        _fid = fn.get("identifier", "?")
        _lines.append(f"    FUNCTION: {_fid}")
    _lines.append(_pf_bar())
    print("\n".join(_lines))

    return {
        "config": config,
        "snapshot": snapshot,
        "genie_table_refs": genie_table_refs,
        "domain": domain,
        "apply_mode": apply_mode,
        "configured_cols": _configured_cols,
    }


def preflight_collect_uc_metadata(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    catalog: str,
    schema: str,
    config: dict,
    snapshot: dict,
    genie_table_refs: list,
    apply_mode: str = "genie_config",
    configured_cols: int = 0,
    *,
    warehouse_id: str = "",
) -> dict:
    """Sub-step 2: Collect UC columns, tags, routines, FK constraints.

    Mutates ``config`` (adds ``_uc_columns``, ``_uc_foreign_keys``,
    ``_data_profile``, ``_join_overlaps``).

    Returns a dict with keys: uc_columns, uc_tags, uc_routines, uc_fk.
    """
    _validate_core_access(w, spark, genie_table_refs)

    if apply_mode in ("both", "uc_artifact"):
        _validate_write_access(spark, genie_table_refs)

    prefetched = snapshot.get("_prefetched_uc_metadata", {}) if isinstance(snapshot, dict) else {}
    _pf = prefetched if isinstance(prefetched, dict) else {}
    _actual_source: dict[str, str] = {}

    def _usable_prefetch(key: str) -> list | None:
        if key not in _pf:
            return None
        val = _pf[key]
        if val is None:
            logger.info("Prefetched %s failed upstream, falling back", key)
            return None
        if isinstance(val, list):
            _actual_source[key] = "prefetched"
            if not val:
                logger.info("Prefetched %s is empty (0 rows) — accepted, no fallback needed", key)
            return val
        return None

    _collection_errors: dict[str, str] = {}

    def _rest_collect(fetch_fn: Any, label: str, source_key: str) -> list[dict] | None:
        try:
            print(f"[PREFLIGHT] Attempting REST API for {label}...", flush=True)
            rows = fetch_fn()
            if rows:
                _actual_source[source_key] = "rest_api"
                print(f"[PREFLIGHT] ✓ REST API returned {len(rows)} {label}", flush=True)
                return rows
            print(f"[PREFLIGHT] REST API returned 0 {label}, falling back to Spark", flush=True)
        except Exception as exc:
            print(f"[PREFLIGHT] REST API failed for {label}: {type(exc).__name__}: {exc}", flush=True)
        return None

    def _spark_collect(fetch_fn: Any, label: str, source_key: str) -> list[dict]:
        _actual_source[source_key] = "spark"
        rows, err = _collect_or_empty(fetch_fn, label)
        if err:
            _collection_errors[source_key] = err
        return rows

    if genie_table_refs:
        uc_columns_dicts = (
            _usable_prefetch("uc_columns")
            or _rest_collect(
                lambda: get_columns_for_tables_rest(w, genie_table_refs),
                "columns (genie tables)", "uc_columns",
            )
            or _spark_collect(
                lambda: get_columns_for_tables(spark, genie_table_refs),
                "columns (genie tables)", "uc_columns",
            )
        )
        uc_tags_dicts = (
            _usable_prefetch("uc_tags")
            or _rest_collect(
                lambda: get_tags_for_tables_rest(w, genie_table_refs),
                "tags (genie tables)", "uc_tags",
            )
            or _spark_collect(
                lambda: get_tags_for_tables(spark, genie_table_refs),
                "tags (genie tables)", "uc_tags",
            )
        )
        uc_routines_dicts = (
            _usable_prefetch("uc_routines")
            or _rest_collect(
                lambda: get_routines_for_schemas_rest(w, genie_table_refs),
                "routines (genie schemas)", "uc_routines",
            )
            or _spark_collect(
                lambda: get_routines_for_schemas(spark, genie_table_refs),
                "routines (genie schemas)", "uc_routines",
            )
        )
        _fk_pre = _usable_prefetch("uc_foreign_keys")
        if _fk_pre is not None:
            uc_fk_dicts = _fk_pre
        else:
            uc_fk_dicts = _rest_collect(
                lambda: get_foreign_keys_for_tables_rest(w, genie_table_refs),
                "foreign keys (genie tables)", "uc_foreign_keys",
            )
            if uc_fk_dicts is None:
                try:
                    uc_fk_dicts = get_foreign_keys_for_tables(spark, genie_table_refs)
                    _actual_source["uc_foreign_keys"] = "spark"
                except Exception as exc:
                    logger.warning("Spark FK fallback failed: %s", exc)
                    uc_fk_dicts = []
        uc_fk_dicts = uc_fk_dicts if isinstance(uc_fk_dicts, list) else []
    else:
        uc_columns_dicts = (
            _usable_prefetch("uc_columns")
            or _spark_collect(lambda: get_columns(spark, catalog, schema), "columns", "uc_columns")
        )
        uc_tags_dicts = (
            _usable_prefetch("uc_tags")
            or _spark_collect(lambda: get_tags(spark, catalog, schema), "tags", "uc_tags")
        )
        uc_routines_dicts = (
            _usable_prefetch("uc_routines")
            or _spark_collect(lambda: get_routines(spark, catalog, schema), "routines", "uc_routines")
        )
        uc_fk_dicts = []

    uc_columns_dicts = uc_columns_dicts if isinstance(uc_columns_dicts, list) else []
    uc_tags_dicts = uc_tags_dicts if isinstance(uc_tags_dicts, list) else []
    uc_routines_dicts = uc_routines_dicts if isinstance(uc_routines_dicts, list) else []
    uc_fk_dicts = uc_fk_dicts if isinstance(uc_fk_dicts, list) else []

    if not uc_tags_dicts:
        print(
            "[PREFLIGHT] Tags: 0 found — this is OK. "
            "Tags are informational metadata only and not required for optimization. "
            "Optimization will proceed using column and routine metadata.",
            flush=True,
        )

    logger.info(
        "UC metadata: %d columns, %d tags, %d routines, %d FK constraints",
        len(uc_columns_dicts), len(uc_tags_dicts), len(uc_routines_dicts),
        len(uc_fk_dicts),
    )

    _uc_table_names = {
        str(c.get("table_name") or "").strip().lower()
        for c in uc_columns_dicts if isinstance(c, dict) and c.get("table_name")
    }

    _lines = [_pf_section("PREFLIGHT — UC METADATA COLLECTION SUMMARY")]
    _lines.append(_pf_kv("UC Columns", f"{len(uc_columns_dicts):>5}  (covering {len(_uc_table_names)} tables, source: {_actual_source.get('uc_columns', 'unknown')})"))
    _lines.append(_pf_kv("Genie config", f"{configured_cols:>5}  column_configs entries (descriptions/FA/VD)"))
    _lines.append(_pf_kv("Tags", f"{len(uc_tags_dicts):>5}  (source: {_actual_source.get('uc_tags', 'unknown')})"))
    _lines.append(_pf_kv("Routines", f"{len(uc_routines_dicts):>5}  (source: {_actual_source.get('uc_routines', 'unknown')})"))
    _lines.append(_pf_kv("FK Constraints", f"{len(uc_fk_dicts):>4}  (source: {_actual_source.get('uc_foreign_keys', 'unknown')})"))
    if _collection_errors:
        _lines.append("  Collection errors:")
        for k, v in _collection_errors.items():
            _lines.append(f"    {k}: {v[:200]}")
    _lines.append(_pf_bar())

    column_samples: list[str] = []
    for col in uc_columns_dicts[:12]:
        if not isinstance(col, dict):
            continue
        table_name = str(col.get("table_name") or col.get("table") or "").strip()
        col_name = str(col.get("column_name") or col.get("column") or "").strip()
        if table_name and col_name:
            column_samples.append(f"{table_name}.{col_name}")
        elif col_name:
            column_samples.append(col_name)

    tag_samples: list[str] = []
    for tag in uc_tags_dicts[:8]:
        if not isinstance(tag, dict):
            continue
        table_name = str(tag.get("table_name") or "").strip()
        col_name = str(tag.get("column_name") or "").strip()
        tag_name = str(tag.get("tag_name") or tag.get("name") or "").strip()
        tag_value = str(tag.get("tag_value") or tag.get("value") or "").strip()
        target = ".".join(part for part in [table_name, col_name] if part)
        if tag_name:
            tag_samples.append(
                f"{target}: {tag_name}={tag_value}" if target else f"{tag_name}={tag_value}"
            )

    routine_samples: list[str] = []
    for routine in uc_routines_dicts[:8]:
        if not isinstance(routine, dict):
            continue
        routine_name = str(routine.get("routine_name") or routine.get("name") or "").strip()
        if routine_name:
            routine_samples.append(routine_name)

    print("\n".join(_lines))

    config["_uc_columns"] = uc_columns_dicts
    config["_uc_foreign_keys"] = uc_fk_dicts

    referenced_schemas = sorted(
        {f"{c}.{s}" for c, s, _ in genie_table_refs if c and s}
    ) if genie_table_refs else []
    metadata_source = {
        "columns": _actual_source.get("uc_columns", "unknown"),
        "tags": _actual_source.get("uc_tags", "unknown"),
        "routines": _actual_source.get("uc_routines", "unknown"),
    }
    stage_detail: dict[str, Any] = {
        "columns_collected": len(uc_columns_dicts),
        "tags_collected": len(uc_tags_dicts),
        "routines_collected": len(uc_routines_dicts),
        "fk_constraints_collected": len(uc_fk_dicts),
        "column_samples": [s for s in column_samples if s],
        "tag_samples": [s for s in tag_samples if s],
        "routine_samples": [s for s in routine_samples if s],
        "table_ref_count": len(genie_table_refs),
        "referenced_schema_count": len(referenced_schemas),
        "referenced_schemas": referenced_schemas[:12],
        "collection_scope": "genie_assets" if genie_table_refs else "catalog_schema_fallback",
        "metadata_source": metadata_source,
    }
    if _collection_errors:
        stage_detail["collection_errors"] = {
            k: v[:500] for k, v in _collection_errors.items()
        }
    write_stage(
        spark, run_id, "PREFLIGHT_METADATA_COLLECTION", "COMPLETE",
        task_key="preflight", catalog=catalog, schema=schema,
        detail=stage_detail,
    )

    table_names = list(config.get("_tables", [])) + list(config.get("_metric_views", []))
    _mv_names = frozenset(
        n.strip().lower().split(".")[-1]
        for n in config.get("_metric_views", [])
        if isinstance(n, str) and n.strip()
    )
    if table_names and uc_columns_dicts:
        write_stage(
            spark, run_id, "DATA_PROFILING", "STARTED",
            task_key="preflight", catalog=catalog, schema=schema,
        )
        try:
            data_profile = _collect_data_profile(
                spark, table_names, uc_columns_dicts,
                metric_view_names=_mv_names,
                w=w, warehouse_id=warehouse_id, catalog=catalog, schema=schema,
            )
            config["_data_profile"] = data_profile
            _ps = config.get("_parsed_space")
            if isinstance(_ps, dict):
                _ps["_data_profile"] = data_profile
            profiled_tables = len(data_profile)
            total_cols_profiled = sum(
                len(t.get("columns", {})) for t in data_profile.values()
            )
            low_card_count = sum(
                1
                for t in data_profile.values()
                for c in t.get("columns", {}).values()
                if c.get("distinct_values")
            )
            _dp_lines = [_pf_section("PREFLIGHT — DATA PROFILE")]
            _dp_lines.append(_pf_kv("Tables profiled", profiled_tables))
            _dp_lines.append(_pf_kv("Columns profiled", total_cols_profiled))
            _dp_lines.append(_pf_kv("Low-cardinality cols", low_card_count))
            _dp_lines.append(_pf_bar())
            for _dp_table_fqn, _dp_tinfo in sorted(data_profile.items()):
                _dp_row_count = _dp_tinfo.get("row_count", "?")
                _dp_lines.append(f"  {_dp_table_fqn} (~{_dp_row_count} rows)")
                for _dp_col, _dp_cinfo in sorted(_dp_tinfo.get("columns", {}).items()):
                    _dp_card = _dp_cinfo.get("cardinality", "?")
                    _dp_vals = _dp_cinfo.get("distinct_values")
                    _dp_minv = _dp_cinfo.get("min")
                    _dp_maxv = _dp_cinfo.get("max")
                    _dp_parts: list[str] = [f"cardinality={_dp_card}"]
                    if _dp_vals:
                        _dp_shown = _dp_vals[:5]
                        _dp_suffix = f" ... +{len(_dp_vals) - 5} more" if len(_dp_vals) > 5 else ""
                        _dp_parts.append(f"values={_dp_shown}{_dp_suffix}")
                    if _dp_minv is not None:
                        _dp_parts.append(f"range=[{_dp_minv}, {_dp_maxv}]")
                    _dp_lines.append(f"    {_dp_col}: {', '.join(_dp_parts)}")
                _dp_lines.append("")
            _dp_lines.append(_pf_bar())
            print("\n".join(_dp_lines))
            write_stage(
                spark, run_id, "DATA_PROFILING", "COMPLETE",
                task_key="preflight", catalog=catalog, schema=schema,
                detail={
                    "tables_profiled": profiled_tables,
                    "columns_profiled": total_cols_profiled,
                    "low_cardinality_columns": low_card_count,
                },
            )
        except Exception:
            logger.warning("Data profiling failed — continuing without profile", exc_info=True)
            config["_data_profile"] = {}
            _ps = config.get("_parsed_space")
            if isinstance(_ps, dict):
                _ps["_data_profile"] = {}
            write_stage(
                spark, run_id, "DATA_PROFILING", "COMPLETE",
                task_key="preflight", catalog=catalog, schema=schema,
                detail={"error": "profiling failed, continuing without profile"},
            )
    else:
        config["_data_profile"] = {}
        _ps = config.get("_parsed_space")
        if isinstance(_ps, dict):
            _ps["_data_profile"] = {}

    try:
        _update_run_status(
            spark, run_id, catalog, schema,
            config_snapshot=config,
        )
    except Exception:
        logger.warning(
            "Could not update config_snapshot with data profile for %s",
            run_id, exc_info=True,
        )

    if uc_fk_dicts:
        try:
            join_overlaps = _compute_join_overlaps(
                spark, uc_fk_dicts,
                w=w, warehouse_id=warehouse_id, catalog=catalog, schema=schema,
            )
            config["_join_overlaps"] = join_overlaps
            if join_overlaps:
                logger.info(
                    "Computed join overlaps for %d FK pairs", len(join_overlaps),
                )
        except Exception:
            logger.debug("Join overlap computation failed", exc_info=True)
            config["_join_overlaps"] = []
    else:
        config["_join_overlaps"] = []

    return {
        "uc_columns": uc_columns_dicts,
        "uc_tags": uc_tags_dicts,
        "uc_routines": uc_routines_dicts,
        "uc_fk": uc_fk_dicts,
    }


def preflight_generate_benchmarks(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    catalog: str,
    schema: str,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    *,
    space_id: str = "",
    experiment_name: str | None = None,
    warehouse_id: str = "",
) -> dict:
    """Sub-step 3: Load existing or generate new benchmarks.

    Returns a dict with keys: benchmarks, regenerated.
    """
    uc_schema = f"{catalog}.{schema}"

    if experiment_name is None:
        experiment_name = _resolve_experiment_path(
            space_id=space_id, domain=domain,
        )

    try:
        _ensure_experiment_parent_dir(w, experiment_name)
        register_benchmark_prompts(uc_schema, domain, experiment_name)
    except Exception:
        logger.warning(
            "Benchmark prompt registration failed — tracing will be limited",
            exc_info=True,
        )

    with mlflow.start_run(run_name="benchmark_generation") as _bench_run:
        mlflow.set_tags({
            "genie.space_id": space_id,
            "genie.domain": domain,
            "genie.stage": "benchmark_generation",
            "genie.run_id": run_id,
        })

        benchmarks, _benchmarks_regenerated = _load_or_generate_benchmarks(
            w, spark, config, uc_columns, uc_tags, uc_routines,
            domain, catalog, schema, uc_schema, run_id,
            warehouse_id=warehouse_id,
        )

        mlflow.log_params({
            "benchmark_count": len(benchmarks),
            "regenerated": _benchmarks_regenerated,
        })

    _lines = [_pf_section("PREFLIGHT — BENCHMARK GENERATION")]
    _lines.append(_pf_kv("Benchmarks loaded", len(benchmarks)))
    _lines.append(_pf_kv("Regenerated", _benchmarks_regenerated))
    _lines.append(_pf_kv("MLflow run", _bench_run.info.run_id))
    _lines.append(_pf_bar())
    for bm in benchmarks[:10]:
        _bq = str(bm.get("question", ""))[:80]
        _bid = bm.get("id", bm.get("question_id", "?"))
        _lines.append(f"    [{_bid}] {_bq}")
    if len(benchmarks) > 10:
        _lines.append(f"    ... and {len(benchmarks) - 10} more")
    _lines.append(_pf_bar())
    print("\n".join(_lines))

    return {"benchmarks": benchmarks, "regenerated": _benchmarks_regenerated}


def preflight_validate_benchmarks(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    catalog: str,
    schema: str,
    config: dict,
    benchmarks: list[dict],
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    *,
    warehouse_id: str = "",
) -> dict:
    """Sub-step 4: Validate benchmarks via EXPLAIN gating.

    Returns a dict with keys: benchmarks (filtered), pre_count, invalid_errors.
    """
    MIN_VALID_BENCHMARKS = 5

    validation_results = validate_benchmarks(
        benchmarks, spark, catalog=catalog, gold_schema=schema,
        w=w, warehouse_id=warehouse_id,
    )
    pre_count = len(benchmarks)
    filtered_benchmarks: list[dict] = []
    invalid_errors: list[str] = []
    rejected_details: list[str] = []
    for benchmark, validation in zip(benchmarks, validation_results):
        if validation.get("valid"):
            benchmark["validation_status"] = "valid"
            benchmark["validation_reason_code"] = benchmark.get("validation_reason_code", "ok")
            benchmark["validation_error"] = None
            filtered_benchmarks.append(benchmark)
        else:
            err = str(validation.get("error") or "").strip()
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = benchmark.get(
                "validation_reason_code",
                "sql_compile_error",
            )
            benchmark["validation_error"] = err or benchmark.get("validation_error")
            if err:
                invalid_errors.append(err[:200])
            bid = benchmark.get("id", benchmark.get("question_id", "?"))
            bq = benchmark.get("question", "?")[:80]
            logger.warning(
                "BENCHMARK REJECTED: id=%s question='%s' error=%s",
                bid, bq, err[:200],
            )
            rejected_details.append(f"    - {bid}: \"{bq}\" — {err[:120]}")
    benchmarks = filtered_benchmarks
    if len(benchmarks) < pre_count:
        logger.warning(
            "Discarded %d/%d benchmarks that failed validation (sample_errors=%s)",
            pre_count - len(benchmarks),
            pre_count,
            invalid_errors[:5],
        )
        _err_categories: dict[str, int] = {}
        for err in invalid_errors:
            low = err.lower()
            if "unresolved_column" in low:
                cat = "UNRESOLVED_COLUMN"
            elif "table_or_view_not_found" in low or "does not exist" in low:
                cat = "MISSING_TABLE/VIEW"
            elif "tvf" in low or "table_valued_function" in low:
                cat = "TVF_ERROR"
            elif "syntax" in low or "parse" in low:
                cat = "SYNTAX_ERROR"
            elif "permission" in low:
                cat = "PERMISSION_ERROR"
            else:
                cat = "OTHER"
            _err_categories[cat] = _err_categories.get(cat, 0) + 1

    _lines = [_pf_section("PREFLIGHT — BENCHMARK VALIDATION")]
    _lines.append(_pf_kv("Total benchmarks", pre_count))
    _lines.append(_pf_kv("Valid", len(benchmarks)))
    _lines.append(_pf_kv("Rejected", pre_count - len(benchmarks)))
    if pre_count > len(benchmarks):
        _err_categories_v: dict[str, int] = {}
        for err in invalid_errors:
            low = err.lower()
            if "unresolved_column" in low:
                cat = "UNRESOLVED_COLUMN"
            elif "table_or_view_not_found" in low or "does not exist" in low:
                cat = "MISSING_TABLE/VIEW"
            elif "tvf" in low or "table_valued_function" in low:
                cat = "TVF_ERROR"
            elif "syntax" in low or "parse" in low:
                cat = "SYNTAX_ERROR"
            elif "permission" in low:
                cat = "PERMISSION_ERROR"
            else:
                cat = "OTHER"
            _err_categories_v[cat] = _err_categories_v.get(cat, 0) + 1
        if _err_categories_v:
            _lines.append("  Error categories:")
            for cat, cnt in sorted(_err_categories_v.items(), key=lambda x: -x[1]):
                _lines.append(f"    {cat}: {cnt}")
        _lines.append("  Rejected details:")
        for rd in rejected_details[:20]:
            _lines.append(rd)
    if benchmarks:
        _lines.append("  Valid benchmark questions:")
        for vb in benchmarks[:15]:
            _vbq = str(vb.get("question", ""))[:80]
            _vbid = vb.get("id", vb.get("question_id", "?"))
            _lines.append(f"    [{_vbid}] {_vbq}")
        if len(benchmarks) > 15:
            _lines.append(f"    ... and {len(benchmarks) - 15} more")
    _lines.append(_pf_bar())
    print("\n".join(_lines))

    if len(benchmarks) < MIN_VALID_BENCHMARKS:
        logger.warning(
            "Only %d valid benchmarks after filtering (min %d). "
            "Re-generating from scratch using Genie space assets.",
            len(benchmarks), MIN_VALID_BENCHMARKS,
        )
        genie_benchmarks_regen = extract_genie_space_benchmarks(
            config, spark, catalog=catalog, schema=schema,
            w=w, warehouse_id=warehouse_id,
        )
        write_stage(
            spark, run_id, "BENCHMARK_REGENERATION", "STARTED",
            task_key="preflight", catalog=catalog, schema=schema,
            detail={"reason": "too_few_valid_benchmarks", "valid_count": len(benchmarks), "discarded_errors": invalid_errors[:5]},
        )
        benchmarks = generate_benchmarks(
            w, config, uc_columns, uc_tags, uc_routines,
            domain, catalog, schema, spark,
            target_count=TARGET_BENCHMARK_COUNT,
            genie_space_benchmarks=genie_benchmarks_regen,
            warehouse_id=warehouse_id,
        )
        write_stage(
            spark, run_id, "BENCHMARK_REGENERATION", "COMPLETE",
            task_key="preflight",
            detail={"regenerated_count": len(benchmarks)},
            catalog=catalog, schema=schema,
        )

    if not benchmarks:
        raise RuntimeError(
            f"All {pre_count} benchmarks failed validation even after regeneration. "
            f"Sample errors: {invalid_errors[:5]}. "
            "Check that the Genie space's referenced tables actually exist."
        )

    return {"benchmarks": benchmarks, "pre_count": pre_count, "invalid_errors": invalid_errors}


def preflight_load_human_feedback(
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
) -> dict:
    """Sub-step 5: Load human corrections from prior labeling sessions.

    Returns a dict with key: human_corrections (list[dict]).
    """
    uc_schema = f"{catalog}.{schema}"
    _human_corrections: list[dict] = []
    try:
        from genie_space_optimizer.optimization.labeling import (
            ensure_labeling_schemas,
            ingest_human_feedback,
            sync_corrections_to_dataset,
        )
        ensure_labeling_schemas()

        prior_runs = load_runs_for_space(spark, space_id, catalog, schema)
        _prior_session_name = ""
        if not prior_runs.empty:
            completed = prior_runs[
                (prior_runs["run_id"] != run_id)
                & (prior_runs["status"].isin(["CONVERGED", "STALLED", "MAX_ITERATIONS"]))
            ]
            if not completed.empty:
                _prior_session_name = completed.iloc[0].get("labeling_session_name", "") or ""
        if _prior_session_name:
            _benchmark_table = f"{uc_schema}.genie_benchmarks_{domain}"
            sync_corrections_to_dataset(_prior_session_name, _benchmark_table)
            feedback = ingest_human_feedback(_prior_session_name)
            _human_corrections = feedback.get("corrections", [])
            if _human_corrections:
                logger.info(
                    "Loaded %d human corrections from prior labeling session '%s'",
                    len(_human_corrections), _prior_session_name,
                )
    except Exception:
        logger.warning("Human feedback ingestion skipped (no prior session or module unavailable)", exc_info=True)

    _lines = [_pf_section("PREFLIGHT — PAST HUMAN FEEDBACK")]
    _lines.append(_pf_kv("Corrections loaded", len(_human_corrections)))
    if _human_corrections:
        _type_counts: dict[str, int] = {}
        for c in _human_corrections:
            ct = c.get("type", c.get("correction_type", "unknown"))
            _type_counts[ct] = _type_counts.get(ct, 0) + 1
        for ct, cnt in sorted(_type_counts.items()):
            _lines.append(_pf_kv(f"  {ct}", cnt))
        sample = _human_corrections[0]
        _sq = str(sample.get("question", sample.get("question_text", "")))[:80]
        _lines.append(_pf_kv("Sample", f'"{_sq}"'))
    else:
        _lines.append(_pf_kv("Status", "No prior labeling sessions found"))
    _lines.append(_pf_bar())
    print("\n".join(_lines))

    return {"human_corrections": _human_corrections}


def preflight_setup_experiment(
    w: "WorkspaceClient",
    spark: "SparkSession",
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    config: dict,
    benchmarks: list[dict],
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    genie_table_refs: list,
    experiment_name: str | None = None,
) -> dict:
    """Sub-step 6: Create MLflow experiment, register judges, create model.

    Returns a dict with keys: model_id, experiment_name, experiment_id,
    prompt_registrations.
    """
    uc_schema = f"{catalog}.{schema}"

    if experiment_name is None:
        experiment_name = _resolve_experiment_path(space_id=space_id, domain=domain)

    _ensure_experiment_parent_dir(w, experiment_name)
    try:
        mlflow.set_experiment(experiment_name)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot create MLflow experiment at {experiment_name}: {exc}"
        ) from exc
    exp = mlflow.get_experiment_by_name(experiment_name)
    experiment_id = exp.experiment_id if exp else ""
    logger.info("Experiment: %s (id=%s)", experiment_name, experiment_id)

    try:
        from genie_space_optimizer import __version__ as _pipeline_version
    except ImportError:
        _pipeline_version = "0.0.0"
    try:
        mlflow.set_experiment_tags({
            "genie.space_id": space_id,
            "genie.domain": domain,
            "genie.pipeline_version": _pipeline_version,
            "genie.catalog": catalog,
            "genie.schema": schema,
        })
    except Exception:
        logger.debug("Failed to set experiment-level tags", exc_info=True)

    initial_instructions = _get_general_instructions(config.get("_parsed_space", config))
    if initial_instructions:
        register_instruction_version(
            uc_schema=uc_schema,
            space_id=space_id,
            instruction_text=initial_instructions,
            run_id=run_id,
            lever=0,
            iteration=0,
            accuracy=0.0,
            domain=domain,
        )

    import os as _os
    _wh_id = _os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "")
    _flag_stale_temporal_benchmarks(
        benchmarks, spark, w=w, warehouse_id=_wh_id,
    )

    _asset_fp = compute_asset_fingerprint(config)
    for _b in benchmarks:
        _b["asset_fingerprint"] = _asset_fp

    _uc_table = f"{uc_schema}.genie_benchmarks_{domain}"
    logger.info(
        "Dropping benchmark table %s before persist to eliminate stale duplicates "
        "(benchmarks=%d, asset_fingerprint=%s)",
        _uc_table, len(benchmarks), _asset_fp,
    )
    _drop_benchmark_table(spark, _uc_table)

    create_evaluation_dataset(
        spark, benchmarks, uc_schema, domain,
        space_id=space_id, catalog=catalog, gold_schema=schema,
        experiment_id=experiment_id,
    )

    _lines = [_pf_section("PREFLIGHT — EXPERIMENT & MODEL SETUP")]
    _lines.append(_pf_kv("Experiment", experiment_name))
    _lines.append(_pf_kv("Experiment ID", experiment_id))
    _lines.append(_pf_kv("Model creation", "deferred to baseline eval"))
    _lines.append(_pf_kv("Eval dataset", f"synced ({len(benchmarks)} benchmarks)"))
    _lines.append(_pf_kv("Instructions", "registered" if initial_instructions else "none to register"))
    _lines.append(_pf_bar())
    print("\n".join(_lines))

    _instr_items = (
        config.get("_parsed_space", config)
        .get("instructions", {})
        .get("text_instructions", [])
    )
    _instr_count = sum(
        len(ti.get("content", [])) if isinstance(ti.get("content"), list) else (1 if ti.get("content") else 0)
        for ti in _instr_items
    )
    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "COMPLETE",
        task_key="preflight",
        detail={
            "table_count": len(genie_table_refs) if genie_table_refs else 0,
            "instruction_count": _instr_count,
            "benchmark_count": len(benchmarks),
            "experiment_name": experiment_name,
            "model_id": None,
        },
        catalog=catalog, schema=schema,
    )

    return {
        "model_id": None,
        "experiment_name": experiment_name,
        "experiment_id": experiment_id,
    }


def run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
    apply_mode: str = "genie_config",
) -> tuple[dict, list[dict], str | None, str, list[dict]]:
    """Execute the full preflight sequence (Stage 1).

    Wrapper that calls 6 sub-steps in sequence. Each sub-step is individually
    callable from a notebook cell for transparency.

    Returns:
        (config, benchmarks, model_id, experiment_name, human_corrections)
    """
    ctx1 = preflight_fetch_config(
        w, spark, run_id, space_id, catalog, schema, domain, apply_mode,
    )
    config = ctx1["config"]
    snapshot = ctx1["snapshot"]
    genie_table_refs = ctx1["genie_table_refs"]
    domain = ctx1["domain"]

    ctx2 = preflight_collect_uc_metadata(
        w, spark, run_id, catalog, schema, config, snapshot,
        genie_table_refs, apply_mode=apply_mode,
        configured_cols=ctx1.get("configured_cols", 0),
    )

    ctx3 = preflight_generate_benchmarks(
        w, spark, run_id, catalog, schema, config,
        ctx2["uc_columns"], ctx2["uc_tags"], ctx2["uc_routines"],
        domain,
    )
    benchmarks = ctx3["benchmarks"]

    ctx4 = preflight_validate_benchmarks(
        w, spark, run_id, catalog, schema, config, benchmarks,
        ctx2["uc_columns"], ctx2["uc_tags"], ctx2["uc_routines"],
        domain,
    )
    benchmarks = ctx4["benchmarks"]

    ctx5 = preflight_load_human_feedback(
        spark, run_id, space_id, catalog, schema, domain,
    )

    ctx6 = preflight_setup_experiment(
        w, spark, run_id, space_id, catalog, schema, domain,
        config, benchmarks,
        ctx2["uc_columns"], ctx2["uc_tags"], ctx2["uc_routines"],
        genie_table_refs, experiment_name,
    )

    return (
        config,
        benchmarks,
        ctx6["model_id"],
        ctx6["experiment_name"],
        ctx5["human_corrections"],
    )


def _load_or_generate_benchmarks(
    w: WorkspaceClient,
    spark: SparkSession,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    uc_schema: str,
    run_id: str,
    warehouse_id: str = "",
) -> tuple[list[dict], bool]:
    """Load existing benchmarks or generate new ones from Genie space + LLM.

    Returns:
        A tuple of (benchmarks, regenerated) where *regenerated* is ``True``
        when a full GENERATE or RE-GENERATE occurred (the caller should drop
        the stale UC table before persisting) and ``False`` for REUSE / TOP-UP.

    Strategy:
      1. Extract curated benchmarks from the Genie Space config (example_question_sqls,
         sample_questions). These are always included as the authoritative ground truth.
      2. Try loading previously persisted benchmarks from UC dataset.
         If enough exist AND they already include the curated ones, reuse them.
      3. Otherwise, generate synthetic benchmarks via LLM to augment the curated set.
    """
    genie_benchmarks = extract_genie_space_benchmarks(
        config, spark, catalog=catalog, schema=schema,
        w=w, warehouse_id=warehouse_id,
    )
    curated_with_sql = sum(1 for b in genie_benchmarks if b.get("expected_sql"))
    curated_question_only = sum(1 for b in genie_benchmarks if not b.get("expected_sql"))
    write_stage(
        spark, run_id, "GENIE_BENCHMARK_EXTRACTION", "COMPLETE",
        task_key="preflight", catalog=catalog, schema=schema,
        detail={
            "genie_space_benchmarks": len(genie_benchmarks),
            "with_sql": curated_with_sql,
            "question_only": curated_question_only,
        },
    )

    print(
        f"\n-- BENCHMARK LOADING " + "-" * 31 + "\n"
        f"  Target count: {TARGET_BENCHMARK_COUNT}\n"
        f"  Curated from Genie Space: {len(genie_benchmarks)} "
        f"({curated_with_sql} with SQL, {curated_question_only} question-only)"
    )

    existing = load_benchmarks_from_dataset(spark, uc_schema, domain)
    if existing and len(existing) >= 5:
        validation_results = validate_benchmarks(
            existing, spark, catalog=catalog, gold_schema=schema,
            w=w, warehouse_id=warehouse_id,
        )
        valid_existing = [
            b for b, v in zip(existing, validation_results)
            if v.get("valid")
        ]
        invalid_existing = [
            (b, v) for b, v in zip(existing, validation_results)
            if not v.get("valid")
        ]
        invalid_count = len(invalid_existing)

        _rejected_lines: list[str] = []
        for b, v in invalid_existing:
            bid = b.get("id", b.get("question_id", "?"))
            bq = b.get("question", "?")[:80]
            berr = str(v.get("error", ""))[:120]
            _rejected_lines.append(f"    - {bid}: \"{bq}\" — {berr}")
            logger.warning(
                "BENCHMARK REJECTED (re-validation): id=%s question='%s' error=%s",
                bid, bq, berr,
            )

        print(
            f"  Existing in UC table: {len(existing)}\n"
            f"  Valid after re-validation: {len(valid_existing)} ({invalid_count} rejected)"
        )
        if _rejected_lines:
            print("  Rejected reasons:\n" + "\n".join(_rejected_lines[:10]))

        if len(valid_existing) >= 5:
            # ── Schema fingerprint check ─────────────────────────────
            current_fp = compute_asset_fingerprint(config)
            stored_fp = ""
            for _bm in valid_existing:
                _sfp = (_bm.get("asset_fingerprint") or "")
                if _sfp:
                    stored_fp = _sfp
                    break
            if stored_fp and stored_fp != current_fp:
                _cur_refs = sorted(set(
                    (t if isinstance(t, str) else t.get("identifier", ""))
                    for t in config.get("_tables", [])
                ))
                print(
                    f"  Schema fingerprint CHANGED: stored={stored_fp}, current={current_fp}\n"
                    f"  Current tables: {_cur_refs[:10]}\n"
                    f"  Decision: RE-GENERATE (schema changed)\n"
                    + "-" * 52
                )
                logger.info(
                    "Asset fingerprint mismatch (%s vs %s) — forcing benchmark regeneration",
                    stored_fp, current_fp,
                )
                valid_existing = []

            # ── Semantic alignment check on reuse ──────────────────────
            if valid_existing:
                try:
                    from genie_space_optimizer.optimization.benchmarks import (
                        validate_question_sql_alignment,
                    )
                    _align_targets = [b for b in valid_existing if b.get("expected_sql")]
                    if _align_targets:
                        _align_results = validate_question_sql_alignment(_align_targets)
                        _align_rejected = 0
                        for _ab, _ar in zip(_align_targets, _align_results):
                            if not _ar.get("aligned", True):
                                _align_rejected += 1
                                logger.warning(
                                    "Benchmark REJECTED on reuse (alignment): %s -- %s",
                                    _ab.get("question", "")[:80],
                                    "; ".join(_ar.get("issues", [])),
                                )
                        if _align_rejected:
                            _rejected_ids = {
                                id(_ab)
                                for _ab, _ar in zip(_align_targets, _align_results)
                                if not _ar.get("aligned", True)
                            }
                            valid_existing = [b for b in valid_existing if id(b) not in _rejected_ids]
                            print(
                                f"  Alignment check: {_align_rejected} rejected "
                                f"({len(valid_existing)} remain)"
                            )
                except Exception as _align_err:
                    logger.warning("Alignment check on reuse skipped: %s", _align_err)

            curated_questions = {b.get("question", "").lower().strip() for b in genie_benchmarks}
            existing_questions = {b.get("question", "").lower().strip() for b in valid_existing}
            missing_curated = curated_questions - existing_questions

            print(f"  Missing curated: {len(missing_curated)}")

            if not missing_curated and valid_existing:
                if len(valid_existing) >= TARGET_BENCHMARK_COUNT:
                    print(
                        f"  Decision: REUSE ({len(valid_existing)} valid, target {TARGET_BENCHMARK_COUNT} met)\n"
                        + "-" * 52
                    )
                    logger.info(
                        "Loaded %d valid existing benchmarks from UC dataset (all %d curated included)",
                        len(valid_existing), len(genie_benchmarks),
                    )
                    for benchmark in valid_existing:
                        benchmark.setdefault("provenance", "synthetic")
                        benchmark.setdefault("validation_status", "valid")
                        benchmark.setdefault("validation_reason_code", "ok")
                        benchmark.setdefault("validation_error", None)
                        benchmark.setdefault("correction_source", "")
                    if len(valid_existing) > MAX_BENCHMARK_COUNT:
                        from genie_space_optimizer.optimization.evaluation import _truncate_benchmarks
                        valid_existing = _truncate_benchmarks(valid_existing, MAX_BENCHMARK_COUNT)
                    return valid_existing, False
                else:
                    gap = TARGET_BENCHMARK_COUNT - len(valid_existing)
                    print(
                        f"  Decision: TOP-UP ({len(valid_existing)} valid, "
                        f"need {gap} more to reach target {TARGET_BENCHMARK_COUNT})\n"
                        + "-" * 52
                    )
                    logger.warning(
                        "Only %d valid benchmarks (target %d). Generating %d more to top up.",
                        len(valid_existing), TARGET_BENCHMARK_COUNT, gap,
                    )
                    for benchmark in valid_existing:
                        benchmark.setdefault("provenance", "synthetic")
                        benchmark.setdefault("validation_status", "valid")
                        benchmark.setdefault("validation_reason_code", "ok")
                        benchmark.setdefault("validation_error", None)
                        benchmark.setdefault("correction_source", "")

                    write_stage(
                        spark, run_id, "BENCHMARK_GENERATION", "STARTED",
                        task_key="preflight", catalog=catalog, schema=schema,
                        detail={"reason": "top_up", "existing_valid": len(valid_existing)},
                    )
                    new_benchmarks = generate_benchmarks(
                        w, config, uc_columns, uc_tags, uc_routines,
                        domain, catalog, schema, spark,
                        target_count=TARGET_BENCHMARK_COUNT,
                        genie_space_benchmarks=genie_benchmarks,
                        existing_benchmarks=valid_existing,
                        warehouse_id=warehouse_id,
                    )
                    write_stage(
                        spark, run_id, "BENCHMARK_GENERATION", "COMPLETE",
                        task_key="preflight",
                        detail={
                            "total_count": len(new_benchmarks),
                            "top_up_reason": f"{len(valid_existing)}<{TARGET_BENCHMARK_COUNT}",
                        },
                        catalog=catalog, schema=schema,
                    )
                    if len(new_benchmarks) > MAX_BENCHMARK_COUNT:
                        from genie_space_optimizer.optimization.evaluation import _truncate_benchmarks
                        new_benchmarks = _truncate_benchmarks(new_benchmarks, MAX_BENCHMARK_COUNT)
                    return new_benchmarks, False

            print(
                f"  Decision: RE-GENERATE (missing {len(missing_curated)} curated questions)\n"
                + "-" * 52
            )
            logger.info(
                "UC dataset has %d valid benchmarks but missing %d curated Genie space questions. "
                "Re-generating to include them.",
                len(valid_existing), len(missing_curated),
            )
        else:
            print(
                f"  Decision: RE-GENERATE (only {len(valid_existing)} valid, need >=5)\n"
                + "-" * 52
            )
            logger.info(
                "Only %d valid benchmarks remain after re-validation (need >=5). "
                "Re-generating from scratch.",
                len(valid_existing),
            )
    else:
        print(
            f"  Existing in UC table: {len(existing) if existing else 0}\n"
            f"  Decision: GENERATE (no sufficient existing benchmarks)\n"
            + "-" * 52
        )

    logger.info(
        "Generating benchmarks: %d curated from Genie space + synthetic to reach %d",
        len(genie_benchmarks), TARGET_BENCHMARK_COUNT,
    )
    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    benchmarks = generate_benchmarks(
        w, config, uc_columns, uc_tags, uc_routines,
        domain, catalog, schema, spark,
        target_count=TARGET_BENCHMARK_COUNT,
        genie_space_benchmarks=genie_benchmarks,
        warehouse_id=warehouse_id,
    )

    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "COMPLETE",
        task_key="preflight",
        detail={
            "total_count": len(benchmarks),
            "curated_count": sum(1 for b in benchmarks if b.get("provenance") == "curated"),
            "synthetic_count": sum(1 for b in benchmarks if b.get("provenance") == "synthetic"),
            "auto_corrected_count": sum(1 for b in benchmarks if b.get("provenance") == "auto_corrected"),
            "valid_count": sum(1 for b in benchmarks if b.get("validation_status") == "valid"),
        },
        catalog=catalog, schema=schema,
    )
    if len(benchmarks) > MAX_BENCHMARK_COUNT:
        from genie_space_optimizer.optimization.evaluation import _truncate_benchmarks
        benchmarks = _truncate_benchmarks(benchmarks, MAX_BENCHMARK_COUNT)
    return benchmarks, True
