"""
Benchmark management — loading, validation, splitting, and corrections.

Benchmarks are stored as MLflow evaluation datasets in UC (no YAML files).
"""

from __future__ import annotations

import hashlib
import io
import logging
import random
import re as _re
from contextlib import contextmanager
from typing import Any

from genie_space_optimizer.common.config import HELD_OUT_RATIO, TEMPLATE_VARIABLES
from genie_space_optimizer.common.genie_client import detect_asset_type

logger = logging.getLogger(__name__)


@contextmanager
def _quiet_grpc_logs():
    """Capture ``pyspark.sql.connect.logging`` to a buffer instead of stdout.

    Yields a summary object whose ``.get()`` returns a one-line digest
    of any captured gRPC errors (empty string if none).  This avoids
    multi-KB stacktrace spam while retaining signal for edge cases
    where the gRPC log contains info not present in the Python exception.
    """
    grpc_logger = logging.getLogger("pyspark.sql.connect.logging")
    prev_propagate = grpc_logger.propagate
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message).200s"))
    grpc_logger.addHandler(handler)
    grpc_logger.propagate = False

    class _Summary:
        def get(self) -> str:
            raw = buf.getvalue()
            if not raw:
                return ""
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            if len(lines) <= 1:
                return lines[0] if lines else ""
            return f"{lines[0]} (+{len(lines) - 1} more gRPC errors)"

    try:
        yield _Summary()
    finally:
        grpc_logger.removeHandler(handler)
        grpc_logger.propagate = prev_propagate
        handler.close()
        buf.close()


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _set_sql_context(spark: Any, catalog: str, gold_schema: str) -> None:
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if gold_schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(gold_schema)}")


def resolve_sql(sql: str, **kwargs: str) -> str:
    """Substitute ``${catalog}`` / ``${gold_schema}`` template variables."""
    if not sql:
        return sql
    for tmpl_var, param_name in TEMPLATE_VARIABLES.items():
        if param_name in kwargs:
            sql = sql.replace(tmpl_var, kwargs[param_name])
    return sql


# ── MV alias collision detection / auto-fix ──────────────────────────


def detect_mv_alias_sort_collision(sql: str) -> str | None:
    """Return a warning if MEASURE(col) AS col is followed by ORDER BY col.

    Spark's Catalyst planner re-wraps the output alias in a second MEASURE()
    when the alias matches the source column name, causing
    MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.
    Returns None if no collision detected.
    """
    if not sql:
        return None
    measure_aliases = _re.findall(
        r'MEASURE\s*\(\s*(\w+)\s*\)\s+AS\s+(\w+)', sql, _re.IGNORECASE,
    )
    if not measure_aliases:
        return None
    order_clause = _re.search(
        r'ORDER\s+BY\s+(.*?)(?:LIMIT|$)', sql, _re.IGNORECASE | _re.DOTALL,
    )
    if not order_clause:
        return None
    bare_order_cols: set[str] = set()
    for token in _re.split(r'[,\s]+', order_clause.group(1)):
        clean = token.strip().rstrip(';').lower()
        if clean and clean not in ('asc', 'desc', 'nulls', 'last', 'first', ''):
            bare_order_cols.add(clean)
    for source_col, alias in measure_aliases:
        if source_col.lower() == alias.lower() and alias.lower() in bare_order_cols:
            return (
                f"MEASURE({source_col}) aliased as '{alias}' with ORDER BY '{alias}'"
            )
    return None


def fix_mv_alias_sort_collision(sql: str) -> str:
    """Rewrite ORDER BY alias to ORDER BY MEASURE(col) when collision detected."""
    if not sql or not detect_mv_alias_sort_collision(sql):
        return sql
    measure_aliases = _re.findall(
        r'MEASURE\s*\(\s*(\w+)\s*\)\s+AS\s+(\w+)', sql, _re.IGNORECASE,
    )
    colliding = {
        alias.lower(): source_col
        for source_col, alias in measure_aliases
        if source_col.lower() == alias.lower()
    }

    def _rewrite_order(m: _re.Match) -> str:
        order_body = m.group(1)
        for alias_lower, source_col in colliding.items():
            order_body = _re.sub(
                rf'\b{_re.escape(alias_lower)}\b(?!\s*\()',
                f'MEASURE({source_col})',
                order_body,
                flags=_re.IGNORECASE,
            )
        return f'ORDER BY {order_body}'

    return _re.sub(
        r'ORDER\s+BY\s+(.*?)(?=\bLIMIT\b|$)',
        _rewrite_order, sql, flags=_re.IGNORECASE | _re.DOTALL,
    )


# ═══════════════════════════════════════════════════════════════════════
# 1. Loading
# ═══════════════════════════════════════════════════════════════════════


def _normalize_benchmark_row(row: dict) -> dict:
    """Flatten MLflow evaluation dataset nested structs to a flat benchmark dict.

    MLflow ``genai.datasets`` stores records as ``{inputs: {...}, expectations: {...}}``.
    All downstream consumers expect flat dicts with top-level ``question``,
    ``expected_sql``, ``id``, etc.  This function handles both formats so the
    loader is resilient to schema changes.
    """
    if "inputs" not in row and "expectations" not in row:
        return row

    flat: dict = {}

    inputs = row.get("inputs")
    if isinstance(inputs, dict):
        flat.update(inputs)
    elif hasattr(inputs, "asDict"):
        flat.update(inputs.asDict())

    expectations = row.get("expectations")
    if isinstance(expectations, dict):
        flat.update(expectations)
    elif hasattr(expectations, "asDict"):
        flat.update(expectations.asDict())

    if not flat.get("expected_sql") and flat.get("expected_response"):
        flat["expected_sql"] = flat["expected_response"]

    if flat.get("question_id") and not flat.get("id"):
        flat["id"] = flat["question_id"]

    for k, v in row.items():
        if k not in ("inputs", "expectations") and k not in flat:
            flat[k] = v

    return flat


def load_benchmarks_from_dataset(
    spark_or_dataset: Any,
    uc_schema: str,
    domain: str,
    _max_retries: int = 3,
) -> list[dict]:
    """Load benchmarks from an MLflow evaluation dataset in UC.

    Table name convention: ``{uc_schema}.genie_benchmarks_{domain}``.

    Issues ``REFRESH TABLE`` before reading to avoid
    ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`` when the table was recently
    dropped and recreated by the preflight task.

    Args:
        spark_or_dataset: A Spark session or a pre-loaded DataFrame/list.
        uc_schema: Fully-qualified UC schema (``catalog.schema``).
        domain: Domain identifier (e.g. ``cost``, ``booking``).

    Returns:
        List of benchmark question dicts with ``question``, ``expected_sql``,
        ``expected_asset``, ``category``, etc.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"

    if isinstance(spark_or_dataset, list):
        return spark_or_dataset

    try:
        if hasattr(spark_or_dataset, "read"):
            spark = spark_or_dataset
            for attempt in range(_max_retries):
                try:
                    from genie_space_optimizer.common.delta_helpers import _safe_refresh
                    _safe_refresh(spark, _quote_identifier_fqn(table_name))
                    df = spark.table(table_name)
                    rows = df.collect()
                    benchmarks = [_normalize_benchmark_row(r.asDict(recursive=True)) for r in rows]
                    if rows and "inputs" in (rows[0].asDict()):
                        logger.debug("Normalized %d benchmark rows from nested MLflow format", len(rows))
                    from genie_space_optimizer.common.config import MAX_BENCHMARK_COUNT
                    if len(benchmarks) > MAX_BENCHMARK_COUNT:
                        benchmarks = benchmarks[:MAX_BENCHMARK_COUNT]
                    return benchmarks
                except Exception as read_err:
                    err_msg = str(read_err)
                    if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" in err_msg and attempt < _max_retries - 1:
                        import time as _time
                        wait = 5 * (attempt + 1)
                        logger.warning(
                            "Delta schema change on attempt %d/%d for %s — retrying in %ds",
                            attempt + 1, _max_retries, table_name, wait,
                        )
                        _time.sleep(wait)
                        continue
                    raise
        else:
            df = spark_or_dataset
            rows = df.collect()
            benchmarks = [_normalize_benchmark_row(r.asDict(recursive=True)) for r in rows]
            from genie_space_optimizer.common.config import MAX_BENCHMARK_COUNT
            if len(benchmarks) > MAX_BENCHMARK_COUNT:
                benchmarks = benchmarks[:MAX_BENCHMARK_COUNT]
            return benchmarks
    except Exception:
        logger.exception("Failed to load benchmarks from %s", table_name)
        return []
    return []


# ═══════════════════════════════════════════════════════════════════════
# 2. Validation
# ═══════════════════════════════════════════════════════════════════════


def _extract_table_references(sql: str) -> list[tuple[str, bool]]:
    """Extract fully-qualified table references (catalog.schema.table) from SQL.

    Returns a list of ``(fqn, is_tvf)`` tuples.  *is_tvf* is ``True`` when
    the reference is immediately followed by ``(`` in the SQL, indicating
    a table-valued function call.
    """
    import re
    pattern = re.compile(
        r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
        r"(`[^`]+`\.`[^`]+`\.`[^`]+`"
        r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*)"
        r"(\s*\()?",
        re.IGNORECASE,
    )
    seen: dict[str, bool] = {}
    for match in pattern.finditer(sql):
        ref = match.group(1).replace("`", "")
        has_paren = bool(match.group(2) and match.group(2).strip())
        if ref and ref not in seen:
            seen[ref] = has_paren
    return [(ref, is_tvf) for ref, is_tvf in seen.items()]


def _verify_table_exists(
    spark: Any,
    fqn: str,
    is_tvf: bool = False,
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Check whether a table/view/metric-view exists via SELECT ... LIMIT 0.

    TVF references (``is_tvf=True``) are assumed valid since they cannot be
    verified with table-style SELECT syntax.

    When *w* and *warehouse_id* are provided, routes the check through the
    SQL warehouse Statement Execution API; otherwise uses Spark SQL.
    """
    if is_tvf:
        return True, ""
    try:
        if w and warehouse_id:
            from genie_space_optimizer.optimization.evaluation import (
                _execute_sql_via_warehouse,
            )
            _execute_sql_via_warehouse(
                w, warehouse_id,
                f"SELECT * FROM {_quote_identifier_fqn(fqn)} LIMIT 0",
            )
        else:
            spark.sql(f"SELECT * FROM {_quote_identifier_fqn(fqn)} LIMIT 0")
        return True, ""
    except Exception as e:
        msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in msg or "cannot be found" in msg.lower():
            return False, f"Table/view does not exist: {fqn}"
        if "UNRESOLVABLE_TABLE_VALUED_FUNCTION" in msg:
            return True, ""
        return True, ""


def _quote_identifier_fqn(fqn: str) -> str:
    """Quote a fully-qualified name like catalog.schema.table."""
    parts = fqn.split(".")
    return ".".join(_quote_identifier(p) for p in parts)


def _resolve_params_with_defaults(
    sql: str,
    parameters: list[dict] | None = None,
) -> tuple[str, bool]:
    """Replace ``:param_name`` placeholders with their default values.

    Returns ``(resolved_sql, all_resolved)`` where *all_resolved* is True
    only if every parameter had a usable default value.
    """
    if not parameters:
        return sql, False

    from genie_space_optimizer.optimization.evaluation import _extract_sql_params

    params_in_sql = _extract_sql_params(sql)
    if not params_in_sql:
        return sql, True

    defaults: dict[str, str] = {}
    for p in parameters:
        if not isinstance(p, dict):
            continue
        name = p.get("name", "")
        dv = p.get("default_value", "")
        if isinstance(dv, dict):
            vals = dv.get("values", [])
            dv = vals[0] if vals else ""
        if name and dv:
            defaults[name] = str(dv)

    resolved = sql
    all_resolved = True
    for param in params_in_sql:
        if param in defaults:
            resolved = resolved.replace(f":{param}", f"'{defaults[param]}'")
        else:
            all_resolved = False

    return resolved, all_resolved


_MV_JOIN_RE = _re.compile(r"\bJOIN\b", _re.IGNORECASE)


def validate_ground_truth_sql(
    sql: str,
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
    *,
    execute: bool = False,
    parameters: list[dict] | None = None,
    w: Any = None,
    config: dict | None = None,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Validate a single expected SQL via EXPLAIN + table existence checks.

    Three-phase validation:
      1. EXPLAIN: catches syntax errors and unresolvable column references.
      2. Table existence: catches hallucinated table/view names that EXPLAIN
         sometimes doesn't catch (e.g. metric views with MEASURE() syntax).
      3. Execution sanity (optional, ``execute=True``): runs the query with
         ``LIMIT 1`` to verify it produces at least one row and doesn't fail
         at runtime on data type mismatches.

    When *w* and *warehouse_id* are provided, EXPLAIN and execution calls
    are routed through the SQL Warehouse Statement Execution API (no gRPC).
    Falls back to ``spark.sql()`` otherwise.

    When *parameters* are provided, attempts to substitute default values
    before running EXPLAIN rather than short-circuiting on parameterized SQL.

    Returns ``(is_valid, error_message)``.
    """
    resolved = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
    if not resolved or not resolved.strip():
        return False, "Empty SQL"
    resolved = fix_mv_alias_sort_collision(resolved)

    from genie_space_optimizer.optimization.evaluation import _extract_sql_params

    _params = _extract_sql_params(resolved)
    if _params:
        resolved_with_defaults, all_resolved = _resolve_params_with_defaults(
            resolved, parameters,
        )
        if all_resolved:
            logger.info(
                "Substituted defaults for %d params — running EXPLAIN on resolved SQL",
                len(_params),
            )
            resolved = resolved_with_defaults
        else:
            logger.warning(
                "GT SQL contains parameterized placeholders %s (some without defaults) — "
                "skipping EXPLAIN validation",
                _params,
            )
            return True, ""

    try:
        if w and warehouse_id:
            from genie_space_optimizer.optimization.evaluation import (
                _execute_sql_via_warehouse,
            )
            explain_df = _execute_sql_via_warehouse(
                w, warehouse_id, f"EXPLAIN {resolved}",
                catalog=catalog, schema=gold_schema,
            )
            if not explain_df.empty and "plan" in explain_df.columns:
                plan_text = "\n".join(str(v) for v in explain_df["plan"].tolist())
                if "Error occurred during query planning" in plan_text:
                    raise RuntimeError(plan_text)
        else:
            with _quiet_grpc_logs():
                _set_sql_context(spark, catalog, gold_schema)
                spark.sql(f"EXPLAIN {resolved}")
    except Exception as e:
        err_msg = str(e)
        if "UNBOUND_SQL_PARAMETER" in err_msg:
            logger.warning(
                "EXPLAIN hit UNBOUND_SQL_PARAMETER — treating as valid parameterized SQL"
            )
            return True, ""
        if "UNRESOLVED_COLUMN" in err_msg:
            import re as _re
            col_match = _re.search(r"name `([^`]+)`", err_msg)
            suggest_match = _re.search(r"Did you mean one of the following\? \[([^\]]+)\]", err_msg)
            col_name = col_match.group(1) if col_match else "?"
            suggestion = suggest_match.group(1) if suggest_match else "?"

            # F10 — gate the MEASURE() hint on evidence the unresolved
            # column is actually a metric-view measure. Previously
            # every UNRESOLVED_COLUMN got the same MEASURE() hint
            # unconditionally, including cases where the unresolved
            # token was a missing TABLE reference (e.g. ``dim_date``
            # without qualification). The misleading hint then
            # propagated into the correction-LLM prompt as
            # ``validation_error``, steering the model toward the
            # wrong fix (wrap-in-MEASURE instead of qualify-table).
            # The hint now fires only when either
            #   (a) the Spark error explicitly reports
            #       ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` — the
            #       canonical signal that a measure needs wrapping, or
            #   (b) the unresolved column matches a known measure
            #       name across any metric view in ``config`` — which
            #       handles the case where Spark collapses the
            #       failure into a plain UNRESOLVED_COLUMN while
            #       still being a measure issue.
            from genie_space_optimizer.optimization.evaluation import (
                metric_view_error_kind,
            )

            include_measure_hint = metric_view_error_kind(err_msg) == "missing_measure"
            if not include_measure_hint and col_name != "?" and config:
                _lc_col = col_name.lower()
                _parsed = config.get("_parsed_space", config)
                _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
                _mvs = _ds.get("metric_views", []) if isinstance(_ds, dict) else []
                for _mv in _mvs:
                    if not isinstance(_mv, dict):
                        continue
                    for _measure in _mv.get("measures", []) or []:
                        _mname = ""
                        if isinstance(_measure, dict):
                            _mname = str(_measure.get("name") or "").strip()
                        elif isinstance(_measure, str):
                            _mname = _measure.strip()
                        if _mname and _mname.lower() == _lc_col:
                            include_measure_hint = True
                            break
                    if include_measure_hint:
                        break

            base = (
                f"UNRESOLVED_COLUMN: `{col_name}` — "
                f"suggestion: {suggestion}"
            )
            if include_measure_hint:
                return False, (
                    f"{base} "
                    f"(hint: use MEASURE({col_name}) for metric view measures in ORDER BY)"
                )
            return False, base
        return False, err_msg

    table_refs = _extract_table_references(resolved)
    for ref, is_tvf in table_refs:
        exists, err = _verify_table_exists(
            spark, ref, is_tvf=is_tvf,
            w=w, warehouse_id=warehouse_id,
        )
        if not exists:
            return False, err

    # ── Metric view JOIN ban ────────────────────────────────────────
    # MEASURE() queries cannot use direct JOINs (METRIC_VIEW_JOIN_NOT_SUPPORTED).
    # However, the CTE-first pattern IS valid: MEASURE() inside a WITH clause,
    # then JOIN the CTE result to a dimension table.
    uses_measure = "MEASURE(" in resolved.upper()
    if not uses_measure and config:
        _parsed = config.get("_parsed_space", config)
        _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
        _mv_names = {
            (mv.get("identifier") or mv.get("name") or "").lower().split(".")[-1]
            for mv in (_ds.get("metric_views", []) if isinstance(_ds, dict) else [])
            if isinstance(mv, dict) and (mv.get("identifier") or mv.get("name"))
        }
        if _mv_names and any(mv in resolved.lower() for mv in _mv_names):
            uses_measure = True
    if uses_measure and _MV_JOIN_RE.search(resolved):
        import re as _re_mod
        _uses_cte = bool(_re_mod.search(r"\bWITH\b\s+\w+\s+AS\s*\(", resolved, _re_mod.IGNORECASE))
        if not _uses_cte:
            return False, (
                "METRIC_VIEW_JOIN: Metric view / MEASURE() SQL cannot use direct JOINs "
                "(METRIC_VIEW_JOIN_NOT_SUPPORTED). Use the CTE-first pattern: "
                "materialize the metric view in a WITH clause, then JOIN the CTE result."
            )

    if execute:
        if w and warehouse_id:
            try:
                from genie_space_optimizer.optimization.evaluation import (
                    _execute_sql_via_warehouse,
                )
                result_df = _execute_sql_via_warehouse(
                    w, warehouse_id,
                    f"SELECT * FROM ({resolved}) _vgt LIMIT 1",
                    catalog=catalog, schema=gold_schema,
                )
                if result_df.empty:
                    return False, (
                        "EMPTY_RESULT: Query returned 0 rows — likely wrong filter or empty table"
                    )
            except Exception as exec_err:
                return False, f"EXECUTION_ERROR: {str(exec_err)[:300]}"
        else:
            with _quiet_grpc_logs() as grpc:
                try:
                    result = spark.sql(f"SELECT * FROM ({resolved}) _vgt LIMIT 1").collect()
                    if len(result) == 0:
                        return False, (
                            "EMPTY_RESULT: Query returned 0 rows — likely wrong filter or empty table"
                        )
                except Exception as exec_err:
                    grpc_detail = grpc.get()
                    suffix = f" [grpc: {grpc_detail}]" if grpc_detail else ""
                    return False, f"EXECUTION_ERROR: {str(exec_err)[:300]}{suffix}"

    return True, ""


def validate_benchmarks(
    benchmarks: list[dict],
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
    *,
    w: Any = None,
    warehouse_id: str = "",
    config: dict | None = None,
) -> list[dict]:
    """Validate each benchmark's ``expected_sql`` via EXPLAIN.

    When *config* is provided, also checks for metric view JOIN violations
    that would be caught at eval time by ``_precheck_benchmarks_for_eval``.

    Returns a list of validation result dicts:
    ``{question, expected_sql, valid, error}``.
    """
    results: list[dict] = []
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        question = b.get("question", "")
        is_valid, error = validate_ground_truth_sql(
            sql, spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id, config=config,
        )
        results.append(
            {
                "question": question,
                "expected_sql": sql,
                "valid": is_valid,
                "error": error,
            }
        )
    return results


# ═══════════════════════════════════════════════════════════════════════
# 2b. Question-SQL Alignment Validation (LLM-based)
# ═══════════════════════════════════════════════════════════════════════


def validate_question_sql_alignment(
    benchmarks: list[dict],
    *,
    batch_size: int = 10,
) -> list[dict]:
    """Check whether each benchmark's GT SQL answers exactly what the question asks.

    Uses a lightweight LLM call to detect misalignment issues such as extra
    filters, extra columns, missing aggregation, or wrong interpretation.

    Returns a list of ``{question, aligned, issues}`` dicts, one per benchmark.
    Benchmarks without ``expected_sql`` are marked as aligned (nothing to check).
    """
    import json

    from genie_space_optimizer.common.config import (
        BENCHMARK_ALIGNMENT_CHECK_PROMPT,
        LLM_ENDPOINT,
        format_mlflow_template,
    )

    from genie_space_optimizer.common.config import REQUIRE_GROUND_TRUTH_SQL

    results: list[dict] = []
    to_check: list[tuple[int, dict]] = []
    for i, b in enumerate(benchmarks):
        sql = b.get("expected_sql", "")
        if not sql or not sql.strip():
            if REQUIRE_GROUND_TRUTH_SQL:
                results.append({
                    "question": b.get("question", ""),
                    "aligned": False,
                    "issues": ["missing_expected_sql"],
                })
            else:
                results.append({"question": b.get("question", ""), "aligned": True, "issues": []})
        else:
            results.append({"question": b.get("question", ""), "aligned": True, "issues": []})
            to_check.append((i, b))

    if not to_check:
        return results

    for batch_start in range(0, len(to_check), batch_size):
        batch = to_check[batch_start : batch_start + batch_size]
        batch_payload = [
            {"question": b.get("question", ""), "expected_sql": b.get("expected_sql", "")}
            for _, b in batch
        ]
        prompt = format_mlflow_template(
            BENCHMARK_ALIGNMENT_CHECK_PROMPT,
            benchmarks_json=json.dumps(batch_payload, indent=2),
        )

        try:
            from genie_space_optimizer.optimization.evaluation import (
                _link_prompt_to_trace,
                get_registered_prompt_name,
            )
            from genie_space_optimizer.optimization.llm_client import call_llm

            _link_prompt_to_trace(get_registered_prompt_name("benchmark_alignment_check"))

            raw, _response = call_llm(
                None,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            checks = json.loads(raw)

            for j, check in enumerate(checks):
                if j < len(batch):
                    idx = batch[j][0]
                    results[idx]["aligned"] = check.get("aligned", True)
                    results[idx]["issues"] = check.get("issues", [])
        except Exception as exc:
            logger.warning(
                "Alignment check failed for batch starting at %d: %s",
                batch_start, exc,
            )

    misaligned = sum(1 for r in results if not r["aligned"])
    if misaligned:
        logger.info(
            "Alignment check: %d/%d benchmarks flagged as misaligned",
            misaligned, len(benchmarks),
        )
        for r in results:
            if not r["aligned"]:
                logger.info(
                    "  Misaligned: %s — %s",
                    r["question"][:80], "; ".join(r["issues"]),
                )

    return results


# ═══════════════════════════════════════════════════════════════════════
# 2c. Predicate Value Validation (data-profile-grounded)
# ═══════════════════════════════════════════════════════════════════════


_EQ_PREDICATE_RE = _re.compile(
    r"""
    (?:`?(\w+)`?\s*\.\s*)?   # optional table alias: t. or `t`.
    `?(\w+)`?                # column name
    \s*=\s*                  # equals sign
    '([^']*)'                # single-quoted literal
    """,
    _re.VERBOSE | _re.IGNORECASE,
)

_IN_PREDICATE_RE = _re.compile(
    r"""
    (?:`?(\w+)`?\s*\.\s*)?   # optional table alias
    `?(\w+)`?                # column name
    \s+IN\s*\(               # IN (
    ([^)]+)                  # values inside parens
    \)
    """,
    _re.VERBOSE | _re.IGNORECASE,
)

_FROM_JOIN_RE = _re.compile(
    r"""
    (?:FROM|JOIN)\s+
    `?([a-zA-Z0-9_.]+)`?     # table FQN
    (?:\s+(?:AS\s+)?`?(\w+)`?)?  # optional alias
    """,
    _re.VERBOSE | _re.IGNORECASE,
)


def _extract_table_aliases(sql: str) -> dict[str, str]:
    """Extract {alias_or_leaf: full_table_name} from FROM/JOIN clauses."""
    aliases: dict[str, str] = {}
    for m in _FROM_JOIN_RE.finditer(sql):
        table_fqn = m.group(1)
        alias = m.group(2)
        leaf = table_fqn.split(".")[-1].strip("`").lower()
        norm_fqn = table_fqn.replace("`", "").lower()
        if alias:
            aliases[alias.lower()] = norm_fqn
        aliases[leaf] = norm_fqn
    return aliases


def _extract_predicates(sql: str) -> list[dict]:
    """Extract equality and IN predicates from SQL WHERE clauses."""
    predicates: list[dict] = []
    for m in _EQ_PREDICATE_RE.finditer(sql):
        predicates.append({
            "table_alias": (m.group(1) or "").lower(),
            "column": m.group(2).lower(),
            "values": [m.group(3)],
        })
    for m in _IN_PREDICATE_RE.finditer(sql):
        raw_vals = m.group(3)
        values = [v.strip().strip("'\"") for v in raw_vals.split(",")]
        predicates.append({
            "table_alias": (m.group(1) or "").lower(),
            "column": m.group(2).lower(),
            "values": values,
        })
    return predicates


def validate_predicate_values(
    benchmarks: list[dict],
    data_profile: dict[str, dict],
    *,
    fuzzy_threshold: float = 0.85,
) -> list[dict]:
    """Check WHERE clause literal values against profiled distinct values.

    For each benchmark, extracts equality/IN predicates from expected_sql,
    resolves column references to profiled tables, and checks whether the
    literal values exist in the profiled distinct_values.

    Returns a list of dicts (one per benchmark) with keys:
      - ``question``: the benchmark question text
      - ``valid``: True if all predicates match (or can't be checked)
      - ``mismatches``: list of ``{column, table, literal, profiled_values, suggestion}``
    """
    from difflib import SequenceMatcher

    profile_lower: dict[str, dict] = {}
    for tbl, tinfo in data_profile.items():
        norm_key = tbl.replace("`", "").lower()
        cols_lower: dict[str, dict] = {}
        for col, cinfo in tinfo.get("columns", {}).items():
            cols_lower[col.lower()] = cinfo
        profile_lower[norm_key] = {"columns": cols_lower}

    results: list[dict] = []
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        question = b.get("question", "")
        if not sql or not sql.strip():
            results.append({"question": question, "valid": True, "mismatches": []})
            continue

        aliases = _extract_table_aliases(sql)
        predicates = _extract_predicates(sql)
        mismatches: list[dict] = []

        for pred in predicates:
            col_name = pred["column"]
            tbl_alias = pred["table_alias"]

            resolved_table = aliases.get(tbl_alias, "") if tbl_alias else ""
            candidate_tables: list[str] = []
            if resolved_table:
                candidate_tables.append(resolved_table)
            else:
                candidate_tables.extend(aliases.values())

            profiled_values: list[str] | None = None
            matched_table = ""
            for ct in candidate_tables:
                for profile_key in profile_lower:
                    if ct in profile_key or profile_key.endswith(ct):
                        col_info = profile_lower[profile_key].get("columns", {}).get(col_name)
                        if col_info and col_info.get("distinct_values"):
                            profiled_values = col_info["distinct_values"]
                            matched_table = profile_key
                            break
                if profiled_values is not None:
                    break

            if profiled_values is None:
                continue

            profiled_lower = {str(v).lower(): str(v) for v in profiled_values}
            for literal in pred["values"]:
                if literal.lower() in profiled_lower:
                    continue

                best_match = ""
                best_score = 0.0
                for pv_lower, pv_original in profiled_lower.items():
                    score = SequenceMatcher(None, literal.lower(), pv_lower).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = pv_original

                mismatches.append({
                    "column": col_name,
                    "table": matched_table,
                    "literal": literal,
                    "profiled_values": profiled_values[:20],
                    "suggestion": best_match if best_score >= fuzzy_threshold else None,
                    "fuzzy_score": round(best_score, 3),
                })

        results.append({
            "question": question,
            "valid": len(mismatches) == 0,
            "mismatches": mismatches,
        })

    flagged = sum(1 for r in results if not r["valid"])
    if flagged:
        logger.info(
            "Predicate value check: %d/%d benchmarks have value mismatches",
            flagged, len(benchmarks),
        )
        for r in results:
            if not r["valid"]:
                for mm in r["mismatches"]:
                    logger.info(
                        "  Mismatch: %s — %s.%s='%s' not in profiled values %s (suggestion=%s)",
                        r["question"][:60], mm["table"], mm["column"],
                        mm["literal"], mm["profiled_values"][:5], mm["suggestion"],
                    )

    return results


# ═══════════════════════════════════════════════════════════════════════
# 2d. GT Execution Check (non-empty result validation)
# ═══════════════════════════════════════════════════════════════════════


def validate_gt_returns_results(
    benchmarks: list[dict],
    spark: Any,
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
    max_checks: int = 50,
) -> list[dict]:
    """Run a COUNT(*) wrapper around each GT SQL to verify it returns rows.

    Benchmarks whose expected_sql yields zero rows are flagged — this
    typically indicates incorrect WHERE clause values or referencing
    the wrong table partition.

    Returns a list of ``{question, has_results, row_count, error}`` dicts.
    """
    from genie_space_optimizer.optimization.evaluation import _exec_sql

    _sql_kw: dict[str, Any] = dict(w=w, warehouse_id=warehouse_id, catalog=catalog, schema=schema)
    results: list[dict] = []

    for b in benchmarks[:max_checks]:
        sql = b.get("expected_sql", "")
        question = b.get("question", "")
        if not sql or not sql.strip():
            results.append({"question": question, "has_results": True, "row_count": -1, "error": None})
            continue

        count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql.rstrip(';')}) _gt_check"
        try:
            df = _exec_sql(count_sql, spark, **_sql_kw)
            if df is not None and not df.empty:
                cnt = int(df.iloc[0]["cnt"])
                results.append({
                    "question": question,
                    "has_results": cnt > 0,
                    "row_count": cnt,
                    "error": None,
                })
            else:
                results.append({"question": question, "has_results": False, "row_count": 0, "error": "empty result"})
        except Exception as exc:
            results.append({
                "question": question,
                "has_results": True,
                "row_count": -1,
                "error": str(exc)[:200],
            })

    empty_count = sum(1 for r in results if not r["has_results"])
    if empty_count:
        logger.warning(
            "GT execution check: %d/%d benchmarks returned 0 rows",
            empty_count, len(results),
        )
        for r in results:
            if not r["has_results"]:
                logger.warning("  Empty GT: %s (error=%s)", r["question"][:80], r.get("error"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 3. Train/Held-Out Split
# ═══════════════════════════════════════════════════════════════════════


def assign_splits(
    benchmarks: list[dict],
    train_ratio: float = 1.0 - HELD_OUT_RATIO,
    seed: int = 42,
) -> list[dict]:
    """Assign ``split`` field using deterministic random sampling.

    Split assignment is intentionally independent of benchmark provenance.
    User-authored, sample-derived, synthetic, and gap-fill questions all
    participate in the same random split so the held-out set is a real random
    sample of the final validated corpus.
    """
    from genie_space_optimizer.common.config import (
        MIN_HELD_OUT_BENCHMARK_COUNT,
        MIN_TRAIN_BENCHMARK_COUNT,
    )

    n = len(benchmarks)
    if n == 0:
        return benchmarks
    if n == 1:
        benchmarks[0]["split"] = "train"
        return benchmarks

    if n >= MIN_TRAIN_BENCHMARK_COUNT + MIN_HELD_OUT_BENCHMARK_COUNT:
        held_out_count = MIN_HELD_OUT_BENCHMARK_COUNT
    else:
        held_out_count = max(1, min(n - 1, int(round(n * (1.0 - train_ratio)))))

    indices = list(range(n))
    rng = random.Random(seed)
    rng.shuffle(indices)
    held_out_indices = set(indices[:held_out_count])

    for i, benchmark in enumerate(benchmarks):
        benchmark["split"] = "held_out" if i in held_out_indices else "train"

    return benchmarks


# ═══════════════════════════════════════════════════════════════════════
# 4. MLflow Record Building
# ═══════════════════════════════════════════════════════════════════════


def build_eval_records(benchmarks: list[dict]) -> list[dict]:
    """Convert benchmarks to MLflow evaluation record format.

    Each record has ``inputs`` (question, question_id) and ``expectations``
    (expected_sql, expected_asset, expected_facts, required_tables, etc.).
    """
    _VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})
    records: list[dict] = []
    for b in benchmarks:
        question = b.get("question", "")
        qid = b.get("question_id") or hashlib.md5(
            question.encode()
        ).hexdigest()[:8]

        _raw_asset = b.get("expected_asset", "TABLE")
        _esql = b.get("expected_sql", "")
        _asset = (
            _raw_asset.strip().upper()
            if isinstance(_raw_asset, str) and _raw_asset and _raw_asset.strip().upper() in _VALID_ASSET_TYPES
            else detect_asset_type(_esql)
        )

        records.append(
            {
                "inputs": {
                    "question": question,
                    "question_id": qid,
                },
                "expectations": {
                    "expected_sql": b.get("expected_sql", ""),
                    "expected_asset": _asset,
                    "expected_facts": b.get("expected_facts", []),
                    "required_tables": b.get("required_tables", []),
                    "required_columns": b.get("required_columns", []),
                    "category": b.get("category", ""),
                    "split": b.get("split", "train"),
                },
            }
        )
    return records


# ═══════════════════════════════════════════════════════════════════════
# 5. Corrections
# ═══════════════════════════════════════════════════════════════════════


_CORRECTABLE_VERDICTS = {"genie_correct", "arbiter_repair"}


def apply_benchmark_corrections(
    corrections: list[dict],
    spark: Any,
    uc_schema: str,
    domain: str,
    *,
    w: Any = None,
    warehouse_id: str = "",
    data_profile: dict | None = None,
) -> dict:
    """Apply arbiter corrections to the MLflow evaluation dataset.

    Each correction dict should have:
    - ``question``: the benchmark question to correct
    - ``new_expected_sql``: the corrected SQL
    - ``verdict``: ``genie_correct`` or ``arbiter_repair``

    Before applying, validates:
    1. Syntactic validity (EXPLAIN)
    2. Semantic alignment with the question (LLM check)
    3. Predicate values against data profile (if available)

    Returns ``{applied: int, skipped: int, errors: list[str]}``.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    applied = 0
    skipped = 0
    errors: list[str] = []

    for c in corrections:
        question = c.get("question", "")
        new_sql = c.get("new_expected_sql", "")
        verdict = c.get("verdict", "")

        if verdict not in _CORRECTABLE_VERDICTS:
            skipped += 1
            continue

        if not new_sql:
            errors.append(f"Empty new_expected_sql for question: {question[:50]}")
            skipped += 1
            continue

        is_valid, val_err = validate_ground_truth_sql(
            new_sql, spark, execute=True, w=w, warehouse_id=warehouse_id,
        )
        if not is_valid:
            errors.append(
                f"Correction SQL invalid for '{question[:50]}': {val_err[:200]}"
            )
            logger.warning(
                "Skipping arbiter correction — SQL fails validation: %s — %s",
                question[:60], val_err[:200],
            )
            skipped += 1
            continue

        try:
            alignment = validate_question_sql_alignment(
                [{"question": question, "expected_sql": new_sql}]
            )
            if alignment and not alignment[0].get("aligned", True):
                issues = "; ".join(alignment[0].get("issues", []))
                logger.warning(
                    "Skipping arbiter correction — SQL misaligned with question: "
                    "'%s' — issues: %s",
                    question[:60], issues,
                )
                errors.append(
                    f"Alignment mismatch for '{question[:50]}': {issues[:150]}"
                )
                skipped += 1
                continue
        except Exception:
            logger.debug(
                "Alignment check failed for correction, proceeding cautiously",
                exc_info=True,
            )

        if data_profile:
            try:
                pred_results = validate_predicate_values(
                    [{"question": question, "expected_sql": new_sql}],
                    data_profile,
                )
                if pred_results and not pred_results[0]["valid"]:
                    unfixable = [
                        m for m in pred_results[0]["mismatches"]
                        if not m.get("suggestion")
                    ]
                    if unfixable:
                        mm_desc = "; ".join(
                            f"{m['column']}='{m['literal']}'" for m in unfixable
                        )
                        logger.warning(
                            "Skipping arbiter correction — predicate value "
                            "mismatch: '%s' — %s",
                            question[:60], mm_desc,
                        )
                        errors.append(
                            f"Predicate mismatch for '{question[:50]}': {mm_desc[:150]}"
                        )
                        skipped += 1
                        continue
            except Exception:
                logger.debug(
                    "Predicate check failed for correction, proceeding cautiously",
                    exc_info=True,
                )

        try:
            escaped_sql = new_sql.replace("'", "\\'")
            escaped_q = question.replace("'", "\\'")
            spark.sql(
                f"""
                UPDATE {table_name}
                SET expected_sql = '{escaped_sql}',
                    corrected_by = 'arbiter',
                    correction_verdict = '{verdict}'
                WHERE question = '{escaped_q}'
                """
            )
            applied += 1
        except Exception as e:
            errors.append(f"Failed to update '{question[:50]}': {e}")
            skipped += 1

    return {"applied": applied, "skipped": skipped, "errors": errors}


def quarantine_benchmark_question(
    spark: Any,
    uc_schema: str,
    domain: str,
    question: str,
    *,
    reason: str = "",
) -> bool:
    """Quarantine a benchmark question by setting ``quarantined_at`` and ``quarantine_reason``.

    Quarantined questions are excluded from the accuracy denominator so the
    optimizer stops wasting lever budget on questions with broken ground truth.

    The columns are added dynamically if they don't exist yet (safe for
    existing tables that predate this feature).

    Returns ``True`` if the row was updated, ``False`` otherwise.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"

    for col, dtype in [("quarantined_at", "TIMESTAMP"), ("quarantine_reason", "STRING")]:
        try:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")
        except Exception:
            pass

    escaped_q = question.replace("'", "\\'")
    escaped_reason = reason.replace("'", "\\'")
    try:
        spark.sql(
            f"""
            UPDATE {table_name}
            SET quarantined_at = CURRENT_TIMESTAMP(),
                quarantine_reason = '{escaped_reason}'
            WHERE question = '{escaped_q}'
              AND quarantined_at IS NULL
            """
        )
        return True
    except Exception as e:
        logger.warning("Failed to quarantine question '%s': %s", question[:60], e)
        return False


def get_quarantined_questions(
    spark: Any,
    uc_schema: str,
    domain: str,
) -> set[str]:
    """Return the set of question IDs that are currently quarantined."""
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        df = spark.sql(
            f"SELECT question_id FROM {table_name} WHERE quarantined_at IS NOT NULL"
        ).toPandas()
        return set(df["question_id"].dropna().astype(str).tolist())
    except Exception:
        return set()


# ── SQL Snippet Validation (Lever 6) ──────────────────────────────────


def _extract_primary_table(sql: str, metadata_snapshot: dict) -> str | None:
    """Extract the primary table referenced in a SQL snippet expression.

    Looks for fully-qualified table references (catalog.schema.table) that
    appear in the metadata snapshot's data_sources (both tables and metric views).
    """
    ds = metadata_snapshot.get("data_sources", {})
    all_sources: list = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])
    table_ids = {
        t.get("identifier", "").lower(): t.get("identifier", "")
        for t in all_sources if isinstance(t, dict) and t.get("identifier")
    }

    sql_lower = sql.lower()
    for tid_lower, tid in table_ids.items():
        parts = tid_lower.split(".")
        short_name = parts[-1] if parts else tid_lower
        if short_name in sql_lower or tid_lower in sql_lower:
            return tid

    if table_ids:
        return next(iter(table_ids.values()))

    return None


def _resolve_primary_table_fqn(
    table_identifier: str, *, catalog: str, gold_schema: str,
) -> str:
    """Return the fully-qualified ``catalog.schema.table`` identifier.

    Handles the two shapes we see in practice:

    - Already FQ (``foo.bar.baz``) — returned as-is after placeholder
      substitution (so ``${catalog}.${gold_schema}.t`` resolves too).
    - Short (``baz``) — wrapped in placeholders, then resolved.
    """
    if not table_identifier:
        return ""
    if "." not in table_identifier:
        templated = f"${{catalog}}.${{gold_schema}}.{table_identifier}"
    else:
        templated = table_identifier
    return resolve_sql(templated, catalog=catalog, gold_schema=gold_schema)


def _collect_columns_by_table(metadata_snapshot: dict) -> dict[str, set[str]]:
    """Build ``{table_identifier_lower: {column_name_lower, ...}}``.

    Includes metric view measures and dimensions alongside regular columns so
    a SQL snippet like ``SUM(mv_sales.cy_sales)`` has its bare form
    (``SUM(cy_sales)``) recognised and prefixed.
    """
    ds = metadata_snapshot.get("data_sources", {})
    all_sources: list = []
    if isinstance(ds, dict):
        all_sources.extend(ds.get("tables", []) or [])
        all_sources.extend(ds.get("metric_views", []) or [])

    out: dict[str, set[str]] = {}
    for t in all_sources:
        if not isinstance(t, dict):
            continue
        tid = (t.get("identifier") or t.get("name") or "").strip().lower()
        if not tid:
            continue
        cols: set[str] = set()
        for col in (t.get("columns", []) or []):
            name = (col.get("name") or "").strip().lower()
            if name:
                cols.add(name)
        for cc in (t.get("column_configs", []) or []):
            name = (cc.get("column_name") or "").strip().lower()
            if name:
                cols.add(name)
        # Metric views expose measures + dimensions as first-class references
        # that can appear in raw SQL expressions against the MV.
        for m in (t.get("measures", []) or []):
            name = (m.get("name") or "").strip().lower()
            if name:
                cols.add(name)
        for d in (t.get("dimensions", []) or []):
            name = (d.get("name") or "").strip().lower()
            if name:
                cols.add(name)
        out[tid] = cols
    return out


# ``AS alias`` and ``WITH alias AS (`` — their identifiers must never be
# prefixed as columns. Kept intentionally permissive: a false-positive on an
# alias just means we skip prefixing, which is always safe.
_CTE_AS_ALIAS_RE = _re.compile(
    r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)", _re.IGNORECASE,
)
_WITH_ALIAS_RE = _re.compile(
    r"\bWITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", _re.IGNORECASE,
)
# Explicit table references in a SQL body (FROM / JOIN). We do not need a
# full parser — the goal is to bound the ambiguity guard to tables that the
# SQL actually touches, not every table in the snapshot.
_FROM_JOIN_TABLE_RE = _re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_.`]*)",
    _re.IGNORECASE,
)


def _extract_cte_aliases(sql: str) -> set[str]:
    """Return a set of lowercase identifiers that look like CTE / AS aliases."""
    aliases: set[str] = set()
    for m in _CTE_AS_ALIAS_RE.finditer(sql):
        aliases.add(m.group(1).lower())
    for m in _WITH_ALIAS_RE.finditer(sql):
        aliases.add(m.group(1).lower())
    return aliases


def _extract_referenced_tables(sql: str, known_tables: dict[str, set[str]]) -> set[str]:
    """Return the subset of ``known_tables`` whose identifier appears in ``sql``.

    We match by short name (last segment) OR full identifier — the SQL might
    reference either. Only tables actually mentioned in ``FROM`` / ``JOIN``
    are returned; this scopes the ambiguity guard to the query's real
    universe rather than every source in the snapshot.
    """
    referenced: set[str] = set()
    mentioned: set[str] = set()
    for m in _FROM_JOIN_TABLE_RE.finditer(sql):
        raw = m.group(1).strip("`").lower()
        mentioned.add(raw)
        if "." in raw:
            mentioned.add(raw.split(".")[-1])
    for tid in known_tables:
        short = tid.split(".")[-1]
        if tid in mentioned or short in mentioned:
            referenced.add(tid)
    return referenced


def _auto_prefix_bare_columns(
    sql: str,
    table_identifier: str,
    metadata_snapshot: dict,
    *,
    catalog: str = "",
    gold_schema: str = "",
) -> tuple[str, list[str]]:
    """Prefix bare column references with the fully-qualified table name.

    The Genie API rejects ``column`` references and short-form
    ``table.column`` references at serving time — SQL snippets stored in
    ``instructions.sql_snippets.*`` must use
    ``catalog.schema.table.column``. This was the root cause of the bug
    from the failing run.

    Behaviour:

    - Resolves ``${catalog}`` / ``${gold_schema}`` placeholders via
      :func:`resolve_sql` before emitting the prefix.
    - Emits the full identifier (``catalog.schema.table``) — not the short
      table name — so every stored snippet is usable by the serving path.
    - Ambiguity guard: if a bare column matches columns on 2+ tables that
      the SQL references (FROM / JOIN), the column is skipped and a
      warning is appended. Single-table snippets (the common case) are
      unaffected.
    - Metric views: ``measures[].name`` and ``dimensions[].name`` are
      discovered alongside regular columns so ``SUM(cy_sales)`` against an
      MV with a ``cy_sales`` measure prefixes correctly.
    - CTE / subquery aliases (``WITH x AS (...)`` or ``FROM t AS x``) are
      never rewritten as columns.

    Returns ``(sql, warnings)``. Warnings is a (possibly empty) list of
    human-readable strings; never raises.
    """
    warnings: list[str] = []
    if not sql:
        return sql, warnings

    # Resolve the primary table to its FQ form up front. Without a primary
    # table we cannot decide which prefix to emit — return the SQL as-is and
    # leave the warning so the caller can drop the snippet if strict.
    primary_fqn = _resolve_primary_table_fqn(
        table_identifier, catalog=catalog, gold_schema=gold_schema,
    )
    if not primary_fqn:
        warnings.append(
            "no primary table identifier provided; columns left un-prefixed"
        )
        return sql, warnings

    all_columns = _collect_columns_by_table(metadata_snapshot)
    primary_lower = primary_fqn.lower()

    # Try the exact FQ key first; fall back to any entry whose short name
    # matches (handles metadata snapshots that store short-form identifiers).
    primary_cols: set[str] = set()
    if primary_lower in all_columns:
        primary_cols = all_columns[primary_lower]
    else:
        primary_short = primary_lower.split(".")[-1]
        for tid, cols in all_columns.items():
            if tid.split(".")[-1] == primary_short:
                primary_cols = cols
                break

    if not primary_cols:
        warnings.append(
            f"no columns known for primary table {primary_fqn!r}; "
            "columns left un-prefixed"
        )
        return sql, warnings

    referenced_tables = _extract_referenced_tables(sql, all_columns)
    # Scope the ambiguity guard to tables actually mentioned in the SQL plus
    # the primary. Bare expressions (no FROM/JOIN) shrink the universe to
    # just the primary, which is the common case for sql_snippets.
    in_scope = referenced_tables | {primary_lower}

    cte_aliases = _extract_cte_aliases(sql)

    result = sql
    # Longest-column-first so columns whose name is a prefix of another
    # (``revenue`` vs ``revenue_amt``) don't partially-match.
    for col in sorted(primary_cols, key=len, reverse=True):
        col_lower = col.lower()
        if col_lower in cte_aliases:
            continue
        # Ambiguity: column is present on >1 in-scope table.
        other_tables_with_col = [
            tid for tid in in_scope
            if tid != primary_lower and col_lower in all_columns.get(tid, set())
        ]
        if other_tables_with_col:
            warnings.append(
                f"column {col!r} is ambiguous — present on primary {primary_fqn} "
                f"and also on {sorted(other_tables_with_col)!r}; skipped"
            )
            continue

        pattern = _re.compile(
            r'(?<![.\w`])(' + _re.escape(col) + r')(?!\w)',
            _re.IGNORECASE,
        )

        def _replacer(m: _re.Match, _cur: str = result) -> str:
            start = m.start()
            # Already qualified (``t.col``, ``schema.t.col``, or backtick-
            # quoted) — leave alone.
            prefix_check = _cur[:start]
            if prefix_check.rstrip().endswith(".") or prefix_check.rstrip().endswith("`"):
                return m.group(0)
            return f"{primary_fqn}.{m.group(1)}"

        result = pattern.sub(_replacer, result)

    return result, warnings


def normalize_sql_snippet(
    sql: str,
    snippet_type: str,
    metadata_snapshot: dict,
    *,
    catalog: str = "",
    gold_schema: str = "",
    spark: Any = None,
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[str, list[str]]:
    """Return the stored form of a SQL snippet: strip wrappers, FQ-prefix, EXPLAIN.

    This is the *storage* shape — no execution check, no data read. Used
    for (a) repairing existing snippets in-place (the common case: a
    space already has snippets with short-form prefixes) and (b) pre-
    validating freshly-mined candidates before the heavier execution
    check in :func:`validate_sql_snippet`.

    Steps:

    1. Trim the SQL and drop trailing semicolons.
    2. Drop wrapper clauses — Genie stores the raw expression, never
       ``SELECT …`` or ``WHERE …``.
    3. Prefix bare columns with ``catalog.schema.table`` (see
       :func:`_auto_prefix_bare_columns`).
    4. If an execution backend is available, run ``EXPLAIN`` on the
       wrapped form to catch syntax / resolution errors. This is
       optional — callers can skip it by omitting both ``spark`` and
       ``w``/``warehouse_id``.

    Returns ``(normalized_sql, warnings)``. On EXPLAIN failure, the
    error is added to ``warnings`` but the normalized SQL is still
    returned so the caller can decide whether to store it anyway.
    """
    warnings: list[str] = []
    if not sql or not isinstance(sql, str):
        return sql, warnings

    cleaned = sql.strip()
    # Drop trailing semicolons & comments on a single line.
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()

    # Strip ``SELECT``/``WHERE`` wrappers a well-meaning LLM might emit —
    # Genie rejects them at the snippet boundary. This is a best-effort
    # unwrap; it won't defeat a legitimate subquery that starts with
    # SELECT.
    if snippet_type == "filter":
        upper = cleaned.upper()
        if upper.startswith("WHERE "):
            cleaned = cleaned[6:].lstrip()

    # Locate the primary table so we can FQ-prefix.
    table = _extract_primary_table(cleaned, metadata_snapshot)
    if not table:
        warnings.append("cannot determine primary table; skipping FQ-prefix")
        prefixed_sql = cleaned
    else:
        prefixed_sql, prefix_warnings = _auto_prefix_bare_columns(
            cleaned, table, metadata_snapshot,
            catalog=catalog, gold_schema=gold_schema,
        )
        warnings.extend(prefix_warnings)

    # EXPLAIN guard — caller opts in by providing an execution backend.
    have_backend = (spark is not None) or (w is not None and warehouse_id)
    if have_backend and table:
        resolved_table = _resolve_primary_table_fqn(
            table, catalog=catalog, gold_schema=gold_schema,
        )
        if snippet_type == "filter":
            wrapped = f"SELECT 1 FROM {resolved_table} WHERE {prefixed_sql} LIMIT 1"
        else:
            wrapped = f"SELECT {prefixed_sql} FROM {resolved_table} LIMIT 1"
        try:
            if w and warehouse_id:
                from genie_space_optimizer.optimization.evaluation import (
                    _execute_sql_via_warehouse,
                )
                _execute_sql_via_warehouse(
                    w, warehouse_id, f"EXPLAIN {wrapped}",
                    catalog=catalog, schema=gold_schema,
                )
            elif spark is not None:
                _set_sql_context(spark, catalog, gold_schema)
                spark.sql(f"EXPLAIN {wrapped}")
        except Exception as exc:
            warnings.append(f"EXPLAIN failed: {exc}")

    return prefixed_sql, warnings


# S8 — tautology detectors for ``snippet_type == "filter"``.
# Cheap syntactic pre-check before we spend a warehouse round-trip. Each
# pattern targets a canonical form the model regularly emits as a "safe
# no-op": ``1=1``, ``TRUE``, ``col = col``, ``x IS NOT NULL OR x IS NULL``.
_VACUOUS_FILTER_PATTERNS: tuple[_re.Pattern[str], ...] = (
    _re.compile(r"^\s*1\s*=\s*1\s*$"),
    _re.compile(r"^\s*(true|TRUE)\s*$"),
    _re.compile(r"^\s*(\w+)\s*=\s*\1\s*$"),
    _re.compile(
        r"^\s*(\w+)\s+IS\s+NOT\s+NULL\s+OR\s+\1\s+IS\s+NULL\s*$",
        _re.IGNORECASE,
    ),
    _re.compile(
        r"^\s*(\w+)\s+IS\s+NULL\s+OR\s+\1\s+IS\s+NOT\s+NULL\s*$",
        _re.IGNORECASE,
    ),
)


def _is_vacuous_filter_syntactic(sql: str) -> bool:
    """Return True when ``sql`` textually matches a tautology template."""
    candidate = (sql or "").strip().strip(";").strip()
    if candidate.startswith("(") and candidate.endswith(")"):
        candidate = candidate[1:-1].strip()
    return any(p.match(candidate) for p in _VACUOUS_FILTER_PATTERNS)


# B5 — match ``<identifier> = '<literal>'`` or ``<identifier> = <numeric>``.
# The zero-row-rejection guard below uses this to limit the false-positive
# blast radius: range / inequality / IN-list filters that legitimately
# match no current rows (e.g. ``created_at > 2030-01-01``) bypass the
# guard and stay accepted.
_SIMPLE_EQUALITY_PATTERN = _re.compile(
    r"""^\s*
        (?:[A-Za-z_][A-Za-z0-9_]*\.)*       # optional table / schema prefix
        [A-Za-z_][A-Za-z0-9_]*              # identifier
        \s*=\s*
        (?:                                 # right-hand-side literal:
            '(?:[^']|'')*'                   #   single-quoted string
          | -?\d+(?:\.\d+)?                  #   numeric
        )
        \s*$
    """,
    _re.IGNORECASE | _re.VERBOSE,
)


def _is_simple_equality(sql: str) -> bool:
    """Return True when *sql* is a single equality on a literal RHS."""
    candidate = (sql or "").strip().strip(";").strip()
    if candidate.startswith("(") and candidate.endswith(")"):
        candidate = candidate[1:-1].strip()
    return bool(_SIMPLE_EQUALITY_PATTERN.match(candidate))


def validate_sql_snippet(
    sql: str,
    snippet_type: str,
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[bool, str, str]:
    """Validate a SQL snippet: normalize + EXPLAIN + execute.

    Three-phase validation mirroring ``validate_ground_truth_sql``:

    1. :func:`normalize_sql_snippet` — strip wrappers, FQ-prefix bare
       columns, EXPLAIN. This is the shape we'll store.
    2. Execution — runs the wrapped SQL with ``LIMIT 1``.

    Wrapping rules for execution:

    - measure:    ``SELECT <sql> FROM <table> LIMIT 1``
    - filter:     ``SELECT 1 FROM <table> WHERE <sql> LIMIT 1``
    - expression: ``SELECT <sql> FROM <table> LIMIT 1``

    S8 — when ``snippet_type == "filter"`` and ``GSO_REJECT_VACUOUS_FILTERS``
    is on (default), two additional guards run:

    - A syntactic pre-check that rejects ``1=1``, ``TRUE``, ``col = col``,
      and the ``x IS NULL OR x IS NOT NULL`` tautology without touching
      the warehouse.
    - A selectivity post-check (after the LIMIT-1 probe succeeds) that
      runs ``SELECT COUNT(*) total, COUNT(*) FILTER (WHERE <filter>)``
      on the resolved table and rejects the snippet when ``filtered >=
      total`` (the filter restricts nothing). If the table is empty
      (``total == 0``) we skip the check — emptiness cannot prove vacuity.

    Returns ``(is_valid, error_message, prefixed_sql)`` — callers should
    use ``prefixed_sql`` (the 3rd element) when storing the snippet so
    the FQ form is persisted.
    """
    from genie_space_optimizer.common.config import REJECT_VACUOUS_FILTERS

    table = _extract_primary_table(sql, metadata_snapshot)
    if not table:
        return False, "Cannot determine primary table for SQL snippet", sql

    prefixed_sql, warnings = normalize_sql_snippet(
        sql, snippet_type, metadata_snapshot,
        catalog=catalog, gold_schema=gold_schema,
        spark=spark, w=w, warehouse_id=warehouse_id,
    )
    # ``normalize_sql_snippet`` surfaces EXPLAIN failures via warnings.
    for warning in warnings:
        if warning.startswith("EXPLAIN failed:"):
            return False, warning, prefixed_sql

    # S8 — syntactic tautology pre-check. Rejects the most common
    # no-op shapes before we spend a warehouse round-trip.
    if (
        REJECT_VACUOUS_FILTERS
        and snippet_type == "filter"
        and _is_vacuous_filter_syntactic(prefixed_sql)
    ):
        return False, f"filter is tautological: {prefixed_sql}", prefixed_sql

    resolved_table = _resolve_primary_table_fqn(
        table, catalog=catalog, gold_schema=gold_schema,
    )
    if snippet_type == "filter":
        wrapped = f"SELECT 1 FROM {resolved_table} WHERE {prefixed_sql} LIMIT 1"
    else:
        wrapped = f"SELECT {prefixed_sql} FROM {resolved_table} LIMIT 1"

    def _run_sql(statement: str) -> Any:
        if w and warehouse_id:
            from genie_space_optimizer.optimization.evaluation import (
                _execute_sql_via_warehouse,
            )
            return _execute_sql_via_warehouse(
                w, warehouse_id, statement,
                catalog=catalog, schema=gold_schema,
            )
        if spark is not None:
            _set_sql_context(spark, catalog, gold_schema)
            return spark.sql(statement)
        raise RuntimeError("No SQL execution backend available")

    def _first_row_values(result: Any) -> list[Any]:
        """Return the first row as a list regardless of backend shape."""
        if result is None:
            return []
        if hasattr(result, "collect"):
            rows = result.collect()
            if not rows:
                return []
            first = rows[0]
            if hasattr(first, "asDict"):
                return list(first.asDict().values())
            try:
                return list(first)
            except TypeError:
                return [first]
        if hasattr(result, "result") and hasattr(result.result, "data_array"):
            data = result.result.data_array or []
            return list(data[0]) if data else []
        if isinstance(result, list) and result:
            row = result[0]
            if isinstance(row, dict):
                return list(row.values())
            if isinstance(row, (list, tuple)):
                return list(row)
            return [row]
        return []

    try:
        _run_sql(wrapped)
    except Exception as exc:
        # Tier 3.13: classify CAST_INVALID_INPUT separately. This fires
        # when a filter-type snippet compares a STRING literal to a
        # non-STRING column (typically BIGINT booleans encoded as 'Y'/'N',
        # where the UC column is numeric). The error is NOT a parser
        # bug — it's a type-mismatch in the proposed filter itself, so
        # we return a cleaner reason code. Callers that see
        # ``cast_invalid_input`` know to reject the proposal without
        # a broad "Execution failed" blob in the log.
        _msg = str(exc)
        if "CAST_INVALID_INPUT" in _msg or "cannot be cast to" in _msg:
            return (
                False,
                (
                    f"cast_invalid_input: filter predicate compares values "
                    f"that Databricks SQL can't coerce. Check the column's "
                    f"UC-declared type vs the literal in the snippet. "
                    f"Detail: {_msg[:200]}"
                ),
                prefixed_sql,
            )
        try:
            from genie_space_optimizer.optimization.evaluation import (
                is_metric_view_error,
            )
            if is_metric_view_error(_msg):
                return False, _msg, prefixed_sql
        except Exception:
            pass
        return False, f"Execution failed: {exc}", prefixed_sql

    # S8 — post-execution selectivity probe. EXPLAIN + LIMIT 1 passed;
    # now verify the filter actually restricts the result set. Silently
    # skipped for empty tables (``total == 0``) because vacuity cannot
    # be proven on zero rows.
    if REJECT_VACUOUS_FILTERS and snippet_type == "filter":
        selectivity_stmt = (
            f"SELECT COUNT(*) AS total, "
            f"COUNT(*) FILTER (WHERE {prefixed_sql}) AS filtered "
            f"FROM {resolved_table}"
        )
        try:
            result = _run_sql(selectivity_stmt)
            values = _first_row_values(result)
        except Exception as exc:
            # Selectivity probe is a guard, not a requirement. If it
            # fails we fall back to the lenient pre-S8 behaviour.
            logger.debug(
                "Selectivity probe failed for %s: %s; accepting filter.",
                prefixed_sql, exc,
            )
            return True, "", prefixed_sql
        if len(values) >= 2:
            try:
                total_count = int(values[0])
                filtered_count = int(values[1])
            except (TypeError, ValueError):
                return True, "", prefixed_sql
            if total_count > 0 and filtered_count >= total_count:
                return (
                    False,
                    f"filter is vacuous: selects all rows "
                    f"({filtered_count}/{total_count})",
                    prefixed_sql,
                )
            # B5 — zero-rows guard. Strategists occasionally
            # hallucinate equality literals (e.g. ``open_status_code
            # = 'Y'`` when stored values are ``'O'`` / ``'C'``); the
            # filter parses, EXPLAINs cleanly, and selects zero rows
            # at runtime. Reject only when the filter is a simple
            # equality on a literal — range / inequality / IN-list
            # filters often legitimately match zero current rows
            # (e.g. ``created_at > 2030-01-01``).
            if (
                total_count > 0
                and filtered_count == 0
                and _is_simple_equality(prefixed_sql)
            ):
                return (
                    False,
                    f"filter matches zero rows ({filtered_count}/{total_count}); "
                    f"likely hallucinated literal — actual stored values may differ",
                    prefixed_sql,
                )

    return True, "", prefixed_sql
