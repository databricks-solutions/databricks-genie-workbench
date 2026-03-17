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
            return [_normalize_benchmark_row(r.asDict(recursive=True)) for r in rows]
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


def validate_ground_truth_sql(
    sql: str,
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
    *,
    execute: bool = False,
    parameters: list[dict] | None = None,
    w: Any = None,
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
            _execute_sql_via_warehouse(
                w, warehouse_id, f"EXPLAIN {resolved}",
                catalog=catalog, schema=gold_schema,
            )
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
            return False, (
                f"UNRESOLVED_COLUMN: `{col_name}` — "
                f"suggestion: {suggestion} "
                f"(hint: use MEASURE({col_name}) for metric view measures in ORDER BY)"
            )
        return False, err_msg

    table_refs = _extract_table_references(resolved)
    for ref, is_tvf in table_refs:
        exists, err = _verify_table_exists(
            spark, ref, is_tvf=is_tvf,
            w=w, warehouse_id=warehouse_id,
        )
        if not exists:
            return False, err

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
) -> list[dict]:
    """Validate each benchmark's ``expected_sql`` via EXPLAIN.

    Returns a list of validation result dicts:
    ``{question, expected_sql, valid, error}``.
    """
    results: list[dict] = []
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        question = b.get("question", "")
        is_valid, error = validate_ground_truth_sql(
            sql, spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
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

    results: list[dict] = []
    to_check: list[tuple[int, dict]] = []
    for i, b in enumerate(benchmarks):
        sql = b.get("expected_sql", "")
        if not sql or not sql.strip():
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
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

            from genie_space_optimizer.optimization.evaluation import (
                _link_prompt_to_trace,
                get_registered_prompt_name,
            )

            _link_prompt_to_trace(get_registered_prompt_name("benchmark_alignment_check"))

            w = WorkspaceClient()
            response = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
                temperature=0.0,
            )
            choices = response.choices or []
            raw = (choices[0].message.content or "").strip() if choices and choices[0].message else ""
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
# 3. Train/Held-Out Split
# ═══════════════════════════════════════════════════════════════════════


def assign_splits(
    benchmarks: list[dict],
    train_ratio: float = 1.0 - HELD_OUT_RATIO,
    seed: int = 42,
) -> list[dict]:
    """Assign ``split`` field (``train`` or ``held_out``) to each benchmark.

    Curated benchmarks (``provenance == "curated"``) are always assigned to
    ``train`` so the optimizer always sees user-authored questions.  Only
    non-curated benchmarks participate in the random held-out split.

    Uses deterministic shuffle based on *seed* for reproducibility.
    """
    curated_indices = {
        i for i, b in enumerate(benchmarks) if b.get("provenance") == "curated"
    }
    splittable = [i for i in range(len(benchmarks)) if i not in curated_indices]

    rng = random.Random(seed)
    rng.shuffle(splittable)
    cutoff = int(len(splittable) * train_ratio)

    for i in curated_indices:
        benchmarks[i]["split"] = "train"
    for rank, idx in enumerate(splittable):
        benchmarks[idx]["split"] = "train" if rank < cutoff else "held_out"

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
) -> dict:
    """Apply arbiter corrections to the MLflow evaluation dataset.

    Each correction dict should have:
    - ``question``: the benchmark question to correct
    - ``new_expected_sql``: the corrected SQL
    - ``verdict``: ``genie_correct`` or ``arbiter_repair``

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
