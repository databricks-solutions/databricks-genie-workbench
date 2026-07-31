"""Live benchmark preparation utilities for the four-task GSO workflow.

This module contains only the benchmark generation, validation, dataset, local
prompt-template, and score-normalization helpers used by the native v2 path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

import json
import logging
import re
import time

from difflib import get_close_matches

from typing import Any, Callable, Iterator, Union

import mlflow

import pandas as pd

from mlflow.entities import SpanType

from genie_space_optimizer.common.config import (
    BENCHMARK_CATEGORIES,
    BENCHMARK_CORRECTION_PROMPT,
    BENCHMARK_COVERAGE_GAP_PROMPT,
    BENCHMARK_GENERATION_PROMPT,
    COVERAGE_GAP_SOFT_CAP_FACTOR,
    DEFAULT_THRESHOLDS,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    MAX_BENCHMARK_COUNT,
    TARGET_BENCHMARK_COUNT,
    format_mlflow_template,
    scoring_v2_is_legacy,
)

from genie_space_optimizer.common.genie_client import (
    _extract_benchmark_sql_answer,
    detect_asset_type,
    resolve_sql,
    sanitize_sql,
)

logger = logging.getLogger(__name__)

_PROVENANCE_PRIORITY = [
    "curated", "curated_sql_generated", "reused", "synthetic",
    "auto_corrected", "coverage_gap_fill",
]

def _truncate_benchmarks(benchmarks: list[dict], max_count: int) -> list[dict]:
    """Truncate benchmarks to *max_count* using provenance-based priority.

    Curated benchmarks are kept first, then synthetic, auto_corrected,
    coverage_gap_fill, and finally any other provenance.  Within each
    tier the original order (which respects category diversity) is preserved.
    """
    if len(benchmarks) <= max_count:
        return benchmarks
    buckets: dict[str, list[dict]] = {p: [] for p in _PROVENANCE_PRIORITY}
    buckets["other"] = []
    for b in benchmarks:
        prov = b.get("provenance", "other")
        buckets.get(prov, buckets["other"]).append(b)
    result: list[dict] = []
    for p in _PROVENANCE_PRIORITY + ["other"]:
        for b in buckets[p]:
            if len(result) >= max_count:
                break
            result.append(b)
    logger.warning("Truncated benchmarks from %d to %d", len(benchmarks), len(result))
    return result

_TEMPORAL_QUESTION_RE = re.compile(
    r"\b(this year|last \d+ months?|last \d+ days?|current year"
    r"|year-to-date|ytd|this month|this quarter|past \d+ months?)\b",
    re.IGNORECASE,
)

def _flag_stale_temporal_benchmarks(
    benchmarks: list[dict],
    spark: "SparkSession",
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Flag benchmarks whose GT SQL returns 0 rows due to stale temporal filters.

    Sets ``temporal_stale=True`` on benchmarks where the question contains
    temporal patterns and the GT SQL returns 0 rows.  Flagged benchmarks are
    excluded from accuracy scoring in ``_compute_arbiter_adjusted_accuracy``.

    When *w* and *warehouse_id* are provided, routes the check through the
    SQL warehouse; otherwise uses Spark SQL.
    """
    from genie_space_optimizer.optimization.benchmarks import _quiet_grpc_logs

    flagged_count = 0
    for b in benchmarks:
        q = b.get("question", "")
        sql = b.get("expected_sql", "")
        if not _TEMPORAL_QUESTION_RE.search(q):
            continue
        if not sql:
            continue
        try:
            with _quiet_grpc_logs():
                if w and warehouse_id:
                    result_df = _execute_sql_via_warehouse(
                        w, warehouse_id, f"SELECT * FROM ({sql}) LIMIT 1",
                    )
                    if result_df.empty:
                        b["temporal_stale"] = True
                        flagged_count += 1
                        logger.info(
                            "Temporal benchmark '%s' returns 0 rows -- flagged as stale",
                            q[:60],
                        )
                else:
                    df = spark.sql(sql).limit(1)
                    if df.count() == 0:
                        b["temporal_stale"] = True
                        flagged_count += 1
                        logger.info(
                            "Temporal benchmark '%s' returns 0 rows -- flagged as stale",
                            q[:60],
                        )
        except Exception:
            pass
    if flagged_count:
        logger.warning(
            "Flagged %d/%d benchmarks as temporal-stale (excluded from accuracy)",
            flagged_count,
            len(benchmarks),
        )
    return benchmarks

_FENCED_BLOCK_RE = re.compile(
    r"```(?:json|JSON)?\s*\n?(?P<body>.*?)```", re.DOTALL,
)

def _extract_balanced_json_value(text: str) -> dict | list | None:
    """Return the longest balanced JSON object/array embedded in ``text``."""
    candidates: list[tuple[int, dict | list]] = []
    pairs = {"{": "}", "[": "]"}
    for start, ch in enumerate(text):
        if ch not in pairs:
            continue
        stack: list[str] = []
        in_string = False
        escaped = False
        for idx in range(start, len(text)):
            cur = text[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif cur == "\\":
                    escaped = True
                elif cur == '"':
                    in_string = False
                continue
            if cur == '"':
                in_string = True
            elif cur in pairs:
                stack.append(pairs[cur])
            elif stack and cur == stack[-1]:
                stack.pop()
                if not stack:
                    candidate = text[start: idx + 1]
                    try:
                        value = json.loads(candidate)
                    except json.JSONDecodeError:
                        break
                    if isinstance(value, (dict, list)):
                        candidates.append((len(candidate), value))
                    break
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]

def _extract_json(content: str | None, *, strict: bool = False) -> dict | list | None:
    """Extract a JSON value from LLM response text with lenient wrapping.

    Returns ``None`` for empty / whitespace-only / fenced-but-empty /
    non-JSON content so callers can treat "no parseable response" as a
    typed soft failure. Pass ``strict=True`` to preserve the legacy
    raise-on-error behaviour for code paths that need a hard failure
    (e.g. ``_traced_llm_call`` ``response_validator``).
    """
    if content is None:
        if strict:
            raise ValueError("No content to parse as JSON")
        return None
    text = content.strip()
    if not text:
        if strict:
            raise ValueError("Empty content cannot be parsed as JSON")
        return None

    # Fenced block anywhere in the string — prefer it over the surrounding
    # prose so a preamble like "Here is the JSON:\n```json\n{...}\n```" works.
    fence_match = _FENCED_BLOCK_RE.search(text)
    if fence_match:
        fenced = fence_match.group("body").strip()
        if fenced:
            try:
                return json.loads(fenced)
            except json.JSONDecodeError:
                recovered = _extract_balanced_json_value(fenced)
                if recovered is not None:
                    return recovered
                # Fall through — the fenced block might itself be malformed
                # but the surrounding text could still contain valid JSON.
                pass
        else:
            # Fenced block with no body. Treat the same as empty content.
            if strict:
                raise ValueError("Empty fenced block cannot be parsed as JSON")
            return None

    _saved_err: json.JSONDecodeError | None = None

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        _saved_err = exc

    if (
        _saved_err is not None
        and hasattr(_saved_err, "pos")
        and _saved_err.msg.startswith("Extra data")
    ):
        try:
            return json.loads(text[: _saved_err.pos])
        except json.JSONDecodeError:
            pass

    recovered = _extract_balanced_json_value(text)
    if recovered is not None:
        return recovered

    # Regex fallbacks — try the first balanced `{...}` and `[...]`; take the
    # one that parses. We prefer whichever is longer so a nested structure
    # wins over a short sub-literal.
    candidates: list[tuple[int, str]] = []
    obj_match = re.search(r"\{.*\}", text, re.DOTALL)
    if obj_match:
        candidates.append((len(obj_match.group(0)), obj_match.group(0)))
    arr_match = re.search(r"\[.*\]", text, re.DOTALL)
    if arr_match:
        candidates.append((len(arr_match.group(0)), arr_match.group(0)))
    # Longest-first maximises the chance of getting the outermost structure.
    for _, candidate in sorted(candidates, key=lambda c: -c[0]):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    if strict:
        assert _saved_err is not None  # pragma: no cover — invariant
        raise _saved_err
    logger.debug(
        "_extract_json could not parse content; returning None. "
        "first_120_chars=%r error=%s",
        text[:120],
        _saved_err,
    )
    return None

def _call_llm_for_scoring(
    w: "WorkspaceClient",
    prompt: str,
    max_retries: int = LLM_MAX_RETRIES,
) -> dict:
    """Call LLM via the OpenAI SDK with retry + exponential backoff.

    Uses the shared ``llm_client`` so that ``mlflow.openai.autolog()``
    captures token usage, cost, and latency automatically. Prompt content comes
    directly from the version-controlled local templates in ``common.config``.
    """
    from genie_space_optimizer.optimization.llm_client import call_llm

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            text, _response = call_llm(
                w,
                messages=[{"role": "user", "content": prompt}],
                max_retries=1,
                temperature=LLM_TEMPERATURE,
            )
            return _extract_json(text)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
    raise last_err

_MEASURE_ALIAS_COLLISION_PATTERN = re.compile(
    r"MEASURE\s*\(\s*(?:`(\w+)`|([A-Za-z_]\w*))\s*\)"
    r"\s+AS\s+(?:`(\w+)`|([A-Za-z_]\w*))",
    re.IGNORECASE,
)

def _alias_collision_match_groups(m: re.Match) -> tuple[str, str]:
    """Return ``(measure_col, alias)`` for a collision-pattern match.

    Each side is matched in two alternatives — backtick-quoted (group
    1 / 3) or bare (group 2 / 4). Exactly one of each pair will be
    non-empty.
    """
    col = m.group(1) or m.group(2) or ""
    alias = m.group(3) or m.group(4) or ""
    return col, alias

def _measure_alias_collision_rename_map(sql: str) -> dict[str, str]:
    """Return lower-case measure name -> safe alias for MEASURE(m) AS m.

    Task 6 helper: lets ``apply_pre_execute_repairs`` know which
    measures the alias-collision repair will rename so it can rewrite
    ``ORDER BY MEASURE(<original_measure>)`` to the renamed alias.
    """
    rename_map: dict[str, str] = {}
    for m in _MEASURE_ALIAS_COLLISION_PATTERN.finditer(sql or ""):
        col, alias = _alias_collision_match_groups(m)
        if not col or not alias:
            continue
        if col.lower() == alias.lower() and col.lower() not in rename_map:
            rename_map[col.lower()] = f"{col}_value"
    return rename_map

def _repair_measure_alias_collisions(sql: str) -> tuple[str, int]:
    """Rewrite ``MEASURE(col) AS col`` to ``MEASURE(col) AS col_value``.

    PR 15 — when a SELECT projects ``MEASURE(col) AS col`` against a
    metric view, Spark's resolver shadows the underlying measure column
    with the alias. Subsequent references in ORDER BY / HAVING that use
    ``MEASURE(col)`` then resolve to the alias output (a regular
    aggregate expression) and fail the planner with::

        [MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION]
        Resolved attribute(s) "col" missing from "..., col, ..." in
        operator !Aggregate ... measure(col#alias_id) AS measure(col)

    The deterministic fix is to rename the alias so it no longer matches
    the underlying column name. We append ``_value`` because it
    survives downstream prompt-matching and is unambiguous in
    user-facing queries. Bare references to the original alias in
    ORDER BY / HAVING / GROUP BY are remapped to the new alias so
    semantically-equivalent queries continue to return the same
    rows in the same order.

    Returns ``(new_sql, num_collisions_fixed)``. The counter feeds the
    unified-pipeline yield diagnostics (PR 18) so operators can see how
    often this repair fires.
    """
    if not sql or "MEASURE" not in sql.upper():
        return sql, 0

    # First pass: identify collisions, build alias-rename map.
    rename_map = _measure_alias_collision_rename_map(sql)
    if not rename_map:
        return sql, 0

    # Second pass: replace each collision-style alias with the safe one.
    def _replace(m: re.Match) -> str:
        col, alias = _alias_collision_match_groups(m)
        if not col or not alias or col.lower() != alias.lower():
            return m.group(0)
        new_alias = rename_map[col.lower()]
        # Preserve backticks on the rendered output when the original
        # column was backtick-quoted (Databricks measure names that
        # start with a digit must stay backticked).
        col_quoted = f"`{col}`" if not col[:1].isalpha() and col[:1] != "_" else col
        alias_quoted = f"`{new_alias}`" if not new_alias[:1].isalpha() and new_alias[:1] != "_" else new_alias
        return f"MEASURE({col_quoted}) AS {alias_quoted}"

    new_sql = _MEASURE_ALIAS_COLLISION_PATTERN.sub(_replace, sql)

    # Third pass: rewrite bare alias references in ORDER BY / HAVING /
    # GROUP BY clauses (the only places where SELECT aliases are
    # legally usable outside the projection list). We match the old
    # alias as a whole identifier and skip occurrences inside
    # ``MEASURE(...)`` (those still resolve to the underlying column).
    # Conservative: only rewrite within ORDER BY / HAVING tail to avoid
    # touching anything in the FROM / WHERE clauses.
    def _rewrite_clause(text: str) -> str:
        for old_col, new_alias in rename_map.items():
            # Match `old_col` as a whole identifier not inside MEASURE(.
            # The lookbehind on ``MEASURE\s*\(\s*`?`` is variable-width
            # so we approximate with a 12-char window check.
            pattern = re.compile(rf"\b{re.escape(old_col)}\b", re.IGNORECASE)

            def _sub(match: re.Match) -> str:
                start = match.start()
                window_start = max(0, start - 12)
                prefix = text[window_start:start]
                if re.search(r"MEASURE\s*\(\s*`?$", prefix, re.IGNORECASE):
                    return match.group(0)
                return new_alias

            text = pattern.sub(_sub, text)
        return text

    # Locate ORDER BY / HAVING / GROUP BY tails. Rewriting the whole
    # tail captures all three clauses regardless of order.
    tail_anchor = re.search(r"\b(ORDER\s+BY|HAVING|GROUP\s+BY)\b", new_sql, re.IGNORECASE)
    if tail_anchor:
        head = new_sql[: tail_anchor.start()]
        tail = new_sql[tail_anchor.start() :]
        new_sql = head + _rewrite_clause(tail)

    return new_sql, len(rename_map)

def _repair_measure_in_where(
    sql: str,
    mv_measures: dict[str, set[str]],
) -> tuple[str, int]:
    """Rewrite ``WHERE <measure_col> …`` into a CTE-first pattern (PR 20).

    Spark's metric-view planner rejects measure column references inside
    ``WHERE`` / ``HAVING`` / ``ON`` clauses with the same
    ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` error class as a bare
    measure in SELECT. The canonical fix per the
    `Databricks Metric Views docs
    <https://docs.databricks.com/aws/en/business-semantics/metric-views/query>`_
    is to materialize the measure in a CTE alias and filter on the
    alias::

        -- BAD: WHERE references a measure column directly
        SELECT zone, MEASURE(total_sales) AS sales
        FROM mv_x
        WHERE store_day_count > 0
        GROUP BY zone;

        -- GOOD: CTE-first; filter on the materialized alias
        WITH __mv_base AS (
          SELECT zone,
                 MEASURE(total_sales) AS sales,
                 MEASURE(store_day_count) AS store_day_count_value
          FROM mv_x
          GROUP BY zone
        )
        SELECT zone, sales
        FROM __mv_base
        WHERE store_day_count_value > 0;

    The function detects measure column references in the WHERE clause
    using ``mv_measures`` (keyed by short MV name → measure names),
    promotes each referenced measure into the inner SELECT as
    ``MEASURE(m) AS m_value``, and rewrites the WHERE clause to use the
    materialized alias. The outer SELECT replays the original
    projections (by output-name) so callers (LLM correction, example
    SQL gates) see the same shape they would have without the repair.

    Returns ``(new_sql, num_measures_lifted)``. Conservative: returns
    ``(sql, 0)`` unchanged when sqlglot is unavailable, parsing fails,
    the root expression is not a single ``SELECT``, the query already
    has a ``WITH`` clause / outer ``JOIN`` / FROM-side subquery / set-op
    (``UNION``/``EXCEPT``/``INTERSECT``), or no relevant measure column
    appears in the WHERE clause. False negatives only — by design we
    prefer leaving the SQL alone over emitting a wrong rewrite.
    """
    if not sql or not mv_measures or "WHERE" not in sql.upper():
        return sql, 0

    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return sql, 0

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return sql, 0

    if not isinstance(tree, exp.Select):
        # Set-ops (Union/Except/Intersect) and DDL parse to non-Select
        # roots; we don't try to rewrite them.
        return sql, 0

    # Conservative bail-outs: leave anything we don't fully understand
    # alone so the LLM can fix it on retry. sqlglot stores these args
    # under trailing-underscore keys (``with_``, ``from_``) but also
    # exposes ``with``/``from`` aliases on some versions; accept either.
    if tree.args.get("with") or tree.args.get("with_"):
        return sql, 0
    if tree.args.get("joins"):
        return sql, 0

    from_ = tree.args.get("from") or tree.args.get("from_")
    if from_ is not None:
        sources: list[Any] = []
        if hasattr(from_, "expressions") and from_.expressions:
            sources.extend(from_.expressions)
        elif from_.this is not None:
            sources.append(from_.this)
        for src in sources:
            if isinstance(src, exp.Subquery):
                return sql, 0

    where_clause = tree.args.get("where")
    if where_clause is None:
        return sql, 0

    # Subqueries in WHERE (e.g. ``WHERE x IN (SELECT …)``) are out of
    # scope — refusing keeps the rewrite deterministic.
    if any(True for _ in where_clause.find_all(exp.Subquery)):
        return sql, 0

    # Resolve which measures are reachable from the FROM clause of THIS
    # query. Reuses the same alias-aware helper as ``_rewrite_measure_refs``
    # so both repairs see the same set of measure names.
    relevant_measures = _build_relevant_measures(sql, mv_measures)
    if not relevant_measures:
        return sql, 0
    all_measure_names: set[str] = set()
    for s in relevant_measures.values():
        all_measure_names.update(s)
    if not all_measure_names:
        return sql, 0

    # Collect measure-column references inside WHERE. We match on the
    # column's bare name (case-insensitive) — Spark's resolver does the
    # same when matching a measure column on a metric view.
    measures_in_where: list[str] = []
    seen: set[str] = set()
    for col in where_clause.find_all(exp.Column):
        nm = (col.name or "").lower()
        if nm in all_measure_names and nm not in seen:
            seen.add(nm)
            measures_in_where.append(nm)
    if not measures_in_where:
        return sql, 0

    # Build the inner SELECT: original tree minus its WHERE clause, with
    # ``MEASURE(m) AS m_value`` projections appended for each measure
    # that needs to be available to the outer filter.
    inner = tree.copy()
    inner.set("where", None)

    existing_aliases: set[str] = set()
    for proj in inner.expressions:
        if isinstance(proj, exp.Alias):
            alias_id = proj.args.get("alias")
            if alias_id is not None:
                existing_aliases.add(str(alias_id.name or "").lower())

    alias_map: dict[str, str] = {}
    for m in measures_in_where:
        alias_name = f"{m}_value"
        # Avoid colliding with any pre-existing alias on the inner.
        suffix = 2
        while alias_name.lower() in existing_aliases:
            alias_name = f"{m}_value{suffix}"
            suffix += 1
        alias_map[m] = alias_name
        existing_aliases.add(alias_name.lower())
        new_proj = exp.Alias(
            this=exp.Anonymous(
                this="MEASURE",
                expressions=[exp.column(m)],
            ),
            alias=exp.to_identifier(alias_name),
        )
        inner.expressions.append(new_proj)

    # Rewrite the WHERE clause: rebind each measure-column reference to
    # the materialized alias on the CTE.
    new_where = where_clause.copy()
    for col in new_where.find_all(exp.Column):
        nm = (col.name or "").lower()
        if nm in alias_map:
            col.set("this", exp.to_identifier(alias_map[nm]))
            # Drop any table qualifier — the column now lives on the
            # CTE, not the original metric view.
            col.set("table", None)
            col.set("db", None)
            col.set("catalog", None)

    # Outer projection list: replay the original SELECT's output names
    # so callers see the same shape pre- vs post-repair. Anything we
    # can't unambiguously name (e.g. a non-aliased complex expression)
    # forces a ``SELECT *`` fallback.
    outer_projs: list[exp.Expression] = []
    fallback_to_star = False
    for p in tree.expressions:
        out_name: str | None = None
        if isinstance(p, exp.Alias):
            alias_id = p.args.get("alias")
            if alias_id is not None and alias_id.name:
                out_name = str(alias_id.name)
        elif isinstance(p, exp.Column):
            out_name = p.name
        if out_name:
            outer_projs.append(
                exp.Column(this=exp.to_identifier(out_name)),
            )
        else:
            fallback_to_star = True
            break
    if fallback_to_star or not outer_projs:
        outer_projs = [exp.Star()]

    # Assemble the wrapper. ``Select.with_`` is the only sqlglot API
    # that wires ``WITH … AS (…) SELECT …`` such that the WITH renders
    # when the tree is serialised; setting ``args["with"]`` directly
    # silently drops the CTE on render.
    outer = exp.Select(expressions=outer_projs)
    outer.set(
        "from",
        exp.From(this=exp.Table(this=exp.to_identifier("__mv_base"))),
    )
    outer.set("where", new_where)
    outer = outer.with_("__mv_base", inner, copy=False)

    try:
        return outer.sql(dialect="databricks"), len(measures_in_where)
    except Exception:
        return sql, 0

def _check_metric_view_join_pre(
    sql: str,
    mv_set: set[str],
) -> str | None:
    """Reject SQL that JOINs directly against a metric view (PR 26).

    Returns ``"metric_view_join"`` when the SQL contains a ``JOIN``
    whose left or right operand is a known metric view (resolved by
    short-name match against ``mv_set``) and the SQL does NOT already
    use a ``WITH`` clause to materialize the MV. Returns ``None``
    otherwise (no JOIN, no MV in the JOIN, or the MV is wrapped in a
    CTE).

    The ``mv_set`` should contain short basenames (lowercased), e.g.
    ``{"mv_sales", "mv_returns"}``. ``cat.sch.mv_sales`` references in
    the SQL match because the resolver compares the basename
    (``mv_sales``).

    Conservative: returns ``None`` when sqlglot is unavailable, parsing
    fails, the SQL already contains a top-level ``WITH`` (the LLM
    likely emitted the CTE-first pattern already), or there is no
    ``JOIN`` at all.
    """
    if not sql or not mv_set:
        return None
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return None

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return None

    if not isinstance(tree, exp.Select):
        return None
    if tree.args.get("with") or tree.args.get("with_"):
        # CTE present — assume the LLM already emitted the documented
        # CTE-first pattern. False negatives here only surface as the
        # generic ``metric_view_join`` Spark error downstream, which the
        # correction loop can still fix.
        return None
    joins = tree.args.get("joins") or []
    if not joins:
        return None

    mv_lower = {m.lower() for m in mv_set if m}

    def _table_basename(t: exp.Table) -> str:
        nm = (t.name or "").strip("`").lower()
        return nm

    # Collect every operand on the FROM + JOIN side of the query.
    from_ = tree.args.get("from") or tree.args.get("from_")
    operand_tables: list[exp.Table] = []
    if from_ is not None:
        for src in getattr(from_, "expressions", None) or [from_.this]:
            if isinstance(src, exp.Table):
                operand_tables.append(src)
    for j in joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table):
            operand_tables.append(right)

    for t in operand_tables:
        if _table_basename(t) in mv_lower:
            return "metric_view_join"
    return None

def _repair_metric_view_join(
    sql: str,
    mv_set: set[str],
    mv_measures: dict[str, set[str]] | None = None,
) -> tuple[str, int]:
    """Wrap each metric view referenced in a JOIN with a CTE (PR 26).

    For each MV referenced in the FROM or any JOIN clause, builds a
    ``WITH __mv_<n> AS (SELECT <referenced_dims>, MEASURE(<m>) AS <m>,
    … FROM <mv>)`` CTE and rewrites the original Table node to
    reference the CTE alias. Outer-query references to the MV alias's
    columns continue to work because the CTE projects the same column
    names; outer ``MEASURE(alias.measure)`` calls are flattened to
    ``alias.measure`` (the CTE has already materialized the measure).

    Returns ``(new_sql, num_mvs_wrapped)``. Conservative — returns
    ``(sql, 0)`` unchanged when sqlglot is unavailable, parsing fails,
    the SQL already has a ``WITH`` clause, no MV appears in the
    query, the rewrite would be ambiguous (e.g. unqualified column
    references that could resolve to either side of the JOIN), or any
    transform raises.

    The repair is best-effort: when it cannot produce a clean rewrite
    it returns ``(sql, 0)`` so the caller (synthesis pre-check) records
    the candidate as ``metric_view_join`` rejected and lets the
    correction loop / LLM hint do the work on the next round.
    """
    if not sql or not mv_set:
        return sql, 0
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:
        return sql, 0

    try:
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return sql, 0

    if not isinstance(tree, exp.Select):
        return sql, 0
    if tree.args.get("with") or tree.args.get("with_"):
        return sql, 0
    joins = tree.args.get("joins") or []
    if not joins:
        return sql, 0

    mv_lower = {m.lower() for m in mv_set if m}
    measures_by_mv = {k.lower(): set(v) for k, v in (mv_measures or {}).items()}

    def _table_basename(t: exp.Table) -> str:
        return (t.name or "").strip("`").lower()

    # Identify each MV reference (FROM and every JOIN side). Track the
    # alias the LLM used so we can rebind outer-query column refs.
    from_ = tree.args.get("from") or tree.args.get("from_")
    mv_refs: list[tuple[exp.Table, str]] = []  # (table_node, alias)

    def _alias_of(t: exp.Table) -> str:
        a = t.args.get("alias")
        if isinstance(a, exp.TableAlias) and a.name:
            return str(a.name)
        return _table_basename(t)

    if from_ is not None:
        for src in getattr(from_, "expressions", None) or [from_.this]:
            if isinstance(src, exp.Table) and _table_basename(src) in mv_lower:
                mv_refs.append((src, _alias_of(src)))
    for j in joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table) and _table_basename(right) in mv_lower:
            mv_refs.append((right, _alias_of(right)))

    if not mv_refs:
        return sql, 0

    # Build a CTE for each MV ref and rewrite the Table node in place.
    # The CTE projects every column referenced from the MV alias in
    # the rest of the SQL plus the MV's known measures (when we have
    # them) wrapped in MEASURE(). Unknown column shapes (no qualifier,
    # ambiguous resolution) cause us to bail.
    new_tree = tree.copy()
    # Re-bind operand_tables on the COPY since we just deep-copied.
    new_from = new_tree.args.get("from") or new_tree.args.get("from_")
    new_joins = new_tree.args.get("joins") or []

    # Record (table_node, original_user_alias, cte_alias, mv_basename)
    # tuples — the basename is captured BEFORE the rewrite swaps the
    # table's identifier to the CTE alias, otherwise the outer-query
    # MEASURE() flatten step below can't find which MV a given alias
    # belongs to.
    new_mv_refs: list[tuple[exp.Table, str, str, str]] = []
    cte_idx = 0
    if new_from is not None:
        for src in getattr(new_from, "expressions", None) or [new_from.this]:
            if isinstance(src, exp.Table) and _table_basename(src) in mv_lower:
                cte_idx += 1
                cte_alias = f"__mv_{cte_idx}"
                new_mv_refs.append(
                    (src, _alias_of(src), cte_alias, _table_basename(src)),
                )
    for j in new_joins:
        right = j.this if isinstance(j, exp.Join) else None
        if isinstance(right, exp.Table) and _table_basename(right) in mv_lower:
            cte_idx += 1
            cte_alias = f"__mv_{cte_idx}"
            new_mv_refs.append(
                (right, _alias_of(right), cte_alias, _table_basename(right)),
            )

    # Collect referenced columns per MV alias from the entire tree.
    cols_per_alias: dict[str, set[str]] = {
        a.lower(): set() for _, a, _, _ in new_mv_refs
    }
    for col in new_tree.find_all(exp.Column):
        tbl = (col.table or "").strip("`").lower()
        if tbl and tbl in cols_per_alias:
            cols_per_alias[tbl].add((col.name or "").lower())

    cte_definitions: list[tuple[str, exp.Select]] = []
    alias_to_basename: dict[str, str] = {
        a.lower(): basename for _, a, _, basename in new_mv_refs
    }
    for table_node, original_alias, cte_alias, basename in new_mv_refs:
        measures_for_mv = measures_by_mv.get(basename, set())
        referenced_cols = cols_per_alias.get(original_alias.lower(), set())
        if not referenced_cols and not measures_for_mv:
            # Nothing tangible to project; bail conservatively.
            return sql, 0

        # Partition referenced columns into dims vs measures.
        dim_cols: list[str] = []
        measure_cols_used: set[str] = set()
        for c in sorted(referenced_cols):
            if c in measures_for_mv:
                measure_cols_used.add(c)
            else:
                dim_cols.append(c)
        # Always include any known measure that wasn't directly
        # referenced — keeping the CTE's projection a superset of the
        # outer query's needs is safer than under-projecting.
        for m in sorted(measures_for_mv):
            measure_cols_used.add(m)

        cte_projections: list[exp.Expression] = []
        for d in dim_cols:
            cte_projections.append(exp.column(d))
        for m in sorted(measure_cols_used):
            cte_projections.append(
                exp.Alias(
                    this=exp.Anonymous(
                        this="MEASURE",
                        expressions=[exp.column(m)],
                    ),
                    alias=exp.to_identifier(m),
                ),
            )
        if not cte_projections:
            return sql, 0

        # FROM the original MV (preserve full qualification by copying
        # the original table node, sans alias). ``exp.select(...).
        # from_(table)`` is the only sqlglot API that wires the FROM
        # clause such that it renders; ``Select.set('from', From(...))``
        # silently drops the FROM on serialization for newly-built
        # SELECT trees.
        from_table = exp.Table(
            this=exp.to_identifier(table_node.name),
            db=table_node.args.get("db"),
            catalog=table_node.args.get("catalog"),
        )
        cte_select = exp.select(*cte_projections).from_(from_table)
        cte_definitions.append((cte_alias, cte_select))

        # Rewrite the original Table node to reference the CTE alias.
        table_node.set("this", exp.to_identifier(cte_alias))
        table_node.set("db", None)
        table_node.set("catalog", None)
        # Re-pin the alias so outer-query qualified references
        # (``alias.col``) keep resolving — alias text is unchanged.
        if not table_node.args.get("alias"):
            table_node.set(
                "alias",
                exp.TableAlias(this=exp.to_identifier(original_alias)),
            )

    # Outer query: flatten ``MEASURE(alias.measure)`` to
    # ``alias.measure`` because the CTE already materialized the
    # measure under the same column name.
    for anon in list(new_tree.find_all(exp.Anonymous)):
        if (anon.this or "").upper() != "MEASURE":
            continue
        args = anon.expressions or []
        if len(args) != 1 or not isinstance(args[0], exp.Column):
            continue
        col = args[0]
        tbl = (col.table or "").strip("`").lower()
        nm = (col.name or "").lower()
        basename_for_alias = alias_to_basename.get(tbl)
        if (
            basename_for_alias
            and nm in measures_by_mv.get(basename_for_alias, set())
        ):
            anon.replace(col.copy())

    # Attach each CTE in order. ``Select.with_`` chains them so the
    # final serialized SQL has ``WITH __mv_1 AS (…), __mv_2 AS (…)
    # SELECT …``.
    out_tree = new_tree
    for alias, sel in cte_definitions:
        out_tree = out_tree.with_(alias, sel, copy=False)

    try:
        return out_tree.sql(dialect="databricks"), len(cte_definitions)
    except Exception:
        return sql, 0

_NOT_AN_ALIAS = frozenset({
    "on", "using", "where", "group", "order", "having", "limit",
    "union", "intersect", "except", "join", "inner", "left", "right",
    "full", "cross", "outer", "natural", "lateral",
    "as",  # bare AS (no alias word) shouldn't happen but be safe
})

def _build_relevant_measures(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Return ``{alias_or_short: {measure_col, …}}`` for every FROM/JOIN
    table that maps to an entry in *metric_view_measures*.

    Alias-aware: registers BOTH the short table name and any explicit
    alias (``FROM mv AS x`` / ``FROM mv x`` / ``JOIN mv x ON …``) so the
    rewriter can recognise ``mv.col`` *and* ``x.col``.
    """
    out: dict[str, set[str]] = {}
    # Negative lookahead on the alias group prevents the pattern from
    # consuming the next clause keyword (``ON`` / ``JOIN`` / ``WHERE`` /
    # …) as an alias when no alias is present. Without it
    # ``FROM mv1 JOIN mv2`` collapses to a single match and the second
    # MV is silently dropped.
    not_an_alias_alts = "|".join(
        sorted(_NOT_AN_ALIAS, key=len, reverse=True),
    )
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+`?([\w.]+)`?"
        rf"(?:\s+(?:AS\s+)?`?(?!(?:{not_an_alias_alts})\b)([A-Za-z_]\w*)`?)?",
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        ident = (m.group(1) or "").replace("`", "").strip()
        if not ident:
            continue
        short = ident.split(".")[-1].lower()
        alias = (m.group(2) or "").strip()
        if alias.lower() in _NOT_AN_ALIAS:
            alias = ""
        measures = metric_view_measures.get(short, set())
        if not measures:
            continue
        out.setdefault(short, set()).update(measures)
        if alias:
            out.setdefault(alias.lower(), set()).update(measures)
    return out

def _rewrite_measure_refs(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> str:
    """Wrap bare metric-view measure references with ``MEASURE()``.

    Covers SELECT, HAVING, and ORDER BY clauses. Skips WHERE and ON
    clauses (Spark forbids ``MEASURE()`` there; the diagnostic the user
    sees on a violation is clearer than a silently-wrapped reference).

    Alias-aware: handles both unqualified bare references
    (``SELECT gross_sales FROM mv_x``) and qualified references
    (``SELECT x.gross_sales FROM mv_x x``). The latter mode lets the
    rewriter cover spaces where the LLM emits an alias even when it
    technically isn't required, which the original short-name-only
    parser missed entirely.

    ``metric_view_measures`` maps lowercased short table names to sets
    of lowercased measure column names.
    """
    if not metric_view_measures or not sql:
        return sql

    relevant_measures = _build_relevant_measures(sql, metric_view_measures)
    if not relevant_measures:
        return sql

    all_measure_names: set[str] = set()
    for s in relevant_measures.values():
        all_measure_names |= s

    already_measured = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)

    # Single combined pattern: optional ``alias.`` prefix + column. The
    # negative lookbehind on ``[\w.]`` prevents matching the middle
    # component of a 3-part identifier (``catalog.schema.table``); the
    # negative lookahead on ``\s*\(`` prevents wrapping function calls.
    measure_token = re.compile(
        r"(?<![\w.])([A-Za-z_]\w*\.)?([A-Za-z_]\w*)\b(?!\s*\()",
    )

    def _rewrite_clause(text: str) -> str:
        def _repl(m: re.Match) -> str:
            full = m.group(0)
            alias_dot = m.group(1) or ""
            col = m.group(2)
            start = m.start()
            window_start = max(0, start - 12)
            if already_measured.search(text[window_start:start]):
                return full
            col_lower = col.lower()
            if alias_dot:
                alias = alias_dot[:-1].lower()
                measures = relevant_measures.get(alias)
                if measures and col_lower in measures:
                    return f"MEASURE({full})"
                return full
            if col_lower in all_measure_names:
                return f"MEASURE({col})"
            return full

        return measure_token.sub(_repl, text)

    def _next_clause_offset(haystack: str) -> int:
        """Return offset (relative to ``haystack`` start) of the next
        clause-boundary keyword, or ``len(haystack)`` when no boundary
        is present.
        """
        m = re.search(
            r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT)\b",
            haystack,
            re.IGNORECASE,
        )
        return m.start() if m else len(haystack)

    # SELECT clause — between SELECT and FROM. Constrained to the head of
    # the statement; nested subqueries are not handled (the existing
    # implementation didn't either).
    select_match = re.search(r"\bSELECT\b", sql, re.IGNORECASE)
    from_match = re.search(r"\bFROM\b", sql, re.IGNORECASE)
    if select_match and from_match and select_match.end() < from_match.start():
        head = sql[: select_match.end()]
        clause = sql[select_match.end() : from_match.start()]
        tail = sql[from_match.start() :]
        sql = head + _rewrite_clause(clause) + tail

    # HAVING clause — between HAVING and the next clause boundary.
    having_match = re.search(r"\bHAVING\b", sql, re.IGNORECASE)
    if having_match:
        offset = _next_clause_offset(sql[having_match.end():])
        having_end = having_match.end() + offset
        head = sql[: having_match.end()]
        clause = sql[having_match.end() : having_end]
        tail = sql[having_end:]
        sql = head + _rewrite_clause(clause) + tail

    # ORDER BY clause — between ORDER BY and the next boundary
    # (LIMIT / set-op / end of statement).
    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if order_match:
        offset = _next_clause_offset(sql[order_match.end():])
        order_end = order_match.end() + offset
        head = sql[: order_match.end()]
        clause = sql[order_match.end() : order_end]
        tail = sql[order_end:]
        sql = head + _rewrite_clause(clause) + tail

    return sql

_OUTER_AGG_AROUND_MEASURE_RE = re.compile(
    r"\b(SUM|AVG|COUNT|MIN|MAX|MEDIAN|STDDEV|STDDEV_POP|STDDEV_SAMP|"
    r"VAR|VAR_POP|VAR_SAMP|VARIANCE|ANY_VALUE)\s*\(\s*"
    r"(MEASURE\s*\([^()]*\))\s*\)",
    re.IGNORECASE,
)

def _strip_outer_agg_around_measure(sql: str) -> tuple[str, int]:
    """Strip a redundant aggregate that wraps a single ``MEASURE(x)`` arg.

    The LLM occasionally emits ``SUM(MEASURE(gross_sales))`` even though
    metric-view measure references must NOT be re-aggregated by the user
    — Spark expands ``MEASURE(gross_sales)`` to ``SUM(gross_sales)``
    internally, which yields ``SUM(MEASURE(SUM(gross_sales)))`` and a
    ``NESTED_AGGREGATE_FUNCTION`` rejection. Stripping the outer
    aggregate is the deterministic fix.

    Behaviour:
      - When the aggregate's *only* argument is a ``MEASURE(...)`` call
        (case-insensitive), the aggregate node is replaced with the
        inner ``MEASURE(...)`` call.
      - Non-aggregate wrappers like ``COALESCE(MEASURE(x), 0)`` are left
        alone — only true aggregates on the allowed list are stripped.
      - Multi-arg aggregates such as ``COUNT(MEASURE(x), 1)`` are left
        alone (extremely rare, but the regex requires a single arg).
      - Falls back to a regex-only path when sqlglot fails to parse the
        SQL (best-effort; the regex is intentionally conservative).

    Returns ``(new_sql, count)`` where ``count`` is the number of
    aggregate-strip rewrites applied. Used by the proposal-side and the
    correction pipelines so both fix the same LLM mode identically.
    """
    if not sql or "MEASURE" not in sql.upper():
        return sql, 0

    # Try sqlglot AST first — handles whitespace, comments, and nested
    # parens correctly.
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:  # pragma: no cover - sqlglot is a hard dep, but be safe.
        sqlglot = None  # type: ignore[assignment]

    count = 0
    if sqlglot is not None:
        try:
            tree = sqlglot.parse_one(sql, read="databricks")
        except Exception:
            tree = None
        if tree is not None:
            agg_class_names = {
                "Sum", "Avg", "Count", "Min", "Max", "Median",
                "Stddev", "StddevPop", "StddevSamp",
                "Variance", "VariancePop", "VarianceSamp",
                "AnyValue",
            }
            for node in list(tree.walk()):
                # ``walk()`` yields tuples in some sqlglot versions.
                expr_node = node[0] if isinstance(node, tuple) else node
                if not isinstance(expr_node, exp.AggFunc):
                    continue
                if type(expr_node).__name__ not in agg_class_names:
                    continue
                arg = expr_node.this
                if arg is None:
                    continue
                # Single-arg aggregate only. ``args`` may carry
                # ``distinct``/``order_by`` siblings — those are fine
                # to drop with the outer agg.
                if (
                    isinstance(arg, exp.Anonymous)
                    and str(arg.name or "").upper() == "MEASURE"
                ):
                    expr_node.replace(arg.copy())
                    count += 1
            if count:
                try:
                    return tree.sql(dialect="databricks"), count
                except Exception:
                    pass  # fall through to regex
            else:
                # AST traversed cleanly with no rewrites — done.
                return sql, 0

    # Regex fallback — used when sqlglot fails to parse OR fails to
    # render. Conservative: the inner-MEASURE arg list is matched as
    # a single ``[^()]*`` chunk so MEASURE calls with embedded parens
    # (rare but possible inside CASE expressions) are skipped.
    new_sql, n = _OUTER_AGG_AROUND_MEASURE_RE.subn(r"\2", sql)
    return new_sql, n

_MV_ERROR_MARKERS: tuple[str, ...] = (
    "UNSUPPORTED_METRIC_VIEW_USAGE",
    "METRIC_VIEW_UNSUPPORTED_USAGE",
    "METRIC_VIEW_MISSING_MEASURE_FUNCTION",
    "METRIC_VIEW_JOIN_NOT_SUPPORTED",
)

def is_metric_view_error(reason: Any) -> bool:
    """Return True when *reason* names any known metric-view error class.

    Accepts ``None`` and non-string inputs (returns ``False``) so call
    sites can pass exception messages, ``GateResult.reason`` payloads,
    or raw strings interchangeably.
    """
    if reason is None:
        return False
    if not isinstance(reason, str):
        try:
            reason = str(reason)
        except Exception:
            return False
    upper = reason.upper()
    return any(marker in upper for marker in _MV_ERROR_MARKERS)

def metric_view_error_kind(reason: Any) -> str | None:
    """Return a stable kind string for the metric-view error in *reason*.

    Returns one of ``"unsupported_usage"`` (the generic planner rejection
    that surfaces as either ``METRIC_VIEW_UNSUPPORTED_USAGE`` or
    ``UNSUPPORTED_METRIC_VIEW_USAGE``), ``"missing_measure"``,
    ``"join_not_supported"``, or ``None`` when the input does not name a
    metric-view error.

    When the payload mentions multiple kinds (e.g. a generic
    unsupported_usage frame that also references the more specific
    missing_measure subclass), the most specific kind wins so callers
    that dispatch on the kind string get the right repair behaviour.
    """
    if reason is None:
        return None
    if not isinstance(reason, str):
        try:
            reason = str(reason)
        except Exception:
            return None
    upper = reason.upper()
    if "METRIC_VIEW_MISSING_MEASURE_FUNCTION" in upper:
        return "missing_measure"
    if "METRIC_VIEW_JOIN_NOT_SUPPORTED" in upper:
        return "join_not_supported"
    if (
        "UNSUPPORTED_METRIC_VIEW_USAGE" in upper
        or "METRIC_VIEW_UNSUPPORTED_USAGE" in upper
    ):
        return "unsupported_usage"
    return None

def _entry_has_measure_columns(entry: Any) -> bool:
    """Return True if a data-source entry has any measure-typed column.

    Genie's serialized space sometimes places a metric view under
    ``data_sources.tables`` rather than ``data_sources.metric_views``
    (depends on whether the user formally registered it as an MV in
    the space; the underlying UC asset is still a metric view and
    Spark enforces the ``MEASURE()`` contract regardless of where the
    config records it). The deterministic signal is a column_config
    with ``column_type == "measure"`` or ``is_measure: True`` — both
    indicate a column that must be wrapped in ``MEASURE()`` when
    referenced in a SELECT/ORDER BY against the asset. Keeps this
    function side-effect free so callers can reuse it cheaply across
    the synthesis hot path.
    """
    if not isinstance(entry, dict):
        return False
    for cc in entry.get("column_configs", []) or []:
        if not isinstance(cc, dict):
            continue
        if str(cc.get("column_type", "")).lower() == "measure":
            return True
        if cc.get("is_measure"):
            return True
    return False

def _iter_effective_metric_view_entries(config: dict) -> Iterator[dict]:
    """Yield each effective metric-view data-source entry from *config*.

    Walks both ``data_sources.metric_views`` (always treated as MVs) and
    ``data_sources.tables`` (filtered by :func:`_entry_has_measure_columns`).
    Mirrors the canonical Genie shape so downstream callers can extract
    measures / dimensions without caring which list the MV originally
    landed in. De-duplicates by identifier so a snapshot that pre-reclassified
    one of its MVs cannot double-yield it.
    """
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return
    seen: set[str] = set()
    for mv in ds.get("metric_views", []) or []:
        if not isinstance(mv, dict):
            continue
        ident = (mv.get("identifier") or "").strip().lower()
        if ident and ident in seen:
            continue
        if ident:
            seen.add(ident)
        yield mv
    for tbl in ds.get("tables", []) or []:
        if not isinstance(tbl, dict):
            continue
        if not _entry_has_measure_columns(tbl):
            continue
        ident = (tbl.get("identifier") or "").strip().lower()
        if ident and ident in seen:
            continue
        if ident:
            seen.add(ident)
        yield tbl

def effective_metric_view_identifiers(config: dict) -> set[str]:
    """Return the set of identifier strings for all effective metric views.

    The "effective" view unifies entries that Genie placed under
    ``metric_views`` with entries placed under ``tables`` whose column
    configs declare measures — the only signal Spark cares about when
    enforcing the ``MEASURE()`` contract. Used by the MV ``SELECT *``
    guard, the MEASURE auto-wrap rewriter, the metric-view prompt
    block, and the data-profile skip-list so all four agree on what
    counts as an MV regardless of how Genie's serializer happened to
    classify it on this fetch.
    """
    out: set[str] = set()
    for mv in _iter_effective_metric_view_entries(config):
        ident = (mv.get("identifier") or "").strip()
        if ident:
            out.add(ident)
    return out

def effective_metric_view_identifiers_with_catalog(config: dict) -> set[str]:
    """Like :func:`effective_metric_view_identifiers` plus catalog detection.

    Unions the column-config heuristic (which only fires when Genie's
    serialized space declares a measure-typed column on the entry) with
    the runtime catalog detection cached at ``config["_metric_view_yaml"]``
    by :func:`preflight._detect_metric_views_via_catalog`.

    Use this variant from sites that gate on "is this asset an MV?" —
    MEASURE auto-wrap, MV ``SELECT *`` guard, MV prompt block, and the
    data-profile skip-list. Without the catalog union we miss MVs that
    Genie serialized under ``data_sources.tables`` without measure
    column configs (the actual failure mode that motivated the helper).

    PR 30 — When ``config["_asset_semantics"]`` is populated, the
    semantics map is the primary source of truth and the legacy union
    is folded in as a safety net for callers that pre-date the
    contract.
    """
    out: set[str] = set()
    base_lower: set[str] = set()

    try:
        from genie_space_optimizer.common.asset_semantics import (
            metric_view_identifiers as _sem_mv_idents,
        )
        for ident in _sem_mv_idents(config):
            if ident:
                out.add(ident)
                base_lower.add(ident.lower())
    except Exception:
        pass

    base = effective_metric_view_identifiers(config)
    for ident in base:
        if ident and ident.lower() not in base_lower:
            out.add(ident)
            base_lower.add(ident.lower())

    cache = config.get("_metric_view_yaml")
    if not isinstance(cache, dict):
        _ps = config.get("_parsed_space")
        if isinstance(_ps, dict):
            cache = _ps.get("_metric_view_yaml")
    if isinstance(cache, dict):
        for ident in cache.keys():
            ident_str = str(ident).strip()
            if ident_str and ident_str.lower() not in base_lower:
                out.add(ident_str)
                base_lower.add(ident_str.lower())
    return out

def effective_table_identifiers(config: dict) -> set[str]:
    """Return identifiers from ``_tables`` that are not effective MVs.

    Excludes ``data_sources.tables`` entries reclassified as metric
    views by :func:`effective_metric_view_identifiers_with_catalog` so
    callers enumerating "real" tables (e.g. data profiling, table
    allowlist rendering) skip MV-shaped entries without manual
    filtering.
    """
    mv_idents = {
        ident.lower()
        for ident in effective_metric_view_identifiers_with_catalog(config)
    }
    out: set[str] = set()
    for tbl in config.get("_tables", []) or []:
        ident = str(tbl).strip()
        if ident and ident.lower() not in mv_idents:
            out.add(ident)
    return out

def build_metric_view_measures(config: dict) -> dict[str, set[str]]:
    """Build ``{lowered_short_name: {measure_col, ...}}`` for all effective MVs.

    "Effective" means we walk three sources and union the results:

    1. ``data_sources.metric_views`` — Genie's explicit MV serialization.
    2. ``data_sources.tables`` entries with at least one measure-typed
       column config (legacy serialization where MVs land under tables).
    3. ``config["_metric_view_yaml"]`` — the catalog-detection cache
       populated by :func:`metric_view_catalog.detect_metric_views_via_catalog`.
       This is the *only* path that catches MVs whose Genie payload omits
       both ``column_type='measure'`` and ``is_measure``, which is the
       common-in-production failure mode that PR 19 fixes.

    This is the single source of truth used by the MEASURE auto-wrap
    rewriter — keeping detection here ensures the unified pipeline, the
    preflight pipeline, and the benchmark/example correction loops all
    rewrite the same set of columns.

    PR 30 — When ``config["_asset_semantics"]`` is populated, the
    semantics map is consulted first and its measures are unioned with
    the legacy paths. This keeps the rewriter consistent across every
    detection ladder while preserving back-compat for snapshots that
    pre-date the contract.
    """
    result: dict[str, set[str]] = {}

    try:
        from genie_space_optimizer.common.asset_semantics import (
            metric_view_measures_by_short_name as _sem_measures,
        )
        for short, ms in _sem_measures(config).items():
            if not short or not ms:
                continue
            existing = result.setdefault(short, set())
            existing.update(ms)
    except Exception:
        pass

    for mv in _iter_effective_metric_view_entries(config):
        identifier = mv.get("identifier", "")
        short_name = identifier.split(".")[-1].lower() if identifier else ""
        if not short_name:
            continue
        measures: set[str] = set()
        for cc in mv.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            col_type = str(cc.get("column_type", "")).lower()
            if col_type == "measure" or cc.get("is_measure"):
                measures.add(col_name.lower())
        if measures:
            result[short_name] = measures

    # PR 19: union with the catalog-detection cache. The cache is keyed
    # by fully-qualified, lower-cased identifier; the rewriter keys on
    # the bare short name so we collapse to the last segment.
    cache = config.get("_metric_view_yaml") or {}
    if not cache:
        parsed = config.get("_parsed_space")
        if isinstance(parsed, dict):
            cache = parsed.get("_metric_view_yaml") or {}
    if isinstance(cache, dict):
        for fq_ident, yaml_doc in cache.items():
            if not isinstance(yaml_doc, dict):
                continue
            short = str(fq_ident).split(".")[-1].lower()
            if not short:
                continue
            measures = result.setdefault(short, set())
            for m in yaml_doc.get("measures") or []:
                if isinstance(m, dict):
                    name = m.get("name")
                    if isinstance(name, str) and name:
                        measures.add(name.lower())
        # Drop any short names whose measure set ended up empty (e.g. an
        # entry made it into the cache but the YAML had no measures
        # block) so callers don't probe rewriter logic against empty
        # sets.
        result = {k: v for k, v in result.items() if v}
    return result

def _parse_struct_field_names(data_type: str) -> list[str]:
    """Return top-level struct field names from a Spark ``struct<…>`` type.

    Tracks angle / paren depth so nested types do not bleed top-level fields.
    Returns an empty list when the type is not a struct.
    """
    if not data_type:
        return []
    s = data_type.strip()
    if not s.lower().startswith("struct<") or not s.endswith(">"):
        return []
    body = s[len("struct<"):-1]
    fields: list[str] = []
    depth = 0
    cursor = 0
    for i, ch in enumerate(body):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        elif ch == "," and depth == 0:
            chunk = body[cursor:i]
            cursor = i + 1
            colon = chunk.find(":")
            if colon > 0:
                fields.append(chunk[:colon].strip())
    chunk = body[cursor:]
    colon = chunk.find(":")
    if colon > 0:
        fields.append(chunk[:colon].strip())
    return [f for f in fields if f]

def build_table_columns(
    config: dict,
) -> dict[str, dict[str, set[str]]]:
    """Build per-table column / struct-column index from the Genie config.

    Returns ``{lower_short_name: {"columns": {…}, "struct_columns": {…}}}``
    covering both ``data_sources.tables`` and ``data_sources.metric_views``.
    Used by :func:`_check_dangling_qualifiers` to decide whether a
    ``<qual>.<col>`` reference can be resolved against the FROM/JOIN tables.
    """
    result: dict[str, dict[str, set[str]]] = {}
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return result
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return result
    sources: list[dict] = []
    sources.extend(ds.get("tables", []) or [])
    sources.extend(ds.get("metric_views", []) or [])
    for tbl in sources:
        if not isinstance(tbl, dict):
            continue
        identifier = (tbl.get("identifier") or tbl.get("name") or "").strip()
        short = identifier.split(".")[-1].lower()
        if not short:
            continue
        columns: set[str] = set()
        struct_columns: set[str] = set()
        for cc in tbl.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col_name = (cc.get("column_name") or cc.get("name") or "").strip()
            if not col_name:
                continue
            columns.add(col_name.lower())
            data_type = str(cc.get("data_type", "") or "")
            if _parse_struct_field_names(data_type):
                struct_columns.add(col_name.lower())
        existing = result.setdefault(
            short, {"columns": set(), "struct_columns": set()},
        )
        existing["columns"].update(columns)
        existing["struct_columns"].update(struct_columns)
    return result

_QUALIFIED_REF_RE = re.compile(
    r"(?:(?<![\w`.])|(?<=^))`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?",
)

_SQL_RESERVED_BEFORE_DOT = frozenset({
    "select", "from", "where", "group", "order", "by", "having",
    "join", "inner", "left", "right", "full", "cross", "outer",
    "on", "and", "or", "not", "in", "is", "null", "as", "distinct",
    "case", "when", "then", "else", "end", "with", "union", "intersect",
    "except", "values", "limit", "offset", "fetch", "next", "rows",
    "only", "between", "like", "ilike", "exists", "cast", "interval",
})

def _strip_from_join_clauses(sql: str) -> str:
    """Return *sql* with FROM/JOIN clause heads removed up to the next
    statement keyword.

    We only want to flag ``<qual>.<col>`` references that appear in
    SELECT / WHERE / GROUP BY / HAVING / ORDER BY positions — the FROM
    and JOIN clauses legitimately carry ``catalog.schema.table`` forms
    where the head of the dot is a catalog or schema name, not a column
    qualifier. Stripping those substrings out before scanning avoids
    false-positive flags on the catalog component.
    """
    if not sql:
        return sql
    # Match: FROM/JOIN <ws> <table-spec> until the next clause boundary.
    # The terminator is the next SQL clause keyword or end of statement.
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\b[\s\S]*?"
        r"(?=\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT|"
        r"FROM|JOIN|ON|WHEN|END|CROSS|INNER|LEFT|RIGHT|FULL|OUTER)\b|$|;|\))",
        re.IGNORECASE,
    )
    return pattern.sub(" ", sql)

def _extract_cte_names(sql: str) -> set[str]:
    """PR 31 — Extract top-level CTE names declared in a ``WITH`` clause.

    Recognizes the CTE-first pattern produced by the metric-view
    repair path::

        WITH __mv_1 AS (SELECT ... FROM cat.sch.mv1),
             base   AS (SELECT ... FROM cat.sch.fact)
        SELECT base.col, __mv_1.measure_value FROM base ...

    Without this, the dangling-qualifier check rejects every CTE
    alias as unresolved because ``base`` and ``__mv_1`` never appear
    on a FROM/JOIN clause's *table* slot — only as references later
    in the query body.

    Implementation is regex-based and bounded by the AS-paren depth
    to avoid scanning into subqueries. ``RECURSIVE`` is honored.
    """
    out: set[str] = set()
    if not sql:
        return out

    # Find the first WITH (case-insensitive) at the top level. We don't
    # try to recover from arbitrary leading whitespace/comments — both
    # are tolerated by the simple regex.
    with_match = re.search(
        r"\bWITH\s+(?:RECURSIVE\s+)?",
        sql,
        re.IGNORECASE,
    )
    if not with_match:
        return out

    # CTE names are followed by ``AS`` (with optional column list in
    # parens). We parse depth-aware to locate each CTE definition's
    # closing paren before grabbing the next name.
    pos = with_match.end()
    n = len(sql)
    cte_pattern = re.compile(
        r"\s*`?([A-Za-z_]\w*)`?\s*(?:\([^()]*\))?\s*AS\s*\(",
        re.IGNORECASE,
    )
    while pos < n:
        m = cte_pattern.match(sql, pos)
        if not m:
            break
        cte_name = m.group(1).lower()
        if cte_name and cte_name not in _SQL_RESERVED_BEFORE_DOT:
            out.add(cte_name)
        # Walk past the CTE body — count parens starting at the opening one.
        depth = 1
        i = m.end()
        while i < n and depth > 0:
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        # After the body, expect either ``,`` (next CTE) or end of WITH.
        # Skip whitespace.
        while i < n and sql[i].isspace():
            i += 1
        if i < n and sql[i] == ",":
            pos = i + 1
            continue
        break
    return out

def _extract_from_join_aliases(sql: str) -> set[str]:
    """Return the set of effective qualifiers visible in FROM/JOIN clauses.

    For each entry the set includes:
      - The table's short name (last dot component) — covers unaliased
        references like ``mv_x.col``.
      - The alias when present — covers ``mv_x AS x`` / ``mv_x x``.
      - PR 31 — CTE names declared in any ``WITH`` clause, so
        ``FROM base`` and ``base.col`` references resolve when ``base``
        is a top-level CTE rather than an actual table.

    Implementation is regex-based to avoid a hard sqlglot dependency in
    the hot repair path. Handles backticks and ``AS`` keyword. The alias
    group uses a negative lookahead to avoid consuming the next clause
    keyword (e.g. ``JOIN cat.sch.t ON …`` — ``ON`` is not the alias) so
    multiple FROM/JOIN entries in the same statement all get indexed.
    """
    out: set[str] = set()
    # The alias group's identifier must NOT be a reserved keyword, so we
    # exclude FROM/JOIN/ON/WHERE/etc to keep them anchoring boundaries.
    reserved_alts = "|".join(sorted(_SQL_RESERVED_BEFORE_DOT, key=len, reverse=True))
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+`?([\w.]+)`?"
        rf"(?:\s+(?:AS\s+)?`?(?!(?:{reserved_alts})\b)([A-Za-z_]\w*)`?)?",
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        ident = (m.group(1) or "").strip()
        alias = (m.group(2) or "").strip()
        if ident:
            short = ident.split(".")[-1]
            if short and short.lower() not in _SQL_RESERVED_BEFORE_DOT:
                out.add(short.lower())
        if alias and alias.lower() not in _SQL_RESERVED_BEFORE_DOT:
            out.add(alias.lower())
    # PR 31 — also accept CTE names declared in a ``WITH`` clause.
    out.update(_extract_cte_names(sql))
    return out

def _check_dangling_qualifiers(
    sql: str,
    table_columns: dict[str, dict[str, set[str]]],
) -> list[str]:
    """Detect ``<qual>.<col>`` references whose qualifier isn't in scope.

    A qualifier is in scope when it is one of:
      - A FROM/JOIN table short name (``FROM mv_x`` → ``mv_x``).
      - An explicit alias (``FROM mv_x AS x`` → ``x``; ``JOIN t y`` → ``y``).
      - The name of a struct column on any FROM/JOIN table — covers
        ``dim_location.region`` where ``dim_location`` is a struct column
        on a metric view in FROM.

    Anything else is dangling. The most common shape we want to catch is
    the LLM analogising ``dim_location.region`` (real struct field) onto
    ``dim_date.year`` (a separate metric view that must be JOINed).

    Returns a sorted list of unresolved qualifier strings (deduplicated).
    Empty list means the SQL has no dangling qualifier — does NOT mean the
    SQL is otherwise valid (downstream EXPLAIN still owns truth).
    """
    if not sql or not sql.strip() or not table_columns:
        return []

    aliases = _extract_from_join_aliases(sql)
    if not aliases:
        return []

    # Collect struct column names visible from any FROM/JOIN table.
    visible_struct_cols: set[str] = set()
    for alias in aliases:
        info = table_columns.get(alias)
        if info:
            visible_struct_cols |= info.get("struct_columns", set())

    allowed = aliases | visible_struct_cols

    # Strip out FROM/JOIN tails so catalog.schema.table doesn't generate
    # false positives.
    body = _strip_from_join_clauses(sql)

    unresolved: set[str] = set()
    for m in _QUALIFIED_REF_RE.finditer(body):
        qual = m.group(1).lower()
        if qual in _SQL_RESERVED_BEFORE_DOT:
            continue
        # Skip when the match is part of a longer dotted chain
        # (``cat.sch.tbl.col`` or ``cat.sch.tbl``). The regex matches
        # only the first two segments so the trailing ``.`` would still
        # be present in the body. A 3+ part column reference is fine
        # in Spark SQL when the prefix matches a FROM table; the head
        # catalog/schema components must NOT be flagged.
        end = m.end()
        if end < len(body) and body[end:end + 1] == ".":
            continue
        if qual in allowed:
            continue
        unresolved.add(qual)

    return sorted(unresolved)

_SELECT_STAR_RE = re.compile(r"\bSELECT\s+\*\s+FROM\b", re.IGNORECASE)

def _guard_mv_select_star(
    sql: str,
    metric_view_names: set[str],
) -> tuple[bool, str]:
    """Reject ``SELECT *`` queries that target metric views.

    Returns ``(is_ok, reason)``.  When *is_ok* is False the benchmark
    should be sent to the correction pipeline or quarantined.
    """
    if not _SELECT_STAR_RE.search(sql):
        return True, ""
    sql_lower = sql.lower()
    mv_leaves = {n.lower().split(".")[-1] for n in metric_view_names}
    for mv in mv_leaves:
        if mv in sql_lower:
            return (
                False,
                f"SELECT * not supported on metric view '{mv}' "
                "— must explicitly list dimensions and MEASURE() columns",
            )
    return True, ""

def normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    """Convert 0-1 scale → 0-100 scale; leave 0-100 unchanged."""
    normalized: dict[str, float] = {}
    for key, val in scores.items():
        if 0 <= val <= 1.0:
            normalized[key] = round(val * 100, 2)
        else:
            normalized[key] = round(val, 2)
    return normalized

def all_thresholds_met(
    scores: dict[str, float],
    targets: dict[str, float] | None = None,
) -> bool:
    """Return True when the official Benchmark API accuracy meets target.

    Phase 3 (D2): the 9 scored judges are retired, so gating is on API accuracy
    alone — ``DEFAULT_THRESHOLDS`` now holds only the ``result_correctness``
    accuracy gate (== ``num_correct/num_questions`` on the official path) and no
    per-judge thresholds remain. The loop still honours any explicitly-passed
    multi-key ``targets`` for backward compatibility, but the default path checks
    accuracy only. This answers only the objective-complete question; the unified
    loop owns candidate acceptance.

    ``scores`` should be on a 0-100 scale.
    """
    targets = targets or DEFAULT_THRESHOLDS
    for judge, threshold in targets.items():
        actual = scores.get(judge)
        if actual is None and judge == "result_correctness":
            # Accept the headline-accuracy alias when the carrier key is absent.
            actual = scores.get("overall_accuracy")
        if actual is None:
            return False
        if actual < threshold:
            return False
    return True

_VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})

def _normalize_expected_asset(
    raw: Any,
    expected_sql: str,
    hint: Any = None,
) -> str:
    """Normalize ``expected_asset`` to a valid type category.

    Resolution precedence (default scoring-v2 mode):

    1. ``raw`` — if it is already one of ``MV``/``TVF``/``TABLE`` use it.
       Benchmarks authored post-fix will populate this explicitly.
    2. ``hint`` (``expected_asset_hint`` on the benchmark) — explicit
       author override used when the stored ``expected_asset`` is a
       table *name* rather than a type category. This beats detection
       and prevents ``detect_asset_type`` from mis-labeling tables that
       happen to start with ``mv_`` (B1 companion fix).
    3. Fallback to ``detect_asset_type(expected_sql)``.

    Under ``GSO_SCORING_V2=off`` the hint is ignored to preserve
    byte-identical legacy behavior.
    """
    upper = raw.strip().upper() if isinstance(raw, str) and raw else ""
    if upper in _VALID_ASSET_TYPES:
        return upper
    if not scoring_v2_is_legacy():
        hint_upper = (
            hint.strip().upper() if isinstance(hint, str) and hint else ""
        )
        if hint_upper in _VALID_ASSET_TYPES:
            return hint_upper
    return detect_asset_type(expected_sql)

def _extract_sql_function_calls(sql: str, catalog: str, schema: str) -> set[str]:
    """Extract fully-qualified function names called with parentheses."""
    if not sql or not catalog or not schema:
        return set()
    pattern = re.compile(
        rf"(?i)\b{re.escape(catalog)}\s*\.\s*{re.escape(schema)}\s*\.\s*([a-zA-Z_][\w]*)\s*\(",
    )
    return {m.group(1).lower() for m in pattern.finditer(sql)}

def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"

def _set_sql_context(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Ensure Spark SQL context is aligned to target catalog/schema."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")

def _execute_sql_via_warehouse(
    w: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    catalog: str = "",
    schema: str = "",
    wait_timeout: str = "50s",
) -> pd.DataFrame:
    """Execute SQL via the SQL warehouse Statement Execution API.

    Returns a pandas DataFrame on success (may be empty for DDL/EXPLAIN).
    Raises ``RuntimeError`` on failure with the warehouse error message.
    """
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    from genie_space_optimizer.common.query_tags import gso_query_tags

    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        catalog=catalog or None,
        schema=schema or None,
        wait_timeout=wait_timeout,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        query_tags=gso_query_tags(purpose="benchmark_validation"),
    )
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        manifest_schema = resp.manifest.schema if resp.manifest else None
        schema_cols = manifest_schema.columns if manifest_schema else None
        columns = [str(c.name or "") for c in (schema_cols or [])]
        rows: list[dict] = []
        if resp.result and resp.result.data_array:
            for row_data in resp.result.data_array:
                rows.append(dict(zip(columns, row_data)))
        return pd.DataFrame(rows, columns=pd.Index(columns) if columns else None)

    state = str(resp.status.state) if resp.status and resp.status.state else "UNKNOWN"
    statement_id = getattr(resp, "statement_id", None) or ""
    if state in {"PENDING", "RUNNING"}:
        raise RuntimeError(
            "SQL warehouse query did not finish within "
            f"wait_timeout={wait_timeout}; state={state}; statement_id={statement_id}"
        )

    error_msg = ""
    if resp.status and resp.status.error:
        error_msg = resp.status.error.message or str(resp.status.error)
    raise RuntimeError(
        error_msg
        or f"SQL warehouse query failed with state={state}; statement_id={statement_id}"
    )

def _exec_sql(
    sql: str,
    spark: Any,
    *,
    w: Any = None,
    warehouse_id: str = "",
    catalog: str = "",
    schema: str = "",
) -> "pd.DataFrame":
    """Execute SQL via warehouse (primary) or Spark (fallback).

    Returns a pandas DataFrame in both cases.  When the warehouse is
    available and *warehouse_id* is set, routes through the Statement
    Execution API.  Otherwise falls back to ``spark.sql().toPandas()``.
    """
    if w and warehouse_id:
        try:
            return _execute_sql_via_warehouse(
                w, warehouse_id, sql,
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.debug(
                "Warehouse SQL failed, falling back to Spark: %s",
                sql[:120], exc_info=True,
            )
    if catalog:
        _set_sql_context(spark, catalog, schema)
    return spark.sql(sql).toPandas()

_SQL_PARAM_RE = re.compile(
    r"(?<![:\w])"     # not preceded by : or word char (avoids ::cast, timestamps)
    r":([a-zA-Z_]\w*)"  # :param_name
    r"(?!\s*:)"        # not followed by : (avoids :: cast operator)
)

def _extract_sql_params(sql: str) -> list[str]:
    """Return SQL named-parameter placeholders (e.g. :min_amount) found in *sql*."""
    if not sql:
        return []
    return _SQL_PARAM_RE.findall(sql)

def _classify_sql_validation_error(message: str) -> str:
    """Classify SQL validation failures into stable reason codes.

    PR 16 added the following codes to enable class-specific repair
    hints in the LLM correction prompt:

    * ``mv_missing_measure_function`` — bare measure column referenced
      against an MV; fix is to wrap with ``MEASURE()``.
    * ``mv_alias_collision`` — ``MEASURE(col) AS col`` shadowed the
      underlying column; fix is to rename the alias.

    PR 20 added:

    * ``mv_measure_in_where`` — a measure column was referenced inside
      a ``WHERE`` / ``HAVING`` / ``ON`` clause (Spark forbids this even
      when wrapped in ``MEASURE()``). Fix is the CTE-first rewrite.
    """
    lowered = (message or "").lower()
    if "metric_view_missing_measure_function" in lowered:
        # Disambiguate: if the planner cited a WHERE/HAVING/ON clause
        # the LLM needs the CTE-first hint, not the wrap-in-MEASURE
        # hint. Spark's error message text varies by release; we look
        # for any of the three clause keywords near the error preamble.
        if any(
            kw in lowered
            for kw in (
                "in where",
                "in the where",
                "where clause",
                "in having",
                "having clause",
                "in on",
                " on clause",
            )
        ):
            return "mv_measure_in_where"
        return "mv_missing_measure_function"
    if (
        "metric_view_unsupported_usage" in lowered
        or "unsupported_metric_view_usage" in lowered
    ):
        return "mv_unsupported_usage"
    if (
        "missing_attributes.resolved_attribute_appear_in_operation" in lowered
        or "resolved attribute" in lowered
        and "appear in the operation" in lowered
    ):
        return "mv_alias_collision"
    if "metric_view_join_not_supported" in lowered:
        return "metric_view_join"
    # PR 32 — categorical string cast to numeric.
    if (
        "cast_invalid_input" in lowered
        or "cannot be cast to" in lowered
        or "cannot be parsed as" in lowered
    ):
        return "cast_invalid_input"
    if "insufficient_permissions" in lowered or "permission denied" in lowered:
        return "permission_blocked"
    if "does not have execute on routine" in lowered:
        return "permission_blocked"
    if "unresolved_column" in lowered:
        if "join" in lowered:
            return "bad_join_key"
        return "unknown_column"
    if "table_or_view_not_found" in lowered or "cannot be found" in lowered:
        return "missing_object"
    if "parseexception" in lowered or "syntax error" in lowered:
        return "syntax_error"
    return "sql_compile_error"

_REPAIR_HINTS_BY_REASON: dict[str, str] = {
    "mv_missing_measure_function": (
        "FIX: A bare measure column was referenced against a metric "
        "view. Wrap every measure column in MEASURE() in the SELECT "
        "and ORDER BY clauses (NEVER in WHERE / HAVING / ON). The "
        "Metric Views section above lists which columns are measures."
    ),
    "mv_measure_in_where": (
        "FIX: A measure column appeared in a WHERE / HAVING / ON "
        "clause. Spark forbids this even when wrapped in MEASURE(). "
        "Use the CTE-first pattern from the Metric Views docs: "
        "materialize each filtered measure as ``MEASURE(m) AS m_value`` "
        "in a WITH-clause SELECT, then filter on the alias in the "
        "outer query. Example:\n"
        "  WITH __mv_base AS (\n"
        "    SELECT zone, MEASURE(total_sales) AS sales,\n"
        "           MEASURE(store_day_count) AS store_day_count_value\n"
        "    FROM mv_x GROUP BY zone\n"
        "  )\n"
        "  SELECT zone, sales FROM __mv_base WHERE store_day_count_value > 0;"
    ),
    "mv_alias_collision": (
        "FIX: MEASURE(col) was aliased back to the same column name "
        "(e.g. MEASURE(cy_sales) AS cy_sales), which Spark resolves as "
        "a re-application of MEASURE on the alias. Rename the alias "
        "to something distinct (e.g. cy_sales_value) and update any "
        "ORDER BY / HAVING references."
    ),
    "unknown_column": (
        "FIX: A column reference doesn't exist on the cited asset. "
        "Replace it with a column from the Column Allowlist that "
        "matches the question intent. NEVER stem or invent column "
        "names; use the FQ identifier as written in the allowlist."
    ),
    "missing_object": (
        "FIX: The SQL references a table / view / function that does "
        "not exist. Replace with an allowlisted asset from VALID Data "
        "Assets. NEVER stem or aliase the asset identifier."
    ),
    "metric_view_join": (
        "FIX: Direct JOIN against a metric view triggered "
        "METRIC_VIEW_JOIN_NOT_SUPPORTED. Use the CTE-first pattern: "
        "materialize the metric view query in a WITH clause, then "
        "JOIN the CTE result to the dimension table."
    ),
    "cast_invalid_input": (
        "FIX: A CAST to a numeric type failed because the column's "
        "actual values are categorical strings (e.g. 'Y'/'N', "
        "'true'/'false'). Do NOT cast categorical flag columns to "
        "BIGINT/INT/DOUBLE. Instead, compare directly to the "
        "string literal (``WHERE flag_col = 'Y'``) or use a CASE "
        "expression to map categories to 0/1 (``CASE WHEN col = 'Y' "
        "THEN 1 ELSE 0 END``). The Column value profile section "
        "above lists the actual sampled values."
    ),
    "bad_join_key": (
        "FIX: The JOIN ON clause references a column that doesn't "
        "exist on one side of the join. Use the Join Specifications "
        "section to pick the correct join keys."
    ),
    "syntax_error": (
        "FIX: SQL parse error. Re-author the query — preserve the "
        "question intent but write it in valid Spark SQL."
    ),
}

def _repair_hint_for_reason(reason: str) -> str:
    """Return a class-specific repair hint or empty string if unknown.

    The hint is appended to the ``benchmarks_to_fix`` payload so the
    LLM correction call gets a deterministic nudge toward the right
    fix instead of guessing from the raw error string.
    """
    return _REPAIR_HINTS_BY_REASON.get(reason, "")

AUTO_OPTIMIZE_TAG_PREFIX = "[auto-optimize] "

def _coerce_question_text(raw: Any) -> str:
    if isinstance(raw, list):
        return " ".join(str(part) for part in raw).strip()
    return str(raw or "").strip()

def _strip_legacy_auto_optimize_prefix(question: str) -> str:
    text = str(question or "").strip()
    if text.startswith(AUTO_OPTIMIZE_TAG_PREFIX):
        return text[len(AUTO_OPTIMIZE_TAG_PREFIX):].strip()
    return text

def _extract_sql_answer(answers: Any) -> str:
    return _extract_benchmark_sql_answer(answers)

def _normalized_question_key(question: str) -> str:
    text = _strip_legacy_auto_optimize_prefix(str(question or ""))
    return re.sub(r"\s+", " ", text.strip().lower())

def _extract_example_sql_question_keys(config: dict) -> set[str]:
    parsed = config.get("_parsed_space", config)
    if not isinstance(parsed, dict):
        return set()
    keys: set[str] = set()

    def _walk(container: dict) -> None:
        example_sqls = container.get("example_question_sqls")
        if not isinstance(example_sqls, list):
            return
        for item in example_sqls:
            if isinstance(item, dict):
                key = _normalized_question_key(_coerce_question_text(item.get("question", "")))
                if key:
                    keys.add(key)

    _walk(parsed)
    inst = parsed.get("instructions", {})
    if isinstance(inst, dict):
        _walk(inst)
    return keys

def _filter_example_sql_mirrored_benchmarks(
    benchmarks: list[dict],
    config: dict,
) -> list[dict]:
    blocked = _extract_example_sql_question_keys(config)
    if not blocked:
        return benchmarks
    filtered = [
        b for b in benchmarks
        if _normalized_question_key(str(b.get("question", ""))) not in blocked
    ]
    dropped = len(benchmarks) - len(filtered)
    if dropped:
        logger.info(
            "Dropped %d benchmark row(s) mirrored in example_question_sqls",
            dropped,
        )
    return filtered

def extract_genie_space_benchmarks(
    config: dict,
    spark: SparkSession,
    catalog: str = "",
    schema: str = "",
    *,
    w: Any = None,
    warehouse_id: str = "",
    preserve_invalid_sql: bool = False,
) -> list[dict]:
    """Extract benchmark questions from a Genie Agent config.

    Sources:
      1. ``benchmarks.questions`` — user-authored benchmark questions, with
         optional SQL answers.
      2. ``config.sample_questions`` — user-authored natural-language sample
         questions that need ground-truth SQL generation.

    ``instructions.example_question_sqls`` are training examples and are
    intentionally excluded from the benchmark corpus.

    When ``preserve_invalid_sql`` is true, source SQL that fails the initial
    validation pass remains attached to the benchmark so a later quality
    review can report the actual validation error.  Repair-enabled callers
    retain the historical question-only fallback by using the default.
    """
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict) or not parsed_space:
        parsed_space = config if isinstance(config, dict) else {}

    benchmarks: list[dict] = []
    seen_questions: set[str] = set()

    def _append_question(
        *,
        question: str,
        expected_sql: str,
        source: str,
        category: str,
        space_question_id: str = "",
    ) -> None:
        normalized_question = _strip_legacy_auto_optimize_prefix(question)
        q_lower = normalized_question.lower().strip()
        if not q_lower or q_lower in seen_questions:
            return
        seen_questions.add(q_lower)

        validation_status = "question_only"
        validation_reason_code = "missing_expected_sql"
        validation_error: str | None = "No valid expected SQL in Genie benchmark source"
        sql = expected_sql.strip()
        if sql:
            from genie_space_optimizer.optimization.benchmarks import fix_mv_alias_sort_collision
            sql = fix_mv_alias_sort_collision(sql)
            is_valid, err = validate_ground_truth_sql(
                sql,
                spark,
                catalog=catalog,
                gold_schema=schema,
                w=w,
                warehouse_id=warehouse_id,
            )
            if is_valid:
                validation_status = "valid"
                validation_reason_code = "ok"
                validation_error = None
            else:
                logger.warning(
                    "Genie Agent benchmark source SQL failed validation: %s -- %s",
                    normalized_question[:60],
                    err,
                )
                if preserve_invalid_sql:
                    validation_status = "invalid"
                    validation_error = str(err or "SQL validation failed")
                else:
                    sql = ""
                    validation_status = "question_only"
                validation_reason_code = "invalid_source_sql"

        benchmark = {
            "question": normalized_question,
            "expected_sql": sql,
            "expected_asset": detect_asset_type(sql) if sql else "TABLE",
            "category": category,
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": source,
            "provenance": "curated",
            "validation_status": validation_status,
            "validation_reason_code": validation_reason_code,
            "validation_error": validation_error,
        }
        if space_question_id:
            benchmark["space_question_id"] = space_question_id
        benchmarks.append(benchmark)

    bench_section = parsed_space.get("benchmarks", {})
    if not isinstance(bench_section, dict):
        bench_section = {}
    bench_questions = bench_section.get("questions", [])
    for bq in bench_questions if isinstance(bench_questions, list) else []:
        if not isinstance(bq, dict):
            continue
        question = _coerce_question_text(bq.get("question", ""))
        expected_sql = _extract_sql_answer(bq.get("answer", []))
        _append_question(
            question=question,
            expected_sql=expected_sql,
            source="genie_benchmark",
            category="user_benchmark",
            space_question_id=str(bq.get("id") or "").strip(),
        )

    cfg_block = parsed_space.get("config", {})
    if not isinstance(cfg_block, dict):
        cfg_block = {}
    sample_questions = cfg_block.get("sample_questions", [])
    for sq in sample_questions if isinstance(sample_questions, list) else []:
        if not isinstance(sq, dict):
            continue
        _append_question(
            question=_coerce_question_text(sq.get("question", "")),
            expected_sql="",
            source="sample_question",
            category="sample_question",
        )

    benchmarks = _filter_example_sql_mirrored_benchmarks(benchmarks, config)

    logger.info(
        "Extracted %d benchmark question(s) from Genie Agent config "
        "(%d with SQL, %d requiring SQL generation)",
        len(benchmarks),
        sum(1 for b in benchmarks if b.get("expected_sql")),
        sum(1 for b in benchmarks if not b.get("expected_sql")),
    )
    return benchmarks


def extract_review_only_benchmarks(
    config: dict,
    spark: SparkSession,
    catalog: str = "",
    schema: str = "",
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Return only native pre-run benchmark questions for review-only runs.

    Sample questions intentionally remain outside the optimization corpus: they
    do not have user-approved ground-truth SQL and admitting them would turn a
    review-only run into implicit benchmark generation.
    """
    return [
        benchmark
        for benchmark in extract_genie_space_benchmarks(
            config,
            spark,
            catalog=catalog,
            schema=schema,
            w=w,
            warehouse_id=warehouse_id,
            preserve_invalid_sql=True,
        )
        if benchmark.get("source") == "genie_benchmark"
    ]

def _build_valid_assets_context(config: dict) -> str:
    """Build an explicit allowlist of Genie Agent data assets for the LLM prompt.

    Uses the *effective* MV / table classification so that any
    ``data_sources.tables`` entries Genie serialized but which carry
    measure-typed column configs are surfaced to the LLM as METRIC
    VIEW (the only label that triggers the MEASURE() worked example
    in the prompt). Otherwise the LLM happily emits ``SUM(measure)``
    against an MV and the execute gate rejects every candidate with
    ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``.
    """
    mv_idents = effective_metric_view_identifiers_with_catalog(config)
    table_idents = effective_table_identifiers(config)
    lines: list[str] = []
    for tbl in sorted(table_idents):
        lines.append(f"- TABLE: {tbl}")
    for mv in sorted(mv_idents):
        lines.append(f"- METRIC VIEW: {mv}")
    for fn in config.get("_functions", []):
        lines.append(f"- FUNCTION: {fn}")
    return "\n".join(lines) if lines else "(no assets configured)"

def _space_table_asset_candidates(config: dict) -> set[str]:
    candidates: set[str] = set()
    for raw in sorted(
        effective_table_identifiers(config)
        | effective_metric_view_identifiers_with_catalog(config)
    ):
        candidates.update(_identifier_candidates(str(raw)))
    return {c for c in candidates if c}

def _space_function_candidates(config: dict) -> set[str]:
    candidates: set[str] = set()
    for raw in config.get("_functions", []) if isinstance(config.get("_functions"), list) else []:
        candidates.update(_identifier_candidates(str(raw)))
    return {c for c in candidates if c}

def _uc_column_table_candidates(row: dict) -> set[str]:
    table_name = str(row.get("table_name") or "").strip()
    catalog_name = str(row.get("catalog_name") or "").strip()
    schema_name = str(row.get("schema_name") or "").strip()
    candidates = _identifier_candidates(table_name)
    if catalog_name and schema_name and table_name:
        candidates.update(_identifier_candidates(f"{catalog_name}.{schema_name}.{table_name}"))
    if schema_name and table_name:
        candidates.update(_identifier_candidates(f"{schema_name}.{table_name}"))
    return {c for c in candidates if c}

def _filter_uc_columns_to_space_assets(config: dict, uc_columns: list[dict]) -> list[dict]:
    allowed = _space_table_asset_candidates(config)
    if not allowed:
        return []
    return [
        col for col in uc_columns
        if isinstance(col, dict) and (_uc_column_table_candidates(col) & allowed)
    ]

def _filter_uc_routines_to_space_functions(config: dict, uc_routines: list[dict]) -> list[dict]:
    allowed = _space_function_candidates(config)
    if not allowed:
        return []
    filtered: list[dict] = []
    for routine in uc_routines:
        if not isinstance(routine, dict):
            continue
        raw_name = str(routine.get("routine_name") or routine.get("specific_name") or "").strip()
        if raw_name and (_identifier_candidates(raw_name) & allowed):
            filtered.append(routine)
    return filtered

def _filter_data_profile_to_space_assets(config: dict) -> dict[str, dict]:
    profile = config.get("_data_profile", {})
    if not isinstance(profile, dict):
        return {}
    allowed = _space_table_asset_candidates(config)
    if not allowed:
        return {}
    scoped: dict[str, dict] = {}
    for table, table_info in profile.items():
        if _identifier_candidates(str(table)) & allowed:
            scoped[str(table)] = table_info
    return scoped

def _format_data_profile_context(config: dict, data_profile: dict[str, dict] | None = None) -> str:
    """Build a compact data-profile section for benchmark generation prompts.

    Renders per-table row counts, per-column cardinality, distinct values
    for low-cardinality columns, and min/max ranges for numeric/date columns.
    """
    profile = data_profile if data_profile is not None else config.get("_data_profile", {})
    if not profile:
        return "(no data profile available)"
    lines: list[str] = []
    for table, tinfo in sorted(profile.items()):
        row_count = tinfo.get("row_count", "?")
        lines.append(f"### {table} (~{row_count} rows)")
        for col, cinfo in sorted(tinfo.get("columns", {}).items()):
            card = cinfo.get("cardinality", "?")
            vals = cinfo.get("distinct_values")
            minv = cinfo.get("min")
            maxv = cinfo.get("max")
            parts = [f"cardinality={card}"]
            if vals:
                parts.append(f"values={vals}")
            if minv is not None:
                parts.append(f"range=[{minv}, {maxv}]")
            lines.append(f"  - {col}: {', '.join(parts)}")
    return "\n".join(lines)

def _build_schema_contexts(
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, str]:
    """Build the schema context strings for benchmark prompts."""
    scoped_uc_columns = _filter_uc_columns_to_space_assets(config, uc_columns)
    scoped_uc_routines = _filter_uc_routines_to_space_functions(config, uc_routines)

    tables_context = "\n".join(
        f"- {c.get('table_name', '')}.{c.get('column_name', '')} ({c.get('data_type', '')}): {c.get('comment', '')}"
        for c in scoped_uc_columns
    )

    # -- Metric views: enrich with measure/dimension column detail --
    # Walk the *effective* MV set: union of ``data_sources.metric_views``
    # plus any ``data_sources.tables`` entries that carry measure-typed
    # column configs. This catches the case where Genie serialized an
    # MV under ``tables`` (e.g. when ``metric_views: 0`` in the config
    # but Spark plans the asset as MetricView) — without this fixup the
    # prompt's metric-view block reads "(none)", the LLM never gets
    # the MEASURE() worked example, and the execute gate rejects every
    # candidate against the MV.
    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict) or not parsed_space:
        parsed_space = config if isinstance(config, dict) else {}

    mv_lines: list[str] = []
    for mv in _iter_effective_metric_view_entries(config):
        ident = (mv.get("identifier") or "").strip()
        if not ident:
            continue
        measures: list[str] = []
        dimensions: list[str] = []
        for cc in mv.get("column_configs", []) or []:
            if not isinstance(cc, dict):
                continue
            col = cc.get("column_name", "")
            if not col:
                continue
            if (
                str(cc.get("column_type", "")).lower() == "measure"
                or cc.get("is_measure")
            ):
                measures.append(col)
            else:
                dimensions.append(col)
        parts = [f"- {ident}"]
        if measures:
            parts.append(f"  Measures (use MEASURE() syntax): {', '.join(measures)}")
        if dimensions:
            parts.append(f"  Dimensions (for GROUP BY / WHERE): {', '.join(dimensions)}")
        if not measures and not dimensions:
            parts.append("  (no column detail available)")
        mv_lines.append("\n".join(parts))
    if mv_lines:
        # PR 26 — explicit anti-pattern reminder + a positive minimal
        # example so the LLM has both the rule ("never JOIN MVs
        # directly") AND a worked template ("CTE-first pattern") in
        # the context that lists this run's metric views. The hint is
        # in addition to the per-template ``no direct JOINs`` rule
        # text already baked into the synthesis prompts so even
        # custom prompts that override those rules still surface the
        # anti-pattern alongside the MV detail.
        mv_lines.append(
            "\nAnti-pattern reminder for the metric views above:\n"
            "  Do NOT JOIN metric views directly. Spark rejects every "
            "such query with METRIC_VIEW_JOIN_NOT_SUPPORTED.\n"
            "  Compute every required measure inside a per-MV CTE "
            "(SELECT the dims you need + MEASURE(<m>) AS <m>), then "
            "JOIN the CTE results in the outer query. Example:\n"
            "    WITH __mv_sales AS (\n"
            "      SELECT region, MEASURE(total_sales) AS total_sales\n"
            "      FROM cat.sch.mv_sales\n"
            "    )\n"
            "    SELECT s.region, s.total_sales, d.region_name\n"
            "    FROM __mv_sales s\n"
            "    JOIN cat.sch.dim_region d ON s.region = d.region_code;"
        )
    metric_views_context = "\n".join(mv_lines) if mv_lines else "(none)"

    tvfs = config.get("_functions", [])
    tvfs_context = "\n".join(
        f"- {r.get('routine_name', '')}: {r.get('routine_definition', '')[:200]}"
        for r in scoped_uc_routines
    ) if scoped_uc_routines else (
        "\n".join(f"- {t}" for t in tvfs) if tvfs else "(none)"
    )

    # -- Join specifications --
    inst = parsed_space.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    ds_js = parsed_space.get("data_sources", {})
    if not isinstance(ds_js, dict):
        ds_js = {}
    join_specs = (
        inst.get("join_specs", []) if isinstance(inst.get("join_specs"), list) else []
    ) or (
        ds_js.get("join_specs", []) if isinstance(ds_js.get("join_specs"), list) else []
    )
    if join_specs:
        js_lines: list[str] = []
        for js in join_specs:
            left = js.get("left", {})
            right = js.get("right", {})
            sql_parts = js.get("sql", [])
            predicate = sql_parts[0] if isinstance(sql_parts, list) and sql_parts else str(sql_parts)
            js_lines.append(
                f"- {left.get('identifier', '?')} <-> {right.get('identifier', '?')}: {predicate[:200]}"
            )
        join_specs_context = "\n".join(js_lines)
    else:
        join_specs_context = "(No join specifications configured.)"

    instructions = config.get("_instructions", [])
    instructions_context = "\n".join(
        f"- {i.get('text', i) if isinstance(i, dict) else i}" for i in instructions
    ) if instructions else "(none)"

    cfg_block = parsed_space.get("config", {})
    if not isinstance(cfg_block, dict):
        cfg_block = {}
    sample_questions = cfg_block.get("sample_questions", [])
    if not isinstance(sample_questions, list) or not sample_questions:
        # Legacy serialized spaces stored sample_questions at the top level;
        # keep that fallback so older fixtures still render.
        legacy = parsed_space.get("sample_questions", [])
        if isinstance(legacy, list):
            sample_questions = legacy
    sample_questions_context = "\n".join(
        f"- {_coerce_question_text(q.get('question', q) if isinstance(q, dict) else q)}"
        for q in sample_questions
    ) if sample_questions else "(none)"

    columns_by_table: dict[str, list[str]] = {}
    for c in scoped_uc_columns:
        if not isinstance(c, dict):
            continue
        tbl = str(c.get("table_name") or "").strip()
        col = str(c.get("column_name") or "").strip()
        dtype = str(c.get("data_type") or "").strip().upper()
        if tbl and col:
            entry = f"{col} ({dtype})" if dtype else col
            columns_by_table.setdefault(tbl, []).append(entry)
    column_allowlist_lines: list[str] = []
    for tbl_name in sorted(columns_by_table):
        column_allowlist_lines.append(f"{tbl_name}: {', '.join(columns_by_table[tbl_name])}")
    column_allowlist = "\n".join(column_allowlist_lines) if column_allowlist_lines else "(no columns)"

    return {
        "tables_context": tables_context,
        "metric_views_context": metric_views_context,
        "tvfs_context": tvfs_context,
        "join_specs_context": join_specs_context,
        "instructions_context": instructions_context,
        "sample_questions_context": sample_questions_context,
        "valid_assets_context": _build_valid_assets_context(config),
        "column_allowlist": column_allowlist,
        "data_profile_context": _format_data_profile_context(
            config,
            _filter_data_profile_to_space_assets(config),
        ),
    }

def _validate_benchmark_sql(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    execute: bool = False,
    w: Any = None,
    warehouse_id: str = "",
) -> tuple[bool, str]:
    """Validate a benchmark's expected_sql. Returns (is_valid, error)."""
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    resolved = resolve_sql(sql, catalog, schema)
    sanitized = sanitize_sql(resolved)
    if not sanitized.strip():
        return False, "Empty SQL"
    return validate_ground_truth_sql(
        sanitized, spark, catalog=catalog, gold_schema=schema, execute=execute,
        w=w, warehouse_id=warehouse_id,
    )


def _index_question_candidates(
    candidates: list[dict],
    *,
    fallback_prefix: str,
) -> list[tuple[str, dict]]:
    """Assign an unambiguous request-local identity to each candidate.

    Existing benchmark IDs are preserved when available. The request-local
    fallback is positional, so SQL-only LLM responses can be merged back onto
    the exact input row without matching on mutable question text.
    """
    indexed: list[tuple[str, dict]] = []
    used: set[str] = set()
    for index, candidate in enumerate(candidates):
        base = str(
            candidate.get("question_id")
            or candidate.get("id")
            or candidate.get("space_question_id")
            or f"{fallback_prefix}_{index + 1:03d}"
        ).strip()
        if not base:
            base = f"{fallback_prefix}_{index + 1:03d}"
        question_id = base
        suffix = 2
        while question_id in used:
            question_id = f"{base}__{suffix}"
            suffix += 1
        used.add(question_id)
        indexed.append((question_id, candidate))
    return indexed

def _attempt_sql_correction(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    invalid_candidates: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    allowlist: dict[str, Any],
    *,
    correction_prompt_template: str,
    correction_prompt_key: str,
    warehouse_id: str = "",
    repair_counters: dict[str, int] | None = None,
) -> list[dict]:
    """Send invalid SQL candidates back to the LLM for correction.

    Shared between benchmark and example-SQL generation paths. Callers
    differ only in the local prompt template and stable trace key — the
    per-candidate error payload (``benchmarks_to_fix`` JSON), the
    schema context, the metadata + SQL revalidation, and the returned
    provenance are all identical. Returns corrected candidates that
    pass both ``_enforce_metadata_constraints`` and
    ``_validate_benchmark_sql`` (the latter named historically; it is
    generic EXPLAIN+execute validation).

    Note: the LLM output field is still ``expected_sql`` regardless of
    caller, because the correction-prompt contracts (both benchmark and
    example variants) share that schema.

    ``repair_counters`` (optional): when provided, F8 deterministic
    repairs (stem qualification + MEASURE() wrapping) are counted
    under the keys ``repaired_stemmed_identifiers`` and
    ``repaired_measure_refs``. The unified pipeline threads this dict
    so its summary banner can surface the same F4/F5 counters that the
    preflight pipeline already displays. When ``None``, repairs still
    fire (they can only help) but the counts are discarded.
    """
    if not invalid_candidates:
        return []

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    indexed_candidates = _index_question_candidates(
        invalid_candidates,
        fallback_prefix="repair",
    )
    candidates_by_id = {question_id: b for question_id, b in indexed_candidates}

    def _benchmark_payload(question_id: str, b: dict) -> dict:
        err_str = str(b.get("validation_error", "") or "")
        # PR 16: emit class-specific repair hints so the LLM gets a
        # deterministic nudge toward the correct fix instead of
        # re-deriving the diagnosis from the raw error string. Reuse
        # the validation reason code already attached to the
        # benchmark when available (avoids a re-classification round
        # trip); fall back to classifying the error string when the
        # caller didn't pre-classify.
        reason = str(b.get("validation_reason_code") or "").strip()
        if not reason:
            reason = _classify_sql_validation_error(err_str)
        repair_hint = _repair_hint_for_reason(reason)
        execution_note = (
            "Query returns 0 rows — pick realistic filter values from the Data Profile"
            if err_str == "Query returns 0 rows"
            else ""
        )
        return {
            "question_id": question_id,
            "question": b["question"],
            "original_expected_sql": b["expected_sql"],
            "error": err_str or "unknown",
            "validation_reason_code": reason,
            "repair_hint": repair_hint,
            "execution_note": execution_note,
        }

    benchmarks_to_fix = json.dumps(
        [_benchmark_payload(question_id, b) for question_id, b in indexed_candidates],
        indent=2,
    )

    prompt = format_mlflow_template(
        correction_prompt_template,
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
        column_allowlist=ctx.get("column_allowlist", "(no columns)"),
        metric_views_context=ctx.get("metric_views_context", "None"),
        tvfs_context=ctx.get("tvfs_context", "None"),
        join_specs_context=ctx.get("join_specs_context", "None"),
        data_profile_context=ctx.get("data_profile_context", "(no data profile available)"),
        benchmarks_to_fix=benchmarks_to_fix,
    )
    assert "{{ join_specs_context }}" not in prompt, (
        "SQL correction prompt rendered with unresolved join_specs_context"
    )

    try:
        with mlflow.start_span(
            name="benchmark_correction", span_type=SpanType.CHAIN,
        ) as _corr_span:
            try:
                _corr_span.set_inputs({
                    "candidate_count": len(invalid_candidates),
                    "prompt_template": correction_prompt_key,
                })
            except Exception:
                pass
            response = _call_llm_for_scoring(w, prompt)
            try:
                _corr_span.set_outputs({
                    "correction_count": (
                        len(response) if isinstance(response, list)
                        else len(response.get("benchmarks", []))
                    ),
                })
            except Exception:
                pass
        corrections: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning(
            "SQL correction LLM call failed (prompt_template=%s)",
            correction_prompt_key,
            exc_info=True,
        )
        return []

    # F8 — prepare the identifier/measure universes ONCE for the
    # whole correction batch so the per-candidate repair loop below
    # stays O(1) per candidate in dict-lookup terms. Both helpers are
    # side-effect-free so an empty universe is a clean no-op.
    from genie_space_optimizer.optimization.sql_identifier_repair import (
        repair_stemmed_identifiers_in_sql,
    )

    # Canonical identifiers come from ``config`` (the source of truth)
    # NOT ``allowlist["assets"]``. The allowlist expands every asset
    # into its short-form variants via ``_identifier_candidates`` for
    # metadata enforcement — e.g. ``cat.sch.mv`` also registers
    # ``sch.mv`` and ``mv``. Feeding those short forms into the
    # stem-repair helper would make the leaf stem point to multiple
    # "canonicals", marking it ambiguous and blocking the rewrite
    # exactly where production needs it to fire. Using the config's
    # primary identifiers keeps the unified repair logic 1:1 with
    # ``_repair_stemmed_identifiers`` in the preflight pipeline.
    canonical_assets: list[str] = []
    for key in ("_tables", "_metric_views"):
        for ident in config.get(key, []) or []:
            ident_s = str(ident).strip()
            if ident_s and ident_s not in canonical_assets:
                canonical_assets.append(ident_s)

    mv_measures = build_metric_view_measures(config)
    table_columns = build_table_columns(config)
    # PR 26 — short MV-name set for the direct-JOIN repair, mirrors
    # the unified synthesis path so the correction pipeline applies
    # the same CTE-first rewriter when the LLM's corrected SQL still
    # emits a direct JOIN against an MV.
    _mv_names_corr = effective_metric_view_identifiers_with_catalog(config)
    mv_short_set_corr: set[str] = {
        str(n).split(".")[-1].lower() for n in (_mv_names_corr or set()) if n
    }
    mv_short_set_corr.update(
        k.lower() for k in (mv_measures or {}).keys() if k
    )

    corrected: list[dict] = []
    returned_ids: set[str] = set()
    for response_item in corrections:
        if not isinstance(response_item, dict):
            continue
        question_id = str(response_item.get("question_id") or "").strip()
        original = candidates_by_id.get(question_id)
        if original is None or question_id in returned_ids:
            logger.warning(
                "Ignoring SQL correction with unknown or duplicate question_id: %s",
                question_id or "(missing)",
            )
            continue
        returned_ids.add(question_id)
        # The model is only authoritative for SQL repair. In particular, do
        # not accept model-returned question text or metadata: reconstruct the
        # candidate from the exact input row selected by stable identity.
        c = {
            **original,
            "expected_sql": response_item.get("expected_sql"),
            "unfixable_reason": response_item.get("unfixable_reason"),
        }
        sql = c.get("expected_sql")
        if not sql or c.get("unfixable_reason"):
            logger.info(
                "Candidate unfixable: %s — %s",
                str(c.get("question", ""))[:60],
                c.get("unfixable_reason", ""),
            )
            continue

        # F8 — apply the same deterministic repairs the preflight
        # pipeline runs (F4 stem qualification + F5 MEASURE() wrap)
        # to the LLM's corrected output BEFORE metadata/execute
        # validation. This closes the gap the field log surfaced:
        # the unified pipeline rejected candidates with bare table
        # stems (e.g. ``FROM dim_date`` when the allowlist held
        # ``cat.sch.mv_<domain>_dim_date``) even though the preflight
        # pipeline would have repaired them deterministically. Now
        # both pipelines handle the same failure shape identically.
        sql_str = str(sql)
        repaired_sql, stem_subs = repair_stemmed_identifiers_in_sql(
            sql_str, canonical_assets,
        )
        if stem_subs:
            sql_str = repaired_sql
            c["expected_sql"] = sql_str
            if repair_counters is not None:
                repair_counters["repaired_stemmed_identifiers"] = (
                    repair_counters.get("repaired_stemmed_identifiers", 0)
                    + len(stem_subs)
                )
        if mv_measures:
            wrapped_sql = _rewrite_measure_refs(sql_str, mv_measures)
            if wrapped_sql != sql_str:
                # ``_rewrite_measure_refs`` doesn't return a list of
                # wraps; count the net diff in MEASURE( occurrences
                # as a proxy. This matches the counter semantics the
                # preflight pipeline uses (count of rewrites applied).
                before_count = len(
                    re.findall(r"\bMEASURE\s*\(", sql_str, re.IGNORECASE),
                )
                after_count = len(
                    re.findall(r"\bMEASURE\s*\(", wrapped_sql, re.IGNORECASE),
                )
                sql_str = wrapped_sql
                c["expected_sql"] = sql_str
                if repair_counters is not None and after_count > before_count:
                    repair_counters["repaired_measure_refs"] = (
                        repair_counters.get("repaired_measure_refs", 0)
                        + (after_count - before_count)
                    )
            # Fix 5: strip ``SUM(MEASURE(x))`` → ``MEASURE(x)``. Runs
            # AFTER the measure-wrap so wraps the LLM emitted directly
            # (no outer agg) aren't double-touched, and wraps the
            # rewriter just inserted are normalised the same way as
            # those the LLM wrote inline.
            stripped_sql, strip_count = _strip_outer_agg_around_measure(
                sql_str,
            )
            if strip_count:
                sql_str = stripped_sql
                c["expected_sql"] = sql_str
                if repair_counters is not None:
                    repair_counters["stripped_outer_aggregate_around_measure"] = (
                        repair_counters.get(
                            "stripped_outer_aggregate_around_measure", 0,
                        ) + strip_count
                    )
        # PR 15: rename ``MEASURE(col) AS col`` to avoid Spark's
        # MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.
        sql_str, _alias_fixes = _repair_measure_alias_collisions(sql_str)
        if _alias_fixes and repair_counters is not None:
            repair_counters["repaired_measure_alias_collisions"] = (
                repair_counters.get("repaired_measure_alias_collisions", 0)
                + _alias_fixes
            )
            c["expected_sql"] = sql_str

        # PR 20: lift measure-column references out of WHERE into a
        # CTE-first pattern. Conservative: no-op when no measure appears
        # in WHERE, when the SQL already has a WITH clause, when there's
        # an outer JOIN / set-op / subquery, or when sqlglot can't parse.
        if mv_measures:
            sql_str, _where_lifts = _repair_measure_in_where(sql_str, mv_measures)
            if _where_lifts and repair_counters is not None:
                repair_counters["repaired_measure_in_where"] = (
                    repair_counters.get("repaired_measure_in_where", 0)
                    + _where_lifts
                )
                c["expected_sql"] = sql_str

        # PR 26 — apply the same CTE-first rewriter for direct JOINs
        # on metric views. When the LLM correction round still emits a
        # raw MV-on-X join, hoist each MV into a CTE before the
        # downstream EXPLAIN/execute gate so we don't wastefully
        # re-prompt for a fix the rewriter can apply deterministically.
        if mv_short_set_corr:
            _join_reason = _check_metric_view_join_pre(
                sql_str, mv_short_set_corr,
            )
            if _join_reason:
                repaired_sql_join, _join_wraps = _repair_metric_view_join(
                    sql_str, mv_short_set_corr, mv_measures,
                )
                if _join_wraps:
                    sql_str = repaired_sql_join
                    c["expected_sql"] = sql_str
                    if repair_counters is not None:
                        repair_counters["repaired_metric_view_join"] = (
                            repair_counters.get(
                                "repaired_metric_view_join", 0,
                            ) + _join_wraps
                        )

        # Fix 3b: short-circuit candidates with dangling qualifiers
        # (``<qual>.<col>`` where ``qual`` is neither a FROM/JOIN table,
        # an explicit alias, nor a struct column on any FROM table).
        # The most common shape we want to catch is the LLM analogising
        # a real struct field (``dim_location.region``) onto a separate
        # dim table (``dim_date.year``) — these always fail the EXPLAIN
        # gate downstream, so we save the round-trip by rejecting here.
        # Auto-injecting JOINs is intentionally out of scope (would
        # require trustworthy FK direction inference); the rejection
        # alone is high signal for the strategist on the next loop.
        if table_columns:
            unresolved = _check_dangling_qualifiers(sql_str, table_columns)
            if unresolved:
                c["unfixable_reason"] = (
                    f"unresolved_qualifier: {','.join(unresolved)} "
                    "(not in FROM/aliases/struct cols)"
                )
                if repair_counters is not None:
                    repair_counters["rejected_unresolved_qualifier"] = (
                        repair_counters.get("rejected_unresolved_qualifier", 0)
                        + 1
                    )
                logger.info(
                    "Candidate rejected for dangling qualifier(s): %s — %s",
                    c.get("question", "")[:60], c["unfixable_reason"],
                )
                continue
        sql = sql_str

        # The repair response is intentionally SQL-only. Refresh auxiliary
        # table metadata from that SQL and discard stale model-authored column
        # annotations from the invalid input; otherwise a corrected query can
        # still be rejected solely because its old ``required_*`` hints refer
        # to the pre-repair SQL. SQL metadata enforcement below remains the
        # authoritative safety check.
        c["required_tables"] = sorted(_extract_sql_asset_references(sql_str))
        c["required_columns"] = []

        metadata_ok, _reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=c,
            sql=str(sql),
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.warning(
                "Corrected candidate violates metadata constraints: %s — %s",
                c.get("question", "")[:60],
                reason_message,
            )
            continue
        is_valid, err = _validate_benchmark_sql(
            sql, spark, catalog, schema,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            c["provenance"] = "auto_corrected"
            c["validation_status"] = "valid"
            c["validation_reason_code"] = "ok"
            c["validation_error"] = None
            c["correction_source"] = "llm_correction"
            corrected.append(c)
        else:
            logger.warning(
                "Corrected candidate still invalid: %s — %s", c.get("question", "")[:60], err,
            )
    return corrected

def _attempt_benchmark_correction(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    invalid_benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    allowlist: dict[str, Any],
    *,
    warehouse_id: str = "",
) -> list[dict]:
    """Benchmark-variant adapter for :func:`_attempt_sql_correction`.

    Preserves the historical signature + behaviour so existing call
    sites inside :func:`generate_benchmarks` (including the alignment
    correction loop) stay byte-identical post-refactor.
    """
    return _attempt_sql_correction(
        w=w, config=config, uc_columns=uc_columns, uc_routines=uc_routines,
        invalid_candidates=invalid_benchmarks,
        catalog=catalog, schema=schema, spark=spark, allowlist=allowlist,
        correction_prompt_template=BENCHMARK_CORRECTION_PROMPT,
        correction_prompt_key="benchmark_correction",
        warehouse_id=warehouse_id,
    )

MAX_CORRECTION_ROUNDS = 2

_SQL_REFERENCE_PATTERN = re.compile(
    r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`\.`[^`]+`\.`[^`]+`"
    r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*)",
    re.IGNORECASE,
)

_SQL_FQ_ROUTINE_CALL_PATTERN = re.compile(
    r"(?<![\w`])"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\.\s*"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\.\s*"
    r"(`[^`]+`|[A-Za-z_]\w*)\s*\(",
    re.IGNORECASE,
)

def _clean_sql_identifier_part(value: str) -> str:
    return (value or "").strip().strip("`").lower()

def _extract_fully_qualified_routine_calls(sql: str) -> set[str]:
    """Return fully qualified routine calls like ``catalog.schema.name(``.

    The extractor is intentionally catalog/schema-independent. Benchmark
    provenance must not depend on the optimizer's current SQL context because
    the failing 7now case used a valid physical UC routine that was not a
    Genie Agent asset.
    """
    calls: set[str] = set()
    for match in _SQL_FQ_ROUTINE_CALL_PATTERN.finditer(sql or ""):
        catalog = _clean_sql_identifier_part(match.group(1))
        schema = _clean_sql_identifier_part(match.group(2))
        name = _clean_sql_identifier_part(match.group(3))
        if catalog and schema and name:
            calls.add(f"{catalog}.{schema}.{name}")
    return calls

def _benchmark_space_routine_violations(sql: str, config: dict) -> list[str]:
    """Return fully-qualified routine calls not registered in the Genie Agent."""
    calls = _extract_fully_qualified_routine_calls(sql)
    if not calls:
        return []
    allowed = _space_function_candidates(config)
    violations: list[str] = []
    for call in sorted(calls):
        if not (_identifier_candidates(call) & allowed):
            violations.append(call)
    return violations

def _mark_function_not_in_space_if_needed(candidate: dict, config: dict) -> bool:
    """Mark a benchmark candidate invalid when SQL calls unregistered routines.

    Returns ``True`` when the candidate was mutated (i.e. a routine the
    Genie Agent does not own was found). The candidate's
    ``validation_status``, ``validation_reason_code``, ``validation_error``,
    and ``quarantine_reason_*`` keys are stamped so downstream consumers
    treat it as a quarantined invalid benchmark rather than a Genie failure.
    """
    violations = _benchmark_space_routine_violations(
        str(candidate.get("expected_sql") or ""),
        config,
    )
    if not violations:
        return False

    message = (
        "Benchmark SQL references routine(s) that exist in UC but are not "
        f"registered in this Genie Agent: {violations[:5]}"
    )
    candidate["validation_status"] = "invalid"
    candidate["validation_reason_code"] = "function_not_in_space"
    candidate["validation_error"] = message
    candidate["quarantine_reason_code"] = "function_not_in_space"
    candidate["quarantine_reason_detail"] = message
    candidate["unregistered_routines"] = violations
    return True

def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", (value or "").lower())

def _identifier_candidates(value: str) -> set[str]:
    cleaned = (value or "").replace("`", "").strip().lower()
    if not cleaned:
        return set()
    parts = [p for p in cleaned.split(".") if p]
    candidates = {cleaned}
    if parts:
        candidates.add(parts[-1])
    if len(parts) >= 2:
        candidates.add(".".join(parts[-2:]))
    return candidates

def _build_metadata_allowlist(
    *,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, Any]:
    allowed_assets: set[str] = set()
    allowed_columns: set[str] = set()
    normalized_to_column: dict[str, str] = {}
    allowed_routines: set[str] = set()

    for key in ("_tables", "_metric_views", "_functions"):
        for raw in config.get(key, []) if isinstance(config.get(key), list) else []:
            if not raw:
                continue
            allowed_assets.update(_identifier_candidates(str(raw)))

    scoped_columns = _filter_uc_columns_to_space_assets(config, uc_columns)
    scoped_routines = _filter_uc_routines_to_space_functions(config, uc_routines)

    for col in scoped_columns:
        if not isinstance(col, dict):
            continue
        col_name = str(col.get("column_name") or "").strip()
        table_name = str(col.get("table_name") or "").strip()
        if col_name:
            allowed_columns.add(col_name.lower())
            normalized_to_column.setdefault(_normalize_name(col_name), col_name)
        if table_name and col_name:
            fq_col = f"{table_name}.{col_name}".lower()
            allowed_columns.add(fq_col)
            normalized_to_column.setdefault(_normalize_name(fq_col), f"{table_name}.{col_name}")

    for routine in scoped_routines:
        if not isinstance(routine, dict):
            continue
        raw_name = str(
            routine.get("routine_name")
            or routine.get("specific_name")
            or ""
        ).strip()
        if not raw_name:
            continue
        allowed_routines.update(_identifier_candidates(raw_name))

    for fn in config.get("_functions", []) if isinstance(config.get("_functions"), list) else []:
        allowed_routines.update(_identifier_candidates(str(fn)))

    return {
        "assets": allowed_assets,
        "columns": allowed_columns,
        "column_index": normalized_to_column,
        "routines": allowed_routines,
    }

def _extract_sql_asset_references(sql: str) -> set[str]:
    refs: set[str] = set()
    text = sql or ""
    for match in _SQL_REFERENCE_PATTERN.finditer(text):
        # Skip TVF-style references — anything immediately followed by an
        # opening paren is a function call. Routine validation handles those
        # via _extract_sql_function_calls / allowlist["routines"], so we
        # don't want to double-count them as unknown assets.
        end = match.end()
        if end < len(text) and text[end] == "(":
            continue
        refs.update(_identifier_candidates(match.group(1)))
    return refs

_JOIN_TABLE_RE = re.compile(
    r"\bFROM\s+[`\"]?(\w+(?:\.\w+)*)[`\"]?"
    r"|\bJOIN\s+[`\"]?(\w+(?:\.\w+)*)[`\"]?",
    re.IGNORECASE,
)

def _extract_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract normalized ``(table_a, table_b)`` pairs from JOIN clauses."""
    refs = [
        (m.group(1) or m.group(2)).replace("`", "").split(".")[-1].lower()
        for m in _JOIN_TABLE_RE.finditer(sql)
    ]
    pairs: set[tuple[str, str]] = set()
    for i in range(1, len(refs)):
        a, b = sorted([refs[0], refs[i]])
        pairs.add((a, b))
    return pairs

def _compute_asset_coverage(
    benchmarks: list[dict],
    config: dict,
) -> dict[str, Any]:
    """Identify which Genie Agent assets have/lack benchmark coverage.

    Collects covered assets from ``required_tables`` and ``expected_sql``
    SQL references across all benchmarks, then diffs against the full asset
    list from the Genie Agent config.

    Returns a dict with ``covered``, ``uncovered_tables``,
    ``uncovered_mvs``, ``uncovered_functions``, and ``uncovered_joins``
    sets (leaf-name normalised).
    """
    covered: set[str] = set()
    covered_join_pairs: set[tuple[str, str]] = set()
    for b in benchmarks:
        for tbl in b.get("required_tables", []):
            covered.update(_identifier_candidates(str(tbl)))
        sql = str(b.get("expected_sql") or "")
        if sql:
            covered.update(_extract_sql_asset_references(sql))
            covered_join_pairs.update(_extract_join_pairs(sql))

    def _leaf(name: str) -> str:
        parts = name.replace("`", "").strip().split(".")
        return parts[-1].lower() if parts else ""

    all_tables = {_leaf(t) for t in config.get("_tables", []) if t}
    all_mvs = {_leaf(m) for m in config.get("_metric_views", []) if m}
    all_functions = {_leaf(f) for f in config.get("_functions", []) if f}

    covered_leaves = {_leaf(c) for c in covered if c}

    # Configured join pairs from Genie Agent join specs
    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}
    _inst = parsed_space.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    _ds = parsed_space.get("data_sources", {})
    if not isinstance(_ds, dict):
        _ds = {}
    join_specs = (
        _inst.get("join_specs", []) if isinstance(_inst.get("join_specs"), list) else []
    ) or (
        _ds.get("join_specs", []) if isinstance(_ds.get("join_specs"), list) else []
    )
    configured_join_pairs: set[tuple[str, str]] = set()
    for js in join_specs:
        l_name = _leaf(js.get("left", {}).get("identifier", ""))
        r_name = _leaf(js.get("right", {}).get("identifier", ""))
        if l_name and r_name:
            pair: tuple[str, str] = (min(l_name, r_name), max(l_name, r_name))
            configured_join_pairs.add(pair)

    return {
        "covered": covered_leaves,
        "uncovered_tables": all_tables - covered_leaves,
        "uncovered_mvs": all_mvs - covered_leaves,
        "uncovered_functions": all_functions - covered_leaves,
        "uncovered_joins": configured_join_pairs - covered_join_pairs,
    }

def _fill_coverage_gaps(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: "SparkSession",
    allowlist: dict[str, Any],
    domain: str,
    existing_questions: set[str],
    category_performance: dict[str, dict] | None = None,
    *,
    warehouse_id: str = "",
    target_benchmark_count: int = TARGET_BENCHMARK_COUNT,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> list[dict]:
    """Generate targeted benchmarks for Genie Agent assets with zero coverage.

    Runs after the main generation pipeline. Identifies uncovered assets via
    ``_compute_asset_coverage``, then makes a single LLM call asking for 1-2
    questions per uncovered asset.  Results go through the same metadata
    constraint and SQL validation pipeline as normal benchmarks.

    When *category_performance* is provided, categories performing below the
    median accuracy are highlighted in the prompt so the LLM prioritises
    generating questions for weak areas.

    Returns only validated gap-fill benchmarks (may be empty).
    """
    soft_cap = min(
        int(target_benchmark_count * COVERAGE_GAP_SOFT_CAP_FACTOR),
        max_benchmark_count,
    )
    if len(benchmarks) >= soft_cap:
        logger.info(
            "Skipping coverage gap-fill: benchmark count %d already at soft cap %d",
            len(benchmarks), soft_cap,
        )
        return []

    coverage = _compute_asset_coverage(benchmarks, config)
    uncovered_tables = coverage["uncovered_tables"]
    uncovered_mvs = coverage["uncovered_mvs"]
    uncovered_functions = coverage["uncovered_functions"]
    uncovered_joins: set[tuple[str, str]] = coverage.get("uncovered_joins", set())

    if not uncovered_tables and not uncovered_mvs and not uncovered_functions and not uncovered_joins:
        logger.info("All Genie Agent assets and join paths already covered by benchmarks")
        return []

    # Prioritise MVs and TVFs (higher routing-issue risk), then tables, then joins.
    budget = soft_cap - len(benchmarks)
    ordered_uncovered: list[str] = []
    for mv in sorted(uncovered_mvs):
        ordered_uncovered.append(f"METRIC VIEW: {mv}")
    for fn in sorted(uncovered_functions):
        ordered_uncovered.append(f"FUNCTION: {fn}")
    for tbl in sorted(uncovered_tables):
        ordered_uncovered.append(f"TABLE: {tbl}")
    for left, right in sorted(uncovered_joins):
        ordered_uncovered.append(f"JOIN PATH: {left} <-> {right}")

    # Each uncovered asset targets ~2 questions; trim to budget.
    max_assets = max(budget // 2, 1)
    targeted = ordered_uncovered[:max_assets]

    logger.info(
        "Coverage gap-fill: %d uncovered items (%d tables, %d MVs, %d functions, %d join paths). "
        "Targeting %d within budget of %d.",
        len(ordered_uncovered), len(uncovered_tables),
        len(uncovered_mvs), len(uncovered_functions), len(uncovered_joins),
        len(targeted), budget,
    )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    existing_q_lines = "\n".join(f"- {q}" for q in sorted(existing_questions)) or "(none)"
    uncovered_lines = "\n".join(f"- {a}" for a in targeted)

    weak_categories_context = ""
    if category_performance:
        accuracies = []
        for cat, stats in category_performance.items():
            if cat == "unknown" or stats.get("total", 0) == 0:
                continue
            accuracies.append(stats["correct"] / stats["total"])
        if accuracies:
            median_acc = sorted(accuracies)[len(accuracies) // 2]
            weak_lines = []
            for cat, stats in sorted(category_performance.items()):
                total = stats.get("total", 0)
                if total == 0 or cat == "unknown":
                    continue
                acc = stats["correct"] / total
                if acc < median_acc:
                    weak_lines.append(
                        f"- {cat}: {stats['correct']}/{total} correct ({acc:.0%})"
                    )
            if weak_lines:
                weak_categories_context = (
                    "## Weak Categories (prioritize these)\n"
                    + "\n".join(weak_lines)
                )

    prompt = format_mlflow_template(
        BENCHMARK_COVERAGE_GAP_PROMPT,
        domain=domain,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        uncovered_assets=uncovered_lines,
        existing_questions=existing_q_lines,
        weak_categories_context=weak_categories_context,
        **ctx,
    )

    try:
        response = _call_llm_for_scoring(w, prompt)
        raw: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning("Coverage gap-fill LLM call failed", exc_info=True)
        return []

    valid: list[dict] = []
    for b in raw:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in existing_questions:
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"),
                expected_sql,
                hint=b.get("expected_asset_hint"),
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "coverage_gap_fill",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        metadata_ok, _reason_code, _reason_msg = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.debug(
                "Gap-fill benchmark failed metadata constraints: %s",
                str(benchmark.get("question", ""))[:60],
            )
            continue

        _mv_names = effective_metric_view_identifiers_with_catalog(config)
        _is_star_ok, _ = _guard_mv_select_star(expected_sql, _mv_names)
        if not _is_star_ok:
            continue

        _mv_measures = build_metric_view_measures(config)
        if _mv_measures:
            expected_sql = _rewrite_measure_refs(expected_sql, _mv_measures)
            benchmark["expected_sql"] = expected_sql

        is_valid, err = _validate_benchmark_sql(
            expected_sql, spark, catalog, schema,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            valid.append(benchmark)
        else:
            logger.debug(
                "Gap-fill benchmark failed SQL validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    logger.info(
        "Coverage gap-fill complete: %d valid out of %d generated for %d uncovered assets",
        len(valid), len(raw), len(targeted),
    )
    return valid

def _suggest_column_name(column: str, allowed_index: dict[str, str]) -> str | None:
    if not column:
        return None
    normalized = _normalize_name(column)
    if not normalized:
        return None
    exact = allowed_index.get(normalized)
    if exact:
        return exact
    candidates = list(allowed_index.keys())
    if not candidates:
        return None
    closest = get_close_matches(normalized, candidates, n=1, cutoff=0.72)
    if not closest:
        return None
    return allowed_index.get(closest[0])

def _apply_metadata_field_drift_corrections(
    *,
    sql: str,
    required_columns: list[str],
    allowed_index: dict[str, str],
) -> tuple[str, list[dict[str, str]]]:
    corrected_sql = sql
    applied: list[dict[str, str]] = []
    seen: set[str] = set()

    for col in required_columns:
        token = str(col or "").strip()
        if not token:
            continue
        col_leaf = token.split(".")[-1]
        if not col_leaf:
            continue
        key = col_leaf.lower()
        if key in seen:
            continue
        seen.add(key)

        suggestion = _suggest_column_name(col_leaf, allowed_index)
        if not suggestion:
            continue
        suggestion_leaf = suggestion.split(".")[-1]
        if suggestion_leaf.lower() == col_leaf.lower():
            continue

        pattern = re.compile(rf"(?i)\b{re.escape(col_leaf)}\b")
        updated_sql, count = pattern.subn(suggestion_leaf, corrected_sql)
        if count > 0:
            corrected_sql = updated_sql
            applied.append(
                {
                    "from": col_leaf,
                    "to": suggestion_leaf,
                    "reason": "metadata_field_drift",
                }
            )

    return corrected_sql, applied

def _enforce_metadata_constraints(
    *,
    benchmark: dict,
    sql: str,
    allowlist: dict[str, Any],
    catalog: str,
    schema: str,
) -> tuple[bool, str, str]:
    refs = _extract_sql_asset_references(sql)
    unknown_refs = sorted(ref for ref in refs if ref not in allowlist["assets"])
    if unknown_refs:
        return (
            False,
            "unknown_asset",
            f"SQL references assets not found in metadata: {unknown_refs[:5]}",
        )

    required_tables = benchmark.get("required_tables", [])
    if isinstance(required_tables, list):
        bad_required_tables: list[str] = []
        for item in required_tables:
            candidates = _identifier_candidates(str(item))
            if candidates and not any(c in allowlist["assets"] for c in candidates):
                bad_required_tables.append(str(item))
        if bad_required_tables:
            return (
                False,
                "unknown_asset",
                f"required_tables contains unknown assets: {bad_required_tables[:5]}",
            )

    required_columns = benchmark.get("required_columns", [])
    if isinstance(required_columns, list):
        bad_columns: list[str] = []
        for col in required_columns:
            raw = str(col or "").strip()
            if not raw:
                continue
            col_candidates = _identifier_candidates(raw)
            if any(c in allowlist["columns"] for c in col_candidates):
                continue
            leaf = raw.split(".")[-1].lower()
            if leaf in allowlist["columns"]:
                continue
            bad_columns.append(raw)
        if bad_columns:
            return (
                False,
                "unknown_column",
                f"required_columns contains unknown metadata fields: {bad_columns[:8]}",
            )

    called_functions = _extract_sql_function_calls(sql, catalog, schema)
    unknown_functions = sorted(fn for fn in called_functions if fn not in allowlist["routines"])
    if unknown_functions:
        return (
            False,
            "unknown_routine",
            f"SQL references routines not found in metadata: {unknown_functions[:5]}",
        )

    return True, "ok", ""

def _generate_sql_for_curated_questions(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    question_only_benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    *,
    warehouse_id: str = "",
) -> list[dict]:
    """Generate and validate expected SQL for curated questions that lack it.

    Uses the same LLM + validation pipeline as synthetic benchmark generation.
    Questions that fail SQL generation after retries are dropped.

    Returns only benchmarks that ended up with valid ``expected_sql``.
    """
    if not question_only_benchmarks:
        return []

    from genie_space_optimizer.common.config import (
        CURATED_SQL_GENERATION_PROMPT,
        CURATED_SQL_GENERATION_MAX_RETRIES,
        format_mlflow_template,
    )
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    indexed_questions = _index_question_candidates(
        question_only_benchmarks,
        fallback_prefix="curated",
    )
    questions_by_id = {
        question_id: benchmark for question_id, benchmark in indexed_questions
    }
    questions_json = json.dumps(
        [
            {"question_id": question_id, "question": b["question"]}
            for question_id, b in indexed_questions
        ],
        indent=2,
    )

    prompt = format_mlflow_template(
        CURATED_SQL_GENERATION_PROMPT,
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
        column_allowlist=ctx.get("column_allowlist", "(no columns)"),
        metric_views_context=ctx.get("metric_views_context", "None"),
        tvfs_context=ctx.get("tvfs_context", "None"),
        join_specs_context=ctx.get("join_specs_context", "None"),
        instructions_context=ctx.get("instructions_context", "None"),
        data_profile_context=ctx.get("data_profile_context", "(no data profile available)"),
        questions_json=questions_json,
    )

    try:
        response = _call_llm_for_scoring(w, prompt)
        generated: list[dict] = (
            response if isinstance(response, list) else response.get("benchmarks", [])
        )
    except Exception:
        logger.warning("Curated SQL generation LLM call failed", exc_info=True)
        return []

    enriched: list[dict] = []
    returned_ids: set[str] = set()

    for g in generated:
        if not isinstance(g, dict):
            continue
        question_id = str(g.get("question_id") or "").strip()
        original = questions_by_id.get(question_id)
        if original is None or question_id in returned_ids:
            logger.warning(
                "Ignoring curated SQL result with unknown or duplicate question_id: %s",
                question_id or "(missing)",
            )
            continue
        returned_ids.add(question_id)
        sql = g.get("expected_sql")
        question = str(original.get("question") or "").strip()
        if not sql or g.get("unfixable_reason"):
            logger.info(
                "Curated SQL generation: unfixable '%s' — %s",
                question[:60],
                g.get("unfixable_reason", "no SQL generated"),
            )
            continue

        is_valid, err = validate_ground_truth_sql(
            sql, spark, catalog=catalog, gold_schema=schema,
            w=w, warehouse_id=warehouse_id,
        )
        if not is_valid:
            for _retry in range(CURATED_SQL_GENERATION_MAX_RETRIES):
                corrections = _attempt_benchmark_correction(
                    w, config, uc_columns, uc_routines,
                    [{
                        **original,
                        "question_id": question_id,
                        "question": question,
                        "expected_sql": sql,
                        "validation_error": err,
                    }],
                    catalog, schema, spark,
                    _build_metadata_allowlist(config=config, uc_columns=uc_columns, uc_routines=uc_routines),
                    warehouse_id=warehouse_id,
                )
                if corrections:
                    g = corrections[0]
                    sql = g.get("expected_sql", "")
                    is_valid = bool(sql)
                    break
                logger.info(
                    "Curated SQL correction attempt %d failed for '%s'",
                    _retry + 1, question[:60],
                )

        if is_valid and sql:
            enriched.append({
                **original,
                "question": question,
                "expected_sql": sql,
                "expected_asset": detect_asset_type(sql),
                "category": original.get("category", "curated"),
                "required_tables": original.get("required_tables", []),
                "required_columns": original.get("required_columns", []),
                "expected_facts": original.get("expected_facts", []),
                "source": original.get("source") or "genie_space",
                "provenance": "curated_sql_generated",
                "validation_status": "valid",
                "validation_reason_code": "ok",
                "validation_error": None,
                "correction_source": "curated_sql_generation",
            })
        else:
            logger.warning(
                "Dropping curated question (no valid SQL after retries): %s",
                question[:80],
            )

    logger.info(
        "Curated SQL generation: %d/%d questions got valid SQL",
        len(enriched), len(question_only_benchmarks),
    )

    _data_profile = config.get("_data_profile", {})
    if _data_profile and enriched:
        try:
            from genie_space_optimizer.optimization.benchmarks import (
                validate_predicate_values,
            )
            _pred_results = validate_predicate_values(enriched, _data_profile)
            for _eb, _pr in zip(enriched, _pred_results):
                if not _pr["valid"]:
                    for mm in _pr["mismatches"]:
                        if mm.get("suggestion"):
                            old_sql = _eb.get("expected_sql", "")
                            new_sql = old_sql.replace(
                                f"'{mm['literal']}'", f"'{mm['suggestion']}'",
                            )
                            if new_sql != old_sql:
                                _eb["expected_sql"] = new_sql
                                _eb["correction_source"] = "predicate_value_fix"
                                logger.info(
                                    "Curated SQL auto-corrected predicate: "
                                    "%s='%s' → '%s' in '%s'",
                                    mm["column"], mm["literal"],
                                    mm["suggestion"], _eb["question"][:60],
                                )
        except Exception as exc:
            logger.warning("Predicate value post-check skipped: %s", exc)

    return enriched

def _enforce_instruction_default_filters_on_benchmarks(
    benchmarks: list[dict],
    config: dict,
) -> int:
    """Ensure benchmarks include instruction-mandated default filters in their SQL.

    Reads default filter rules from the Genie Agent instructions and checks
    each benchmark's ``expected_sql``. If a benchmark's SQL is missing a
    mandated filter, appends it to the WHERE clause.

    Returns the count of benchmarks patched.
    """
    try:
        from genie_space_optimizer.optimization.optimizer_utils import (
            _extract_instruction_default_filters,
        )
    except ImportError:
        return 0

    parsed_space = config.get("_parsed_space", config)
    default_filters = _extract_instruction_default_filters(parsed_space)
    if not default_filters:
        return 0

    patched = 0
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        if not sql or not sql.strip():
            continue
        sql_lower = sql.lower()
        for df in default_filters:
            col = df["column"]
            val = df["value"]
            if col.lower() in sql_lower:
                continue
            if "where" in sql_lower:
                sql = re.sub(
                    r"(?i)\bWHERE\b",
                    f"WHERE {col} = '{val}' AND",
                    sql,
                    count=1,
                )
            else:
                group_match = re.search(r"(?i)\b(GROUP\s+BY|ORDER\s+BY|LIMIT)\b", sql)
                if group_match:
                    pos = group_match.start()
                    sql = sql[:pos] + f"WHERE {col} = '{val}' " + sql[pos:]
                else:
                    sql = sql.rstrip().rstrip(";") + f" WHERE {col} = '{val}'"
            b["expected_sql"] = sql
            b["_instruction_filter_patched"] = True
            patched += 1
            logger.info(
                "Added instruction-mandated filter '%s=%s' to benchmark: %s",
                col, val, b.get("question", "")[:80],
            )
    return patched

def _compute_synthetic_target(
    *,
    target_count: int,
    curated_count: int,
    existing_count: int,
) -> int:
    """Return how many synthetic benchmarks are needed to reach target_count."""
    return max(target_count - curated_count - existing_count, 0)

def _make_benchmark_id_allocator(existing_benchmarks: list[dict]) -> Callable[[str, int], str]:
    """Return an allocator that never reuses benchmark IDs in this corpus."""
    used_ids = {
        str(b.get("id", "") or "").strip()
        for b in existing_benchmarks
        if str(b.get("id", "") or "").strip()
    }

    def allocate(prefix: str, start: int) -> str:
        idx = max(int(start), 1)
        while True:
            candidate = f"{prefix}_{idx:03d}"
            if candidate not in used_ids:
                used_ids.add(candidate)
                return candidate
            idx += 1

    return allocate

def generate_benchmarks(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    spark: SparkSession,
    target_count: int = TARGET_BENCHMARK_COUNT,
    genie_space_benchmarks: list[dict] | None = None,
    existing_benchmarks: list[dict] | None = None,
    warehouse_id: str = "",
    *,
    max_benchmark_count: int = MAX_BENCHMARK_COUNT,
) -> list[dict]:
    """Generate benchmark questions via LLM from Genie Agent context.

    Pipeline:
      1. Start with curated Genie Agent benchmarks (if provided)
      2. Calculate how many synthetic benchmarks to generate to reach target
      3. Build schema context from actual Genie Agent assets + UC metadata
      4. Call LLM with BENCHMARK_GENERATION_PROMPT (includes valid asset allowlist)
      5. Enforce strict metadata constraints (assets/routines/required fields)
      6. Run deterministic metadata drift auto-correction (field suggestions)
      7. Validate each expected_sql via EXPLAIN + table existence check
      8. Send remaining invalid benchmarks to correction LLM (bounded retries)
      9. Persist provenance + validation metadata per benchmark record

    Args:
        existing_benchmarks: Previously validated benchmarks to keep. When
            provided, these are carried forward and the generation targets
            only the gap (``target_count - len(existing_benchmarks)``).
    """
    curated = genie_space_benchmarks or []
    _existing = existing_benchmarks or []
    curated_questions = {b.get("question", "").lower().strip() for b in curated}
    existing_questions = {b.get("question", "").lower().strip() for b in _existing}
    curated_questions |= existing_questions
    synthetic_target = _compute_synthetic_target(
        target_count=min(target_count, max_benchmark_count),
        curated_count=len(curated),
        existing_count=len(_existing),
    )
    allowlist = _build_metadata_allowlist(
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
    )

    if curated:
        logger.info(
            "Starting with %d curated Genie Agent benchmarks (%d with SQL). "
            "Generating %d synthetic to reach target of %d.",
            len(curated),
            sum(1 for b in curated if b.get("expected_sql")),
            synthetic_target,
            target_count,
        )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    all_existing = list(curated) + list(_existing)
    existing_questions_context = ""
    if all_existing:
        existing_questions_context = (
            "\n\n## Already Covered Questions (do NOT duplicate these)\n"
            + "\n".join(f"- {b.get('question', '')}" for b in all_existing)
        )

    if synthetic_target > 0:
        prompt = format_mlflow_template(
            BENCHMARK_GENERATION_PROMPT,
            domain=domain,
            target_count=synthetic_target,
            categories=json.dumps(BENCHMARK_CATEGORIES),
            **ctx,
        )
        if existing_questions_context:
            prompt += existing_questions_context

        with mlflow.start_span(
            name="benchmark_generation", span_type=SpanType.CHAIN,
        ) as _bench_span:
            try:
                _bench_span.set_inputs({
                    "domain": domain,
                    "prompt_template": "benchmark_generation",
                })
            except Exception:
                pass
            response = _call_llm_for_scoring(w, prompt)
            try:
                _bench_span.set_outputs({
                    "raw_benchmark_count": (
                        len(response) if isinstance(response, list)
                        else len(response.get("benchmarks", []))
                    ),
                })
            except Exception:
                pass
        raw_benchmarks: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    else:
        logger.info(
            "Skipping synthetic benchmark generation: target met by curated/existing rows "
            "(curated=%d, existing=%d, target=%d, max=%d)",
            len(curated), len(_existing), target_count, max_benchmark_count,
        )
        raw_benchmarks = []

    valid_benchmarks: list[dict] = []
    invalid_benchmarks: list[dict] = []
    accepted_questions: set[str] = set()

    def _register_valid(candidate: dict) -> None:
        question = str(candidate.get("question") or "").strip().lower()
        if not question or question in accepted_questions or question in curated_questions:
            return
        accepted_questions.add(question)
        valid_benchmarks.append(candidate)

    for b in raw_benchmarks:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in curated_questions:
            logger.debug("Skipping synthetic duplicate of curated question: %s", q_lower[:50])
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"),
                expected_sql,
                hint=b.get("expected_asset_hint"),
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "synthetic",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        # Task 2 — quarantine benchmarks whose SQL calls a routine that is
        # physically resolvable in UC but not registered in this Genie
        # Space's ``data_sources.functions``. Genie cannot see those
        # functions at runtime, so the benchmark would otherwise produce a
        # misleading judge failure that the lever loop would chase.
        if _mark_function_not_in_space_if_needed(benchmark, config):
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark quarantined: function_not_in_space: %s — %s",
                str(benchmark.get("question", ""))[:60],
                benchmark.get("validation_error", ""),
            )
            continue

        metadata_ok, reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            # Deterministic correction for common field drift before LLM-based correction.
            if reason_code == "unknown_column":
                corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                    sql=expected_sql,
                    required_columns=[str(c) for c in benchmark.get("required_columns", [])],
                    allowed_index=allowlist["column_index"],
                )
                if replacements and corrected_sql != expected_sql:
                    candidate = dict(benchmark)
                    candidate["expected_sql"] = corrected_sql
                    candidate["provenance"] = "auto_corrected"
                    candidate["correction_source"] = "metadata_suggestion"
                    candidate["field_drift_fixes"] = replacements
                    candidate_ok, _, candidate_msg = _enforce_metadata_constraints(
                        benchmark=candidate,
                        sql=corrected_sql,
                        allowlist=allowlist,
                        catalog=catalog,
                        schema=schema,
                    )
                    if candidate_ok:
                        is_candidate_valid, candidate_err = _validate_benchmark_sql(
                            corrected_sql, spark, catalog, schema,
                            w=w, warehouse_id=warehouse_id,
                        )
                        if is_candidate_valid:
                            candidate["validation_status"] = "valid"
                            candidate["validation_reason_code"] = "ok"
                            candidate["validation_error"] = None
                            _register_valid(candidate)
                            continue
                        reason_message = candidate_err
                    else:
                        reason_message = candidate_msg

            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = reason_code
            benchmark["validation_error"] = reason_message
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed metadata constraints: %s — %s",
                str(benchmark.get("question", ""))[:60],
                reason_message,
            )
            continue

        # MV guard: reject SELECT * on metric views (PR 14: effective MVs).
        _mv_names = effective_metric_view_identifiers_with_catalog(config)
        _is_star_ok, _star_reason = _guard_mv_select_star(expected_sql, _mv_names)
        if not _is_star_ok:
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = "mv_select_star"
            benchmark["validation_error"] = _star_reason
            invalid_benchmarks.append(benchmark)
            continue

        # MV auto-fix: wrap bare measures in MEASURE()
        _mv_measures = build_metric_view_measures(config)
        if _mv_measures:
            expected_sql = _rewrite_measure_refs(expected_sql, _mv_measures)
            benchmark["expected_sql"] = expected_sql

        is_valid, err = _validate_benchmark_sql(
            expected_sql, spark, catalog, schema, execute=True,
            w=w, warehouse_id=warehouse_id,
        )
        if is_valid:
            benchmark["validation_status"] = "valid"
            benchmark["validation_reason_code"] = "ok"
            benchmark["validation_error"] = None
            _register_valid(benchmark)
        else:
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = _classify_sql_validation_error(err)
            benchmark["validation_error"] = err
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    for correction_round in range(MAX_CORRECTION_ROUNDS):
        if not invalid_benchmarks:
            break
        logger.info(
            "Correction round %d: attempting to fix %d invalid benchmarks",
            correction_round + 1, len(invalid_benchmarks),
        )
        metadata_corrected: list[dict] = []
        still_invalid: list[dict] = []
        for invalid in invalid_benchmarks:
            expected_sql = str(invalid.get("expected_sql") or "")
            if not expected_sql:
                still_invalid.append(invalid)
                continue
            corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                sql=expected_sql,
                required_columns=[str(c) for c in invalid.get("required_columns", [])],
                allowed_index=allowlist["column_index"],
            )
            if not replacements or corrected_sql == expected_sql:
                still_invalid.append(invalid)
                continue
            candidate = dict(invalid)
            candidate["expected_sql"] = corrected_sql
            candidate["field_drift_fixes"] = replacements
            candidate["provenance"] = "auto_corrected"
            candidate["correction_source"] = "metadata_suggestion_loop"
            candidate_ok, candidate_reason, candidate_message = _enforce_metadata_constraints(
                benchmark=candidate,
                sql=corrected_sql,
                allowlist=allowlist,
                catalog=catalog,
                schema=schema,
            )
            if not candidate_ok:
                candidate["validation_status"] = "invalid"
                candidate["validation_reason_code"] = candidate_reason
                candidate["validation_error"] = candidate_message
                still_invalid.append(candidate)
                continue
            candidate_valid, candidate_err = _validate_benchmark_sql(
                corrected_sql, spark, catalog, schema,
                w=w, warehouse_id=warehouse_id,
            )
            if candidate_valid:
                candidate["validation_status"] = "valid"
                candidate["validation_reason_code"] = "ok"
                candidate["validation_error"] = None
                metadata_corrected.append(candidate)
                continue
            candidate["validation_status"] = "invalid"
            candidate["validation_reason_code"] = _classify_sql_validation_error(candidate_err)
            candidate["validation_error"] = candidate_err
            still_invalid.append(candidate)

        for corrected in metadata_corrected:
            _register_valid(corrected)
        invalid_benchmarks = still_invalid
        if not invalid_benchmarks:
            break

        corrected = _attempt_benchmark_correction(
            w, config, uc_columns, uc_routines,
            invalid_benchmarks, catalog, schema, spark, allowlist,
            warehouse_id=warehouse_id,
        )
        for corrected_item in corrected:
            _register_valid(corrected_item)
        corrected_questions = {
            str(c.get("question") or "").strip().lower()
            for c in corrected
            if str(c.get("question") or "").strip()
        }
        invalid_benchmarks = [
            b for b in invalid_benchmarks
            if str(b.get("question") or "").strip().lower() not in corrected_questions
        ]

    if invalid_benchmarks:
        logger.warning(
            "Discarded %d benchmarks after %d correction rounds (unfixable): %s",
            len(invalid_benchmarks),
            MAX_CORRECTION_ROUNDS,
            [b.get("question", "")[:50] for b in invalid_benchmarks[:3]],
        )

    # ── Post-validation: check question-SQL alignment via LLM ──────────
    try:
        from genie_space_optimizer.optimization.benchmarks import (
            validate_question_sql_alignment,
        )
        alignment_targets = [b for b in valid_benchmarks if b.get("expected_sql")]
        if alignment_targets:
            alignment_results = validate_question_sql_alignment(alignment_targets)
            _newly_invalid: list[dict] = []
            for b, ar in zip(alignment_targets, alignment_results):
                if not ar.get("aligned", True):
                    b["alignment_issues"] = ar.get("issues", [])
                    b["validation_status"] = "invalid"
                    b["validation_reason_code"] = "alignment_mismatch"
                    b["validation_error"] = "; ".join(ar.get("issues", []))
                    _newly_invalid.append(b)
                    logger.warning(
                        "Benchmark REJECTED (alignment): %s -- %s",
                        b.get("question", "")[:80],
                        "; ".join(ar.get("issues", [])),
                    )
            if _newly_invalid:
                valid_benchmarks = [b for b in valid_benchmarks if b not in _newly_invalid]
                _alignment_corrected = _attempt_benchmark_correction(
                    w, config, uc_columns, uc_routines,
                    _newly_invalid, catalog, schema, spark, allowlist,
                    warehouse_id=warehouse_id,
                )
                for c in _alignment_corrected:
                    _register_valid(c)
                logger.info(
                    "Alignment check: %d rejected, %d corrected, %d discarded",
                    len(_newly_invalid), len(_alignment_corrected),
                    len(_newly_invalid) - len(_alignment_corrected),
                )
    except Exception as _align_err:
        logger.warning("Alignment validation skipped: %s", _align_err)

    all_benchmarks: list[dict] = list(_existing)
    # Reserve native benchmark IDs up front so generated rows cannot collide
    # with a curated row that must be updated in place when its SQL is repaired.
    reserved_curated_ids = [
        {
            "id": str(b.get("space_question_id") or "").strip(),
        }
        for b in curated
        if b.get("source") == "genie_benchmark"
        and str(b.get("space_question_id") or "").strip()
    ]
    allocate_benchmark_id = _make_benchmark_id_allocator(
        all_benchmarks + reserved_curated_ids,
    )

    from genie_space_optimizer.common.config import REQUIRE_GROUND_TRUTH_SQL

    curated_with_sql = [b for b in curated if str(b.get("expected_sql", "") or "").strip()]
    curated_no_sql = [b for b in curated if not str(b.get("expected_sql", "") or "").strip()]

    if curated_no_sql and REQUIRE_GROUND_TRUTH_SQL:
        logger.info(
            "Generating ground-truth SQL for %d curated question-only benchmarks",
            len(curated_no_sql),
        )
        enriched_curated = _generate_sql_for_curated_questions(
            w, config, uc_columns, uc_routines,
            curated_no_sql, catalog, schema, spark,
            warehouse_id=warehouse_id,
        )
        curated_with_sql.extend(enriched_curated)
        _dropped = len(curated_no_sql) - len(enriched_curated)
        if _dropped:
            logger.warning(
                "Dropped %d curated questions that could not get valid SQL "
                "(enriched %d/%d)",
                _dropped, len(enriched_curated), len(curated_no_sql),
            )
        _dropped_questions = [
            b["question"][:80] for b in curated_no_sql
            if b["question"].strip().lower() not in {
                e["question"].strip().lower() for e in enriched_curated
            }
        ]
        if _dropped_questions:
            logger.info(
                "Dropped curated questions: %s",
                "; ".join(_dropped_questions[:10]),
            )
    elif curated_no_sql:
        curated_with_sql.extend(curated_no_sql)

    effective_curated = curated_with_sql

    for idx, b in enumerate(effective_curated):
        native_question_id = (
            str(b.get("space_question_id") or "").strip()
            if b.get("source") == "genie_benchmark"
            else ""
        )
        question_id = native_question_id or allocate_benchmark_id(
            f"{domain}_gs", idx + 1,
        )
        priority = "P0"
        expected_sql = str(b.get("expected_sql", "") or "")
        curated_status = "question_only" if not expected_sql else str(
            b.get("validation_status", "valid"),
        )
        curated_row = {
            "id": question_id,
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"),
                expected_sql,
                hint=b.get("expected_asset_hint"),
            ),
            "expected_asset_hint": b.get("expected_asset_hint", ""),
            "category": b.get("category", "curated"),
            "required_tables": b.get("required_tables", []),
            "required_columns": b.get("required_columns", []),
            "expected_facts": b.get("expected_facts", []),
            "priority": priority,
            "split": "",
            "source": b.get("source") or "genie_space",
            "provenance": b.get("provenance") or "curated",
            "validation_status": curated_status,
            "validation_reason_code": (
                "ok" if expected_sql else "missing_expected_sql"
            ),
            "validation_error": (
                None if expected_sql
                else "No expected SQL in curated sample question"
            ),
            "correction_source": b.get("correction_source", ""),
        }
        if native_question_id:
            curated_row["space_question_id"] = native_question_id
        all_benchmarks.append(curated_row)

    offset = len(effective_curated)
    for idx, b in enumerate(valid_benchmarks):
        question_id = allocate_benchmark_id(domain, offset + idx + 1)
        priority = "P0" if idx < 3 else "P1"
        _b_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _b_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _b_esql,
                    hint=b.get("expected_asset_hint"),
                ),
                "expected_asset_hint": b.get("expected_asset_hint", ""),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": "",
                "source": b.get("source") or "llm_generated",
                "provenance": b.get("provenance") or "synthetic",
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": b.get("correction_source", ""),
            }
        )

    # ── Coverage gap-fill: ensure every asset has at least one benchmark ──
    all_accepted_questions = (
        curated_questions
        | accepted_questions
        | {str(b.get("question", "")).lower().strip() for b in _existing}
    )
    remaining_budget = max(max_benchmark_count - len(all_benchmarks), 0)
    if remaining_budget <= 0:
        gap_fill_benchmarks: list[dict] = []
    else:
        gap_fill_benchmarks = _fill_coverage_gaps(
            w=w,
            config=config,
            uc_columns=uc_columns,
            uc_routines=uc_routines,
            benchmarks=all_benchmarks,
            catalog=catalog,
            schema=schema,
            spark=spark,
            allowlist=allowlist,
            domain=domain,
            existing_questions=all_accepted_questions,
            warehouse_id=warehouse_id,
            target_benchmark_count=min(target_count, max_benchmark_count),
            max_benchmark_count=max_benchmark_count,
        )
    gap_fill_offset = len(curated) + len(valid_benchmarks)
    for idx, b in enumerate(gap_fill_benchmarks):
        question_id = allocate_benchmark_id(f"{domain}_gf", gap_fill_offset + idx + 1)
        _gf_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _gf_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"),
                    _gf_esql,
                    hint=b.get("expected_asset_hint"),
                ),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": "P1",
                "split": "",
                "source": "llm_generated",
                "provenance": "coverage_gap_fill",
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": "",
            }
        )

    # ── Post-generation: enforce instruction-mandated default filters ──
    _filter_patched = _enforce_instruction_default_filters_on_benchmarks(
        all_benchmarks, config,
    )
    if _filter_patched:
        logger.info(
            "Post-generation filter enforcement: patched %d benchmark(s) "
            "with instruction-mandated default filters",
            _filter_patched,
        )

    from genie_space_optimizer.optimization.benchmarks import assign_splits

    if len(all_benchmarks) > max_benchmark_count:
        all_benchmarks = _truncate_benchmarks(all_benchmarks, max_benchmark_count)
    all_benchmarks = assign_splits(all_benchmarks)

    logger.info(
        "Final benchmark set: %d total (%d curated from Genie Agent, "
        "%d synthetic, %d gap-fill, %d discarded out of %d raw generated)",
        len(all_benchmarks),
        len(curated),
        len(valid_benchmarks),
        len(gap_fill_benchmarks),
        len(invalid_benchmarks),
        len(raw_benchmarks),
    )
    return all_benchmarks
