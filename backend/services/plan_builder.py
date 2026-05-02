"""Parallel plan generation — builds a Genie Space plan via concurrent LLM calls.

Instead of one monolithic LLM call that generates the entire plan JSON (slow,
truncation-prone), this module splits the plan into 5 independent sections and
generates them in parallel. Each section gets a focused prompt and a small
max_tokens budget, then results are assembled programmatically.

Parallel calls:
  A: table descriptions + column_configs  (mostly programmatic, LLM for descriptions)
  B: sample_questions + text_instructions
  C: example_sqls
  D: benchmarks
  E: join_specs + measures + filters + expressions (analytics)
"""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from backend.services.auth import run_in_context
from backend.services.llm_utils import call_serving_endpoint, parse_json_from_llm_response, get_llm_model
from backend.services.create_agent_tools import _test_sql

logger = logging.getLogger(__name__)

# Thread pool size matches the 5 parallel plan sections (tables, questions,
# example_sqls, benchmarks, analytics) so all sections start simultaneously.
_CONCURRENCY = 3

# SQL validation can have up to 15 tasks (5 example_sqls + 10 benchmarks).
# Higher parallelism than plan generation since each task is a SQL warehouse call.
_VALIDATION_CONCURRENCY = 8

_METRIC_VIEW_SQL_RULES = """\
## Metric View SQL Rules
When querying a Databricks metric view:
- Explicitly select dimensions and measures; never use SELECT *.
- Wrap every metric-view measure column in MEASURE(), for example MEASURE(`Net Sales`) AS net_sales.
- Group by selected dimensions using GROUP BY ALL or an explicit GROUP BY dimension list.
- Do not join a metric view directly to another table. Query the metric view in a CTE first, then join the CTE result.
"""

_METRIC_VIEW_MEASURE_ERROR = "METRIC_VIEW_MISSING_MEASURE_FUNCTION"


def generate_plan(
    tables_context: list[dict],
    inspection_summaries: dict[str, Any],
    user_requirements: str,
) -> dict:
    """Generate a complete Genie Space plan via parallel LLM calls.

    Args:
        tables_context: List of table dicts from describe_table results, each with
            keys like "table" / "table_name", "columns", "comment", "row_count".
        inspection_summaries: Combined inspection data — quality, usage, profiles.
        user_requirements: Concatenated user messages describing their needs.

    Returns:
        Assembled plan dict ready for present_plan, or dict with "error" key.
    """
    shared_context = _build_shared_context(tables_context, inspection_summaries, user_requirements)

    section_specs: list[tuple[str, callable, dict]] = [
        ("tables", _gen_tables, {"shared": shared_context, "tables_context": tables_context}),
        ("questions", _gen_questions_instructions, {"shared": shared_context}),
        ("example_sqls", _gen_example_sqls, {"shared": shared_context}),
        ("benchmarks", _gen_benchmarks, {"shared": shared_context}),
        ("analytics", _gen_analytics, {"shared": shared_context}),
    ]

    results: dict[str, dict] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
        futures = {
            pool.submit(run_in_context(fn, **kwargs)): name
            for name, fn, kwargs in section_specs
        }
        for future in as_completed(futures):
            section_name = futures[future]
            try:
                results[section_name] = future.result()
            except Exception as e:
                logger.exception("Plan section %s failed", section_name)
                errors.append(f"{section_name}: {e}")
                results[section_name] = {}

    plan = _assemble(results, tables_context)

    # Validate example SQLs and benchmarks (test execution + repair)
    validated = _validate_plan_sqls(plan, shared_context=shared_context)
    if validated:
        errors.extend(validated)

    # Validate analytics SQL (measures, filters, expressions, joins)
    analytics_warnings = _validate_analytics_sql(plan)
    if analytics_warnings:
        errors.extend(analytics_warnings)

    # Check benchmark question-SQL alignment (LLM-based)
    alignment_warnings = _validate_benchmark_alignment(plan, shared_context)
    if alignment_warnings:
        errors.extend(alignment_warnings)

    if errors:
        plan["_generation_warnings"] = errors
        logger.warning("Plan generated with %d section error(s): %s", len(errors), errors)

    return plan


def _table_id(t: dict) -> str:
    """Extract the canonical table identifier from a describe_table result."""
    return t.get("table") or t.get("table_name") or t.get("identifier", "?")


def _is_metric_view_context(t: dict) -> bool:
    return "METRIC_VIEW" in str(t.get("table_type") or "").upper()


def _table_plan_entry(t: dict) -> dict:
    """Convert a describe_table result into a plan table entry."""
    name = _table_id(t)
    cols = t.get("columns", [])
    recs = t.get("recommendations", {})
    exclude = set(recs.get("exclude_etl", []))

    column_configs = []
    for c in cols:
        col_name = c.get("name", "?")
        entry: dict = {"column_name": col_name}
        if c.get("description"):
            entry["description"] = c["description"]
        if col_name in exclude:
            entry["excluded"] = True
        column_configs.append(entry)

    return {
        "identifier": name,
        "description": t.get("comment", ""),
        "column_configs": column_configs,
    }


def _metric_view_plan_entry(t: dict) -> dict:
    """Convert a describe_table result into a plan metric-view entry."""
    cols = t.get("columns", [])
    column_configs = []
    for c in cols:
        entry: dict = {
            "column_name": c.get("name", "?"),
            "enable_format_assistance": True,
        }
        if c.get("description"):
            entry["description"] = c["description"]
        column_configs.append(entry)

    return {
        "identifier": _table_id(t),
        "description": t.get("comment", ""),
        "column_configs": column_configs,
    }


def _call_llm_section(prompt: str, max_tokens: int, section_name: str) -> dict:
    """Call the LLM serving endpoint and parse the JSON response.

    Raises RuntimeError (re-raised from the original) on failure so the
    ThreadPoolExecutor in generate_plan can catch and log it per-section.
    """
    try:
        response = call_serving_endpoint(
            [{"role": "user", "content": prompt}],
            model=get_llm_model(),
            max_tokens=max_tokens,
        )
        return parse_json_from_llm_response(response)
    except Exception as e:
        logger.exception("%s generation failed", section_name)
        raise RuntimeError(f"{section_name} LLM call failed: {e}") from e


def _build_shared_context(
    tables_context: list[dict],
    inspection_summaries: dict[str, Any],
    user_requirements: str,
) -> str:
    """Build the shared context block that all 4 LLM calls receive."""
    parts: list[str] = []

    if user_requirements:
        parts.append(f"## User Requirements\n{user_requirements}")

    table_lines = []
    has_metric_view = False
    for t in tables_context:
        name = _table_id(t)
        comment = t.get("comment", "")
        cols = t.get("columns", [])
        is_metric_view = _is_metric_view_context(t)
        has_metric_view = has_metric_view or is_metric_view
        col_summary = ", ".join(c.get("name", "?") for c in cols[:20])
        if len(cols) > 20:
            col_summary += f" (+{len(cols) - 20} more)"
        row_count = t.get("row_count", "?")
        type_label = " [METRIC_VIEW]" if is_metric_view else ""
        table_lines.append(f"- **{name}**{type_label} ({len(cols)} cols, ~{row_count} rows): {comment}")
        table_lines.append(f"  Columns: {col_summary}")

        recs = t.get("recommendations", {})
        if recs.get("exclude_etl"):
            table_lines.append(f"  ETL columns to exclude: {', '.join(recs['exclude_etl'])}")

    if table_lines:
        parts.append("## Tables\n" + "\n".join(table_lines))
    if has_metric_view:
        parts.append(_METRIC_VIEW_SQL_RULES)

    quality = inspection_summaries.get("quality")
    if quality and not quality.get("error"):
        parts.append(f"## Data Quality\n{json.dumps(quality, indent=2, default=str)[:3000]}")

    profiles = inspection_summaries.get("profiles")
    if profiles:
        # Only include profiles for tables the user selected, not ones they explored and discarded
        selected_ids = {_table_id(t) for t in tables_context}
        relevant = {k: v for k, v in profiles.items() if k in selected_ids} if selected_ids else profiles
        parts.append(f"## Column Profiles (actual values from data)\n{_summarize_profiles(relevant)}")

    usage = inspection_summaries.get("usage")
    if usage and not usage.get("error"):
        parts.append(f"## Usage Patterns\n{_summarize_usage(usage)}")

    return "\n\n".join(parts)


def _normalize_sql(sql: str) -> str:
    """Normalize a SQL snippet to a pattern key for deduplication.

    Strips string literals, numbers, and extra whitespace so that
    repeated executions of the same query (with different filter values)
    collapse to the same pattern.
    """
    s = sql.lower()
    s = re.sub(r"'[^']*'", "?", s)          # strip string literals
    s = re.sub(r"\b\d+(\.\d+)?\b", "?", s)  # strip numeric literals
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _summarize_usage(usage: dict) -> str:
    """Convert raw profile_table_usage output into compact, dense signal.

    Instead of dumping JSON (mostly structure overhead), surfaces:
    - Top columns by query frequency (already extracted by _extract_column_patterns)
    - Unique query patterns (deduplicated by normalized SQL, not raw text)
    - Lineage summary (upstream sources, downstream consumers)
    """
    lines: list[str] = []

    # Column frequency — already computed, zero processing needed
    col_usage: dict[str, int] = usage.get("column_usage", {})
    if col_usage:
        top_cols = sorted(col_usage.items(), key=lambda x: -x[1])[:10]
        lines.append("Most queried columns: " + ", ".join(f"{c} ({n}x)" for c, n in top_cols))

    # Unique query patterns per table
    seen_patterns: set[str] = set()
    pattern_lines: list[str] = []

    for tbl, info in usage.get("tables", {}).items():
        if not isinstance(info, dict):
            continue
        short = tbl.split(".")[-1]
        queries = info.get("recent_queries", [])
        for q in queries:
            preview = q.get("query_preview", "")
            if not preview:
                continue
            pattern = _normalize_sql(preview)
            if pattern in seen_patterns:
                continue
            seen_patterns.add(pattern)
            pattern_lines.append(f"  [{short}] {preview[:120]}")

    if pattern_lines:
        lines.append(f"Unique query patterns ({len(seen_patterns)} total):")
        lines.extend(pattern_lines[:15])  # cap at 15 to control token spend

    # Lineage summary
    summary = usage.get("summary", {})
    downstream = summary.get("total_downstream_consumers", 0)
    upstream = summary.get("total_upstream_sources", 0)
    if downstream or upstream:
        lines.append(f"Lineage: {upstream} upstream source(s), {downstream} downstream consumer(s)")

    return "\n".join(lines) if lines else "No recent query activity found."


def _summarize_profiles(profiles: dict) -> str:
    """Convert accumulated profile_columns data into compact text.

    profiles is a dict keyed by table identifier, each value is a dict
    of column_name → {"distinct_values": [...], "has_more": bool}.
    Renders as: table.column: val1, val2, val3 (+ more)
    This ensures the LLM sees REAL data values, not hallucinated ones.
    """
    lines: list[str] = []
    for table_id, col_profiles in profiles.items():
        if not isinstance(col_profiles, dict):
            continue
        short_table = table_id.split(".")[-1] if "." in table_id else table_id
        for col_name, info in col_profiles.items():
            if not isinstance(info, dict) or "error" in info:
                continue
            values = info.get("distinct_values", [])
            if not values:
                continue
            val_str = ", ".join(str(v) for v in values[:10])
            suffix = " (+ more)" if info.get("has_more") else ""
            lines.append(f"  {short_table}.{col_name}: {val_str}{suffix}")
            if len(lines) >= 60:  # cap to control LLM context size
                break
        if len(lines) >= 60:
            break
    if not lines:
        return "No column profiles available."
    return "IMPORTANT — Use ONLY these actual values in SQL and instructions:\n" + "\n".join(lines)


def _gen_tables(shared: str, tables_context: list[dict]) -> dict:
    """Generate table descriptions and column_configs.

    Mostly programmatic (columns come from inspection), with an LLM call
    to generate human-readable descriptions for ambiguous columns.
    Falls back to raw metadata on LLM failure — table configs are optional enrichment.
    """
    tables = []
    metric_views = []
    for t in tables_context:
        if _is_metric_view_context(t):
            metric_views.append(_metric_view_plan_entry(t))
        else:
            tables.append(_table_plan_entry(t))

    if not tables:
        return {"tables": [], "metric_views": metric_views}

    prompt = (
        "You are enriching table and column metadata for a Databricks Genie Space.\n\n"
        "For each table below:\n"
        "1. If the table description is empty or vague, write a clear 1-2 sentence description.\n"
        "2. Add a brief description for EVERY column that doesn't already have one.\n"
        "   Even columns with obvious names (like 'date') benefit from context (e.g., 'Order placement date').\n"
        "3. Keep existing descriptions unchanged unless they are clearly wrong.\n"
        "4. Add `synonyms` (array of strings) for columns with abbreviated or technical names that "
        "users might refer to differently. E.g., 'cust_id' → [\"customer ID\", \"account number\"], "
        "'txn_amt' → [\"transaction amount\", \"payment amount\"]. Only add synonyms where they add value "
        "— skip columns with clear, unambiguous names.\n\n"
        "Return ONLY valid JSON: {\"tables\": [...]}\n\n"
        f"Current tables:\n```json\n{json.dumps(tables, indent=2)}\n```\n\n"
        f"Context:\n{shared[:3000]}"
    )

    try:
        result = _call_llm_section(prompt, max_tokens=4096, section_name="tables")
        if "tables" not in result:
            result = {"tables": tables}
        if metric_views:
            result["metric_views"] = metric_views
        return result
    except Exception:
        logger.warning("Table description enrichment failed, using raw metadata")
        result = {"tables": tables}
        if metric_views:
            result["metric_views"] = metric_views
        return result


def _gen_questions_instructions(shared: str) -> dict:
    """Generate sample_questions and text_instructions."""
    prompt = (
        "You are creating sample questions and text instructions for a Databricks Genie Space.\n\n"
        "Based on the context below, generate:\n"
        "1. **suggested_display_name**: A concise, professional name for the Genie Space "
        "(e.g., 'NYC Taxi Revenue Performance', 'TPC-H Sales Analytics', 'Customer Support Dashboard')\n"
        "2. **sample_questions**: EXACTLY 5 natural-language questions a business user would ask\n"
        "3. **text_instructions**: Domain knowledge for the Genie agent, organized under the "
        "canonical GSL section headers (see rules below).\n\n"
        "Text instructions should contain ONLY business logic and terminology — NOT SQL formulas, "
        "filter expressions, or join definitions (those go in other sections).\n\n"
        "**Section vocabulary (use these exact headers, omit any that are empty, keep this order):**\n"
        "- `## PURPOSE` — one or two bullets stating the space's scope and audience.\n"
        "- `## DISAMBIGUATION` — clarification-question triggers and term-resolution rules "
        "(e.g., \"When the user asks about 'customer performance' without a time range, ask them to clarify the period\"; "
        "\"'Q1' means calendar Q1 unless the user says 'fiscal Q1'\").\n"
        "- `## DATA QUALITY NOTES` — caveats the model needs to know: NULL handling, known bad rows, "
        "column semantics not captured in the column description.\n"
        "- `## CONSTRAINTS` — hard guardrails: what never to show (PII columns, secrets), what not to do.\n"
        "- `## Instructions you must follow when providing summaries` — summary-customization behavior "
        "(rounding rules, mandatory caveats). **Use this exact heading — it is Databricks's blessed string.**\n\n"
        "IMPORTANT: Keep text_instructions UNDER 2,000 characters total. Be concise — use bullet points, "
        "not paragraphs. If you have more than 2,000 chars of context, prioritize the most important rules "
        "and drop the rest. Long instructions push out higher-value SQL context in Genie's prompt window.\n"
        "NEVER include SQL code (SELECT, WHERE, JOIN, GROUP BY, etc.) in text instructions — "
        "those patterns belong in Example SQLs, Measures, Filters, or Expressions.\n"
        "CRITICAL: Only reference category names, tiers, statuses, and labels that appear in the "
        "Column Profiles section below. Do NOT invent terms — use real data values.\n\n"
        "Return ONLY valid JSON:\n"
        '{"suggested_display_name": "...", "sample_questions": ["..."], "text_instructions": '
        '["## PURPOSE\\n- Answer ... for ... users.", '
        '"## DISAMBIGUATION\\n- When the user says X, interpret as Y.", '
        '"## CONSTRAINTS\\n- Never show PII columns."]}\n\n'
        f"Context:\n{shared}"
    )
    return _call_llm_section(prompt, max_tokens=3000, section_name="questions/instructions")


def _gen_example_sqls(shared: str) -> dict:
    """Generate example_sqls (question + SQL pairs that teach Genie query patterns).

    Hard cap: exactly 12 pairs. At ~350 tokens each (question + SQL + params + guidance + JSON),
    12 pairs = ~4200 tokens — within the 6000 max_tokens budget even for
    complex multi-table queries with CTEs.
    """
    prompt = (
        "You are creating example SQL queries for a Databricks Genie Space.\n\n"
        "Generate EXACTLY 12 question+SQL pairs that teach Genie how to write correct queries.\n"
        "   - Use fully-qualified table names (catalog.schema.table)\n"
        "   - Use parameterized SQL (:param_name) when the question involves user-supplied values\n"
        "   - Each parameter needs: name, type_hint (STRING/INTEGER/DOUBLE/DECIMAL/DATE/BOOLEAN), "
        "default_value (real value from data), description\n"
        "   - The question should be concrete (use the default value, not a placeholder)\n"
        "   - Mix: ~4 hardcoded patterns + ~8 parameterized queries\n"
        "   - Cover diverse patterns: aggregation, filtering, grouping, joins, date ranges, top-N\n"
        "   - IMPORTANT: Each example MUST include a `usage_guidance` field — a short sentence "
        "describing when Genie should use this pattern. E.g., 'Use for any top-N ranking question', "
        "'Use when filtering by date range', 'Use for revenue aggregation by region'.\n"
        "   - IMPORTANT: Generate no more than 12 pairs total.\n"
        "   - CRITICAL: Only use filter values that appear in the Column Profiles section below.\n"
        "     Do NOT invent status values, tier names, or category labels — use real data.\n\n"
        "METRIC VIEW SQL RULES:\n"
        "   - If a query uses a table marked [METRIC_VIEW], wrap measure columns with MEASURE(`Measure Name`).\n"
        "   - Select/group metric-view dimensions normally; use GROUP BY ALL or explicit dimension grouping.\n"
        "   - Never use SELECT * with metric views.\n"
        "   - Do not join a metric view directly to another table; query it in a CTE first, then join the CTE.\n\n"
        "Return ONLY valid JSON:\n"
        '{"example_sqls": [{"question": "...", "sql": "...", "usage_guidance": "...", "parameters": [...]}]}\n\n'
        f"Context:\n{shared}"
    )
    return _call_llm_section(prompt, max_tokens=6000, section_name="example_sqls")


def _gen_benchmarks(shared: str) -> dict:
    """Generate benchmark questions (ground-truth Q+SQL for space quality scoring).

    Hard cap: exactly 10 benchmarks. At ~300 tokens each (question + SQL + JSON),
    10 benchmarks = ~3000 tokens — well within the 5000 max_tokens budget even for
    complex multi-table queries.
    """
    prompt = (
        "You are creating benchmark tests for a Databricks Genie Space.\n\n"
        "Generate EXACTLY 10 question + expected_sql pairs for validation.\n"
        "These are used to score the Genie space quality — cover a representative spread.\n"
        "   - Use fully-qualified table names (catalog.schema.table)\n"
        "   - Benchmarks MUST use hardcoded literal values — NO :param_name placeholders\n"
        "   - Include aggregation, filtering, grouping, and join patterns\n"
        "   - Mix simple single-table queries with multi-table queries\n"
        "   - IMPORTANT: Generate no more than 10 pairs total.\n"
        "   - CRITICAL: Only use filter values that appear in the Column Profiles section below.\n"
        "     Do NOT invent status values, tier names, or category labels — use real data.\n\n"
        "METRIC VIEW SQL RULES:\n"
        "   - If a query uses a table marked [METRIC_VIEW], wrap measure columns with MEASURE(`Measure Name`).\n"
        "   - Select/group metric-view dimensions normally; use GROUP BY ALL or explicit dimension grouping.\n"
        "   - Never use SELECT * with metric views.\n"
        "   - Do not join a metric view directly to another table; query it in a CTE first, then join the CTE.\n\n"
        "BENCHMARK QUALITY RULES:\n"
        "   - The expected SQL must be the MOST DIRECT, NATURAL answer to the question.\n"
        "   - Do NOT add extra WHERE clauses the question didn't ask for (e.g., IS NOT NULL checks).\n"
        "   - Do NOT add unnecessary JOINs that the question doesn't require.\n"
        "   - Do NOT add defensive NULL filters or data quality guards.\n"
        "   - The SQL should be what a skilled analyst would write to answer EXACTLY that question.\n"
        "   - If Genie writes a simpler but correct SQL, it should NOT be marked wrong because\n"
        "     your expected SQL has extra unnecessary clauses.\n\n"
        "Return ONLY valid JSON:\n"
        '{"benchmarks": [{"question": "...", "expected_sql": "..."}]}\n\n'
        f"Context:\n{shared}"
    )
    return _call_llm_section(prompt, max_tokens=5000, section_name="benchmarks")


def _gen_analytics(shared: str) -> dict:
    """Generate join_specs, measures, filters, and expressions.

    Hard cap: ~20 items total (up to 5 joins + 5 measures + 5 filters + 4 expressions).
    At ~120 tokens each with fully-qualified names, 20 items = ~2400 tokens — well
    within the 5000 max_tokens budget.
    """
    prompt = (
        "You are creating analytics scaffolding for a Databricks Genie Space.\n\n"
        "Based on the context below, generate ALL of the following sections:\n\n"
        "1. **join_specs**: Table relationships for multi-table queries (up to 5).\n"
        "   Each: {left_table, right_table, left_column, right_column, "
        "relationship (MANY_TO_ONE/ONE_TO_MANY/MANY_TO_MANY)}\n"
        "   - REQUIRED if 2+ tables exist. Skip only if there is exactly 1 table.\n"
        "   - Only include one direction per relationship (e.g., orders→customers, not both directions).\n\n"
        "2. **measures**: EXACTLY 5 reusable aggregation expressions.\n"
        "   Each: {alias, sql (aggregate expr using fully-qualified table.column), display_name}\n"
        "   - Include COUNT, SUM, AVG on numeric/date columns. Always generate these.\n\n"
        "3. **filters**: EXACTLY 5 reusable WHERE clause snippets.\n"
        "   Each: {display_name, sql (WHERE condition without WHERE keyword, fully-qualified columns)}\n"
        "   - Include date range filters, status/category filters, and common lookups.\n\n"
        "4. **expressions**: EXACTLY 3 computed dimension columns.\n"
        "   Each: {alias, sql (expression, fully-qualified columns), display_name}\n"
        "   - Date parts (YEAR/MONTH), CASE labels, concatenations, etc.\n\n"
        "IMPORTANT: measures, filters, and expressions are always applicable — "
        "generate them even for single tables. Do not exceed the counts above.\n"
        "Use fully-qualified table names (catalog.schema.table) in all SQL.\n\n"
        "Return ONLY valid JSON:\n"
        '{"join_specs": [...], "measures": [...], "filters": [...], "expressions": [...]}\n\n'
        f"Context:\n{shared}"
    )
    return _call_llm_section(prompt, max_tokens=5000, section_name="analytics")


def _repair_unbound_sql(item: dict, kind: str, shared_context: str) -> dict | None:
    """Ask the LLM to fix missing parameter default_values in a parameterized SQL.

    For example_sqls: fills in default_value for each :param so _substitute_params
    can run the SQL without unbound placeholders.
    For benchmarks: converts parameterized SQL to a hardcoded equivalent using
    literal values (benchmarks must be concrete — no :param_name syntax allowed).

    Returns the repaired item dict, or None if repair failed.
    """
    if kind == "example_sql":
        sql = item.get("sql", "")
        params = item.get("parameters") or []
        prompt = (
            "The following example SQL has parameters that are missing `default_value` fields. "
            "Fill in a realistic `default_value` for each parameter based on the table context. "
            "The default_value must be a real value that exists in the data — not a placeholder.\n\n"
            f"SQL: {sql}\n\n"
            f"Current parameters: {json.dumps(params)}\n\n"
            "Return ONLY valid JSON with the corrected parameters array:\n"
            '{"parameters": [{"name": "...", "type_hint": "...", "default_value": "...", "description": "..."}]}\n\n'
            f"Context:\n{shared_context[:2000]}"
        )
        try:
            result = _call_llm_section(prompt, max_tokens=512, section_name="param repair")
            repaired_params = result.get("parameters")
            if repaired_params:
                return {**item, "parameters": repaired_params}
        except Exception:
            logger.warning("Parameter repair failed for example SQL: %s", sql[:80])
        return None

    if kind == "benchmark":
        sql = item.get("expected_sql", "")
        prompt = (
            "The following benchmark SQL uses :param_name placeholders. "
            "Rewrite it as a concrete hardcoded SQL using realistic literal values from the table context. "
            "Do NOT use any :param_name syntax in the output.\n\n"
            f"SQL: {sql}\n\n"
            "Return ONLY valid JSON:\n"
            '{"expected_sql": "..."}\n\n'
            f"Context:\n{shared_context[:2000]}"
        )
        try:
            result = _call_llm_section(prompt, max_tokens=512, section_name="benchmark repair")
            repaired_sql = result.get("expected_sql", "")
            # Verify no :param placeholders remain anywhere in the repaired SQL
            if repaired_sql and not re.search(r":[a-zA-Z_]\w*", repaired_sql):
                return {**item, "expected_sql": repaired_sql}
        except Exception:
            logger.warning("Benchmark SQL repair failed: %s", sql[:80])
        return None


def _is_metric_view_measure_error(error: str) -> bool:
    return _METRIC_VIEW_MEASURE_ERROR in error


def _repair_metric_view_sql(item: dict, kind: str, shared_context: str, error: str) -> dict | None:
    """Rewrite SQL that failed because metric-view measures need MEASURE()."""
    sql_key = "sql" if kind == "example_sql" else "expected_sql"
    sql = item.get(sql_key, "")
    if not sql:
        return None

    prompt = (
        "The following Databricks SQL query failed because it queries a metric view measure "
        "without the required MEASURE() aggregate function.\n\n"
        f"Error: {error}\n\n"
        f"Question: {item.get('question', '')}\n\n"
        f"SQL:\n{sql}\n\n"
        "Rewrite the whole SQL using valid Databricks metric-view syntax:\n"
        "- Wrap every metric-view measure column with MEASURE(`Measure Name`) and give it a SQL alias.\n"
        "- Keep metric-view dimensions unwrapped and include them in GROUP BY ALL or an explicit GROUP BY.\n"
        "- Never use SELECT * with metric views.\n"
        "- If the metric view must be joined to another table, query the metric view in a CTE first, then join the CTE result.\n"
        "- Preserve parameter placeholders and existing parameters for example SQLs.\n\n"
        "Return ONLY valid JSON:\n"
        f'{{"{sql_key}": "..."}}\n\n'
        f"Context:\n{shared_context[:4000]}"
    )
    try:
        result = _call_llm_section(prompt, max_tokens=2048, section_name="metric view SQL repair")
        repaired_sql = result.get(sql_key, "")
        if repaired_sql:
            return {**item, sql_key: repaired_sql}
    except Exception:
        logger.warning("Metric-view SQL repair failed: %s", sql[:80])
    return None


def _repair_sql_validation_failure(
    item: dict,
    kind: str,
    shared_context: str,
    reason: str,
) -> dict | None:
    if _is_metric_view_measure_error(reason):
        return _repair_metric_view_sql(item, kind, shared_context, reason)
    return _repair_unbound_sql(item, kind, shared_context)


def _validate_analytics_sql(plan: dict) -> list[str]:
    """Test measures, filters, expressions, and join specs by wrapping in synthetic queries.

    These snippets are SQL fragments, not full queries, so they need to be wrapped
    to test execution. Failures are warnings (snippet dropped from plan).
    """
    warnings: list[str] = []
    tasks: list[tuple[str, int, str]] = []

    # Build synthetic test queries for each snippet type
    measures = plan.get("measures", [])
    for i, m in enumerate(measures):
        sql_expr = m.get("sql", "")
        # Measures reference fully-qualified columns; extract table from the SQL
        table = _extract_table_from_sql(sql_expr)
        if table and sql_expr:
            tasks.append(("measure", i, f"SELECT {sql_expr} FROM {table} LIMIT 1"))

    filters = plan.get("filters", [])
    for i, f in enumerate(filters):
        sql_expr = f.get("sql", "")
        table = _extract_table_from_sql(sql_expr)
        if table and sql_expr:
            tasks.append(("filter", i, f"SELECT COUNT(*) FROM {table} WHERE {sql_expr} LIMIT 1"))

    expressions = plan.get("expressions", [])
    for i, e in enumerate(expressions):
        sql_expr = e.get("sql", "")
        table = _extract_table_from_sql(sql_expr)
        if table and sql_expr:
            tasks.append(("expression", i, f"SELECT {sql_expr} FROM {table} LIMIT 1"))

    join_specs = plan.get("join_specs", [])
    for i, j in enumerate(join_specs):
        left = j.get("left_table", "")
        right = j.get("right_table", "")
        left_col = j.get("left_column", "")
        right_col = j.get("right_column", "")
        if left and right and left_col and right_col:
            tasks.append(("join", i, f"SELECT 1 FROM {left} JOIN {right} ON {left}.{left_col} = {right}.{right_col} LIMIT 1"))

    if not tasks:
        return warnings

    failed_idxs: dict[str, set[int]] = {"measure": set(), "filter": set(), "expression": set(), "join": set()}

    with ThreadPoolExecutor(max_workers=_VALIDATION_CONCURRENCY) as pool:
        futures = {
            pool.submit(run_in_context(_test_sql, sql, None)): (kind, idx)
            for kind, idx, sql in tasks
        }
        for future in as_completed(futures):
            kind, idx = futures[future]
            try:
                result = future.result()
                if not result.get("success"):
                    failed_idxs[kind].add(idx)
                    items = {"measure": measures, "filter": filters, "expression": expressions, "join": join_specs}
                    item = items[kind][idx]
                    name = item.get("display_name") or item.get("alias") or f"#{idx+1}"
                    warnings.append(f"Dropped {kind} {name}: {result.get('error', 'unknown')[:120]}")
            except Exception as e:
                failed_idxs[kind].add(idx)
                warnings.append(f"Dropped {kind} #{idx+1}: {e}")

    # Remove failed items from plan
    if failed_idxs["measure"]:
        plan["measures"] = [m for i, m in enumerate(measures) if i not in failed_idxs["measure"]]
    if failed_idxs["filter"]:
        plan["filters"] = [f for i, f in enumerate(filters) if i not in failed_idxs["filter"]]
    if failed_idxs["expression"]:
        plan["expressions"] = [e for i, e in enumerate(expressions) if i not in failed_idxs["expression"]]
    if failed_idxs["join"]:
        plan["join_specs"] = [j for i, j in enumerate(join_specs) if i not in failed_idxs["join"]]

    total_dropped = sum(len(v) for v in failed_idxs.values())
    logger.info("Analytics SQL validation: tested %d, dropped %d", len(tasks), total_dropped)
    return warnings


def _extract_table_from_sql(sql_fragment: str) -> str | None:
    """Extract a fully-qualified table name (catalog.schema.table) from a SQL fragment."""
    match = re.search(r"(\w+\.\w+\.\w+)", sql_fragment)
    return match.group(1) if match else None


def _validate_benchmark_alignment(plan: dict, shared_context: str) -> list[str]:
    """Check that benchmark expected_sql is the most direct answer to the question.

    Uses an LLM to verify alignment — flags benchmarks where the SQL has unnecessary
    clauses that would cause correct-but-different Genie SQL to be marked wrong.
    """
    benchmarks = plan.get("benchmarks", [])
    if not benchmarks:
        return []

    warnings: list[str] = []
    repaired_count = 0

    with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
        futures = {
            pool.submit(run_in_context(_check_benchmark_alignment, bm, shared_context)): i
            for i, bm in enumerate(benchmarks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                if result and result.get("needs_repair"):
                    new_sql = result.get("simplified_sql", "")
                    new_question = result.get("tightened_question", "")
                    if new_sql:
                        benchmarks[idx]["expected_sql"] = new_sql
                        repaired_count += 1
                    if new_question:
                        benchmarks[idx]["question"] = new_question
                    reason = result.get("reason", "alignment issue")
                    warnings.append(f"Repaired benchmark #{idx+1}: {reason[:120]}")
            except Exception as e:
                logger.warning("Benchmark alignment check failed for #%d: %s", idx + 1, e)

    if repaired_count:
        logger.info("Benchmark alignment: repaired %d/%d benchmarks", repaired_count, len(benchmarks))
    return warnings


def _check_benchmark_alignment(benchmark: dict, shared_context: str) -> dict | None:
    """Check a single benchmark for question-SQL alignment."""
    question = benchmark.get("question", "")
    sql = benchmark.get("expected_sql", "")
    if not question or not sql:
        return None

    prompt = (
        "You are reviewing a benchmark test for a Databricks Genie Space.\n\n"
        f"Question: {question}\n"
        f"Expected SQL: {sql}\n\n"
        "Check if this expected SQL is the MOST DIRECT answer to the question:\n"
        "1. Are there unnecessary WHERE clauses the question didn't ask for? (e.g., IS NOT NULL, status checks)\n"
        "2. Are there unnecessary JOINs that the question doesn't require?\n"
        "3. Are there defensive NULL filters or data quality guards not implied by the question?\n"
        "4. Would a simpler but correct SQL be marked wrong because this SQL has extra clauses?\n\n"
        "If the SQL is already the most direct answer, return: {\"needs_repair\": false}\n"
        "If it needs fixing, return:\n"
        '{"needs_repair": true, "reason": "...", "simplified_sql": "...", "tightened_question": "..."}\n'
        "simplified_sql: The SQL rewritten as the most direct answer.\n"
        "tightened_question: Only provide if the question itself is vague and should be more specific.\n\n"
        "Return ONLY valid JSON."
    )
    try:
        result = _call_llm_section(prompt, max_tokens=1024, section_name="benchmark alignment")
        return result
    except Exception:
        return None


def _validate_plan_sqls(plan: dict, shared_context: str = "") -> list[str]:
    """Test all example_sqls and benchmark SQLs in parallel.

    Unbound-parameter failures get an LLM repair pass (example_sqls have
    default_values filled in; benchmarks are de-parameterized to concrete SQL),
    then re-tested. Hard failures (syntax errors, missing tables) are dropped —
    incorrect SQL in the plan is worse than no SQL.

    Returns a list of warning strings for repaired or dropped items.
    """
    example_sqls: list[dict] = plan.get("example_sqls", [])
    benchmarks: list[dict] = plan.get("benchmarks", [])

    if not example_sqls and not benchmarks:
        return []

    tasks: list[tuple[str, int, str, list[dict] | None]] = []
    for i, eq in enumerate(example_sqls):
        sql = eq.get("sql", "")
        if sql:
            tasks.append(("example_sql", i, sql, eq.get("parameters")))
    for i, bm in enumerate(benchmarks):
        sql = bm.get("expected_sql", "")
        if sql:
            tasks.append(("benchmark", i, sql, None))

    if not tasks:
        return []

    # All three phases (test → repair → retest) share a single pool to avoid
    # repeated thread-creation overhead. Phases are sequential by necessity.
    test_results: dict[tuple[str, int], dict] = {}
    needs_repair: list[tuple[str, int]] = []
    repair_reasons: dict[tuple[str, int], str] = {}
    hard_failures: dict[tuple[str, int], str] = {}
    repaired: dict[tuple[str, int], dict] = {}
    warnings: list[str] = []

    with ThreadPoolExecutor(max_workers=_VALIDATION_CONCURRENCY) as pool:
        # Phase 1: initial test pass
        futures = {
            pool.submit(run_in_context(_test_sql, sql, params)): (kind, idx)
            for kind, idx, sql, params in tasks
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                test_results[key] = future.result()
            except Exception as e:
                test_results[key] = {"success": False, "error": str(e)}

        # Triage: unbound params are repairable; everything else is a hard failure
        # Also check benchmark row counts — 0-row benchmarks have hallucinated filter values
        for (kind, idx), result in test_results.items():
            if not result.get("success"):
                err = result.get("error", "unknown")
                if "Unbound SQL parameters" in err or _is_metric_view_measure_error(err):
                    needs_repair.append((kind, idx))
                    repair_reasons[(kind, idx)] = err
                else:
                    hard_failures[(kind, idx)] = err
            elif kind == "benchmark" and result.get("success"):
                row_count = result.get("row_count", 0)
                if row_count == 0:
                    # Benchmark SQL ran but returned no rows — filter values likely hallucinated
                    needs_repair.append((kind, idx))
                    repair_reasons[(kind, idx)] = "zero-row benchmark"
                    warnings.append(f"Benchmark #{idx+1} returned 0 rows — attempting repair with real values")

        # Phase 2: repair unbound-param failures
        if needs_repair and shared_context:
            repair_items = [
                (kind, idx, example_sqls[idx] if kind == "example_sql" else benchmarks[idx])
                for kind, idx in needs_repair
            ]
            repair_futures = {
                pool.submit(
                    run_in_context(
                        _repair_sql_validation_failure,
                        item,
                        kind,
                        shared_context,
                        repair_reasons.get((kind, idx), ""),
                    )
                ): (kind, idx)
                for kind, idx, item in repair_items
            }
            for future in as_completed(repair_futures):
                key = repair_futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        repaired[key] = result
                except Exception as e:
                    logger.warning("Repair task failed for %s[%d]: %s", key[0], key[1], e)

            # Phase 3: re-test repaired items; failures become hard failures
            if repaired:
                retest_tasks = [
                    (kind, idx,
                     item.get("sql", "") if kind == "example_sql" else item.get("expected_sql", ""),
                     item.get("parameters") if kind == "example_sql" else None)
                    for (kind, idx), item in repaired.items()
                ]
                retest_futures = {
                    pool.submit(run_in_context(_test_sql, sql, params)): (kind, idx)
                    for kind, idx, sql, params in retest_tasks
                }
                for future in as_completed(retest_futures):
                    key = retest_futures[future]
                    kind, idx = key
                    try:
                        result = future.result()
                        if result.get("success"):
                            q = repaired[key].get("question", "?")[:80]
                            reason = repair_reasons.get(key, "")
                            metric_view_repair = _is_metric_view_measure_error(reason)
                            if kind == "example_sql":
                                example_sqls[idx] = repaired[key]
                                if metric_view_repair:
                                    warnings.append(f"Repaired example SQL #{idx+1} ({q}): added MEASURE() for metric view measures")
                                else:
                                    warnings.append(f"Repaired example SQL #{idx+1} ({q}): filled in missing parameter defaults")
                            else:
                                benchmarks[idx] = repaired[key]
                                if metric_view_repair:
                                    warnings.append(f"Repaired benchmark #{idx+1} ({q}): added MEASURE() for metric view measures")
                                else:
                                    warnings.append(f"Repaired benchmark #{idx+1} ({q}): converted to hardcoded SQL")
                        else:
                            hard_failures[key] = result.get("error", "repair re-test failed")
                    except Exception as e:
                        hard_failures[key] = str(e)

            # Unrepaired items (repair returned None) → hard failure
            for kind, idx in needs_repair:
                if (kind, idx) not in repaired and (kind, idx) not in hard_failures:
                    hard_failures[(kind, idx)] = "repair produced no result"

        elif needs_repair:
            for kind, idx in needs_repair:
                reason = repair_reasons.get((kind, idx), "")
                if _is_metric_view_measure_error(reason):
                    hard_failures[(kind, idx)] = "metric-view measure syntax (no context for repair)"
                else:
                    hard_failures[(kind, idx)] = "unbound parameters (no context for repair)"

    # Drop hard failures
    failed_example_idxs = {idx for (kind, idx) in hard_failures if kind == "example_sql"}
    failed_bench_idxs = {idx for (kind, idx) in hard_failures if kind == "benchmark"}

    for (kind, idx), err in hard_failures.items():
        item = example_sqls[idx] if kind == "example_sql" else benchmarks[idx]
        q = item.get("question", "?")[:80]
        warnings.append(f"Dropped {kind} #{idx+1} ({q}): {err[:120]}")

    if failed_example_idxs:
        plan["example_sqls"] = [eq for i, eq in enumerate(example_sqls) if i not in failed_example_idxs]
    if failed_bench_idxs:
        plan["benchmarks"] = [bm for i, bm in enumerate(benchmarks) if i not in failed_bench_idxs]

    kept_ex = len(plan.get("example_sqls", []))
    kept_bm = len(plan.get("benchmarks", []))
    logger.info(
        "SQL validation: tested %d, kept %d examples + %d benchmarks, dropped %d",
        len(tasks), kept_ex, kept_bm, len(hard_failures),
    )
    return warnings


def _assemble(results: dict[str, dict], tables_context: list[dict]) -> dict:
    """Merge parallel results into a single plan dict."""
    plan: dict[str, Any] = {}

    tables_result = results.get("tables", {})
    plan["tables"] = tables_result.get("tables", [])
    plan["metric_views"] = tables_result.get("metric_views", [])

    if not plan["tables"] and tables_context:
        plan["tables"] = [
            _table_plan_entry(t)
            for t in tables_context
            if not _is_metric_view_context(t)
        ]
    if not plan["metric_views"] and tables_context:
        plan["metric_views"] = [
            _metric_view_plan_entry(t)
            for t in tables_context
            if _is_metric_view_context(t)
        ]

    qi = results.get("questions", {})
    plan["sample_questions"] = qi.get("sample_questions", [])
    plan["text_instructions"] = qi.get("text_instructions", [])
    if qi.get("suggested_display_name"):
        plan["suggested_display_name"] = qi["suggested_display_name"]

    plan["example_sqls"] = results.get("example_sqls", {}).get("example_sqls", [])
    plan["benchmarks"] = results.get("benchmarks", {}).get("benchmarks", [])

    analytics = results.get("analytics", {})
    plan["join_specs"] = analytics.get("join_specs", [])
    plan["measures"] = analytics.get("measures", [])
    plan["filters"] = analytics.get("filters", [])
    plan["expressions"] = analytics.get("expressions", [])

    return plan
