"""Parallel plan generation — builds a Genie Space plan via concurrent LLM calls.

Instead of one monolithic LLM call that generates the entire plan JSON (slow,
truncation-prone), this module splits the plan into 4 independent sections and
generates them in parallel. Each section gets a focused prompt and a small
max_tokens budget, then results are assembled programmatically.

Parallel calls:
  A: table descriptions + column_configs  (mostly programmatic, LLM for descriptions)
  B: sample_questions + text_instructions
  C: example_sqls + benchmarks
  D: join_specs + measures + filters + expressions
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from backend.services.llm_utils import call_serving_endpoint, parse_json_from_llm_response, get_llm_model

logger = logging.getLogger(__name__)

_CONCURRENCY = 4


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
        ("sqls", _gen_sqls_benchmarks, {"shared": shared_context}),
        ("analytics", _gen_analytics, {"shared": shared_context}),
    ]

    results: dict[str, dict] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=_CONCURRENCY) as pool:
        futures = {
            pool.submit(fn, **kwargs): name
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

    if errors:
        plan["_generation_warnings"] = errors
        logger.warning("Plan generated with %d section error(s): %s", len(errors), errors)

    return plan


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
    for t in tables_context:
        name = t.get("table") or t.get("table_name") or t.get("identifier", "?")
        comment = t.get("comment", "")
        cols = t.get("columns", [])
        col_summary = ", ".join(c.get("name", "?") for c in cols[:20])
        if len(cols) > 20:
            col_summary += f" (+{len(cols) - 20} more)"
        row_count = t.get("row_count", "?")
        table_lines.append(f"- **{name}** ({len(cols)} cols, ~{row_count} rows): {comment}")
        table_lines.append(f"  Columns: {col_summary}")

        recs = t.get("recommendations", {})
        if recs.get("exclude_etl"):
            table_lines.append(f"  ETL columns to exclude: {', '.join(recs['exclude_etl'])}")

    if table_lines:
        parts.append("## Tables\n" + "\n".join(table_lines))

    quality = inspection_summaries.get("quality")
    if quality and not quality.get("error"):
        parts.append(f"## Data Quality\n{json.dumps(quality, indent=2, default=str)[:2000]}")

    profiles = inspection_summaries.get("profiles")
    if profiles:
        parts.append(f"## Column Profiles\n{json.dumps(profiles, indent=2, default=str)[:2000]}")

    usage = inspection_summaries.get("usage")
    if usage and not usage.get("error"):
        parts.append(f"## Usage Patterns\n{json.dumps(usage, indent=2, default=str)[:1500]}")

    return "\n\n".join(parts)


def _gen_tables(shared: str, tables_context: list[dict]) -> dict:
    """Generate table descriptions and column_configs.

    Mostly programmatic (columns come from inspection), with an LLM call
    to generate human-readable descriptions for ambiguous columns.
    """
    tables = []
    for t in tables_context:
        name = t.get("table") or t.get("table_name") or t.get("identifier", "?")
        cols = t.get("columns", [])
        recs = t.get("recommendations", {})
        exclude = set(recs.get("exclude_etl", []))

        column_configs = []
        for c in cols:
            col_name = c.get("name", "?")
            if col_name in exclude:
                continue
            entry: dict[str, str] = {"column_name": col_name}
            if c.get("description"):
                entry["description"] = c["description"]
            column_configs.append(entry)

        tables.append({
            "identifier": name,
            "description": t.get("comment", ""),
            "column_configs": column_configs,
        })

    if not tables:
        return {"tables": []}

    prompt = (
        "You are enriching table and column metadata for a Databricks Genie Space.\n\n"
        "For each table below, improve the table description (1-2 sentences) and add "
        "a brief description for any column that doesn't already have one. "
        "Only describe columns whose names are ambiguous or domain-specific.\n\n"
        "Return ONLY valid JSON: {\"tables\": [...]}\n\n"
        f"Current tables:\n```json\n{json.dumps(tables, indent=2)}\n```\n\n"
        f"Context:\n{shared[:3000]}"
    )

    try:
        response = call_serving_endpoint(
            [{"role": "user", "content": prompt}],
            model=get_llm_model(),
            max_tokens=2048,
        )
        result = parse_json_from_llm_response(response)
        return result if "tables" in result else {"tables": tables}
    except Exception:
        logger.warning("Table description enrichment failed, using raw metadata")
        return {"tables": tables}


def _gen_questions_instructions(shared: str) -> dict:
    """Generate sample_questions and text_instructions."""
    prompt = (
        "You are creating sample questions and text instructions for a Databricks Genie Space.\n\n"
        "Based on the context below, generate:\n"
        "1. **sample_questions**: 5-8 natural-language questions a business user would ask\n"
        "2. **text_instructions**: Domain knowledge for the Genie agent, organized under "
        "category headers (## Terminology, ## Default Assumptions, ## Data Quality Warnings, etc.)\n\n"
        "Text instructions should contain ONLY business logic and terminology — NOT SQL formulas, "
        "filter expressions, or join definitions (those go in other sections).\n\n"
        "Return ONLY valid JSON:\n"
        '{"sample_questions": ["..."], "text_instructions": ["## Terminology\\n- ...", "## Default Assumptions\\n- ..."]}\n\n'
        f"Context:\n{shared}"
    )

    response = call_serving_endpoint(
        [{"role": "user", "content": prompt}],
        model=get_llm_model(),
        max_tokens=1024,
    )
    return parse_json_from_llm_response(response)


def _gen_sqls_benchmarks(shared: str) -> dict:
    """Generate example_sqls and benchmarks."""
    prompt = (
        "You are creating example SQL queries and benchmark tests for a Databricks Genie Space.\n\n"
        "Based on the context below, generate:\n"
        "1. **example_sqls**: 5-8 question+SQL pairs that teach Genie query patterns.\n"
        "   - Use fully-qualified table names (catalog.schema.table)\n"
        "   - Use parameterized SQL (:param_name) when the question involves user-supplied values\n"
        "   - Each parameter needs: name, type_hint (STRING/DATE/INTEGER/DECIMAL/BOOLEAN), "
        "default_value (real value from data), description\n"
        "   - The question should be concrete (use the default value, not a placeholder)\n"
        "   - Mix: ~3 hardcoded patterns + ~3-5 parameterized queries\n\n"
        "2. **benchmarks**: EXACTLY 12 question + expected_sql pairs for validation.\n"
        "   This is the most important section — the space quality depends on benchmark coverage.\n"
        "   - Cover edge cases (nulls, empty results, ambiguous terms)\n"
        "   - Include time-range and metric-definition tests\n"
        "   - Include aggregation, filtering, grouping, and join patterns\n"
        "   - Mix simple single-table queries with complex multi-table queries\n"
        "   - You MUST generate at least 10 benchmarks. 12 is ideal.\n\n"
        "Return ONLY valid JSON:\n"
        '{"example_sqls": [{"question": "...", "sql": "...", "parameters": [...]}], '
        '"benchmarks": [{"question": "...", "expected_sql": "..."}]}\n\n'
        f"Context:\n{shared}"
    )

    response = call_serving_endpoint(
        [{"role": "user", "content": prompt}],
        model=get_llm_model(),
        max_tokens=4096,
    )
    return parse_json_from_llm_response(response)


def _gen_analytics(shared: str) -> dict:
    """Generate join_specs, measures, filters, and expressions."""
    prompt = (
        "You are creating analytics scaffolding for a Databricks Genie Space.\n\n"
        "Based on the context below, generate:\n"
        "1. **join_specs**: Table relationships for multi-table queries.\n"
        "   Each: {left_table, right_table, left_column, right_column, "
        "relationship (MANY_TO_ONE/ONE_TO_MANY/MANY_TO_MANY)}\n\n"
        "2. **measures**: Reusable aggregation expressions.\n"
        "   Each: {alias, sql (aggregate expr like 'SUM(amount)'), display_name}\n\n"
        "3. **filters**: Reusable WHERE clause snippets.\n"
        "   Each: {display_name, sql (WHERE condition without WHERE keyword)}\n\n"
        "4. **expressions**: Computed dimension columns.\n"
        "   Each: {alias, sql (expression), display_name}\n\n"
        "Only include sections where the data supports them. "
        "If only one table exists, skip join_specs.\n\n"
        "Return ONLY valid JSON:\n"
        '{"join_specs": [...], "measures": [...], "filters": [...], "expressions": [...]}\n\n'
        f"Context:\n{shared}"
    )

    response = call_serving_endpoint(
        [{"role": "user", "content": prompt}],
        model=get_llm_model(),
        max_tokens=1024,
    )
    return parse_json_from_llm_response(response)


def _assemble(results: dict[str, dict], tables_context: list[dict]) -> dict:
    """Merge parallel results into a single plan dict."""
    plan: dict[str, Any] = {}

    tables_result = results.get("tables", {})
    plan["tables"] = tables_result.get("tables", [])

    if not plan["tables"] and tables_context:
        plan["tables"] = [
            {
                "identifier": t.get("table") or t.get("table_name") or t.get("identifier", "?"),
                "description": t.get("comment", ""),
                "column_configs": [
                    {"column_name": c.get("name", "?")}
                    for c in t.get("columns", [])
                ],
            }
            for t in tables_context
        ]

    qi = results.get("questions", {})
    plan["sample_questions"] = qi.get("sample_questions", [])
    plan["text_instructions"] = qi.get("text_instructions", [])

    sqls = results.get("sqls", {})
    plan["example_sqls"] = sqls.get("example_sqls", [])
    plan["benchmarks"] = sqls.get("benchmarks", [])

    analytics = results.get("analytics", {})
    plan["join_specs"] = analytics.get("join_specs", [])
    plan["measures"] = analytics.get("measures", [])
    plan["filters"] = analytics.get("filters", [])
    plan["expressions"] = analytics.get("expressions", [])

    return plan
