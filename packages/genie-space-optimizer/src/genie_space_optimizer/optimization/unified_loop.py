"""Native-only GSO optimization loop.

This module is the cutover path for the v2 optimizer: every iteration is
evaluated by the Databricks Genie Benchmark API, the LLM proposes ordinary
Patch DSL entries, and the existing applier owns config mutation plus rollback.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
from contextlib import nullcontext
from typing import Any

from genie_space_optimizer.common.config import (
    OPTIMIZER_PROMPT_MAX_CHARS,
    PROMPT_NAME_TEMPLATE,
    UNIFIED_OPTIMIZER_PATCH_RESPONSE_SCHEMA,
    UNIFIED_OPTIMIZER_PATCH_RULES,
    UNIFIED_OPTIMIZER_PATCH_SYSTEM_PROMPT,
    format_mlflow_template,
)
from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.optimization.applier import (
    apply_patch_set,
    classify_risk,
    rollback,
)
from genie_space_optimizer.optimization.eval_runner import (
    FULL,
    OfficialBenchmarkRunner,
    build_eval_output_from_official,
    resolve_space_benchmark_qids,
)
from genie_space_optimizer.optimization.evaluation import (
    _extract_json,
    _link_prompt_to_trace,
    get_registered_prompt_name,
)
from genie_space_optimizer.optimization.leakage import (
    BenchmarkCorpus,
    canonicalize_sql,
    is_benchmark_leak,
)
from genie_space_optimizer.optimization.llm_client import call_llm
from genie_space_optimizer.optimization.state import (
    mark_champion_iteration,
    mark_iteration_rolled_back,
    mark_patches_rolled_back,
    update_iteration_loop_state,
    update_run_status,
    write_iteration,
    write_patch,
)

logger = logging.getLogger(__name__)

_ALLOWED_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "update_description",
        "update_column_description",
        "add_column_synonym",
        "add_instruction",
        "update_instruction_section",
        "add_example_sql",
        "add_join_spec",
        "update_join_spec",
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    }
)

_PATCH_TYPE_CANONICAL_LEVER: dict[str, int] = {
    "update_description": 1,
    "update_column_description": 1,
    "add_column_synonym": 1,
    "add_join_spec": 4,
    "update_join_spec": 4,
    "add_instruction": 5,
    "update_instruction_section": 5,
    "add_example_sql": 5,
    "add_sql_snippet_measure": 6,
    "add_sql_snippet_filter": 6,
    "add_sql_snippet_expression": 6,
}

_TEXT_INSTRUCTION_PATCH_TYPES: frozenset[str] = frozenset(
    {"add_instruction", "update_instruction_section"}
)

_EXAMPLE_SQL_PATCH_TYPES: frozenset[str] = frozenset({"add_example_sql"})


def target_accuracy_percent(value: float) -> float:
    """Normalize a job target to the 0-100 scale used by eval rows."""
    return value * 100.0 if value <= 1.0 else value


def _parsed_space(config: dict[str, Any]) -> dict[str, Any]:
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return copy.deepcopy(parsed)
    return copy.deepcopy(config)


def _stable_config_id(config: dict[str, Any]) -> str:
    try:
        payload = json.dumps(config, sort_keys=True, default=str)
    except (TypeError, ValueError):
        payload = str(config)
    return "cfgsha:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _metric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if result != result else result


def _failure_rows(eval_result: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    rows = eval_result.get("rows")
    if not isinstance(rows, list):
        return []

    failures: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        assessment = str(row.get("assessment") or "").upper()
        if assessment == "GOOD":
            continue
        genie_eval = row.get("genie_equivalent_eval")
        reasons = []
        if isinstance(genie_eval, dict):
            reasons = genie_eval.get("assessment_reasons") or []
        failures.append(
            {
                "question_id": row.get("question_id") or row.get("inputs/question_id"),
                "question": row.get("question") or row.get("inputs/question"),
                "assessment": assessment or "NEEDS_REVIEW",
                "assessment_reasons": reasons,
                "generated_sql": row.get("generated_sql") or row.get("outputs/response"),
                "expected_sql": row.get("expected_sql") or row.get("inputs/expected_response"),
                "expected_asset_type": row.get("expected_asset_type"),
                "actual_asset_type": row.get("actual_asset_type"),
            }
        )
        if len(failures) >= limit:
            break
    return failures


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2)


def _short_text(value: Any, *, limit: int = 1200) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0]


def _normalized_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _failure_evidence_text(failures: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for row in failures:
        for key in (
            "question",
            "assessment_reasons",
            "generated_sql",
            "expected_sql",
            "expected_asset_type",
            "actual_asset_type",
        ):
            value = row.get(key)
            if isinstance(value, list):
                chunks.extend(str(v) for v in value)
            elif isinstance(value, dict):
                chunks.append(_stable_json(value))
            elif value:
                chunks.append(str(value))
    return "\n".join(chunks).lower()


def _table_identifier(table: dict[str, Any]) -> str:
    return str(table.get("identifier") or table.get("name") or "").strip()


def _column_name(column: dict[str, Any]) -> str:
    return str(column.get("column_name") or column.get("name") or "").strip()


def _identifier_aliases(identifier: str) -> set[str]:
    ident = identifier.strip().lower()
    if not ident:
        return set()
    parts = [p for p in re.split(r"[.`]", ident) if p]
    aliases = {ident, ident.replace("`", "")}
    if parts:
        aliases.add(parts[-1])
    if len(parts) >= 2:
        aliases.add(".".join(parts[-2:]))
    return {a for a in aliases if a}


def _text_mentions_any(text: str, aliases: set[str]) -> bool:
    padded = f" {text.lower()} "
    for alias in aliases:
        if not alias:
            continue
        if "." in alias or "_" in alias:
            if alias in padded:
                return True
            continue
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(alias)}(?![A-Za-z0-9_])", padded):
            return True
    return False


def _columns_for_prompt(
    table: dict[str, Any],
    evidence_text: str,
    *,
    max_columns: int = 90,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_columns = [
        c for c in (table.get("column_configs") or table.get("columns") or [])
        if isinstance(c, dict)
    ]
    relevant: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []
    for col in raw_columns:
        name = _column_name(col)
        bucket = relevant if name and _text_mentions_any(evidence_text, {name.lower()}) else other
        bucket.append(col)

    selected = relevant + other[: max(0, max_columns - len(relevant))]
    omitted = max(0, len(raw_columns) - len(selected))
    return [
        {
            "column_name": _column_name(col),
            "description": _short_text(col.get("description"), limit=500),
            "synonyms": col.get("synonyms") or [],
            "data_type": col.get("data_type") or col.get("type"),
            "referenced_by_failures": col in relevant,
        }
        for col in selected
    ], {
        "total": len(raw_columns),
        "included": len(selected),
        "referenced_included": len(relevant),
        "omitted": omitted,
        "omitted_names": [_column_name(c) for c in raw_columns if c not in selected][:25],
    }


def _asset_for_prompt(
    table: dict[str, Any],
    evidence_text: str,
    *,
    asset_type: str,
    referenced: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    columns, column_summary = _columns_for_prompt(table, evidence_text)
    return {
        "asset_type": asset_type,
        "identifier": _table_identifier(table),
        "description": _short_text(table.get("description"), limit=1000),
        "columns": columns,
        "referenced_by_failures": referenced,
    }, column_summary


def _function_identifier(function: dict[str, Any]) -> str:
    return str(function.get("identifier") or function.get("name") or function.get("function_name") or "").strip()


def _function_for_prompt(function: dict[str, Any], *, referenced: bool) -> dict[str, Any]:
    return {
        "identifier": _function_identifier(function),
        "description": _short_text(function.get("description") or function.get("comment"), limit=900),
        "parameters": function.get("parameters") or function.get("input_params") or [],
        "referenced_by_failures": referenced,
    }


def _sql_snippets_for_prompt(
    snippets: Any,
    evidence_text: str,
    *,
    max_per_type: int = 8,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if not isinstance(snippets, dict):
        return {}, {"total": 0, "included": 0, "omitted": 0, "omitted_by_type": {}}

    packed: dict[str, list[dict[str, Any]]] = {}
    omitted_by_type: dict[str, int] = {}
    total = 0
    included = 0
    for snippet_type, raw_items in snippets.items():
        items = [item for item in (raw_items or []) if isinstance(item, dict)]
        total += len(items)
        relevant: list[dict[str, Any]] = []
        other: list[dict[str, Any]] = []
        for item in items:
            item_text = _stable_json(item).lower()
            bucket = relevant if _text_mentions_any(evidence_text, set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", item_text))) else other
            bucket.append(item)
        selected = relevant + other[: max(0, max_per_type - len(relevant))]
        omitted_by_type[str(snippet_type)] = max(0, len(items) - len(selected))
        included += len(selected)
        packed[str(snippet_type)] = [
            {
                "id": item.get("id"),
                "display_name": item.get("display_name") or item.get("name"),
                "sql": item.get("sql"),
                "synonyms": item.get("synonyms") or [],
                "instruction": item.get("instruction") or item.get("comment") or [],
                "referenced_by_failures": item in relevant,
            }
            for item in selected
        ]
    return packed, {
        "total": total,
        "included": included,
        "omitted": max(0, total - included),
        "omitted_by_type": omitted_by_type,
    }


def _instruction_sections_for_prompt(
    instructions: dict[str, Any],
    evidence_text: str,
    *,
    max_sections: int = 8,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    sections: list[dict[str, str]] = []
    for item in instructions.get("text_instructions") or []:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        content_text = "\n".join(str(part) for part in content) if isinstance(content, list) else str(content or "")
        if not content_text.strip():
            continue
        current_header = "GENERAL"
        current_lines: list[str] = []
        for line in content_text.splitlines():
            header = re.match(r"^\s*#{1,3}\s+(.+?)\s*$", line)
            if header:
                if current_lines:
                    sections.append({"section": current_header, "content": "\n".join(current_lines).strip()})
                current_header = header.group(1).strip().upper()
                current_lines = []
            else:
                current_lines.append(line)
        if current_lines:
            sections.append({"section": current_header, "content": "\n".join(current_lines).strip()})

    relevant = [
        s for s in sections
        if s["content"] and _text_mentions_any(evidence_text, set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", s["content"].lower())))
    ]
    selected = relevant + [s for s in sections if s not in relevant][: max(0, max_sections - len(relevant))]
    return [
        {"section": s["section"], "content": _short_text(s["content"], limit=1600) or ""}
        for s in selected
    ], {
        "total": len(sections),
        "included": len(selected),
        "omitted": max(0, len(sections) - len(selected)),
        "omitted_sections": [s["section"] for s in sections if s not in selected][:25],
    }


def _optimizer_context_pack(
    config: dict[str, Any],
    eval_result: dict[str, Any],
    *,
    max_chars: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Build a relevance-aware JSON prompt context for the unified optimizer."""
    max_chars = int(max_chars or OPTIMIZER_PROMPT_MAX_CHARS)
    parsed = config.get("_parsed_space") if isinstance(config.get("_parsed_space"), dict) else config
    if not isinstance(parsed, dict):
        parsed = {}
    data_sources = parsed.get("data_sources") if isinstance(parsed.get("data_sources"), dict) else {}
    instructions = parsed.get("instructions") if isinstance(parsed.get("instructions"), dict) else {}
    failures = _failure_rows(eval_result, limit=30)
    evidence_text = _failure_evidence_text(failures)

    column_name_counts: dict[str, int] = {}
    for key in ("tables", "metric_views"):
        for table in data_sources.get(key) or []:
            if not isinstance(table, dict):
                continue
            for col in table.get("column_configs") or table.get("columns") or []:
                if not isinstance(col, dict):
                    continue
                name = _column_name(col).lower()
                if name:
                    column_name_counts[name] = column_name_counts.get(name, 0) + 1

    assets: list[tuple[dict[str, Any], bool, str]] = []
    for asset_type, key in (("table", "tables"), ("metric_view", "metric_views")):
        for table in data_sources.get(key) or []:
            if not isinstance(table, dict):
                continue
            identifier = _table_identifier(table)
            referenced = _text_mentions_any(evidence_text, _identifier_aliases(identifier))
            if not referenced:
                for col in table.get("column_configs") or table.get("columns") or []:
                    if isinstance(col, dict) and _column_name(col):
                        col_name = _column_name(col).lower()
                        if (
                            column_name_counts.get(col_name) == 1
                            and _text_mentions_any(evidence_text, {col_name})
                        ):
                            referenced = True
                            break
            assets.append((table, referenced, asset_type))

    selected_assets = [a for a in assets if a[1]]
    selected_assets.extend(a for a in assets if not a[1])
    included_assets: list[dict[str, Any]] = []
    column_summaries: dict[str, Any] = {}
    omitted_assets: list[str] = []
    context: dict[str, Any] = {
        "title": parsed.get("title") or parsed.get("name"),
        "description": _short_text(parsed.get("description"), limit=1200),
        "previous_eval": {
            "accuracy": eval_result.get("overall_accuracy"),
            "total_questions": eval_result.get("total_questions"),
            "correct_count": eval_result.get("correct_count"),
            "failure_count": len(failures),
            "failures": failures,
        },
        "space_context": {
            "assets": included_assets,
            "functions": [],
            "join_specs": [],
            "sql_snippets": {},
            "text_instruction_sections": [],
        },
        "omitted_context_summary": {},
    }

    for table, referenced, asset_type in selected_assets:
        projected, col_summary = _asset_for_prompt(
            table, evidence_text, asset_type=asset_type, referenced=referenced,
        )
        trial_assets = included_assets + [projected]
        context["space_context"]["assets"] = trial_assets
        if len(_pretty_json(context)) <= max_chars or referenced:
            included_assets.append(projected)
            column_summaries[projected["identifier"]] = col_summary
        else:
            omitted_assets.append(_table_identifier(table))
    context["space_context"]["assets"] = included_assets

    functions = [
        fn for key in ("functions", "table_valued_functions")
        for fn in (data_sources.get(key) or [])
        if isinstance(fn, dict)
    ]
    included_functions: list[dict[str, Any]] = []
    omitted_functions: list[str] = []
    ordered_functions: list[tuple[dict[str, Any], bool]] = []
    for fn in functions:
        identifier = _function_identifier(fn)
        referenced = _text_mentions_any(evidence_text, _identifier_aliases(identifier))
        ordered_functions.append((fn, referenced))
    ordered_functions.sort(key=lambda item: (not item[1], _function_identifier(item[0]).lower()))
    for fn, referenced in ordered_functions:
        projected_fn = _function_for_prompt(fn, referenced=referenced)
        trial = included_functions + [projected_fn]
        context["space_context"]["functions"] = trial
        if len(_pretty_json(context)) <= max_chars or referenced:
            included_functions.append(projected_fn)
        else:
            omitted_functions.append(_function_identifier(fn))
    context["space_context"]["functions"] = included_functions

    included_asset_ids = {str(a.get("identifier") or "").lower() for a in included_assets}
    join_specs = (
        instructions.get("join_specs") if isinstance(instructions.get("join_specs"), list) else []
    ) or (
        data_sources.get("join_specs") if isinstance(data_sources.get("join_specs"), list) else []
    ) or []
    included_join_specs: list[Any] = []
    omitted_join_specs = 0
    for js in join_specs:
        if not isinstance(js, dict):
            continue
        js_text = _stable_json(js).lower()
        relevant = any(asset_id and asset_id in js_text for asset_id in included_asset_ids) or any(
            _text_mentions_any(evidence_text, _identifier_aliases(asset_id)) for asset_id in included_asset_ids
        )
        compact_js = copy.deepcopy(js)
        trial = included_join_specs + [compact_js]
        context["space_context"]["join_specs"] = trial
        if len(_pretty_json(context)) <= max_chars or relevant:
            included_join_specs.append(compact_js)
        else:
            omitted_join_specs += 1
    context["space_context"]["join_specs"] = included_join_specs

    snippets, snippet_summary = _sql_snippets_for_prompt(
        instructions.get("sql_snippets") or {},
        evidence_text,
    )
    context["space_context"]["sql_snippets"] = snippets

    sections, section_summary = _instruction_sections_for_prompt(instructions, evidence_text)
    context["space_context"]["text_instruction_sections"] = sections

    context["omitted_context_summary"] = {
        "assets": {
            "total": len(assets),
            "included": len(included_assets),
            "omitted": max(0, len(assets) - len(included_assets)),
            "omitted_identifiers": omitted_assets[:25],
        },
        "columns_by_asset": column_summaries,
        "functions": {
            "total": len(functions),
            "included": len(included_functions),
            "omitted": max(0, len(functions) - len(included_functions)),
            "omitted_identifiers": omitted_functions[:25],
        },
        "join_specs": {
            "total": len(join_specs),
            "included": len(included_join_specs),
            "omitted": omitted_join_specs,
        },
        "sql_snippets": snippet_summary,
        "instruction_sections": section_summary,
    }

    context_json = _pretty_json(context)
    if len(context_json) > max_chars:
        # Last-resort compaction keeps JSON valid by dropping non-referenced
        # optional context, never by slicing the serialized string.
        context["space_context"]["assets"] = [
            a for a in included_assets if a.get("referenced_by_failures")
        ] or included_assets[:5]
        context["space_context"]["functions"] = [
            f for f in included_functions if f.get("referenced_by_failures")
        ] or included_functions[:5]
        context["space_context"]["text_instruction_sections"] = sections[:2]
        context["space_context"]["sql_snippets"] = {}
        context_json = _pretty_json(context)
    if len(context_json) > max_chars:
        for asset in context["space_context"].get("assets") or []:
            if not isinstance(asset, dict):
                continue
            asset["description"] = _short_text(asset.get("description"), limit=300)
            cols = [
                c for c in asset.get("columns") or []
                if isinstance(c, dict) and c.get("referenced_by_failures")
            ]
            asset["columns"] = cols[:25]
        context["space_context"]["join_specs"] = (context["space_context"].get("join_specs") or [])[:10]
        context_json = _pretty_json(context)
    if len(context_json) > max_chars:
        for failure in (context.get("previous_eval") or {}).get("failures") or []:
            if not isinstance(failure, dict):
                continue
            failure["generated_sql"] = _short_text(failure.get("generated_sql"), limit=2000)
            failure["expected_sql"] = _short_text(failure.get("expected_sql"), limit=2000)
            reasons = failure.get("assessment_reasons")
            if isinstance(reasons, list):
                failure["assessment_reasons"] = reasons[:8]
        context_json = _pretty_json(context)

    final_asset_ids = {
        str(asset.get("identifier") or "")
        for asset in context["space_context"].get("assets") or []
        if isinstance(asset, dict)
    }
    context["omitted_context_summary"]["assets"]["included"] = len(final_asset_ids)
    context["omitted_context_summary"]["assets"]["omitted"] = max(0, len(assets) - len(final_asset_ids))
    context["omitted_context_summary"]["assets"]["omitted_identifiers"] = [
        _table_identifier(table)
        for table, _referenced, _asset_type in assets
        if _table_identifier(table) not in final_asset_ids
    ][:25]
    final_function_ids = {
        str(fn.get("identifier") or "")
        for fn in context["space_context"].get("functions") or []
        if isinstance(fn, dict)
    }
    context["omitted_context_summary"]["functions"]["included"] = len(final_function_ids)
    context["omitted_context_summary"]["functions"]["omitted"] = max(
        0, len(functions) - len(final_function_ids)
    )
    context["omitted_context_summary"]["functions"]["omitted_identifiers"] = [
        _function_identifier(fn)
        for fn in functions
        if _function_identifier(fn) not in final_function_ids
    ][:25]
    context_json = _pretty_json(context)

    stats = {
        "prompt_context_chars": len(context_json),
        "context_hash": hashlib.sha256(context_json.encode("utf-8")).hexdigest(),
        "failure_ids": [f.get("question_id") for f in failures if f.get("question_id")],
        "included_counts": {
            "assets": len(context["space_context"]["assets"]),
            "functions": len(context["space_context"]["functions"]),
            "join_specs": len(context["space_context"]["join_specs"]),
            "sql_snippets": sum(
                len(v) for v in (context["space_context"].get("sql_snippets") or {}).values()
                if isinstance(v, list)
            ),
            "instruction_sections": len(context["space_context"]["text_instruction_sections"]),
        },
        "omitted_counts": {
            "assets": context["omitted_context_summary"]["assets"]["omitted"],
            "functions": context["omitted_context_summary"]["functions"]["omitted"],
            "join_specs": context["omitted_context_summary"]["join_specs"]["omitted"],
            "sql_snippets": context["omitted_context_summary"]["sql_snippets"]["omitted"],
            "instruction_sections": context["omitted_context_summary"]["instruction_sections"]["omitted"],
        },
    }
    return context, stats, context_json


def _native_eval(
    w: Any,
    *,
    space_id: str,
    benchmarks: list[dict[str, Any]],
    iteration: int,
    model_id: str | None = None,
) -> dict[str, Any]:
    qids = resolve_space_benchmark_qids(w, space_id, benchmarks)
    if not qids:
        raise RuntimeError(
            "Native Genie Benchmark API evaluation requires every benchmark to "
            "resolve to a live space benchmark question id. Task benchmark_qc_and_repair "
            "must push benchmarks before optimize runs."
        )
    runner = OfficialBenchmarkRunner(w)
    result = runner.run(space_id, qids, eval_scope=FULL)
    return build_eval_output_from_official(
        result,
        iteration=iteration,
        eval_scope=FULL,
        model_id=model_id,
    )


def _llm_messages(
    *,
    allowed_levers: list[int],
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    failures = _failure_rows(eval_result)
    context, context_stats, _context_json = _optimizer_context_pack(
        current_config,
        eval_result,
        max_chars=OPTIMIZER_PROMPT_MAX_CHARS,
    )
    user = {
        "previous_eval": {
            "accuracy": eval_result.get("overall_accuracy"),
            "total_questions": eval_result.get("total_questions"),
            "correct_count": eval_result.get("correct_count"),
            "failure_count": len(failures),
            "failures": failures,
        },
        "current_space_config": context.get("space_context", {}),
        "omitted_context_summary": context.get("omitted_context_summary", {}),
        "space_title": context.get("title"),
        "space_description": context.get("description"),
        "allowed_levers": allowed_levers,
        "allowed_patch_types": sorted(_ALLOWED_PATCH_TYPES),
        "response_schema": UNIFIED_OPTIMIZER_PATCH_RESPONSE_SCHEMA,
        "patch_rules": UNIFIED_OPTIMIZER_PATCH_RULES,
        "recent_reflections": reflections[-2:],
    }
    user_json = _pretty_json(user)
    context_stats["prompt_chars"] = len(UNIFIED_OPTIMIZER_PATCH_SYSTEM_PROMPT) + len(user_json)
    return [
        {"role": "system", "content": UNIFIED_OPTIMIZER_PATCH_SYSTEM_PROMPT},
        {"role": "user", "content": user_json},
    ], context_stats


def _normalize_llm_patches(raw: Any, *, allowed_levers: list[int]) -> tuple[int | None, str, list[dict[str, Any]]]:
    if isinstance(raw, list):
        parsed = {"patches": raw}
    elif isinstance(raw, dict):
        parsed = raw
    else:
        return None, "LLM returned no parseable JSON object", []

    try:
        proposal_lever = int(parsed.get("lever")) if parsed.get("lever") is not None else None
    except (TypeError, ValueError):
        proposal_lever = None
    if proposal_lever not in allowed_levers:
        proposal_lever = allowed_levers[0] if allowed_levers else None

    rationale = str(parsed.get("rationale") or parsed.get("reason") or "").strip()
    patches_raw = parsed.get("patches")
    if patches_raw is None and isinstance(parsed.get("patch"), dict):
        patches_raw = [parsed["patch"]]
    if not isinstance(patches_raw, list):
        return proposal_lever, rationale or "LLM returned no patches", []

    patches: list[dict[str, Any]] = []
    for idx, item in enumerate(patches_raw):
        if not isinstance(item, dict):
            continue
        patch = dict(item)
        ptype = str(patch.get("type") or patch.get("patch_type") or "").strip()
        if ptype not in _ALLOWED_PATCH_TYPES:
            logger.info("Dropping unsupported LLM patch type %r", ptype)
            continue
        patch["type"] = ptype
        try:
            patch_lever = int(patch.get("lever")) if patch.get("lever") is not None else proposal_lever
        except (TypeError, ValueError):
            patch_lever = proposal_lever
        if patch_lever not in allowed_levers:
            patch_lever = proposal_lever
        canonical_lever = _PATCH_TYPE_CANONICAL_LEVER.get(ptype)
        if canonical_lever in allowed_levers:
            patch_lever = canonical_lever
        if patch_lever is not None:
            patch["lever"] = patch_lever
        patch.setdefault("risk_level", classify_risk(ptype))
        patch.setdefault("proposal_id", f"unified-{idx + 1}")
        patch.setdefault("source_proposal_id", patch["proposal_id"])
        patch.setdefault("patch_family", "unified_llm")
        if rationale:
            patch.setdefault("rationale", rationale)
        patches.append(patch)
    return proposal_lever, rationale, patches


_SQL_SNIPPET_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    }
)

_SNIPPET_TYPE_FROM_PATCH: dict[str, str] = {
    "add_sql_snippet_measure": "measure",
    "add_sql_snippet_filter": "filter",
    "add_sql_snippet_expression": "expression",
}

_SNIPPET_SECTION_FROM_TYPE: dict[str, str] = {
    "measure": "measures",
    "filter": "filters",
    "expression": "expressions",
}

_PROSE_LEAK_PATCH_FIELDS: dict[str, tuple[str, ...]] = {
    "add_instruction": ("new_text", "proposed_value", "value"),
    "update_instruction_section": ("new_text", "proposed_value", "value"),
    "update_description": ("new_text", "description", "structured_sections"),
    "update_column_description": ("new_text", "description", "structured_sections"),
    "add_example_sql": ("example_question", "example_sql", "sql", "new_text"),
    "update_example_sql": ("example_question", "example_sql", "sql", "new_text"),
}


def _flatten_visible_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_flatten_visible_values(item))
        return out
    if isinstance(value, dict):
        out = []
        for item in value.values():
            out.extend(_flatten_visible_values(item))
        return out
    return [str(value)]


def _benchmark_forbidden_strings(
    *,
    benchmarks: list[dict[str, Any]],
    eval_result: dict[str, Any],
) -> tuple[set[str], set[str]]:
    prose: set[str] = set()
    sql: set[str] = set()

    def _add_prose(value: Any) -> None:
        text = _normalized_text(value)
        if len(text) >= 24:
            prose.add(text)

    def _add_sql(value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        fingerprint = canonicalize_sql(text)
        if fingerprint:
            sql.add(fingerprint)
        _add_prose(text)

    for row in benchmarks or []:
        if not isinstance(row, dict):
            continue
        _add_prose(row.get("question") or row.get("inputs/question"))
        _add_sql(row.get("expected_sql") or row.get("expected_response") or row.get("inputs/expected_response"))

    rows = eval_result.get("rows") if isinstance(eval_result, dict) else None
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            _add_prose(row.get("question") or row.get("inputs/question"))
            _add_sql(row.get("expected_sql") or row.get("inputs/expected_response"))
            _add_sql(row.get("generated_sql") or row.get("outputs/response"))
    return prose, sql


def _benchmark_corpus_for_example_sql(
    *,
    benchmarks: list[dict[str, Any]],
    eval_result: dict[str, Any],
) -> BenchmarkCorpus:
    """Build a leakage corpus from benchmark prompts and answer-shaped SQL."""
    rows: list[dict[str, Any]] = []

    def add_row(*, question: Any, sql: Any, qid: Any = "") -> None:
        q = str(question or "").strip()
        s = str(sql or "").strip()
        if not q and not s:
            return
        rows.append({"id": str(qid or ""), "question": q, "expected_sql": s})

    for row in benchmarks or []:
        if not isinstance(row, dict):
            continue
        add_row(
            qid=row.get("id") or row.get("benchmark_id") or row.get("question_id"),
            question=row.get("question") or row.get("inputs/question"),
            sql=row.get("expected_sql")
            or row.get("expected_response")
            or row.get("inputs/expected_response"),
        )

    rows_raw = eval_result.get("rows") if isinstance(eval_result, dict) else None
    if isinstance(rows_raw, list):
        for row in rows_raw:
            if not isinstance(row, dict):
                continue
            qid = row.get("question_id") or row.get("inputs/question_id")
            question = row.get("question") or row.get("inputs/question")
            add_row(
                qid=qid,
                question=question,
                sql=row.get("expected_sql")
                or row.get("expected_response")
                or row.get("inputs/expected_response"),
            )
            add_row(
                qid=f"{qid}:generated" if qid else "",
                question=question,
                sql=row.get("generated_sql") or row.get("outputs/response"),
            )

    return BenchmarkCorpus.from_benchmarks(rows)


def _patch_has_benchmark_prose_leak(
    patch: dict[str, Any],
    *,
    prose_needles: set[str],
    sql_fingerprints: set[str],
) -> tuple[bool, str]:
    patch_type = str(patch.get("type") or patch.get("patch_type") or "")
    fields = _PROSE_LEAK_PATCH_FIELDS.get(patch_type)
    if not fields:
        return False, ""
    for field in fields:
        for value in _flatten_visible_values(patch.get(field)):
            norm = _normalized_text(value)
            if norm and any(needle in norm for needle in prose_needles):
                return True, f"{patch_type}.{field}:benchmark_text_copy"
            if field in {"example_sql", "sql", "new_text"}:
                fp = canonicalize_sql(value)
                if fp and fp in sql_fingerprints:
                    return True, f"{patch_type}.{field}:benchmark_sql_copy"
    return False, ""


def _validate_example_sql_patch(
    patch: dict[str, Any],
    *,
    benchmark_corpus: BenchmarkCorpus,
    w: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    patch_type = str(patch.get("type") or "")
    if patch_type not in _EXAMPLE_SQL_PATCH_TYPES:
        return patch, None

    required = (
        "example_question",
        "example_sql",
        "usage_guidance",
        "source_failure_pattern",
        "affected_qids",
        "semantic_delta_from_benchmark",
        "why_not_benchmark_copy",
    )
    missing = [field for field in required if patch.get(field) in (None, "", [])]
    if missing:
        dropped = dict(patch)
        dropped["drop_reason"] = "example_sql_contract_failed"
        dropped["drop_detail"] = f"missing required fields: {', '.join(missing)}"
        return None, dropped

    leak, reason = is_benchmark_leak(
        patch,
        patch_type,
        benchmark_corpus,
        w=w,
    )
    if leak:
        dropped = dict(patch)
        dropped["drop_reason"] = "benchmark_example_sql_leak"
        dropped["drop_detail"] = reason
        return None, dropped

    return patch, None


def _validate_text_instruction_fallback(
    patch: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    patch_type = str(patch.get("type") or "")
    if patch_type not in _TEXT_INSTRUCTION_PATCH_TYPES:
        return patch, None

    rejected = patch.get("rejected_patch_types")
    if not isinstance(rejected, list) or not rejected:
        dropped = dict(patch)
        dropped["drop_reason"] = "instruction_fallback_unjustified"
        dropped["drop_detail"] = "missing non-empty rejected_patch_types"
        return None, dropped

    invalid_indexes: list[str] = []
    for idx, item in enumerate(rejected):
        if (
            not isinstance(item, dict)
            or not str(item.get("type") or "").strip()
            or not str(item.get("reason") or "").strip()
        ):
            invalid_indexes.append(str(idx))
    if invalid_indexes:
        dropped = dict(patch)
        dropped["drop_reason"] = "instruction_fallback_unjustified"
        dropped["drop_detail"] = (
            "invalid rejected_patch_types entries: " + ", ".join(invalid_indexes)
        )
        return None, dropped

    return patch, None


def _validate_unified_sql_snippet_patch(
    patch: dict[str, Any],
    *,
    current_config: dict[str, Any],
    spark: Any,
    catalog: str,
    schema: str,
    w: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    patch_type = str(patch.get("type") or "")
    if patch_type not in _SQL_SNIPPET_PATCH_TYPES:
        clean = dict(patch)
        clean.pop("validation_passed", None)
        return clean, None

    clean = dict(patch)
    clean.pop("validation_passed", None)
    snippet_type = str(clean.get("snippet_type") or _SNIPPET_TYPE_FROM_PATCH[patch_type]).strip().lower()
    if snippet_type.endswith("s"):
        snippet_type = snippet_type[:-1]
    required = ("sql", "display_name", "instruction", "synonyms", "target_table", "snippet_type")
    missing = [field for field in required if field not in clean or clean.get(field) in (None, "")]
    if missing:
        dropped = dict(clean)
        dropped["drop_reason"] = "snippet_validation_failed"
        dropped["drop_detail"] = f"missing required fields: {', '.join(missing)}"
        return None, dropped
    if snippet_type not in _SNIPPET_SECTION_FROM_TYPE:
        dropped = dict(clean)
        dropped["drop_reason"] = "snippet_validation_failed"
        dropped["drop_detail"] = f"unsupported snippet_type={snippet_type}"
        return None, dropped

    synonyms = clean.get("synonyms")
    if isinstance(synonyms, str):
        synonyms = [synonyms]
    elif not isinstance(synonyms, list):
        synonyms = []

    try:
        from genie_space_optimizer.optimization.benchmarks import validate_sql_snippet

        valid, reason, normalized_sql = validate_sql_snippet(
            str(clean.get("sql") or ""),
            snippet_type,
            current_config,
            spark=spark,
            catalog=catalog,
            gold_schema=schema,
            w=w,
            warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
        )
    except Exception as exc:
        valid, reason, normalized_sql = False, f"{type(exc).__name__}: {exc}", str(clean.get("sql") or "")

    if not valid:
        dropped = dict(clean)
        dropped["drop_reason"] = "snippet_validation_failed"
        dropped["drop_detail"] = reason
        return None, dropped

    from genie_space_optimizer.common.genie_schema import generate_genie_id

    display_name = str(clean.get("display_name") or "").strip()
    alias = str(clean.get("alias") or display_name or snippet_type).strip()
    alias = re.sub(r"[^A-Za-z0-9_]+", "_", alias).strip("_").lower() or snippet_type
    instruction = str(clean.get("instruction") or "").strip()
    snippet = {
        "id": str(clean.get("snippet_id") or clean.get("id") or generate_genie_id()),
        "display_name": display_name,
        "sql": [normalized_sql],
        "synonyms": synonyms,
        "instruction": [instruction] if instruction else [],
    }
    if snippet_type != "filter":
        snippet["alias"] = alias
    clean.update(
        {
            "snippet_type": _SNIPPET_SECTION_FROM_TYPE[snippet_type],
            "synonyms": synonyms,
            "sql": normalized_sql,
            "target": str(clean.get("target_table") or ""),
            "validation_passed": True,
            "sql_snippet": snippet,
        }
    )
    return clean, None


def _preapply_safety_screen(
    patches: list[dict[str, Any]],
    *,
    current_config: dict[str, Any],
    benchmarks: list[dict[str, Any]],
    eval_result: dict[str, Any],
    spark: Any,
    catalog: str,
    schema: str,
    w: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prose_needles, sql_fingerprints = _benchmark_forbidden_strings(
        benchmarks=benchmarks,
        eval_result=eval_result,
    )
    example_sql_corpus = _benchmark_corpus_for_example_sql(
        benchmarks=benchmarks,
        eval_result=eval_result,
    )
    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for patch in patches:
        leaked, reason = _patch_has_benchmark_prose_leak(
            patch,
            prose_needles=prose_needles,
            sql_fingerprints=sql_fingerprints,
        )
        if leaked:
            dropped_patch = dict(patch)
            dropped_patch["drop_reason"] = "benchmark_prose_leak"
            dropped_patch["drop_detail"] = reason
            dropped.append(dropped_patch)
            continue

        validated, validation_drop = _validate_text_instruction_fallback(patch)
        if validation_drop is not None:
            dropped.append(validation_drop)
            continue
        if validated is None:
            continue
        patch = validated

        validated, validation_drop = _validate_example_sql_patch(
            patch,
            benchmark_corpus=example_sql_corpus,
            w=w,
        )
        if validation_drop is not None:
            dropped.append(validation_drop)
            continue
        if validated is None:
            continue
        patch = validated

        validated, validation_drop = _validate_unified_sql_snippet_patch(
            patch,
            current_config=current_config,
            spark=spark,
            catalog=catalog,
            schema=schema,
            w=w,
        )
        if validation_drop is not None:
            dropped.append(validation_drop)
            continue
        if validated is not None:
            kept.append(validated)
    return kept, dropped


def _prompt_name_for_key(prompt_key: str, *, catalog: str = "", schema: str = "") -> str:
    registered = get_registered_prompt_name(prompt_key)
    if registered:
        return registered
    uc_schema = ".".join(part for part in (catalog, schema) if part)
    if uc_schema and "." in uc_schema:
        return format_mlflow_template(
            PROMPT_NAME_TEMPLATE,
            uc_schema=uc_schema,
            judge_name=prompt_key,
        )
    return prompt_key


def _start_chain_span(name: str) -> Any:
    try:
        import mlflow
        from mlflow.entities import SpanType

        return mlflow.start_span(name=name, span_type=SpanType.CHAIN)
    except Exception:
        return nullcontext(None)


def propose_patches(
    w: Any,
    *,
    allowed_levers: list[int],
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
    catalog: str = "",
    schema: str = "",
) -> tuple[int | None, str, list[dict[str, Any]], str]:
    messages, context_stats = _llm_messages(
        allowed_levers=allowed_levers,
        current_config=current_config,
        eval_result=eval_result,
        reflections=reflections,
    )
    prompt_name = _prompt_name_for_key(
        "unified_optimizer_patch",
        catalog=catalog,
        schema=schema,
    )
    with _start_chain_span("unified_optimizer_patch") as span:
        _link_prompt_to_trace(prompt_name)
        try:
            if span is not None:
                span.set_inputs(
                    {
                        "prompt_name": prompt_name,
                        "prompt_chars": context_stats.get("prompt_chars"),
                        "context_chars": context_stats.get("prompt_context_chars"),
                        "context_hash": context_stats.get("context_hash"),
                        "failure_ids": context_stats.get("failure_ids"),
                        "included_counts": context_stats.get("included_counts"),
                        "omitted_counts": context_stats.get("omitted_counts"),
                        "messages": messages,
                    }
                )
        except Exception:
            pass
        text, _response = call_llm(w, messages=messages, max_tokens=4096)
        try:
            if span is not None:
                span.set_outputs({"response_chars": len(text or "")})
        except Exception:
            pass
    parsed = _extract_json(text)
    lever, rationale, patches = _normalize_llm_patches(
        parsed,
        allowed_levers=allowed_levers,
    )
    return lever, rationale, patches, text


def _patch_record(entry: dict[str, Any], *, lever: int, apply_mode: str) -> dict[str, Any]:
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    applied_type = entry.get("applied_patch_type") or patch.get(
        "type",
        action.get("action_type", "unknown"),
    )
    proposal_type = (
        entry.get("proposal_patch_type")
        or patch.get("_proposal_patch_type")
        or patch.get("type", applied_type)
    )
    return {
        "patch_type": proposal_type,
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", patch.get("risk_level", "medium")),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id") or patch.get("proposal_id", ""),
        "applied_patch_type": applied_type,
        "applied_patch_detail": entry.get("applied_patch_detail"),
        "patch_family": patch.get("patch_family"),
        "target_qids": patch.get("target_qids", []),
        "rationale": patch.get("rationale"),
    }


def _loop_state(
    *,
    attempt_no: int,
    attempt_mode: str,
    best_accuracy: float,
    current_hypothesis: dict[str, Any] | None = None,
    decision: str | None = None,
    decision_reason: str | None = None,
    surgical_attempts_used: int,
    target_accuracy: float,
    max_attempts: int,
    terminal_reason: str | None = None,
    config: dict[str, Any] | None = None,
    do_not_repeat: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "attempt_no": attempt_no,
        "attempt_mode": attempt_mode,
        "best_accuracy": best_accuracy,
        "best_config_version_id": _stable_config_id(config or {}),
        "current_hypothesis": current_hypothesis,
        "do_not_repeat": do_not_repeat or [],
        "terminal_reason": terminal_reason,
        "decision": decision,
        "decision_reason": decision_reason,
        "surgical_attempts_used": surgical_attempts_used,
        "next_hypothesis": None,
        "target_accuracy": target_accuracy,
        "max_attempts": max_attempts,
    }


def _stamp_terminal(
    spark: Any,
    *,
    run_id: str,
    iteration: int,
    reason: str,
    best_accuracy: float,
    surgical_attempts_used: int,
    target_accuracy: float,
    max_attempts: int,
    config: dict[str, Any],
    catalog: str,
    schema: str,
) -> None:
    update_iteration_loop_state(
        spark,
        run_id,
        iteration,
        catalog=catalog,
        schema=schema,
        eval_scope=FULL,
        loop_state=_loop_state(
            attempt_no=iteration,
            attempt_mode="baseline" if iteration == 0 else "llm_patch",
            best_accuracy=best_accuracy,
            decision="terminal",
            decision_reason=reason,
            surgical_attempts_used=surgical_attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            terminal_reason=reason,
            config=config,
        ),
    )
    mark_champion_iteration(
        spark,
        run_id,
        iteration,
        catalog=catalog,
        schema=schema,
        eval_scope=FULL,
    )
    update_run_status(
        spark,
        run_id,
        catalog,
        schema,
        best_iteration=iteration,
        best_accuracy=best_accuracy,
        convergence_reason=reason,
    )


def run_unified_optimization_loop(
    w: Any,
    spark: Any,
    *,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict[str, Any]],
    catalog: str,
    schema: str,
    levers: list[int],
    max_attempts: int,
    target_accuracy: float,
    apply_mode: str = "genie_config",
    triggered_by: str = "",
    human_corrections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run baseline eval plus bounded LLM patch attempts."""
    _ = (domain, triggered_by, human_corrections)
    target_accuracy = target_accuracy_percent(float(target_accuracy))
    allowed_levers = [int(l) for l in levers if int(l) in {1, 2, 3, 4, 5, 6}]
    if not allowed_levers:
        allowed_levers = [1, 2, 3, 4, 5, 6]
    max_attempts = max(0, int(max_attempts))

    current_config = _parsed_space(fetch_space_config(w, space_id))
    benchmark_corpus = BenchmarkCorpus.from_benchmarks(benchmarks)

    baseline_eval = _native_eval(
        w,
        space_id=space_id,
        benchmarks=benchmarks,
        iteration=0,
    )
    best_accuracy = _metric(baseline_eval.get("overall_accuracy"))
    best_iteration = 0
    best_eval = baseline_eval
    attempts_used = 0
    accepted = 0
    rolled_back = 0
    reflections: list[dict[str, Any]] = []

    write_iteration(
        spark,
        run_id,
        0,
        baseline_eval,
        catalog=catalog,
        schema=schema,
        lever=None,
        eval_scope=FULL,
        reflection_json={"phase": "baseline"},
        config_snapshot=current_config,
        loop_state=_loop_state(
            attempt_no=0,
            attempt_mode="baseline",
            best_accuracy=best_accuracy,
            decision="accept",
            decision_reason="baseline",
            surgical_attempts_used=0,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
        ),
    )
    update_run_status(
        spark,
        run_id,
        catalog,
        schema,
        best_iteration=0,
        best_accuracy=best_accuracy,
    )

    if baseline_eval.get("eval_run_failed"):
        terminal_reason = "EVAL_INVALID"
        _stamp_terminal(
            spark,
            run_id=run_id,
            iteration=best_iteration,
            reason=terminal_reason,
            best_accuracy=best_accuracy,
            surgical_attempts_used=attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
            catalog=catalog,
            schema=schema,
        )
        return {
            "run_id": run_id,
            "accuracy": best_accuracy,
            "best_iteration": best_iteration,
            "iteration_counter": 0,
            "terminal_reason": terminal_reason,
            "surgical_attempts_used": attempts_used,
            "levers_accepted": [],
            "levers_rolled_back": [],
        }

    if best_accuracy >= target_accuracy:
        terminal_reason = "TARGET_REACHED"
        _stamp_terminal(
            spark,
            run_id=run_id,
            iteration=best_iteration,
            reason=terminal_reason,
            best_accuracy=best_accuracy,
            surgical_attempts_used=attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
            catalog=catalog,
            schema=schema,
        )
        return {
            "run_id": run_id,
            "accuracy": best_accuracy,
            "best_iteration": best_iteration,
            "iteration_counter": 0,
            "terminal_reason": terminal_reason,
            "surgical_attempts_used": attempts_used,
            "levers_accepted": [],
            "levers_rolled_back": [],
        }

    levers_accepted: list[int] = []
    levers_rolled_back: list[int] = []
    terminal_reason: str | None = None

    for iteration in range(1, max_attempts + 1):
        attempts_used = iteration
        lever, rationale, patches, raw_response = propose_patches(
            w,
            allowed_levers=allowed_levers,
            current_config=current_config,
            eval_result=best_eval,
            reflections=reflections,
            catalog=catalog,
            schema=schema,
        )
        hypothesis = {
            "lever": lever,
            "rationale": rationale,
            "proposed_patch_count": len(patches),
            "patch_count": len(patches),
            "raw_response_preview": raw_response[:1000],
        }
        if not patches or lever is None:
            terminal_reason = "NO_NEW_HYPOTHESIS"
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": terminal_reason,
                    "rationale": rationale,
                }
            )
            break

        patches, preapply_dropped = _preapply_safety_screen(
            patches,
            current_config=current_config,
            benchmarks=benchmarks,
            eval_result=best_eval,
            spark=spark,
            catalog=catalog,
            schema=schema,
            w=w,
        )
        if not patches:
            terminal_reason = "NO_NEW_HYPOTHESIS"
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": terminal_reason,
                    "rationale": rationale,
                    "dropped_patches": preapply_dropped,
                }
            )
            break
        hypothesis["patch_count"] = len(patches)
        hypothesis["preapply_dropped_count"] = len(preapply_dropped)
        if preapply_dropped:
            hypothesis["preapply_dropped_reasons"] = [
                p.get("drop_reason") for p in preapply_dropped
            ]

        apply_log = apply_patch_set(
            w,
            space_id,
            patches,
            current_config,
            apply_mode=apply_mode,
            benchmark_corpus=benchmark_corpus,
        )
        if preapply_dropped:
            apply_log["dropped_patches"] = preapply_dropped + list(
                apply_log.get("dropped_patches") or []
            )
        applied_entries = apply_log.get("applied") or []
        for idx, entry in enumerate(applied_entries):
            patch_lever = int(entry.get("patch", {}).get("lever", lever) or lever)
            write_patch(
                spark,
                run_id,
                iteration,
                patch_lever,
                idx,
                _patch_record(entry, lever=patch_lever, apply_mode=apply_mode),
                catalog,
                schema,
            )

        if not apply_log.get("patch_deployed") or not applied_entries:
            terminal_reason = "NO_NEW_HYPOTHESIS"
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": terminal_reason,
                    "rationale": rationale or apply_log.get("patch_error"),
                    "dropped_patches": apply_log.get("dropped_patches") or [],
                }
            )
            break

        candidate_config = copy.deepcopy(apply_log.get("post_snapshot") or current_config)
        candidate_eval = _native_eval(
            w,
            space_id=space_id,
            benchmarks=benchmarks,
            iteration=iteration,
        )
        candidate_accuracy = _metric(candidate_eval.get("overall_accuracy"))

        write_iteration(
            spark,
            run_id,
            iteration,
            candidate_eval,
            catalog=catalog,
            schema=schema,
            lever=lever,
            eval_scope=FULL,
            reflection_json={
                "rationale": rationale,
                "patch_count": len(patches),
                "applied_count": len(applied_entries),
                "dropped_patches": apply_log.get("dropped_patches") or [],
            },
            config_snapshot=candidate_config,
            loop_state=_loop_state(
                attempt_no=iteration,
                attempt_mode="llm_patch",
                best_accuracy=best_accuracy,
                current_hypothesis=hypothesis,
                decision="pending",
                surgical_attempts_used=attempts_used,
                target_accuracy=target_accuracy,
                max_attempts=max_attempts,
                config=candidate_config,
                do_not_repeat=reflections[-2:],
            ),
        )

        if candidate_eval.get("eval_run_failed"):
            rollback(apply_log, w, space_id, metadata_snapshot=current_config)
            mark_patches_rolled_back(
                spark,
                run_id,
                iteration,
                "candidate eval invalid",
                catalog,
                schema,
            )
            terminal_reason = "EVAL_INVALID"
            rolled_back += 1
            levers_rolled_back.append(int(lever))
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": "rolled_back",
                    "reason": terminal_reason,
                    "accuracy": candidate_accuracy,
                }
            )
            break

        if candidate_accuracy > best_accuracy:
            best_accuracy = candidate_accuracy
            best_iteration = iteration
            best_eval = candidate_eval
            current_config = candidate_config
            accepted += 1
            levers_accepted.append(int(lever))
            decision_reason = (
                f"accuracy improved to {candidate_accuracy:.2f} "
                f"from previous best"
            )
            update_iteration_loop_state(
                spark,
                run_id,
                iteration,
                catalog=catalog,
                schema=schema,
                eval_scope=FULL,
                loop_state=_loop_state(
                    attempt_no=iteration,
                    attempt_mode="llm_patch",
                    best_accuracy=best_accuracy,
                    current_hypothesis=hypothesis,
                    decision="accept",
                    decision_reason=decision_reason,
                    surgical_attempts_used=attempts_used,
                    target_accuracy=target_accuracy,
                    max_attempts=max_attempts,
                    config=current_config,
                    do_not_repeat=reflections[-2:],
                ),
            )
            update_run_status(
                spark,
                run_id,
                catalog,
                schema,
                best_iteration=best_iteration,
                best_accuracy=best_accuracy,
            )
            reflections.append(
                {
                    "iteration": iteration,
                    "decision": "accepted",
                    "lever": lever,
                    "accuracy": candidate_accuracy,
                    "rationale": rationale,
                }
            )
            if best_accuracy >= target_accuracy:
                terminal_reason = "TARGET_REACHED"
                break
            continue

        reason = (
            f"candidate accuracy {candidate_accuracy:.2f} did not improve "
            f"on best {best_accuracy:.2f}"
        )
        rollback(apply_log, w, space_id, metadata_snapshot=current_config)
        mark_patches_rolled_back(spark, run_id, iteration, reason, catalog, schema)
        mark_iteration_rolled_back(
            spark,
            run_id,
            iteration,
            catalog=catalog,
            schema=schema,
            eval_scope=FULL,
            reason=reason,
        )
        update_iteration_loop_state(
            spark,
            run_id,
            iteration,
            catalog=catalog,
            schema=schema,
            eval_scope=FULL,
            loop_state=_loop_state(
                attempt_no=iteration,
                attempt_mode="llm_patch",
                best_accuracy=best_accuracy,
                current_hypothesis=hypothesis,
                decision="reject",
                decision_reason=reason,
                surgical_attempts_used=attempts_used,
                target_accuracy=target_accuracy,
                max_attempts=max_attempts,
                config=current_config,
                do_not_repeat=reflections[-2:],
            ),
        )
        rolled_back += 1
        levers_rolled_back.append(int(lever))
        reflections.append(
            {
                "iteration": iteration,
                "decision": "rolled_back",
                "lever": lever,
                "accuracy": candidate_accuracy,
                "reason": reason,
                "rationale": rationale,
            }
        )

    if terminal_reason is None:
        terminal_reason = "MAX_ATTEMPTS"

    _stamp_terminal(
        spark,
        run_id=run_id,
        iteration=best_iteration,
        reason=terminal_reason,
        best_accuracy=best_accuracy,
        surgical_attempts_used=attempts_used,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        config=current_config,
        catalog=catalog,
        schema=schema,
    )

    return {
        "run_id": run_id,
        "accuracy": best_accuracy,
        "best_iteration": best_iteration,
        "iteration_counter": attempts_used,
        "terminal_reason": terminal_reason,
        "surgical_attempts_used": attempts_used,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "accepted_attempts": accepted,
        "rolled_back_attempts": rolled_back,
        "reflections": reflections,
    }
