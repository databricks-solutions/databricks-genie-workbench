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
    WELL_CURATED_SPACE_RUBRIC,
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
from genie_space_optimizer.optimization.benchmarking import (
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
from genie_space_optimizer.optimization.space_quality_enrichment import (
    attach_top_level_description,
    build_prompt_matching_context,
    run_space_quality_enrichment,
    scan_input_for_iq,
)
from genie_space_optimizer.optimization.state import (
    load_all_scored_iterations,
    mark_champion_iteration,
    mark_iteration_rolled_back,
    mark_patches_rolled_back,
    update_iteration_loop_state,
    update_run_status,
    write_iteration,
    write_patch,
    write_artifact,
    write_required_artifact,
)
from genie_space_optimizer.optimization.wide_schema import (
    MAX_ACTIVE_COLUMNS_PER_ASSET,
    _identifier_parts,
    active_column_keys,
    normalize_component,
    project_active_inventory,
    revise_plan_for_column,
    revise_plan_with_profile_outcomes,
    sql_column_evidence,
    validate_inventory,
    validate_selection_plan,
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

_MAX_PROPOSAL_RECOVERY_RETRIES = 1

# Extra proposal retries granted specifically when a pre-apply wipeout was
# caused by a benchmark-leak drop AND viable non-leaking patch types remain.
# This lets the loop PIVOT to a different lever family (snippet / expression /
# join / metadata) instead of terminating NO_NEW_HYPOTHESIS at iteration 0 —
# the observed failure mode where the LLM reconstructs a benchmark's gold SQL
# as an "example" and the firewall correctly blocks every patch. Bounded so a
# persistently-leaking LLM still terminates.
_MAX_LEAK_PIVOT_RETRIES = 2

# Pre-apply drop reasons that indicate a proposed patch reproduced benchmark
# question/answer text. When an entire proposal is wiped for these reasons, the
# offending patch types are banned for the remainder of the run.
_BENCHMARK_LEAK_DROP_REASONS: frozenset[str] = frozenset(
    {"benchmark_example_sql_leak", "benchmark_prose_leak"}
)

# Eval-run statuses that are TRANSIENT infra faults (not a real "the config is
# broken" signal) — an eval-service timeout or a cancelled run can recover on a
# fresh attempt. A single transient blip should not sink a run that has already
# committed real improvement, so _native_eval retries these before surfacing
# eval_run_failed. EVALUATION_FAILED is NOT here: it means the eval genuinely
# failed and retrying is unlikely to help.
_TRANSIENT_EVAL_STATUSES: frozenset[str] = frozenset(
    {"EVALUATION_TIMEOUT", "EVALUATION_CANCELLED"}
)

# Number of extra attempts _native_eval makes when an eval returns a transient
# status. 2 retries (3 total attempts) covers a brief eval-service degradation
# window without stalling the loop indefinitely on a persistently-down service.
_MAX_TRANSIENT_EVAL_RETRIES = 2

_METADATA_PATCH_TYPES: frozenset[str] = frozenset(
    {"update_description", "update_column_description", "add_column_synonym"}
)

_STRUCTURED_BEHAVIOR_REASON_MARKERS: tuple[str, ...] = (
    "MISSING_COLUMNS",
    "INCOMPLETE_OR_PARTIAL_OUTPUT",
    "MISSING_OR_INCORRECT_FILTER",
    "INCORRECT_FILTER",
    "INCORRECT_METRIC",
    "INCORRECT_FUNCTION",
    "FUNCTION_USAGE",
)


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


def _space_quality_scan_for_prompt(
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
) -> dict[str, Any] | None:
    """Build non-blocking IQ quality context for the optimizer prompt."""
    try:
        from genie_space_optimizer.iq_scan.context import (
            build_space_quality_scan_context,
        )
        from genie_space_optimizer.iq_scan.scoring import calculate_score

        parsed = scan_input_for_iq(current_config) if isinstance(current_config, dict) else {}
        accuracy_percent = _metric(eval_result.get("overall_accuracy"), default=0.0)
        accuracy_fraction = (
            accuracy_percent / 100.0 if accuracy_percent > 1.0 else accuracy_percent
        )
        optimization_run = (
            {"accuracy": accuracy_fraction}
            if eval_result.get("total_questions") is not None
            else None
        )
        scan_result = calculate_score(parsed or {}, optimization_run=optimization_run)
        return build_space_quality_scan_context(scan_result)
    except Exception:
        logger.debug("Failed to build optimizer IQ quality context", exc_info=True)
        return None


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


def _stable_rank(value: Any, fallback: int) -> tuple[int, int]:
    try:
        return int(value), fallback
    except (TypeError, ValueError):
        return 999999, fallback


def _project_prompt_columns(
    config: dict[str, Any],
    data_sources: dict[str, Any],
) -> dict[str, Any]:
    """Replace serialized columns with the active UC prompt projection.

    Wide-schema selection is persisted in ``_uc_columns`` while the serialized
    Space necessarily retains every configured column. Prompt construction must
    use the former as an allowlist; otherwise failure text can pull an evicted
    column from the latter back into the LLM request.
    """
    raw_active = config.get("_uc_columns")
    if not isinstance(raw_active, list):
        return data_sources

    active_by_asset: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = {}
    for index, raw in enumerate(raw_active):
        if not isinstance(raw, dict):
            continue
        asset_key = (
            normalize_component(raw.get("catalog_name")),
            normalize_component(raw.get("schema_name")),
            normalize_component(raw.get("table_name") or raw.get("table")),
        )
        column_name = normalize_component(raw.get("column_name") or raw.get("column"))
        if not all(asset_key) or not column_name:
            continue
        active_by_asset.setdefault(asset_key, []).append((index, raw))

    projected = copy.deepcopy(data_sources)
    for key in ("tables", "metric_views"):
        assets = projected.get(key)
        if not isinstance(assets, list):
            continue
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            asset_key = _identifier_parts(_table_identifier(asset))
            original_columns = [
                column
                for column in (asset.get("column_configs") or asset.get("columns") or [])
                if isinstance(column, dict)
            ]
            original_by_name = {
                normalize_component(_column_name(column)): column
                for column in original_columns
                if _column_name(column)
            }
            active_rows = active_by_asset.get(asset_key, []) if len(asset_key) == 3 else []
            active_rows.sort(
                key=lambda item: (
                    _stable_rank(item[1].get("stable_rank"), item[0]),
                    normalize_component(
                        item[1].get("column_name") or item[1].get("column")
                    ),
                )
            )

            selected: list[dict[str, Any]] = []
            seen: set[str] = set()
            for _index, active in active_rows:
                name = normalize_component(
                    active.get("column_name") or active.get("column")
                )
                if not name or name in seen:
                    continue
                seen.add(name)
                original = original_by_name.get(name) or {}
                merged = copy.deepcopy(original)
                merged["column_name"] = str(
                    active.get("column_name") or active.get("column") or _column_name(original)
                ).strip()
                merged["data_type"] = (
                    active.get("data_type")
                    or active.get("type_text")
                    or original.get("data_type")
                    or original.get("type")
                )
                comment = active.get("comment") or active.get("description")
                if comment:
                    merged["uc_comment"] = comment
                    if not original.get("description"):
                        merged["description"] = comment
                if active.get("stable_rank") is not None:
                    merged["stable_rank"] = active["stable_rank"]
                if active.get("reason_codes"):
                    merged["reason_codes"] = copy.deepcopy(active["reason_codes"])
                if active.get("profile_status") is not None:
                    merged["profile_status"] = active["profile_status"]
                selected.append(merged)
                if len(selected) >= MAX_ACTIVE_COLUMNS_PER_ASSET:
                    break

            asset["column_configs"] = selected
            asset.pop("columns", None)
    return projected


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
    max_columns: int = MAX_ACTIVE_COLUMNS_PER_ASSET,
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

    selected = relevant[:max_columns]
    selected.extend(other[: max(0, max_columns - len(selected))])
    omitted = max(0, len(raw_columns) - len(selected))
    return [
        {
            "column_name": _column_name(col),
            "description": _short_text(col.get("description"), limit=500),
            "comment": _short_text(col.get("uc_comment"), limit=500),
            "synonyms": col.get("synonyms") or [],
            "data_type": col.get("data_type") or col.get("type"),
            "stable_rank": col.get("stable_rank"),
            "reason_codes": col.get("reason_codes") or [],
            "profile_status": col.get("profile_status"),
            "referenced_by_failures": col in relevant,
        }
        for col in selected
    ], {
        "total": len(raw_columns),
        "included": len(selected),
        "referenced_included": sum(column in relevant for column in selected),
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
    raw_data_sources = (
        parsed.get("data_sources")
        if isinstance(parsed.get("data_sources"), dict)
        else {}
    )
    data_sources = _project_prompt_columns(config, raw_data_sources)
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
    # Fix B: retry a TRANSIENT eval failure (eval-service timeout / cancellation)
    # before surfacing eval_run_failed. A single infra blip during an eval must
    # not terminate a run — especially a restart's baseline eval, which would
    # otherwise stamp EVAL_INVALID and discard already-committed improvement.
    # EVALUATION_FAILED is not transient and is returned on the first attempt.
    eval_output: dict[str, Any] = {}
    for attempt in range(_MAX_TRANSIENT_EVAL_RETRIES + 1):
        result = runner.run(space_id, qids, eval_scope=FULL)
        eval_output = build_eval_output_from_official(
            result,
            iteration=iteration,
            eval_scope=FULL,
            model_id=model_id,
        )
        status = str(eval_output.get("eval_run_status") or "")
        if not eval_output.get("eval_run_failed"):
            return eval_output
        if status not in _TRANSIENT_EVAL_STATUSES:
            return eval_output
        if attempt < _MAX_TRANSIENT_EVAL_RETRIES:
            logger.warning(
                "Transient eval status %s for space=%s iter=%d (attempt %d/%d); "
                "retrying eval",
                status, space_id, iteration, attempt + 1,
                _MAX_TRANSIENT_EVAL_RETRIES + 1,
            )
    return eval_output


def _llm_messages(
    *,
    allowed_levers: list[int],
    current_config: dict[str, Any],
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
    banned_patch_types: set[str] | frozenset[str] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    failures = _failure_rows(eval_result)
    context, context_stats, _context_json = _optimizer_context_pack(
        current_config,
        eval_result,
        max_chars=OPTIMIZER_PROMPT_MAX_CHARS,
    )
    space_quality_scan = _space_quality_scan_for_prompt(current_config, eval_result)
    banned = set(banned_patch_types or ())
    allowed_patch_types = sorted(_ALLOWED_PATCH_TYPES - banned)
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
        "well_curated_space_rubric": WELL_CURATED_SPACE_RUBRIC,
        "space_quality_scan": space_quality_scan,
        "allowed_levers": allowed_levers,
        "allowed_patch_types": allowed_patch_types,
        "response_schema": UNIFIED_OPTIMIZER_PATCH_RESPONSE_SCHEMA,
        "patch_rules": UNIFIED_OPTIMIZER_PATCH_RULES,
        "recent_reflections": reflections[-2:],
    }
    if banned:
        user["banned_patch_types"] = sorted(banned)
        user["banned_patch_types_reason"] = (
            "These patch types were rejected earlier in this run because every "
            "proposal reproduced a benchmark question or its expected SQL "
            "(train-on-test leakage). They are removed from allowed_patch_types. "
            "Do not propose them again; use a reusable snippet/expression, join "
            "spec, or metadata patch instead."
        )
    user_json = _pretty_json(user)
    context_stats["prompt_chars"] = len(UNIFIED_OPTIMIZER_PATCH_SYSTEM_PROMPT) + len(user_json)
    return [
        {"role": "system", "content": UNIFIED_OPTIMIZER_PATCH_SYSTEM_PROMPT},
        {"role": "user", "content": user_json},
    ], context_stats


def _normalize_llm_patches(
    raw: Any,
    *,
    allowed_levers: list[int],
    banned_patch_types: set[str] | frozenset[str] | None = None,
) -> tuple[int | None, str, list[dict[str, Any]]]:
    banned = set(banned_patch_types or ())
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
        if ptype in banned:
            logger.info("Dropping benchmark-leak-banned LLM patch type %r", ptype)
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

_STRUCTURED_DIRECT_FIT_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "add_join_spec",
        "update_join_spec",
        "add_example_sql",
        *_SQL_SNIPPET_PATCH_TYPES,
    }
)

_LOW_SIGNAL_FALLBACK_PATCH_TYPES: frozenset[str] = frozenset(
    {*_METADATA_PATCH_TYPES, *_TEXT_INSTRUCTION_PATCH_TYPES}
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
    eval_result: dict[str, Any] | None = None,
    w: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    patch_type = str(patch.get("type") or "")
    if patch_type not in _EXAMPLE_SQL_PATCH_TYPES:
        return patch, None

    required_core = (
        "example_question",
        "example_sql",
    )
    missing_core = [field for field in required_core if patch.get(field) in (None, "", [])]
    if missing_core:
        dropped = dict(patch)
        dropped["drop_reason"] = "example_sql_contract_failed"
        dropped["drop_detail"] = f"missing required fields: {', '.join(missing_core)}"
        return None, dropped

    clean = dict(patch)
    required_provenance = (
        "usage_guidance",
        "source_failure_pattern",
        "affected_qids",
        "semantic_delta_from_benchmark",
        "why_not_benchmark_copy",
    )
    missing_provenance = [
        field for field in required_provenance if clean.get(field) in (None, "", [])
    ]
    if missing_provenance:
        failures = _failure_rows(eval_result or {}, limit=5)
        failure_qids = [
            str(f.get("question_id"))
            for f in failures
            if f.get("question_id")
        ]
        defaults = {
            "usage_guidance": (
                clean.get("instruction")
                or "Use as a generalized adjacent SQL construction example."
            ),
            "source_failure_pattern": (
                clean.get("rationale")
                or "Inferred from residual benchmark failures."
            ),
            "affected_qids": failure_qids or ["unknown"],
            "semantic_delta_from_benchmark": (
                "Optimizer LLM omitted this provenance field; retained only "
                "after benchmark leakage checks."
            ),
            "why_not_benchmark_copy": (
                "Optimizer LLM omitted this provenance field; retained only "
                "after deterministic and fuzzy benchmark leakage checks."
            ),
        }
        for field in missing_provenance:
            clean[field] = defaults[field]
        clean["provenance_repaired"] = True
        clean["provenance_repair_detail"] = (
            "filled missing add_example_sql provenance fields: "
            + ", ".join(missing_provenance)
        )

    leak, reason = is_benchmark_leak(
        clean,
        patch_type,
        benchmark_corpus,
        w=w,
    )
    if leak:
        dropped = dict(patch)
        dropped["drop_reason"] = "benchmark_example_sql_leak"
        dropped["drop_detail"] = reason
        return None, dropped

    return clean, None


def _validate_text_instruction_routing(
    patch: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    patch_type = str(patch.get("type") or "")
    if patch_type not in _TEXT_INSTRUCTION_PATCH_TYPES:
        return patch, None

    evidence = patch.get("routing_evidence")
    if evidence is None:
        evidence = patch.get("rejected_patch_types")
    if not isinstance(evidence, list) or not evidence:
        dropped = dict(patch)
        dropped["drop_reason"] = "instruction_routing_unjustified"
        dropped["drop_detail"] = (
            "missing non-empty routing_evidence"
        )
        return None, dropped

    invalid_indexes: list[str] = []
    for idx, item in enumerate(evidence):
        if (
            not isinstance(item, dict)
            or not str(item.get("type") or "").strip()
            or not str(item.get("reason") or "").strip()
        ):
            invalid_indexes.append(str(idx))
    if invalid_indexes:
        dropped = dict(patch)
        dropped["drop_reason"] = "instruction_routing_unjustified"
        dropped["drop_detail"] = (
            "invalid routing_evidence entries: " + ", ".join(invalid_indexes)
        )
        return None, dropped

    clean = dict(patch)
    clean.setdefault("routing_evidence", evidence)
    return clean, None


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
            # Authoritative table the LLM authored the snippet against. Without
            # this the validator scans the bare expression (which names no
            # table) and falls back to the first table in the space, qualifying
            # columns and EXPLAINing against the wrong table.
            target_table=str(clean.get("target_table") or ""),
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
    instruction = str(clean.get("instruction") or "").strip()
    snippet = {
        "id": str(clean.get("snippet_id") or clean.get("id") or generate_genie_id()),
        "display_name": display_name,
        "sql": [normalized_sql],
        "synonyms": synonyms,
        "instruction": [instruction] if instruction else [],
    }
    if snippet_type != "filter" and clean.get("alias"):
        snippet["alias"] = str(clean["alias"])
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


def _dropped_patch_summary(patches: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for patch in patches[:limit]:
        if not isinstance(patch, dict):
            continue
        item = {
            "type": patch.get("type") or patch.get("patch_type"),
            "drop_reason": patch.get("drop_reason"),
            "drop_detail": patch.get("drop_detail"),
        }
        for key in ("target", "table", "column", "snippet_type"):
            if patch.get(key):
                item[key] = patch.get(key)
        summary.append({k: v for k, v in item.items() if v not in (None, "", [])})
    return summary


def _patch_type_list(patches: list[dict[str, Any]]) -> list[str]:
    return [
        str(p.get("type") or p.get("patch_type") or "")
        for p in patches
        if isinstance(p, dict) and (p.get("type") or p.get("patch_type"))
    ]


def _is_metadata_only_patch_set(patches: list[dict[str, Any]]) -> bool:
    patch_types = _patch_type_list(patches)
    return bool(patch_types) and all(pt in _METADATA_PATCH_TYPES for pt in patch_types)


def _residual_failures_need_structured_behavior(eval_result: dict[str, Any]) -> bool:
    rows = eval_result.get("rows")
    if not isinstance(rows, list):
        return False
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("assessment") or "").upper() == "GOOD":
            continue
        reasons = row.get("assessment_reasons")
        genie_eval = row.get("genie_equivalent_eval")
        if not reasons and isinstance(genie_eval, dict):
            reasons = genie_eval.get("assessment_reasons")
        if not isinstance(reasons, list):
            continue
        reason_text = " ".join(str(r or "").upper() for r in reasons)
        if any(marker in reason_text for marker in _STRUCTURED_BEHAVIOR_REASON_MARKERS):
            return True
    return False


def _recent_metadata_only_attempt(reflections: list[dict[str, Any]]) -> bool:
    for reflection in reversed(reflections[-3:]):
        if not isinstance(reflection, dict):
            continue
        if reflection.get("patch_family") == "metadata_only":
            return True
    return False


def _reject_repeated_metadata_only_patch_set(
    patches: list[dict[str, Any]],
    *,
    eval_result: dict[str, Any],
    reflections: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not _is_metadata_only_patch_set(patches):
        return patches, []
    if not _recent_metadata_only_attempt(reflections):
        return patches, []
    if not _residual_failures_need_structured_behavior(eval_result):
        return patches, []

    dropped: list[dict[str, Any]] = []
    for patch in patches:
        clean = dict(patch)
        clean["drop_reason"] = "metadata_repeat_without_structured_behavior"
        clean["drop_detail"] = (
            "A prior metadata-only attempt already ran, while residual failures "
            "still indicate SQL behavior issues. Use join specs, SQL snippets/"
            "expressions, or generalized example SQL instead."
        )
        dropped.append(clean)
    return [], dropped


def _structured_intent_lost_before_apply(
    kept_patches: list[dict[str, Any]],
    dropped_patches: list[dict[str, Any]],
) -> bool:
    """True when structured direct-fit patches were dropped, leaving only fallback.

    This catches the observed failure mode where the LLM selects SQL snippets or
    example SQL for a SQL-construction issue, validation/leakage correctly drops
    those patches, and the loop would otherwise spend an eval attempt on a lone
    text/metadata remnant.
    """
    kept_types = set(_patch_type_list(kept_patches))
    dropped_types = set(_patch_type_list(dropped_patches))
    if not kept_types or not dropped_types:
        return False
    if not (dropped_types & _STRUCTURED_DIRECT_FIT_PATCH_TYPES):
        return False
    if kept_types & _STRUCTURED_DIRECT_FIT_PATCH_TYPES:
        return False
    return kept_types <= _LOW_SIGNAL_FALLBACK_PATCH_TYPES


def _leak_banned_patch_types(
    dropped_patches: list[dict[str, Any]] | None,
) -> set[str]:
    """Patch types dropped for reproducing benchmark question/answer text.

    These are banned for the rest of the run (Fix #1) so a pivot proposal
    cannot repeat the leaking family — the observed failure mode where the
    LLM reconstructs a benchmark's gold SQL as an example and every patch is
    firewalled.
    """
    banned: set[str] = set()
    for patch in dropped_patches or []:
        if not isinstance(patch, dict):
            continue
        if str(patch.get("drop_reason") or "") in _BENCHMARK_LEAK_DROP_REASONS:
            ptype = str(patch.get("type") or patch.get("patch_type") or "").strip()
            if ptype:
                banned.add(ptype)
    return banned


def _viable_patch_types(
    allowed_levers: list[int],
    banned_patch_types: set[str] | frozenset[str],
) -> set[str]:
    """Non-banned patch types whose canonical lever is still allowed.

    When this set is non-empty after a benchmark-leak wipeout, the loop has a
    real alternative to pivot to (Fix #2) and should not terminate
    NO_NEW_HYPOTHESIS at iteration 0.
    """
    allowed = {int(l) for l in allowed_levers}
    return {
        ptype
        for ptype in _ALLOWED_PATCH_TYPES
        if _PATCH_TYPE_CANONICAL_LEVER.get(ptype) in allowed
        and ptype not in banned_patch_types
    }


def _proposal_retry_budget(
    *,
    leak_pivot_available: bool,
) -> int:
    """Retries allowed for the current iteration's proposal loop.

    A benchmark-leak wipeout with viable non-banned patch types remaining gets
    the wider pivot budget so the LLM can be steered to a different lever
    family; everything else keeps the default single recovery retry.
    """
    if leak_pivot_available:
        return _MAX_LEAK_PIVOT_RETRIES
    return _MAX_PROPOSAL_RECOVERY_RETRIES


def _proposal_failure_reflection(
    *,
    iteration: int,
    stage: str,
    rationale: str,
    hypothesis: dict[str, Any],
    dropped_patches: list[dict[str, Any]] | None = None,
    apply_log: dict[str, Any] | None = None,
    banned_patch_types: set[str] | frozenset[str] | None = None,
    viable_patch_types: set[str] | frozenset[str] | None = None,
) -> dict[str, Any]:
    guidance = (
        "Propose a different patch set that satisfies patch_rules. "
        "Choose the patch type by failure mode and the narrowest config "
        "surface that directly changes the failing behavior. Do not "
        "repeat rejected patches unless the drop reason is fixed. SQL "
        "snippet patches must use expression or predicate fragments, not "
        "full SELECT queries; full-query teaching patterns belong in "
        "generalized, non-benchmark-copy example SQL."
    )
    if banned_patch_types:
        banned_sorted = sorted(banned_patch_types)
        guidance = (
            f"Patch types {banned_sorted} were rejected as benchmark leaks — "
            "every proposed patch reproduced a benchmark's question or expected "
            "SQL. Do NOT propose these patch types again for this run. "
        )
        if viable_patch_types:
            viable_sorted = sorted(viable_patch_types)
            guidance += (
                "These output-shape failures are best fixed with a reusable "
                "primitive, not a full query: pick from "
                f"{viable_sorted}. Prefer add_sql_snippet_expression (e.g. "
                "RANK() OVER (PARTITION BY ... ORDER BY ... DESC), NTILE(10), "
                "PERCENTILE_CONT(0.5)) or add_sql_snippet_measure — these teach "
                "the primitive without copying any benchmark query and are not "
                "subject to the leak firewall."
            )
        else:
            guidance += (
                "No non-leaking patch family remains for the allowed levers."
            )
    reflection = {
        "iteration": iteration,
        "decision": "proposal_rejected",
        "stage": stage,
        "rationale": rationale,
        "hypothesis": hypothesis,
        "guidance": guidance,
    }
    if banned_patch_types:
        reflection["banned_patch_types"] = sorted(banned_patch_types)
    if viable_patch_types is not None:
        reflection["viable_patch_types"] = sorted(viable_patch_types)
    if dropped_patches:
        reflection["dropped_patch_summary"] = _dropped_patch_summary(dropped_patches)
    if apply_log:
        reflection["apply_error"] = apply_log.get("patch_error")
        reflection["apply_dropped_patch_summary"] = _dropped_patch_summary(
            list(apply_log.get("dropped_patches") or [])
        )
    return reflection


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

        validated, validation_drop = _validate_text_instruction_routing(patch)
        if validation_drop is not None:
            dropped.append(validation_drop)
            continue
        if validated is None:
            continue
        patch = validated

        validated, validation_drop = _validate_example_sql_patch(
            patch,
            benchmark_corpus=example_sql_corpus,
            eval_result=eval_result,
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
    banned_patch_types: set[str] | frozenset[str] | None = None,
) -> tuple[int | None, str, list[dict[str, Any]], str]:
    messages, context_stats = _llm_messages(
        allowed_levers=allowed_levers,
        current_config=current_config,
        eval_result=eval_result,
        reflections=reflections,
        banned_patch_types=banned_patch_types,
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
        text, _response = call_llm(
            w,
            messages=messages,
            max_tokens=8192,
            response_format={"type": "json_object"},
        )
        try:
            if span is not None:
                span.set_outputs({"response_chars": len(text or "")})
        except Exception:
            pass
    parsed = _extract_json(text)
    lever, rationale, patches = _normalize_llm_patches(
        parsed,
        allowed_levers=allowed_levers,
        banned_patch_types=banned_patch_types,
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


def _best_persisted_iteration(
    spark: Any,
    run_id: str,
    *,
    catalog: str,
    schema: str,
) -> tuple[int, float] | None:
    """Return ``(iteration, accuracy)`` of the best committed iteration on disk.

    Option A-min: when the loop terminates on a TRANSIENT eval failure (e.g. a
    restart's baseline eval times out), the in-memory ``best_iteration`` is 0 —
    but ``genie_opt_iterations`` may already hold a higher-accuracy, accepted
    (non-rolled-back) iteration from a prior execution of the same run. Stamping
    the terminal state against iteration 0 would clobber that real champion with
    baseline and report it as throwaway work. This reads the durable best so the
    terminal stamp can flag the actual champion instead.

    Selection mirrors ``select_champion_row``'s accuracy fallback: highest
    ``overall_accuracy`` among non-rolled-back full-scope rows. A failed/timed-out
    row scores 0.0 accuracy (see ``build_eval_output_from_official``) so it can
    never win. Returns ``None`` on read failure or no rows, so the caller keeps
    its current in-memory behavior.
    """
    try:
        rows = load_all_scored_iterations(spark, run_id, catalog, schema)
    except Exception:
        logger.warning(
            "Could not load persisted iterations for run %s; terminal stamp "
            "falls back to in-memory best", run_id, exc_info=True,
        )
        return None

    best_iter: int | None = None
    best_acc = float("-inf")
    for row in rows:
        if str(row.get("eval_scope") or "") != FULL:
            continue
        rolled_back = row.get("rolled_back")
        if isinstance(rolled_back, str):
            rolled_back = rolled_back.strip().lower() in {"1", "true", "yes", "y"}
        if rolled_back:
            continue
        try:
            iter_no = int(row.get("iteration"))
        except (TypeError, ValueError):
            continue
        acc = _metric(row.get("overall_accuracy"), default=float("-inf"))
        if acc > best_acc:
            best_acc = acc
            best_iter = iter_no

    if best_iter is None or best_acc == float("-inf"):
        return None
    return best_iter, best_acc


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
    current_hypothesis: dict[str, Any] | None = None,
    do_not_repeat: list[dict[str, Any]] | None = None,
) -> None:
    loop_state = _loop_state(
        attempt_no=iteration,
        attempt_mode="baseline" if iteration == 0 else "llm_patch",
        best_accuracy=best_accuracy,
        current_hypothesis=current_hypothesis,
        decision=None,
        decision_reason=None,
        surgical_attempts_used=surgical_attempts_used,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        terminal_reason=reason,
        config=config,
        do_not_repeat=do_not_repeat,
    )
    if current_hypothesis is None:
        loop_state.pop("current_hypothesis", None)
    if do_not_repeat is None:
        loop_state.pop("do_not_repeat", None)
    # Terminal stamping annotates the winning attempt. Its already-persisted
    # accept/reject decision and rationale remain the attempt-level truth.
    loop_state.pop("decision", None)
    loop_state.pop("decision_reason", None)
    update_iteration_loop_state(
        spark,
        run_id,
        iteration,
        catalog=catalog,
        schema=schema,
        eval_scope=FULL,
        loop_state=loop_state,
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
    benchmarks: list[dict[str, Any]],
    catalog: str,
    schema: str,
    levers: list[int],
    max_attempts: int,
    target_accuracy: float,
    apply_mode: str = "genie_config",
    prompt_matching_context: dict[str, Any] | None = None,
    wide_schema_inventory: dict[str, Any] | None = None,
    wide_schema_plan: dict[str, Any] | None = None,
    wide_schema_parent_artifact_id: str | None = None,
    wide_schema_profile_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run baseline eval plus bounded LLM patch attempts."""
    target_accuracy = target_accuracy_percent(float(target_accuracy))
    allowed_levers = [int(l) for l in levers if int(l) in {1, 2, 3, 4, 5, 6}]
    if not allowed_levers:
        allowed_levers = [1, 2, 3, 4, 5, 6]
    max_attempts = max(0, int(max_attempts))

    if wide_schema_inventory is not None:
        validate_inventory(wide_schema_inventory)
    if wide_schema_plan is not None:
        if wide_schema_inventory is None:
            raise ValueError("wide_schema_plan requires wide_schema_inventory")
        validate_selection_plan(
            wide_schema_plan,
            inventory_hash=wide_schema_inventory["inventory_hash"],
        )
        if wide_schema_profile_budget is None:
            from genie_space_optimizer.optimization.wide_schema_profile import (
                build_profiling_budget,
            )

            wide_schema_profile_budget = build_profiling_budget([
                wide_schema_plan.get("profiling_budget") or {},
            ])

    def _adapt_wide_schema_for_failures(
        eval_result: dict[str, Any],
        config: dict[str, Any],
        plan: dict[str, Any] | None,
        parent_artifact_id: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None, str | None]:
        """Activate omitted columns referenced by the current failed operation.

        Activation and profile completion are persisted as separate immutable
        revisions. Raw SQL is used only for local AST resolution and is never
        written to the plan or prompt-matching artifact.
        """
        if wide_schema_inventory is None or plan is None:
            return config, plan, parent_artifact_id

        failures = _failure_rows(eval_result)
        failure_ids: set[str] = set()
        sql_texts: list[str] = []
        for failure in failures:
            for field in ("question_id", "id", "question"):
                value = str(failure.get(field) or "").strip().casefold()
                if value:
                    failure_ids.add(value)
            for field in ("expected_sql", "ground_truth_sql", "sql"):
                value = failure.get(field)
                if isinstance(value, list):
                    value = " ".join(str(part) for part in value)
                if isinstance(value, str) and value.strip():
                    sql_texts.append(value)
        for benchmark in benchmarks:
            identities = {
                str(benchmark.get(field) or "").strip().casefold()
                for field in ("question_id", "id", "question")
            } - {""}
            if failure_ids and identities & failure_ids:
                value = benchmark.get("expected_sql")
                if isinstance(value, list):
                    value = " ".join(str(part) for part in value)
                if isinstance(value, str) and value.strip():
                    sql_texts.append(value)

        referenced: set[tuple[str, str, str, str]] = set()
        for sql in sql_texts:
            referenced.update(
                tuple(item["column_key"])
                for item in sql_column_evidence(sql, wide_schema_inventory)
            )
        omitted = sorted(referenced - active_column_keys(plan))
        if not omitted:
            return config, plan, parent_artifact_id

        for column_key in omitted:
            try:
                plan = revise_plan_for_column(
                    plan,
                    wide_schema_inventory,
                    column_key,
                    reason="REPAIR_FAILURE",
                    protected_column_keys=referenced,
                )
            except ValueError:
                logger.warning(
                    "Could not activate wide-schema failure target %s",
                    column_key,
                    exc_info=True,
                )
                continue
            record = write_required_artifact(
                spark,
                run_id,
                "wide_schema_selection_plan",
                plan,
                catalog=catalog,
                schema=schema,
                stage_name="optimize",
                source_notebook="run_optimize.py",
                iteration=plan["revision"],
                parent_artifact_id=parent_artifact_id,
            )
            parent_artifact_id = str(record.get("artifact_id") or "") or None
            os.environ["GSO_WIDE_SCHEMA_PLAN_HASH"] = plan["plan_hash"]

        pending = {
            tuple(row["column_key"])
            for asset in plan.get("assets") or []
            for row in asset.get("columns") or []
            if row.get("active") and row.get("profile_status") == "pending"
        }
        if pending:
            warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "").strip()
            profile_result: dict[str, Any] = {}
            if warehouse_id:
                from genie_space_optimizer.optimization.wide_schema_profile import (
                    run_bounded_profile,
                )

                profile_result = run_bounded_profile(
                    w,
                    warehouse_id,
                    wide_schema_inventory,
                    plan,
                    run_id=run_id,
                    budget=wide_schema_profile_budget,
                )
                outcomes = profile_result.get("outcomes") or {}
                write_artifact(
                    spark,
                    run_id,
                    "wide_schema_profile_telemetry",
                    {
                        "stage": "optimize_adaptive",
                        **(profile_result.get("telemetry") or {}),
                        "asset_statement_counts": profile_result.get(
                            "asset_statement_counts"
                        ) or {},
                    },
                    catalog=catalog,
                    schema=schema,
                    stage_name="optimize",
                    source_notebook="run_optimize.py",
                    parent_artifact_id=parent_artifact_id,
                )
            else:
                outcomes = {
                    key: {
                        "profile_status": "metadata_only",
                        "submitted": False,
                        "available_metrics": [],
                    }
                    for key in pending
                }
            plan = revise_plan_with_profile_outcomes(
                plan,
                wide_schema_inventory,
                outcomes,
                profiling_budget=wide_schema_profile_budget,
            )
            record = write_required_artifact(
                spark,
                run_id,
                "wide_schema_selection_plan",
                plan,
                catalog=catalog,
                schema=schema,
                stage_name="optimize",
                source_notebook="run_optimize.py",
                iteration=plan["revision"],
                parent_artifact_id=parent_artifact_id,
            )
            parent_artifact_id = str(record.get("artifact_id") or "") or None
            os.environ["GSO_WIDE_SCHEMA_PLAN_HASH"] = plan["plan_hash"]
            new_profile = profile_result.get("data_profile") or {}
            existing_profile = copy.deepcopy(config.get("_data_profile") or {})
            for asset_id, asset_profile in new_profile.items():
                target = existing_profile.setdefault(
                    asset_id,
                    {"row_count": -1, "columns": {}, "kind": asset_profile.get("kind")},
                )
                if asset_profile.get("row_count", -1) >= 0:
                    target["row_count"] = asset_profile["row_count"]
                target.setdefault("columns", {}).update(asset_profile.get("columns") or {})
            config["_data_profile"] = existing_profile

        config["_uc_columns"] = project_active_inventory(
            wide_schema_inventory,
            plan,
        )
        config["_wide_schema_inventory_hash"] = wide_schema_inventory["inventory_hash"]
        config["_wide_schema_plan_hash"] = plan["plan_hash"]
        config["_wide_schema_inventory_column_count"] = sum(
            len(asset.get("columns") or [])
            for asset in wide_schema_inventory.get("assets") or []
        )
        write_artifact(
            spark,
            run_id,
            "space_metadata",
            build_prompt_matching_context(config),
            catalog=catalog,
            schema=schema,
            stage_name="optimize",
            source_notebook="run_optimize.py",
            parent_artifact_id=parent_artifact_id,
        )
        return config, plan, parent_artifact_id

    raw_config = fetch_space_config(w, space_id)
    try:
        enrichment_result = run_space_quality_enrichment(
            w,
            spark,
            run_id=run_id,
            space_id=space_id,
            raw_config=raw_config,
            catalog=catalog,
            schema=schema,
            prompt_matching_context=prompt_matching_context,
            benchmarks=benchmarks,
        )
        current_config = enrichment_result.current_config
    except Exception:
        logger.warning(
            "Space quality enrichment failed before baseline eval; continuing",
            exc_info=True,
        )
        current_config = attach_top_level_description(_parsed_space(raw_config), raw_config)
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

    current_config, wide_schema_plan, wide_schema_parent_artifact_id = (
        _adapt_wide_schema_for_failures(
            baseline_eval,
            current_config,
            wide_schema_plan,
            wide_schema_parent_artifact_id,
        )
    )

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
        # Option A-min: a failed baseline eval (common on a job restart whose
        # fresh baseline eval times out) must not clobber a real champion a
        # prior execution already committed. Stamp against the best persisted
        # iteration if one out-scores this failed baseline.
        stamp_iteration, stamp_accuracy = best_iteration, best_accuracy
        persisted_best = _best_persisted_iteration(
            spark, run_id, catalog=catalog, schema=schema,
        )
        if persisted_best is not None and persisted_best[1] > best_accuracy:
            stamp_iteration, stamp_accuracy = persisted_best
            logger.warning(
                "Baseline eval failed for run %s; preserving persisted champion "
                "iteration=%d accuracy=%.2f instead of stamping baseline",
                run_id, stamp_iteration, stamp_accuracy,
            )
        _stamp_terminal(
            spark,
            run_id=run_id,
            iteration=stamp_iteration,
            reason=terminal_reason,
            best_accuracy=stamp_accuracy,
            surgical_attempts_used=attempts_used,
            target_accuracy=target_accuracy,
            max_attempts=max_attempts,
            config=current_config,
            catalog=catalog,
            schema=schema,
        )
        return {
            "run_id": run_id,
            "accuracy": stamp_accuracy,
            "best_iteration": stamp_iteration,
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
    last_failed_hypothesis: dict[str, Any] | None = None
    # Patch types banned for the rest of the run after a benchmark-leak wipeout
    # (Fix #1). Threaded into every subsequent proposal so the LLM cannot repeat
    # the leaking family.
    banned_patch_types: set[str] = set()

    while attempts_used < max_attempts:
        iteration = attempts_used + 1
        proposal_retries = 0
        lever: int | None = None
        rationale = ""
        patches: list[dict[str, Any]] = []
        preapply_dropped: list[dict[str, Any]] = []
        apply_log: dict[str, Any] = {}
        applied_entries: list[dict[str, Any]] = []
        hypothesis: dict[str, Any] = {}

        while True:
            lever, rationale, patches, raw_response = propose_patches(
                w,
                allowed_levers=allowed_levers,
                current_config=current_config,
                eval_result=best_eval,
                reflections=reflections,
                catalog=catalog,
                schema=schema,
                banned_patch_types=banned_patch_types,
            )
            hypothesis = {
                "lever": lever,
                "rationale": rationale,
                "proposed_patch_count": len(patches),
                "patch_count": len(patches),
                "proposal_retry": proposal_retries,
                "raw_response_preview": raw_response[:1000],
            }
            if not patches or lever is None:
                hypothesis["failure_stage"] = "llm_no_supported_patches"
                # If a benchmark-leak ban is active and viable patch families
                # remain, the LLM may have produced only banned patches (dropped
                # in normalization). Keep steering it toward the viable pivot
                # rather than terminating (Fix #1/#2).
                viable = _viable_patch_types(allowed_levers, banned_patch_types)
                leak_pivot_available = bool(banned_patch_types) and bool(viable)
                reflection = _proposal_failure_reflection(
                    iteration=iteration,
                    stage="llm_no_supported_patches",
                    rationale=rationale,
                    hypothesis=hypothesis,
                    banned_patch_types=banned_patch_types if leak_pivot_available else None,
                    viable_patch_types=viable if leak_pivot_available else None,
                )
                reflections.append(reflection)
                last_failed_hypothesis = hypothesis
                retry_budget = _proposal_retry_budget(
                    leak_pivot_available=leak_pivot_available
                )
                if proposal_retries < retry_budget:
                    proposal_retries += 1
                    continue
                terminal_reason = "NO_NEW_HYPOTHESIS"
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
            hypothesis["patch_count"] = len(patches)
            hypothesis["preapply_dropped_count"] = len(preapply_dropped)
            if preapply_dropped:
                hypothesis["preapply_dropped_reasons"] = [
                    p.get("drop_reason") for p in preapply_dropped
                ]
                hypothesis["preapply_dropped_summary"] = _dropped_patch_summary(
                    preapply_dropped
                )
            if patches:
                patches, metadata_dropped = _reject_repeated_metadata_only_patch_set(
                    patches,
                    eval_result=best_eval,
                    reflections=reflections,
                )
                if metadata_dropped:
                    preapply_dropped.extend(metadata_dropped)
                    hypothesis["patch_count"] = len(patches)
                    hypothesis["preapply_dropped_count"] = len(preapply_dropped)
                    hypothesis["preapply_dropped_reasons"] = [
                        p.get("drop_reason") for p in preapply_dropped
                    ]
                    hypothesis["preapply_dropped_summary"] = _dropped_patch_summary(
                        preapply_dropped
                    )
            if _structured_intent_lost_before_apply(patches, preapply_dropped):
                hypothesis["structured_intent_lost"] = True
                hypothesis["structured_intent_loss_detail"] = (
                    "structured direct-fit patches were dropped before apply; "
                    "only text/metadata fallback patches survived"
                )
                reflection = _proposal_failure_reflection(
                    iteration=iteration,
                    stage="preapply_lost_structured_intent",
                    rationale=rationale,
                    hypothesis={
                        **hypothesis,
                        "failure_stage": "preapply_lost_structured_intent",
                    },
                    dropped_patches=preapply_dropped,
                )
                reflections.append(reflection)
                last_failed_hypothesis = {
                    **hypothesis,
                    "failure_stage": "preapply_lost_structured_intent",
                }
                if proposal_retries < _MAX_PROPOSAL_RECOVERY_RETRIES:
                    proposal_retries += 1
                    continue
                hypothesis["structured_intent_loss_resolution"] = (
                    "retry_exhausted_applied_surviving_fallback"
                )
            if not patches:
                hypothesis["failure_stage"] = "preapply_rejected_all_patches"
                # Fix #1: a wipeout caused by benchmark leakage bans the
                # offending patch types for the rest of the run so the pivot
                # proposal cannot repeat the leaking family.
                newly_banned = _leak_banned_patch_types(preapply_dropped)
                banned_patch_types |= newly_banned
                viable = _viable_patch_types(allowed_levers, banned_patch_types)
                leak_wipeout = bool(newly_banned)
                if leak_wipeout:
                    hypothesis["benchmark_leak_wipeout"] = True
                    hypothesis["banned_patch_types"] = sorted(banned_patch_types)
                    hypothesis["viable_patch_types"] = sorted(viable)
                reflection = _proposal_failure_reflection(
                    iteration=iteration,
                    stage="preapply_rejected_all_patches",
                    rationale=rationale,
                    hypothesis=hypothesis,
                    dropped_patches=preapply_dropped,
                    banned_patch_types=banned_patch_types if leak_wipeout else None,
                    viable_patch_types=viable if leak_wipeout else None,
                )
                reflections.append(reflection)
                last_failed_hypothesis = hypothesis
                # Fix #2: don't terminate at iteration 0 when a benchmark-leak
                # wipeout still leaves a non-leaking patch family to pivot to —
                # grant the wider pivot retry budget so the LLM can switch levers.
                leak_pivot_available = leak_wipeout and bool(viable)
                retry_budget = _proposal_retry_budget(
                    leak_pivot_available=leak_pivot_available
                )
                if proposal_retries < retry_budget:
                    proposal_retries += 1
                    continue
                terminal_reason = "NO_NEW_HYPOTHESIS"
                break

            hypothesis["patch_types"] = _patch_type_list(patches)
            hypothesis["patch_family"] = (
                "metadata_only" if _is_metadata_only_patch_set(patches) else "mixed_or_structured"
            )

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
            applied_entries = list(apply_log.get("applied") or [])
            if not apply_log.get("patch_deployed") or not applied_entries:
                validation_errors = list(apply_log.get("validation_errors") or [])
                hypothesis["failure_stage"] = "apply_deployed_no_patches"
                if validation_errors:
                    hypothesis["failure_stage"] = "config_validation_failed"
                    hypothesis["validation_errors"] = validation_errors[:20]
                hypothesis["apply_dropped_patch_summary"] = _dropped_patch_summary(
                    list(apply_log.get("dropped_patches") or [])
                )
                reflection = _proposal_failure_reflection(
                    iteration=iteration,
                    stage="apply_deployed_no_patches",
                    rationale=rationale or str(apply_log.get("patch_error") or ""),
                    hypothesis=hypothesis,
                    apply_log=apply_log,
                )
                reflections.append(reflection)
                last_failed_hypothesis = hypothesis
                if validation_errors:
                    terminal_reason = "CONFIG_VALIDATION_FAILED"
                    break
                if proposal_retries < _MAX_PROPOSAL_RECOVERY_RETRIES:
                    proposal_retries += 1
                    continue
                terminal_reason = "NO_NEW_HYPOTHESIS"
                break

            break

        if terminal_reason is not None:
            break

        attempts_used += 1
        iteration = attempts_used
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

        candidate_config = copy.deepcopy(apply_log.get("post_snapshot") or current_config)
        for runtime_key in (
            "_uc_columns",
            "_data_profile",
            "_rls_audit",
            "_asset_semantics",
            "_wide_schema_inventory_hash",
            "_wide_schema_plan_hash",
            "_wide_schema_inventory_column_count",
        ):
            if runtime_key in current_config:
                candidate_config[runtime_key] = copy.deepcopy(
                    current_config[runtime_key]
                )
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
            current_config, wide_schema_plan, wide_schema_parent_artifact_id = (
                _adapt_wide_schema_for_failures(
                    best_eval,
                    current_config,
                    wide_schema_plan,
                    wide_schema_parent_artifact_id,
                )
            )
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
            accepted_reflection = {
                "iteration": iteration,
                "decision": "accepted",
                "lever": lever,
                "accuracy": candidate_accuracy,
                "rationale": rationale,
                "patch_types": hypothesis.get("patch_types", []),
                "patch_family": hypothesis.get("patch_family"),
            }
            if hypothesis.get("patch_family") == "metadata_only":
                accepted_reflection["next_guidance"] = (
                    "A metadata-only attempt has already run. If residual "
                    "failures still show missing columns, incorrect filters, "
                    "metric/function errors, ranking/windowing, or output-shape "
                    "issues, use structured behavioral patches next."
                )
            reflections.append(accepted_reflection)
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
                "patch_types": hypothesis.get("patch_types", []),
                "patch_family": hypothesis.get("patch_family"),
            }
        )

    if terminal_reason is None:
        terminal_reason = "MAX_ATTEMPTS"

    # Option A-min: on a candidate-eval EVAL_INVALID (the mid-loop transient
    # failure path), preserve a higher-accuracy champion a prior execution
    # already committed rather than stamping this execution's lower best. Scoped
    # to EVAL_INVALID: on TARGET_REACHED / MAX_ATTEMPTS / NO_NEW_HYPOTHESIS the
    # in-memory best already equals the persisted best, so this never fires. The
    # champion's config pointer is read off the (already-written) iteration row,
    # so overriding iteration/accuracy here re-flags the correct row safely.
    stamp_iteration, stamp_accuracy = best_iteration, best_accuracy
    if terminal_reason == "EVAL_INVALID":
        persisted_best = _best_persisted_iteration(
            spark, run_id, catalog=catalog, schema=schema,
        )
        if persisted_best is not None and persisted_best[1] > best_accuracy:
            stamp_iteration, stamp_accuracy = persisted_best
            logger.warning(
                "Candidate eval failed for run %s; preserving persisted champion "
                "iteration=%d accuracy=%.2f instead of stamping in-memory best "
                "iteration=%d accuracy=%.2f",
                run_id, stamp_iteration, stamp_accuracy,
                best_iteration, best_accuracy,
            )

    _stamp_terminal(
        spark,
        run_id=run_id,
        iteration=stamp_iteration,
        reason=terminal_reason,
        best_accuracy=stamp_accuracy,
        surgical_attempts_used=attempts_used,
        target_accuracy=target_accuracy,
        max_attempts=max_attempts,
        config=current_config,
        catalog=catalog,
        schema=schema,
        current_hypothesis=(
            last_failed_hypothesis
            if terminal_reason in {"NO_NEW_HYPOTHESIS", "CONFIG_VALIDATION_FAILED"}
            else None
        ),
        do_not_repeat=(
            reflections[-5:]
            if terminal_reason in {"NO_NEW_HYPOTHESIS", "CONFIG_VALIDATION_FAILED"}
            else None
        ),
    )

    return {
        "run_id": run_id,
        "accuracy": stamp_accuracy,
        "best_iteration": stamp_iteration,
        "iteration_counter": attempts_used,
        "terminal_reason": terminal_reason,
        "surgical_attempts_used": attempts_used,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "accepted_attempts": accepted,
        "rolled_back_attempts": rolled_back,
        "reflections": reflections,
    }
