"""
Metadata Optimizer — failure analysis, proposal generation, conflict detection.

Analyzes evaluation failures (ASI) + current metadata snapshot to produce
targeted metadata change proposals grouped by lever.  LLM calls use
Databricks Claude Opus 4.6 via the Foundation Model API.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from collections import Counter, defaultdict
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from genie_space_optimizer.common.config import (
    ADAPTIVE_STRATEGIST_PROMPT,
    APPLY_MODE,
    CONFLICT_RULES,
    DEFAULT_THRESHOLDS,
    DESCRIPTION_ENRICHMENT_PROMPT,
    FAILURE_TAXONOMY,
    GENERIC_FIX_PREFIXES,
    INSTRUCTION_SECTION_ORDER,
    LEVER_1_2_COLUMN_PROMPT,
    LEVER_4_JOIN_DISCOVERY_PROMPT,
    LEVER_4_JOIN_SPEC_PROMPT,
    LEVER_5_HOLISTIC_PROMPT,
    LEVER_5_INSTRUCTION_PROMPT,
    LEVER_NAMES,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LOW_RISK_PATCHES,
    MAX_HOLISTIC_INSTRUCTION_CHARS,
    MAX_PATCH_OBJECTS,
    MAX_VALUE_DICTIONARY_COLUMNS,
    MEDIUM_RISK_PATCHES,
    PATCH_TYPES,
    PROMPT_TOKEN_BUDGET,
    PROPOSAL_GENERATION_PROMPT,
    REGRESSION_THRESHOLD,
    REPEATABILITY_FIX_BY_ASSET,
    SAMPLE_QUESTIONS_PROMPT,
    SPACE_DESCRIPTION_PROMPT,
    STRATEGIST_DETAIL_PROMPT,
    STRATEGIST_PROMPT,
    STRATEGIST_TRIAGE_PROMPT,
    _LEVER_TO_PATCH_TYPE,
    format_mlflow_template,
)
from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields

logger = logging.getLogger(__name__)

_LLM_TIMEOUT_SECONDS = 600
_EXAMPLE_SQL_SIMILARITY_THRESHOLD = 0.85
_INSTR_LOSS_THRESHOLD = 0.5


def _ws_with_timeout(
    w: WorkspaceClient | None,
    timeout: int = _LLM_TIMEOUT_SECONDS,
) -> WorkspaceClient:
    """Return a **new** workspace client whose HTTP session uses *timeout*.

    The Databricks SDK bakes ``http_timeout_seconds`` into the
    ``requests.Session`` at construction time, so mutating the config
    after the fact has no effect.  We therefore always create a fresh
    client.  In a Databricks job the env-var auth (``DATABRICKS_HOST``,
    ``DATABRICKS_TOKEN``, etc.) is inherited automatically.
    """
    from databricks.sdk import client as _sdk_client

    cfg_kwargs: dict[str, Any] = {"http_timeout_seconds": timeout}
    if w is not None:
        for attr in ("host", "token", "azure_workspace_resource_id",
                      "azure_client_id", "azure_client_secret", "azure_tenant_id",
                      "client_id", "client_secret"):
            val = getattr(w.config, attr, None)
            if val:
                cfg_kwargs[attr] = val

    return WorkspaceClient(config=_sdk_client.Config(**cfg_kwargs))


def _estimate_tokens(text: str) -> int:
    """Conservative token estimate (~4 chars per token)."""
    return len(text) // 4


def _truncate_to_budget(
    format_kwargs: dict[str, Any],
    prompt_template: str,
    priority_keys: list[str],
) -> dict[str, Any]:
    """Truncate low-priority context sections to fit within PROMPT_TOKEN_BUDGET.

    ``priority_keys`` lists context keys from LOWEST to HIGHEST priority.
    When the estimated prompt exceeds the budget, the lowest-priority keys
    are truncated first (keeping a summary prefix).
    """
    est = _estimate_tokens(prompt_template) + sum(
        _estimate_tokens(str(v)) for v in format_kwargs.values()
    )
    if est <= PROMPT_TOKEN_BUDGET:
        return format_kwargs

    overshoot = est - PROMPT_TOKEN_BUDGET
    result = dict(format_kwargs)

    for key in priority_keys:
        if overshoot <= 0:
            break
        val = str(result.get(key, ""))
        if not val:
            continue
        char_budget = max(200, len(val) - overshoot * 4)
        if char_budget < len(val):
            truncated = val[:char_budget]
            result[key] = truncated + f"\n... ({len(val) - char_budget} chars truncated for token budget)"
            overshoot -= _estimate_tokens(val) - _estimate_tokens(result[key])

    return result


# ── Traced LLM Call Helper ─────────────────────────────────────────────

def _traced_llm_call(
    w: WorkspaceClient | None,
    system_msg: str,
    prompt: str,
    *,
    span_name: str,
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
) -> tuple[str, Any]:
    """Execute an LLM call wrapped in a ``CHAT_MODEL`` MLflow span.

    The span captures the full prompt messages, model, temperature,
    raw response text, token usage, and retry events.

    Returns ``(raw_text, response_object)`` for the caller to parse.
    Raises the last exception if all retries are exhausted.
    """
    import time

    import mlflow
    from mlflow.entities import SpanEvent, SpanType

    with mlflow.start_span(name=span_name, span_type=SpanType.CHAT_MODEL) as span:
        span.set_inputs({
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt},
            ],
            "model": LLM_ENDPOINT,
            "temperature": temperature,
        })
        span.set_attribute("prompt_chars", len(prompt))

        text = ""
        last_err: Exception | None = None
        for attempt in range(max_retries):
            try:
                wc = _ws_with_timeout(w)
                query_kwargs: dict[str, Any] = {
                    "name": LLM_ENDPOINT,
                    "messages": [
                        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_msg),
                        ChatMessage(role=ChatMessageRole.USER, content=prompt),
                    ],
                    "temperature": temperature,
                }
                if max_tokens is not None:
                    query_kwargs["max_tokens"] = max_tokens
                response = wc.serving_endpoints.query(**query_kwargs)
                choices = getattr(response, "choices", None) or []
                if not choices:
                    raise ValueError("LLM response had no choices")
                content = getattr(choices[0].message, "content", None)
                if not content:
                    raise ValueError("LLM response content is empty")
                text = str(content).strip()

                usage = getattr(response, "usage", None)
                if usage:
                    _usage_attrs: dict[str, Any] = {}
                    for attr in ("total_tokens", "prompt_tokens", "completion_tokens"):
                        val = getattr(usage, attr, None)
                        if val is not None:
                            _usage_attrs[attr] = val
                    if _usage_attrs:
                        span.set_attributes(_usage_attrs)

                span.set_outputs({
                    "response_text": text[:10_000],
                    "response_chars": len(text),
                    "attempts": attempt + 1,
                })
                return text, response

            except Exception as exc:
                last_err = exc
                span.add_event(SpanEvent(
                    name=f"retry_attempt_{attempt + 1}",
                    attributes={"error": str(exc)[:500]},
                ))
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        span.set_outputs({
            "error": str(last_err)[:500] if last_err else "unknown",
            "attempts": max_retries,
        })
        raise last_err  # type: ignore[misc]


def _row_qid(row: dict, *, fallback: str = "unknown") -> str:
    """Extract question_id from an eval-results row regardless of column layout.

    MLflow stores inputs in the ``request`` column (not ``inputs/…``), so we
    parse both layouts to robustly recover the QID.
    """
    direct = (
        row.get("inputs/question_id")
        or row.get("inputs/question")
        or row.get("question_id")
    )
    if direct:
        return str(direct)
    _req = row.get("request") or {}
    if isinstance(_req, str):
        try:
            _req = json.loads(_req)
        except (json.JSONDecodeError, TypeError):
            _req = {}
    if isinstance(_req, dict):
        _kw = _req.get("kwargs", {})
        qid = _kw.get("question_id") or _req.get("question_id") or _req.get("question")
        if qid:
            return str(qid)
    return row.get("question", fallback) or fallback


# ── Dual Persistence Paths ─────────────────────────────────────────────

DUAL_PERSIST_PATHS: dict[int, dict[str, str]] = {
    1: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "gold_layer_design/yaml/{domain}/*.yaml",
    },
    2: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/metric_views/*.yaml",
    },
    3: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/tvfs/*.sql",
    },
    4: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
    5: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# 1. Failure-to-Lever Mapping (pure)
# ═══════════════════════════════════════════════════════════════════════


_JUDGE_TO_LEVER: dict[str, int] = {
    "schema_accuracy": 1,
    "syntax_validity": 1,
    "completeness": 2,
    "logical_accuracy": 2,
    "semantic_equivalence": 2,
    "result_correctness": 2,
    "asset_routing": 5,
}


def _map_to_lever(
    root_cause: str,
    asi_failure_type: str | None = None,
    blame_set: list | str | None = None,
    judge: str | None = None,
) -> int:
    """Map a failure root cause to its primary control lever (1-5).

    ASI ``failure_type`` takes precedence when present since it comes
    directly from the FAILURE_TAXONOMY and is more precise.
    Falls back to the judge name when rationale-based pattern extraction
    yields "other".
    """
    ft = (asi_failure_type if asi_failure_type and asi_failure_type != "other" else None) or root_cause

    if ft == "repeatability_issue":
        bs = str(blame_set).upper() if blame_set else ""
        return 5 if "TVF" in bs else 1

    mapping = {
        "wrong_column": 1,
        "wrong_table": 1,
        "description_mismatch": 1,
        "missing_synonym": 1,
        "wrong_aggregation": 2,
        "wrong_measure": 2,
        "missing_filter": 2,
        "missing_scd_filter": 2,
        "wrong_filter_condition": 2,
        "missing_temporal_filter": 2,
        "wrong_join_type": 5,
        "tvf_parameter_error": 3,
        "wrong_join": 4,
        "missing_join_spec": 4,
        "wrong_join_spec": 4,
        "asset_routing_error": 5,
        "missing_instruction": 5,
        "ambiguous_question": 5,
        "format_difference": 0,
        "extra_columns_only": 0,
        "select_star": 0,
    }

    if asi_failure_type and asi_failure_type in mapping:
        return mapping[asi_failure_type]
    if root_cause in mapping:
        return mapping[root_cause]
    if judge and judge in _JUDGE_TO_LEVER:
        return _JUDGE_TO_LEVER[judge]
    return 5


def _resolve_scope(lever: int, apply_mode: str = APPLY_MODE) -> str:
    """Determine where a patch is applied based on lever and apply_mode.

    Levers 4-5 are always ``genie_config`` (Genie Space native structures).
    Levers 1-3 are governed by ``apply_mode``.
    """
    if lever in (4, 5):
        return "genie_config"
    return apply_mode


# ═══════════════════════════════════════════════════════════════════════
# 2. Failure Clustering (pure)
# ═══════════════════════════════════════════════════════════════════════


def _extract_pattern(rationale: str) -> str:
    """Extract a generalizable pattern from a judge rationale string."""
    r = (rationale or "").lower()
    if not r:
        return "other"
    if "is_current" in r or ("scd" in r and ("filter" in r or "dimension" in r)):
        return "missing_scd_filter"
    if ("left join" in r or "inner join" in r) and ("join type" in r or "instead of" in r or "wrong join" in r):
        return "wrong_join_type"
    if "table" in r and ("wrong" in r or "missing" in r or "incorrect" in r):
        return "wrong_table"
    if "column" in r and ("wrong" in r or "missing" in r):
        return "wrong_column"
    if "aggregation" in r or "measure" in r:
        return "wrong_aggregation"
    if "join" in r:
        return "wrong_join"
    if "filter" in r or "where" in r:
        return "missing_filter"
    if "limit" in r and ("missing" in r or "without" in r):
        return "wrong_filter_condition"
    if "asset" in r or "routing" in r:
        return "asset_routing_error"
    if "instruction" in r or "ambiguous" in r or "unclear" in r:
        return "missing_instruction"
    return "other"


_SQL_KW = re.compile(r"\b(FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN)\s+", re.I)
_SQL_WHERE = re.compile(r"\bWHERE\b", re.I)
_SQL_GROUP = re.compile(r"\bGROUP\s+BY\b", re.I)
_SQL_MEASURE = re.compile(r"\bMEASURE\s*\(", re.I)
_SQL_TVF = re.compile(r"\b(\w+)\s*\((?:[^)]*,){2,}", re.I)
_SQL_AGG = re.compile(r"\b(SUM|AVG|COUNT|MIN|MAX|STDDEV|VARIANCE)\s*\(", re.I)
_SQL_SELECT_STAR = re.compile(r"\bSELECT\s+\*\b", re.I)
_SQL_SCD_FILTER = re.compile(r"\b(is_current|is_active)\s*=\s*(true|1|'true')\b", re.I)
_SQL_JOIN_TYPE = re.compile(r"\b(LEFT|RIGHT|INNER|CROSS|FULL)\s+(OUTER\s+)?JOIN\b", re.I)
_SQL_WHERE_CONDITIONS = re.compile(r"\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\bHAVING\b|;|\Z)", re.I | re.S)


def _extract_sql_tables(sql: str) -> set[str]:
    """Extract table-like references after FROM/JOIN keywords."""
    tables: set[str] = set()
    for m in _SQL_KW.finditer(sql or ""):
        rest = sql[m.end():m.end() + 200].strip()
        token = rest.split()[0] if rest.split() else ""
        token = token.rstrip(",;)")
        if token and not token.upper().startswith("("):
            tables.add(token.lower())
    return tables


_SQL_JOIN_ON = re.compile(
    r"\bJOIN\s+([\w.`]+)\s+(?:AS\s+)?(\w+)?\s*ON\s+(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|;|\Z)",
    re.I | re.S,
)


def _extract_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract (table, join_column) pairs from JOIN...ON clauses.

    Parses each ``JOIN <table> ... ON <condition>`` and extracts every
    ``alias.column`` reference from the ON clause, pairing the joined
    table with the column names used in the condition.
    """
    pairs: set[tuple[str, str]] = set()
    for m in _SQL_JOIN_ON.finditer(sql or ""):
        table = m.group(1).strip("`").lower()
        on_clause = m.group(3)
        cols = re.findall(r"[\w`]+\.`?([\w]+)`?", on_clause)
        for col in cols:
            pairs.add((table, col.lower()))
    return pairs


def _classify_sql_diff(ctx: dict) -> str:
    """Classify a failure's root cause by comparing expected vs generated SQL.

    Accepts either a ``sql_context`` dict (with ``expected_sql`` / ``generated_sql``
    keys) or a full row dict (with ``request`` / ``response`` keys).
    Falls back to ``"other"`` when the SQL pair is missing or no pattern matches.
    """
    expected_sql = (ctx.get("expected_sql") or "").strip()
    generated_sql = (ctx.get("generated_sql") or "").strip()

    if not expected_sql:
        req = ctx.get("request") or {}
        if isinstance(req, str):
            try:
                req = json.loads(req)
            except (json.JSONDecodeError, TypeError):
                req = {}
        expected_sql = (req.get("expected_sql") or "").strip()
    if not generated_sql:
        resp = ctx.get("response") or {}
        if isinstance(resp, str):
            try:
                resp = json.loads(resp)
            except (json.JSONDecodeError, TypeError):
                resp = {}
        generated_sql = (resp.get("response") or "").strip()

    if not expected_sql or not generated_sql:
        return "other"

    exp_lower = expected_sql.lower()
    gen_lower = generated_sql.lower()

    exp_tables = _extract_sql_tables(expected_sql)
    gen_tables = _extract_sql_tables(generated_sql)
    exp_join_pairs = _extract_join_pairs(expected_sql)
    gen_join_pairs = _extract_join_pairs(generated_sql)
    exp_has_join = bool(re.search(r"\bJOIN\b", exp_lower))

    _DIM_PREFIXES = ("dim_", "lookup_", "ref_")
    missing_tables = exp_tables - gen_tables if exp_tables and gen_tables else set()

    # 1. Missing dimension JOIN — GT joins a dim/lookup/ref table that
    #    Genie omits entirely. This is fundamentally a join issue even if
    #    aggregation or filter differences also exist.
    if missing_tables:
        dim_tables = {t for t in missing_tables if any(t.startswith(p) or f".{p}" in t for p in _DIM_PREFIXES)}
        if dim_tables:
            return "wrong_join"

    # 2. Aggregation checks
    exp_aggs = set(_SQL_AGG.findall(exp_lower))
    gen_aggs = set(_SQL_AGG.findall(gen_lower))
    if exp_aggs and not gen_aggs:
        return "missing_aggregation"
    if exp_aggs != gen_aggs and exp_aggs and gen_aggs:
        return "wrong_aggregation"

    # 3. Filter checks
    exp_has_where = bool(_SQL_WHERE.search(exp_lower))
    gen_has_where = bool(_SQL_WHERE.search(gen_lower))
    if exp_has_where and not gen_has_where:
        return "missing_filter"

    # 3b. SCD filter — expected has is_current/is_active = true, generated omits it
    exp_has_scd = bool(_SQL_SCD_FILTER.search(exp_lower))
    gen_has_scd = bool(_SQL_SCD_FILTER.search(gen_lower))
    if exp_has_scd and not gen_has_scd:
        return "missing_scd_filter"

    # 3c. WHERE condition diff — both have WHERE but conditions differ
    if exp_has_where and gen_has_where:
        exp_conds = _SQL_WHERE_CONDITIONS.search(exp_lower)
        gen_conds = _SQL_WHERE_CONDITIONS.search(gen_lower)
        if exp_conds and gen_conds:
            exp_text = re.sub(r"\s+", " ", exp_conds.group(1).strip())
            gen_text = re.sub(r"\s+", " ", gen_conds.group(1).strip())
            if exp_text != gen_text:
                return "wrong_filter_condition"

    # 3d. Join type diff — same tables but different join types (LEFT vs INNER)
    exp_join_types = sorted(_SQL_JOIN_TYPE.findall(exp_lower))
    gen_join_types = sorted(_SQL_JOIN_TYPE.findall(gen_lower))
    if exp_join_types and gen_join_types and exp_join_types != gen_join_types:
        return "wrong_join_type"

    # 4. Wrong join column — both queries join the same tables but on
    #    different columns (e.g. destination_name vs destination_key).
    if exp_join_pairs and gen_join_pairs and exp_join_pairs != gen_join_pairs:
        return "wrong_join_spec"

    # 5. Missing join — GT has a JOIN that Genie doesn't (regardless of
    #    whether Genie has other JOINs).
    if exp_has_join and missing_tables:
        return "wrong_join"

    # 6. SELECT *, TVF, MEASURE, GROUP BY checks
    gen_has_star = bool(_SQL_SELECT_STAR.search(gen_lower))
    exp_has_star = bool(_SQL_SELECT_STAR.search(exp_lower))
    if gen_has_star and not exp_has_star:
        return "select_star"

    exp_has_tvf = bool(_SQL_TVF.search(exp_lower))
    gen_has_tvf = bool(_SQL_TVF.search(gen_lower))
    if exp_has_tvf != gen_has_tvf:
        return "tvf_parameter_error"

    exp_has_measure = bool(_SQL_MEASURE.search(exp_lower))
    gen_has_measure = bool(_SQL_MEASURE.search(gen_lower))
    if exp_has_measure != gen_has_measure:
        return "wrong_measure"

    exp_has_group = bool(_SQL_GROUP.search(exp_lower))
    gen_has_group = bool(_SQL_GROUP.search(gen_lower))
    if exp_has_group != gen_has_group:
        return "wrong_aggregation"

    # 7. Table-set differs (non-dimension tables)
    if exp_tables and gen_tables and exp_tables != gen_tables:
        extra_tables = gen_tables - exp_tables
        date_dims = {t for t in missing_tables if "dim_date" in t or "dim_time" in t}
        if missing_tables == date_dims and not extra_tables:
            return "format_difference"
        if missing_tables or extra_tables:
            return "wrong_table"

    # 8. Same tables, different columns
    if exp_tables == gen_tables and exp_tables:
        exp_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", exp_lower, re.S)
        gen_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", gen_lower, re.S)
        if exp_select and gen_select:
            exp_cols = [c.strip() for c in exp_select[0].split(",")]
            gen_cols = [c.strip() for c in gen_select[0].split(",")]
            if len(exp_cols) > len(gen_cols) and len(gen_cols) >= 1:
                return "extra_columns_only"
        return "wrong_column"

    return "missing_instruction"


def cluster_failures(
    eval_results: dict,
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    run_id: str = "",
    catalog: str = "",
    schema: str = "",
    verbose: bool = True,
) -> list[dict]:
    """Group evaluation failures into actionable clusters.

    Groups by ``(judge, asi_failure_type, blame_set_str)``.  Falls back to
    ``(judge, _extract_pattern(rationale), "")`` when ASI is absent.
    Returns clusters with >= 1 question so even single failures are actionable.

    When ``spark/run_id/catalog/schema`` are provided, enriches with stored
    ASI data from ``genie_eval_asi_results`` Delta table.
    """
    uc_asi_map: dict[tuple[str, str], dict] = {}
    if spark and run_id and catalog and schema:
        try:
            uc_asi = read_asi_from_uc(spark, run_id, catalog, schema)
            for a in uc_asi:
                key = (a.get("question_id", ""), a.get("judge", ""))
                uc_asi_map[key] = a
            if uc_asi_map:
                logger.info("Enriched clustering with %d UC ASI records", len(uc_asi_map))
        except Exception:
            logger.debug("UC ASI enrichment failed", exc_info=True)

    failures: list[dict] = []
    table = None

    results_obj = eval_results.get("eval_result")
    if results_obj is not None and hasattr(results_obj, "tables"):
        table = results_obj.tables.get("eval_results")
    elif results_obj is not None and hasattr(results_obj, "eval_results"):
        table = results_obj.eval_results

    if table is None:
        rows = (
            eval_results.get("eval_results")
            or eval_results.get("rows")
            or eval_results.get("table")
        )
        if isinstance(rows, list):
            table = rows

    if table is None:
        return []

    try:
        import pandas as pd

        if hasattr(table, "iterrows"):
            rows_iter = [row.to_dict() for _, row in table.iterrows()]
        else:
            rows_iter = table if isinstance(table, list) else []
    except ImportError:
        rows_iter = table if isinstance(table, list) else []

    for row in rows_iter:
        if not isinstance(row, dict):
            continue

        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}

        question_id = _row_qid(row)

        sql_ctx = {
            "question": _req.get("question", "") if isinstance(_req, dict) else "",
            "expected_sql": _req.get("expected_sql", "") if isinstance(_req, dict) else "",
            "generated_sql": _resp.get("response", "") if isinstance(_resp, dict) else "",
            "comparison": _resp.get("comparison", {}) if isinstance(_resp, dict) else {},
        }

        _NON_JUDGE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")
        for col_name, val in list(row.items()):
            judge: str | None = None
            if col_name.startswith("feedback/") and col_name.endswith("/value"):
                judge = col_name.removeprefix("feedback/").removesuffix("/value")
            elif col_name.startswith("feedback/") and not any(col_name.endswith(s) for s in _NON_JUDGE_SUFFIXES):
                bare = col_name.removeprefix("feedback/")
                if "/" not in bare:
                    judge = bare
            elif col_name.endswith("/value") and not col_name.startswith("feedback/"):
                judge = col_name.removesuffix("/value")
            if judge and "no" in str(val).lower():
                from genie_space_optimizer.optimization.evaluation import (
                    _parse_asi_from_rationale,
                )
                rationale = (
                    row.get(f"feedback/{judge}/rationale")
                    or row.get(f"{judge}/rationale")
                    or row.get(f"rationale/{judge}")
                    or row.get("rationale", "")
                )
                judge_meta = (
                    row.get(f"feedback/{judge}/metadata")
                    or row.get(f"{judge}/metadata")
                    or {}
                )
                if not isinstance(judge_meta, dict):
                    try:
                        judge_meta = json.loads(judge_meta) if isinstance(judge_meta, str) else {}
                    except (json.JSONDecodeError, TypeError):
                        judge_meta = {}
                if not judge_meta:
                    judge_meta = _parse_asi_from_rationale(rationale)
                asi_failure_type = (
                    judge_meta.get("failure_type")
                    or row.get(f"metadata/{judge}/failure_type")
                    or row.get("metadata/failure_type")
                )
                asi_blame_set = (
                    judge_meta.get("blame_set")
                    or row.get(f"metadata/{judge}/blame_set")
                    or row.get("metadata/blame_set")
                )
                asi_counterfactual = (
                    judge_meta.get("counterfactual_fix")
                    or row.get(f"metadata/{judge}/counterfactual_fix")
                    or row.get("metadata/counterfactual_fix")
                )
                asi_wrong_clause = (
                    judge_meta.get("wrong_clause")
                    or row.get(f"metadata/{judge}/wrong_clause")
                )

                if not asi_failure_type and uc_asi_map:
                    uc_asi_entry = uc_asi_map.get((question_id, judge))
                    if uc_asi_entry:
                        asi_failure_type = asi_failure_type or uc_asi_entry.get("failure_type")
                        asi_blame_set = asi_blame_set or uc_asi_entry.get("blame_set")
                        asi_counterfactual = asi_counterfactual or uc_asi_entry.get("counterfactual_fix")
                        asi_wrong_clause = asi_wrong_clause or uc_asi_entry.get("wrong_clause")

                failures.append(
                    {
                        "question_id": question_id,
                        "judge": judge,
                        "rationale": rationale,
                        "asi_failure_type": asi_failure_type,
                        "asi_blame_set": asi_blame_set,
                        "asi_counterfactual_fix": asi_counterfactual,
                        "asi_wrong_clause": asi_wrong_clause,
                        "sql_context": sql_ctx,
                    }
                )

    question_profiles: dict[str, dict] = {}
    for f in failures:
        qid = f["question_id"]
        if qid not in question_profiles:
            question_profiles[qid] = {
                "judges": set(),
                "root_causes": [],
                "blame_sets": [],
                "counterfactual_fixes": [],
                "wrong_clauses": [],
                "sql_context": f.get("sql_context", {}),
                "failures": [],
            }
        profile = question_profiles[qid]
        profile["judges"].add(f["judge"])
        profile["failures"].append(f)

        asi_ft = f.get("asi_failure_type")
        if asi_ft and asi_ft != "other":
            root = asi_ft
            resolution_method = "asi_metadata"
        else:
            pattern = _extract_pattern(f["rationale"])
            if pattern != "other":
                root = pattern
                resolution_method = "rationale_pattern"
            else:
                root = _classify_sql_diff(f.get("sql_context", {}))
                resolution_method = "sql_diff"
        f["_resolved_root_cause"] = root
        f["_resolution_method"] = resolution_method
        profile["root_causes"].append(root)

        if f.get("asi_blame_set"):
            raw_blame = f["asi_blame_set"]
            items = raw_blame if isinstance(raw_blame, list) else [str(raw_blame)]
            for item in items:
                item_str = str(item).strip()
                if item_str and item_str not in profile["blame_sets"]:
                    profile["blame_sets"].append(item_str)
        if f.get("asi_counterfactual_fix"):
            profile["counterfactual_fixes"].append(f["asi_counterfactual_fix"])
        if f.get("asi_wrong_clause"):
            profile["wrong_clauses"].append(f["asi_wrong_clause"])

    for qid, profile in question_profiles.items():
        cause_counts = Counter(profile["root_causes"])
        profile["dominant_root_cause"] = cause_counts.most_common(1)[0][0] if cause_counts else "other"

    # ── 8a. Per-Question ASI Extraction Trace ───────────────────────────
    _cluster_debug = os.environ.get("CLUSTER_DEBUG", "1").lower() not in ("0", "false", "no")
    if _cluster_debug and question_profiles:
        lines = ["\n== ASI EXTRACTION TRACE ======================================================"]
        if verbose:
            for qid, profile in question_profiles.items():
                lines.append(f"\n--- Q: {qid} " + "-" * max(1, 60 - len(qid)))
                for f in profile["failures"]:
                    judge = f["judge"]
                    verdict = "FAIL"
                    asi_ft = f.get("asi_failure_type")
                    blame = f.get("asi_blame_set")
                    cfix = f.get("asi_counterfactual_fix")
                    wclause = f.get("asi_wrong_clause")
                    resolved = f.get("_resolved_root_cause", "other")
                    method = f.get("_resolution_method", "unknown")
                    lines.append(f"|  Judge: {judge:<24s}|  Verdict: {verdict}")
                    has_asi = bool(asi_ft)
                    lines.append(f"|    ASI metadata found:      {'YES' if has_asi else 'NO'}")
                    if has_asi:
                        lines.append(f"|      failure_type (raw):    {asi_ft}")
                        if blame:
                            lines.append(f"|      blame_set:             {blame}")
                        if cfix:
                            lines.append(f"|      counterfactual_fix:    \"{str(cfix)[:120]}\"")
                        if wclause:
                            lines.append(f"|      wrong_clause:          {wclause}")
                    lines.append(f"|    Final root cause:        {resolved}  (via {method})")
                lines.append(f"|  Dominant root cause:       {profile['dominant_root_cause']}")
                blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else "(none)"
                lines.append(f"|  Cluster group key:         ({profile['dominant_root_cause']}, \"{blame_key}\")")
        else:
            lines.append(f"|  (compact mode — {len(question_profiles)} questions)")
            for qid, profile in question_profiles.items():
                judges = ", ".join(sorted(profile["judges"]))
                blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else "(none)"
                lines.append(
                    f"|  {qid}: root={profile['dominant_root_cause']}  "
                    f"judges=[{judges}]  blame={blame_key}"
                )
        lines.append("-" * 78)
        print("\n".join(lines))

    cluster_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for qid, profile in question_profiles.items():
        blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else ""
        group_key = (profile["dominant_root_cause"], blame_key)
        cluster_groups[group_key].append(qid)

    clusters: list[dict] = []
    for (root_cause, blame_str), qids in cluster_groups.items():
        all_judges: set[str] = set()
        all_counterfactuals: list[str] = []
        all_wrong_clauses: list[str] = []
        sql_contexts: list[dict] = []
        sample_asi_type: str | None = None

        question_traces: list[dict] = []
        for qid in qids:
            profile = question_profiles[qid]
            all_judges.update(profile["judges"])
            all_counterfactuals.extend(profile["counterfactual_fixes"])
            all_wrong_clauses.extend(profile["wrong_clauses"])
            if profile["sql_context"]:
                sql_contexts.append(profile["sql_context"])
            if not sample_asi_type:
                for f in profile["failures"]:
                    if f.get("asi_failure_type"):
                        sample_asi_type = f["asi_failure_type"]
                        break
            q_text = profile["sql_context"].get("question", "") if profile["sql_context"] else ""
            judge_traces = []
            for f in profile["failures"]:
                judge_traces.append({
                    "judge": f["judge"],
                    "verdict": "FAIL",
                    "asi_failure_type_raw": f.get("asi_failure_type"),
                    "resolved_root_cause": f.get("_resolved_root_cause", "other"),
                    "resolution_method": f.get("_resolution_method", "unknown"),
                    "blame_set": f.get("asi_blame_set"),
                    "counterfactual_fix": f.get("asi_counterfactual_fix"),
                    "wrong_clause": f.get("asi_wrong_clause"),
                    "rationale_snippet": (f.get("rationale") or "")[:500],
                })
            question_traces.append({
                "question_id": qid,
                "question_text": q_text[:200],
                "failed_judges": judge_traces,
            })

        unique_qids = sorted(set(qids))
        entry = {
            "cluster_id": f"C{len(clusters) + 1:03d}",
            "root_cause": root_cause,
            "question_ids": unique_qids,
            "affected_judges": sorted(all_judges),
            "affected_judge": sorted(all_judges)[0] if all_judges else "unknown",
            "confidence": min(0.9, 0.5 + 0.1 * len(unique_qids)),
            "asi_failure_type": sample_asi_type,
            "asi_blame_set": [b.strip() for b in blame_str.split("|") if b.strip()] if blame_str else None,
            "asi_wrong_clause": next((wc for wc in all_wrong_clauses if wc), None),
            "asi_counterfactual_fixes": list(dict.fromkeys(cf for cf in all_counterfactuals if cf)),
            "sql_contexts": sql_contexts[:5],
            "question_traces": question_traces,
        }
        clusters.append(entry)

    clusters.sort(key=lambda c: len(c["question_ids"]), reverse=True)

    # ── 8b. Cluster Formation Summary ────────────────────────────────────
    if _cluster_debug and clusters:
        total_failures = sum(len(p["failures"]) for p in question_profiles.values())
        total_judges = len({f["judge"] for p in question_profiles.values() for f in p["failures"]})
        lines = ["\n== CLUSTER FORMATION ========================================================="]
        lines.append(f"|  Total failure entries:     {total_failures} (across {len(question_profiles)} questions, {total_judges} judges)")
        lines.append(f"|  Question profiles:         {len(question_profiles)}")
        lines.append(f"|  Cluster groups formed:     {len(clusters)}")
        for c in clusters:
            cid = c["cluster_id"]
            rc = c["root_cause"]
            blame = c.get("asi_blame_set") or "(none)"
            qids = c["question_ids"]
            lines.append(f"|    {cid} ({rc}, blame=\"{blame}\"): {len(qids)} question(s) {qids}")
        lines.append("=" * 78)
        print("\n".join(lines))

    return clusters


# ═══════════════════════════════════════════════════════════════════════
# 2a. Cluster Priority Scoring (adaptive lever loop)
# ═══════════════════════════════════════════════════════════════════════


def cluster_impact(cluster: dict) -> float:
    """Score a failure cluster by estimated optimisation impact.

    ``impact = question_count × causal_weight × severity × fixability``

    Higher is more impactful.  Used to rank clusters before the adaptive
    strategist call so the LLM receives a suggested priority order.
    """
    from genie_space_optimizer.common.config import (
        CAUSAL_WEIGHT,
        FIXABILITY_WITH_COUNTERFACTUAL,
        FIXABILITY_WITHOUT_COUNTERFACTUAL,
        SEVERITY_WEIGHT,
    )

    q_count = max(len(cluster.get("question_ids", [])), 1)
    judge = cluster.get("affected_judge", "")
    failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")

    causal = CAUSAL_WEIGHT.get(judge, 1.0)
    severity = SEVERITY_WEIGHT.get(failure_type, 0.5)

    has_cf = bool(cluster.get("asi_counterfactual_fixes"))
    fixability = FIXABILITY_WITH_COUNTERFACTUAL if has_cf else FIXABILITY_WITHOUT_COUNTERFACTUAL

    return q_count * causal * severity * fixability


def rank_clusters(clusters: list[dict]) -> list[dict]:
    """Return *clusters* sorted by :func:`cluster_impact` (descending).

    Each cluster dict gets ``impact_score`` and ``rank`` keys added.
    The original list is **not** mutated.
    """
    scored = []
    for c in clusters:
        enriched = dict(c)
        enriched["impact_score"] = cluster_impact(c)
        scored.append(enriched)
    scored.sort(key=lambda c: c["impact_score"], reverse=True)
    for i, c in enumerate(scored, 1):
        c["rank"] = i
    return scored


def format_reflection_buffer(
    reflection_buffer: list[dict],
    full_window: int | None = None,
) -> str:
    """Render the reflection buffer as a prompt-ready string.

    The most recent *full_window* entries are shown in full detail.  Older
    entries are compressed to a single line each.  A DO NOT RETRY block is
    appended at the end listing every target that was previously tried and
    rolled back.
    """
    from genie_space_optimizer.common.config import REFLECTION_WINDOW_FULL

    if full_window is None:
        full_window = REFLECTION_WINDOW_FULL

    if not reflection_buffer:
        return "(No prior iterations. This is the first attempt after baseline evaluation.)"

    lines: list[str] = []
    do_not_retry: list[str] = []
    cutoff = max(0, len(reflection_buffer) - full_window)

    for entry in reflection_buffer[:cutoff]:
        status = "ACCEPTED" if entry.get("accepted") else "ROLLED_BACK"
        action = entry.get("action", "?")[:100]
        delta = entry.get("accuracy_delta", 0.0)
        lines.append(
            f"Iter {entry.get('iteration', '?')}: {action} "
            f"({status}, accuracy delta {delta:+.1f}%)"
        )
        if not entry.get("accepted"):
            do_not_retry.extend(entry.get("do_not_retry", []))

    for entry in reflection_buffer[cutoff:]:
        status = "ACCEPTED" if entry.get("accepted") else "ROLLED_BACK"
        lines.append(f"\nITERATION {entry.get('iteration', '?')} | {status}")
        lines.append(f"  Action: {entry.get('action', '?')}")
        levers = entry.get("levers", [])
        if levers:
            lines.append(f"  Levers: {', '.join(str(l) for l in levers)}")
        targets = entry.get("target_objects", [])
        if targets:
            lines.append(f"  Targets: {', '.join(targets[:10])}")
        deltas = entry.get("score_deltas", {})
        if deltas:
            delta_parts = [f"{k} {v:+.1f}%" for k, v in sorted(deltas.items()) if v != 0]
            if delta_parts:
                lines.append(f"  Score changes: {', '.join(delta_parts)}")
        new_failures = entry.get("new_failures")
        if new_failures:
            lines.append(f"  New failures: {new_failures}")
        if entry.get("rollback_reason"):
            lines.append(f"  Rollback reason: {entry['rollback_reason']}")
        _ref_text = entry.get("reflection_text", "")
        if _ref_text:
            lines.append(f"  Reflection: {_ref_text}")
        _ref_mode = entry.get("refinement_mode", "")
        if _ref_mode and not entry.get("accepted"):
            lines.append(f"  Refinement guidance: {_ref_mode}")
        if not entry.get("accepted"):
            do_not_retry.extend(entry.get("do_not_retry", []))

    if do_not_retry:
        lines.append("\nDO NOT RETRY:")
        for item in sorted(set(do_not_retry)):
            lines.append(f"  - {item}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# 2b. UC Type Enrichment & Join Discovery (Lever 4)
# ═══════════════════════════════════════════════════════════════════════


def enrich_metadata_with_uc_types(
    metadata_snapshot: dict,
    uc_columns: list[dict],
) -> None:
    """Merge UC ``data_type`` and ``comment`` into metadata_snapshot column_configs.

    Mutates *metadata_snapshot* in place.  Each UC row is matched by
    ``table_name`` (unqualified) + ``column_name`` against the tables in
    ``data_sources.tables[].column_configs``.  If a column_config already has
    ``data_type`` set it is left unchanged.
    """
    if not uc_columns:
        return

    uc_lookup: dict[tuple[str, str], dict] = {}
    for row in uc_columns:
        if not isinstance(row, dict):
            continue
        tbl = str(row.get("table_name") or row.get("table") or "").strip().lower()
        col = str(row.get("column_name") or row.get("column") or "").strip().lower()
        if tbl and col:
            uc_lookup[(tbl, col)] = row
        cat = str(row.get("catalog_name") or "").strip().lower()
        sch = str(row.get("schema_name") or "").strip().lower()
        if cat and sch and tbl and col:
            uc_lookup[(f"{cat}.{sch}.{tbl}", col)] = row

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    enriched = 0
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = ident.rsplit(".", 1)[-1].lower() if ident else ""
        fqn_lower = ident.lower() if ident else ""
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            if not isinstance(cc, dict):
                continue
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            uc_row = uc_lookup.get((fqn_lower, col_name)) or uc_lookup.get((short, col_name))
            if uc_row is None:
                continue
            if not cc.get("data_type"):
                dt = uc_row.get("data_type") or uc_row.get("type") or ""
                if dt:
                    cc["data_type"] = str(dt).upper()
                    enriched += 1
            if not cc.get("uc_comment"):
                comment = uc_row.get("comment") or ""
                if comment:
                    cc["uc_comment"] = str(comment)
    logger.info("UC type enrichment: updated %d column_configs", enriched)


# ---------------------------------------------------------------------------
# Proactive Description Enrichment
# ---------------------------------------------------------------------------

_ENRICHMENT_BATCH_THRESHOLD = 30
_MIN_DESCRIPTION_LENGTH = 10


def _is_description_insufficient(desc: Any) -> bool:
    """Return True when a description is too short to be useful (< 10 chars)."""
    if desc is None:
        return True
    if isinstance(desc, list):
        text = " ".join(str(d).strip() for d in desc)
    else:
        text = str(desc).strip()
    return len(text) < _MIN_DESCRIPTION_LENGTH


def _collect_blank_columns(
    metadata_snapshot: dict,
) -> list[dict]:
    """Scan metadata_snapshot for columns with insufficient descriptions.

    A column is eligible when both the Genie Space description and the UC
    comment are shorter than ``_MIN_DESCRIPTION_LENGTH`` characters.

    Returns a list of dicts with keys: table, column, data_type, entity_type,
    table_description, sibling_columns.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        entity_type_for_column,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])

    blanks: list[dict] = []

    for tbl in list(tables) + list(mvs):
        if not isinstance(tbl, dict):
            continue
        identifier = tbl.get("identifier", "") or tbl.get("name", "")
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(str(d) for d in tbl_desc)
        else:
            tbl_desc = str(tbl_desc or "")

        is_mv = tbl in (mvs if isinstance(mvs, list) else [])
        cols = tbl.get("column_configs", tbl.get("columns", []))
        sibling_names = [
            cc.get("column_name", cc.get("name", ""))
            for cc in cols if isinstance(cc, dict)
        ]

        for cc in cols:
            if not isinstance(cc, dict):
                continue
            if cc.get("hidden"):
                continue
            col_name = cc.get("column_name", cc.get("name", ""))
            desc = cc.get("description")
            uc_comment = cc.get("uc_comment", "")

            if not _is_description_insufficient(desc):
                continue
            if uc_comment and len(str(uc_comment).strip()) >= _MIN_DESCRIPTION_LENGTH:
                continue

            data_type = cc.get("data_type", "")
            etype = entity_type_for_column(
                col_name, data_type,
                is_in_metric_view=is_mv,
                enable_entity_matching=bool(cc.get("enable_entity_matching")),
            )
            blanks.append({
                "table": identifier,
                "column": col_name,
                "data_type": data_type,
                "entity_type": etype,
                "table_description": tbl_desc,
                "sibling_columns": sibling_names,
            })

    return blanks


def _lookup_table_profile(data_profile: dict, table_fqn: str) -> dict:
    """Find a table's profile entry by FQN or leaf name."""
    if not data_profile:
        return {}
    hit = data_profile.get(table_fqn) or data_profile.get(table_fqn.lower())
    if hit:
        return hit
    leaf = table_fqn.split(".")[-1].strip("`").lower()
    for key, val in data_profile.items():
        if key.split(".")[-1].strip("`").lower() == leaf:
            return val
    return {}


def _format_enrichment_context(
    blanks: list[dict],
    data_profile: dict | None = None,
) -> str:
    """Format blank columns into a context string grouped by table."""
    by_table: dict[str, list[dict]] = {}
    for b in blanks:
        by_table.setdefault(b["table"], []).append(b)

    lines: list[str] = []
    for tbl_id, cols in by_table.items():
        tbl_desc = cols[0].get("table_description", "") or "(no table description)"
        siblings = cols[0].get("sibling_columns", [])
        target_names = {c["column"] for c in cols}
        sibling_context = [s for s in siblings if s not in target_names]

        tbl_profile = _lookup_table_profile(data_profile or {}, tbl_id)
        row_count = tbl_profile.get("row_count")
        tbl_header = f"Table: {tbl_id} ({tbl_desc[:200]})"
        if row_count is not None and row_count >= 0:
            tbl_header += f" [~{row_count} rows]"
        lines.append(tbl_header)
        lines.append("  Columns needing descriptions:")
        for c in cols:
            col_line = f"    - {c['column']} ({c['data_type'] or 'UNKNOWN'}) [{c['entity_type']}]"
            col_info = tbl_profile.get("columns", {}).get(c["column"], {})
            if col_info:
                hints: list[str] = []
                if col_info.get("cardinality"):
                    hints.append(f"cardinality={col_info['cardinality']}")
                if col_info.get("distinct_values"):
                    vals = col_info["distinct_values"][:10]
                    hints.append(f"values={vals}")
                if col_info.get("min") is not None:
                    hints.append(f"range=[{col_info['min']}, {col_info['max']}]")
                if hints:
                    col_line += f" — {', '.join(hints)}"
            lines.append(col_line)
        if sibling_context:
            lines.append(f"  Sibling columns (for context): {', '.join(sibling_context[:20])}")
        lines.append("")

    return "\n".join(lines)


def _format_data_profile_for_prompt(data_profile: dict | None) -> str:
    """Render data profile as a compact string for enrichment prompt templates."""
    if not data_profile:
        return "(no data profile available)"
    lines: list[str] = []
    for table, tinfo in sorted(data_profile.items()):
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


def _enrich_blank_descriptions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    data_profile: dict | None = None,
) -> list[dict]:
    """Generate structured descriptions for columns that have no description anywhere.

    Returns a list of patch dicts compatible with the Lever 1/2 proposal format.
    Only targets columns where BOTH the Genie Space description AND the UC
    comment are empty and the column is not hidden.
    """
    from genie_space_optimizer.optimization.evaluation import _extract_json

    blanks = _collect_blank_columns(metadata_snapshot)
    if not blanks:
        logger.info("Description enrichment: 0 columns need enrichment — skipping")
        return []

    logger.info(
        "Description enrichment: %d columns with blank descriptions across %d tables",
        len(blanks),
        len({b["table"] for b in blanks}),
    )

    allowlist = _build_identifier_allowlist(metadata_snapshot)
    allowlist_str = _format_identifier_allowlist(allowlist)
    profile_context_str = _format_data_profile_for_prompt(data_profile)

    if len(blanks) <= _ENRICHMENT_BATCH_THRESHOLD:
        batches = [blanks]
    else:
        by_table: dict[str, list[dict]] = {}
        for b in blanks:
            by_table.setdefault(b["table"], []).append(b)
        batches = list(by_table.values())

    all_patches: list[dict] = []
    system_msg = "You generate structured column descriptions for a Databricks Genie Space."

    for batch_idx, batch in enumerate(batches):
        context_str = _format_enrichment_context(batch, data_profile=data_profile)
        format_kwargs: dict[str, Any] = {
            "columns_context": context_str,
            "identifier_allowlist": allowlist_str,
            "data_profile_context": profile_context_str,
        }
        format_kwargs = _truncate_to_budget(
            format_kwargs, DESCRIPTION_ENRICHMENT_PROMPT,
            priority_keys=["columns_context"],
        )
        prompt = format_mlflow_template(DESCRIPTION_ENRICHMENT_PROMPT, **format_kwargs)

        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name=f"enrich_column_descriptions_batch_{batch_idx}",
                max_tokens=4096,
            )
            result = _extract_json(text)
        except Exception:
            logger.warning("Description enrichment: LLM call failed for batch", exc_info=True)
            continue

        batch_lookup = {(b["table"], b["column"]): b for b in batch}

        for change in result.get("changes", []):
            tbl = change.get("table", "")
            col = change.get("column", "")
            sections = change.get("sections", {})
            etype = change.get("entity_type", "")

            if not tbl or not col or not sections:
                continue
            if (tbl, col) not in batch_lookup:
                logger.debug(
                    "Description enrichment: skipping %s.%s — not in eligible set", tbl, col,
                )
                continue

            if not etype:
                etype = batch_lookup[(tbl, col)]["entity_type"]

            all_patches.append({
                "type": "update_column_description",
                "table": tbl,
                "column": col,
                "structured_sections": sections,
                "column_entity_type": etype,
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            })

    logger.info(
        "Description enrichment: generated %d patches for %d blank columns",
        len(all_patches), len(blanks),
    )
    return all_patches


# ---------------------------------------------------------------------------
# Proactive Table Description Enrichment
# ---------------------------------------------------------------------------


def _collect_insufficient_tables(
    metadata_snapshot: dict,
) -> list[dict]:
    """Scan metadata_snapshot for tables with insufficient top-level descriptions.

    A table is eligible when its description is shorter than
    ``_MIN_DESCRIPTION_LENGTH`` characters.

    Returns a list of dicts with keys: table, current_description,
    column_names, column_types, is_metric_view.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])

    insufficient: list[dict] = []

    for tbl_list, is_mv in [(tables, False), (mvs, True)]:
        for tbl in tbl_list:
            if not isinstance(tbl, dict):
                continue
            identifier = tbl.get("identifier", "") or tbl.get("name", "")
            raw_desc = tbl.get("description", "")
            if isinstance(raw_desc, list):
                desc_text = "\n".join(str(d) for d in raw_desc).strip()
            else:
                desc_text = str(raw_desc or "").strip()

            if len(desc_text) >= _MIN_DESCRIPTION_LENGTH:
                continue

            cols = tbl.get("column_configs", tbl.get("columns", []))
            col_info = []
            for cc in cols:
                if not isinstance(cc, dict):
                    continue
                col_info.append({
                    "name": cc.get("column_name", cc.get("name", "")),
                    "data_type": cc.get("data_type", ""),
                })

            insufficient.append({
                "table": identifier,
                "current_description": desc_text,
                "column_names": [c["name"] for c in col_info],
                "column_types": {c["name"]: c["data_type"] for c in col_info},
                "is_metric_view": is_mv,
            })

    return insufficient


def _format_table_enrichment_context(
    tables: list[dict],
    data_profile: dict | None = None,
) -> str:
    """Format insufficient tables into a context string for the LLM prompt."""
    lines: list[str] = []
    for t in tables:
        cur = t.get("current_description", "")
        desc_label = f"({cur[:80]})" if cur else "(none)"
        tbl_profile = _lookup_table_profile(data_profile or {}, t["table"])
        row_count = tbl_profile.get("row_count")
        lines.append(f"Table: {t['table']}")
        lines.append(f"  Current description: {desc_label}")
        if row_count is not None and row_count >= 0:
            lines.append(f"  Row count: ~{row_count}")
        col_parts = []
        for cname in t.get("column_names", [])[:30]:
            ctype = t.get("column_types", {}).get(cname, "")
            col_parts.append(f"{cname} ({ctype})" if ctype else cname)
        if col_parts:
            lines.append(f"  Columns: {', '.join(col_parts)}")
            remaining = len(t.get("column_names", [])) - 30
            if remaining > 0:
                lines.append(f"  (+{remaining} more columns)")
        if t.get("is_metric_view"):
            lines.append("  Type: Metric View")
        col_profiles = tbl_profile.get("columns", {})
        if col_profiles:
            profile_hints: list[str] = []
            for cname, cinfo in list(col_profiles.items())[:15]:
                parts: list[str] = []
                if cinfo.get("distinct_values"):
                    vals = cinfo["distinct_values"][:8]
                    parts.append(f"values={vals}")
                elif cinfo.get("min") is not None:
                    parts.append(f"range=[{cinfo['min']}, {cinfo['max']}]")
                if parts:
                    profile_hints.append(f"{cname}: {', '.join(parts)}")
            if profile_hints:
                lines.append(f"  Data profile: {'; '.join(profile_hints)}")
        lines.append("")
    return "\n".join(lines)


def _enrich_table_descriptions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    data_profile: dict | None = None,
) -> list[dict]:
    """Generate structured descriptions for tables that have insufficient descriptions.

    Returns a list of patch dicts compatible with ``update_description``
    proposals (lever 0, scope ``genie_config``).
    """
    from genie_space_optimizer.common.config import TABLE_DESCRIPTION_ENRICHMENT_PROMPT
    from genie_space_optimizer.optimization.evaluation import _extract_json

    tables = _collect_insufficient_tables(metadata_snapshot)
    if not tables:
        logger.info("Table description enrichment: 0 tables need enrichment — skipping")
        return []

    logger.info(
        "Table description enrichment: %d tables with insufficient descriptions",
        len(tables),
    )

    allowlist = _build_identifier_allowlist(metadata_snapshot)
    allowlist_str = _format_identifier_allowlist(allowlist)
    profile_context_str = _format_data_profile_for_prompt(data_profile)

    if len(tables) <= _ENRICHMENT_BATCH_THRESHOLD:
        batches = [tables]
    else:
        batches = [[t] for t in tables]

    all_patches: list[dict] = []
    system_msg = "You generate structured table descriptions for a Databricks Genie Space."

    for batch_idx, batch in enumerate(batches):
        context_str = _format_table_enrichment_context(batch, data_profile=data_profile)
        format_kwargs: dict[str, Any] = {
            "tables_context": context_str,
            "identifier_allowlist": allowlist_str,
            "data_profile_context": profile_context_str,
        }
        format_kwargs = _truncate_to_budget(
            format_kwargs, TABLE_DESCRIPTION_ENRICHMENT_PROMPT,
            priority_keys=["tables_context"],
        )
        prompt = format_mlflow_template(TABLE_DESCRIPTION_ENRICHMENT_PROMPT, **format_kwargs)

        try:
            text, _response = _traced_llm_call(
                w, system_msg, prompt,
                span_name=f"enrich_table_descriptions_batch_{batch_idx}",
                max_tokens=4096,
            )
            result = _extract_json(text)
        except Exception:
            logger.warning("Table description enrichment: LLM call failed for batch", exc_info=True)
            continue

        batch_lookup = {t["table"]: t for t in batch}

        for change in result.get("changes", []):
            tbl = change.get("table", "")
            sections = change.get("sections", {})

            if not tbl or not sections:
                continue
            if tbl not in batch_lookup:
                logger.debug(
                    "Table description enrichment: skipping %s — not in eligible set", tbl,
                )
                continue

            entity_type = "mv_table" if batch_lookup[tbl].get("is_metric_view") else "table"

            all_patches.append({
                "type": "update_description",
                "table": tbl,
                "structured_sections": sections,
                "table_entity_type": entity_type,
                "lever": 0,
                "risk_level": "low",
                "source": "proactive_enrichment",
            })

    logger.info(
        "Table description enrichment: generated %d patches for %d insufficient tables",
        len(all_patches), len(tables),
    )
    return all_patches


# ── Proactive Space Metadata Generation ──────────────────────────────


def _build_space_schema_context(metadata_snapshot: dict) -> dict[str, str]:
    """Build context strings for tables, metric views, and instructions."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", [])
    mvs = ds.get("metric_views", [])

    def _str_field(val: object) -> str:
        if isinstance(val, list):
            return " ".join(str(s) for s in val)
        return str(val) if val else ""

    table_lines: list[str] = []
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "")
        desc = _str_field(tbl.get("description", ""))
        cols = tbl.get("column_configs", tbl.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:20])}"
            if len(col_names) > 20:
                line += f" (+{len(col_names) - 20} more)"
        table_lines.append(line)

    mv_lines: list[str] = []
    for mv in mvs:
        if not isinstance(mv, dict):
            continue
        ident = mv.get("identifier", "")
        desc = _str_field(mv.get("description", ""))
        cols = mv.get("column_configs", mv.get("columns", []))
        col_names = [
            c.get("column_name", c.get("name", ""))
            for c in cols if isinstance(c, dict)
        ]
        line = f"- {ident}"
        if desc:
            line += f": {desc[:120]}"
        if col_names:
            line += f"\n  Columns: {', '.join(col_names[:15])}"
        mv_lines.append(line)

    instr = metadata_snapshot.get("instructions", {})
    ti_list = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instr_parts: list[str] = []
    for ti in ti_list:
        if not isinstance(ti, dict):
            continue
        raw = ti.get("content", "")
        if isinstance(raw, list):
            raw = "\n".join(str(s) for s in raw)
        if raw:
            instr_parts.append(str(raw)[:200])
    instr_text = "\n".join(instr_parts) or "(none)"

    return {
        "tables_context": "\n".join(table_lines) or "(none)",
        "metric_views_context": "\n".join(mv_lines) or "(none)",
        "instructions_context": instr_text,
    }


def _generate_space_description(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> str:
    """Generate a structured description for a Genie Space from its schema.

    Returns the description text, or ``""`` on failure.
    """
    ctx = _build_space_schema_context(metadata_snapshot)
    format_kwargs = _truncate_to_budget(
        ctx, SPACE_DESCRIPTION_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(SPACE_DESCRIPTION_PROMPT, **format_kwargs)
    system_msg = "You generate structured descriptions for Databricks Genie Spaces."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_space_description",
            max_tokens=2048,
        )
        text = re.sub(r"```[a-z]*\n?", "", text).strip().rstrip("`")
        if len(text) < 30:
            logger.warning("Space description generation: result too short (%d chars)", len(text))
            return ""
        logger.info("Space description generation: produced %d chars", len(text))
        return text
    except Exception:
        logger.warning("Space description generation: LLM call failed", exc_info=True)
        return ""


def _generate_proactive_instructions(
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> str:
    """Generate conservative routing instructions for an empty Genie Space.

    Returns the instruction text (500-1500 chars), or ``""`` on failure.
    """
    from genie_space_optimizer.common.config import PROACTIVE_INSTRUCTION_PROMPT

    ctx = _build_space_schema_context(metadata_snapshot)

    join_specs = []
    ds = metadata_snapshot.get("data_sources", {})
    if isinstance(ds, dict):
        for tbl in ds.get("tables", []):
            if isinstance(tbl, dict):
                for js in tbl.get("join_specs", []):
                    if isinstance(js, dict):
                        sql_parts = js.get("sql", [])
                        cond = sql_parts[0] if sql_parts else ""
                        if cond:
                            join_specs.append(cond)
    ctx["join_specs_context"] = "\n".join(f"- {j}" for j in join_specs) if join_specs else "(none)"

    format_kwargs = _truncate_to_budget(
        ctx, PROACTIVE_INSTRUCTION_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(PROACTIVE_INSTRUCTION_PROMPT, **format_kwargs)
    system_msg = "You generate routing instructions for Databricks Genie Spaces."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_proactive_instructions",
            max_tokens=2048,
        )
        text = re.sub(r"```[a-z]*\n?", "", text).strip().rstrip("`")
        if len(text) < 50:
            logger.warning("Proactive instruction generation: result too short (%d chars)", len(text))
            return ""
        if len(text) > 1500:
            text = text[:1500].rsplit("\n", 1)[0]
        text = normalize_instructions(text)
        logger.info("Proactive instruction generation: produced %d chars", len(text))
        return text
    except Exception:
        logger.warning("Proactive instruction generation: LLM call failed", exc_info=True)
        return ""


def _generate_sample_questions(
    metadata_snapshot: dict,
    description: str = "",
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Generate sample questions for a Genie Space from its schema.

    Returns a list of ``{"id": "<hex>", "question": ["<text>"]}`` dicts,
    or ``[]`` on failure.
    """
    from genie_space_optimizer.common.genie_schema import generate_genie_id
    from genie_space_optimizer.optimization.evaluation import _extract_json

    ctx = _build_space_schema_context(metadata_snapshot)
    ctx["description_context"] = description or "(none)"
    format_kwargs = _truncate_to_budget(
        ctx, SAMPLE_QUESTIONS_PROMPT,
        priority_keys=["tables_context"],
    )
    prompt = format_mlflow_template(SAMPLE_QUESTIONS_PROMPT, **format_kwargs)
    system_msg = "You generate sample questions for Databricks Genie Spaces."

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="generate_sample_questions",
            max_tokens=2048,
        )
        result = _extract_json(text)
    except Exception:
        logger.warning("Sample question generation: LLM call failed", exc_info=True)
        return []

    questions = result.get("questions", [])
    if not isinstance(questions, list) or not questions:
        logger.warning("Sample question generation: no questions in LLM response")
        return []

    sample_questions: list[dict] = []
    for q in questions:
        if not isinstance(q, str) or not q.strip():
            continue
        sample_questions.append({
            "id": generate_genie_id(),
            "question": [q.strip()],
        })

    logger.info("Sample question generation: produced %d questions", len(sample_questions))
    return sample_questions


_JOIN_KEY_SUFFIXES = ("_key", "_id", "_code", "_fk", "_ref", "_num", "_no", "_sk", "_pk")
_DIM_FACT_PATTERNS = ("dim_", "fact_", "bridge_", "link_")

_COMPATIBLE_TYPE_GROUPS: list[set[str]] = [
    {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SHORT"},
    {"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"},
    {"STRING", "VARCHAR", "CHAR"},
    {"DATE"},
    {"TIMESTAMP", "TIMESTAMP_NTZ"},
    {"BOOLEAN"},
]


def _short_name(identifier: str) -> str:
    """Return the unqualified table name from a fully-qualified identifier."""
    return identifier.rsplit(".", 1)[-1] if "." in identifier else identifier


def _strip_suffix(col: str) -> str:
    """Strip known join-key suffixes to get the column stem."""
    lower = col.lower()
    for sfx in _JOIN_KEY_SUFFIXES:
        if lower.endswith(sfx):
            return lower[: -len(sfx)]
    return lower


def _is_fuzzy_match(col_a: str, col_b: str) -> bool:
    """Check whether two column names are fuzzy-matches.

    Returns True when:
    - One full name is a substring of the other
    - They share the same stem after stripping join-key suffixes
    - One *stem* is a substring of the other stem (handles ``prod`` vs ``product``)
    """
    la, lb = col_a.lower(), col_b.lower()
    if la == lb:
        return True
    if la in lb or lb in la:
        return True
    stem_a = _strip_suffix(la)
    stem_b = _strip_suffix(lb)
    if not stem_a or not stem_b:
        return False
    if stem_a == stem_b:
        return True
    if stem_a in stem_b or stem_b in stem_a:
        return True
    return False


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Return True if two UC data types are join-compatible.

    When either type is unknown (empty) we assume compatibility so
    that the LLM can make the final decision.
    """
    if not type_a or not type_b:
        return True
    a, b = type_a.upper().split("(")[0].strip(), type_b.upper().split("(")[0].strip()
    if a == b:
        return True
    for group in _COMPATIBLE_TYPE_GROUPS:
        if a in group and b in group:
            return True
    return False


def _extract_table_pairs_from_clusters(clusters: list[dict]) -> set[tuple[str, str]]:
    """Extract table pairs mentioned in soft signal cluster blame sets and fixes."""
    pairs: set[tuple[str, str]] = set()
    for cl in clusters:
        blame = cl.get("asi_blame_set") or cl.get("blame_set") or []
        if isinstance(blame, str):
            blame = [blame]
        fixes = cl.get("asi_counterfactual_fixes") or cl.get("counterfactual_fixes") or []
        if isinstance(fixes, str):
            fixes = [fixes]

        tables_mentioned: list[str] = []
        for item in list(blame) + list(fixes):
            item_str = str(item).lower()
            for tok in item_str.replace(",", " ").split():
                if "." in tok and len(tok.split(".")) >= 2:
                    tables_mentioned.append(tok.strip())

        for i, t1 in enumerate(tables_mentioned):
            for t2 in tables_mentioned[i + 1:]:
                if t1 != t2:
                    t_a, t_b = sorted((t1, t2))
                    pairs.add((t_a, t_b))
    return pairs


def discover_join_candidates(
    metadata_snapshot: dict,
    soft_signal_clusters: list[dict] | None = None,
) -> list[dict]:
    """Discover potential join relationships and return **hints** for the LLM.

    Scans all table pairs for columns that look like join keys using:

    * Exact name matching on key-suffix columns
    * Fuzzy name matching (substring / shared stem)
    * Data-type compatibility filtering (when types are enriched)
    * Eval feedback from soft signal clusters (table pairs from blame sets)

    Existing join specs are excluded.  Returns a list of hint dicts
    (not final join specs) that feed into the LLM discovery prompt.

    Each hint has the shape::

        {
            "left_table": str,
            "right_table": str,
            "candidate_columns": [
                {"left_col": str, "right_col": str, "reason": str}
            ],
            "type_compatible": bool,
        }
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst, dict):
        _inst = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    existing_pairs: set[tuple[str, str]] = set()
    for spec in join_specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        if isinstance(left_obj, dict) and isinstance(right_obj, dict):
            lt = left_obj.get("identifier", "")
            rt = right_obj.get("identifier", "")
        else:
            lt = spec.get("left_table_name", "")
            rt = spec.get("right_table_name", "")
        if lt and rt:
            existing_pairs.add((lt, rt))
            existing_pairs.add((rt, lt))

    # Build per-table column info: {identifier: [{name, data_type}, ...]}
    table_col_info: dict[str, list[dict[str, str]]] = {}
    for t in tables:
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier", "") or t.get("name", "")
        if not ident:
            continue
        cols: list[dict[str, str]] = []
        for cc in t.get("column_configs", t.get("columns", [])):
            col = cc.get("column_name") or cc.get("name", "")
            if not col:
                continue
            dt = cc.get("data_type", "")
            cols.append({"name": col.lower(), "data_type": str(dt).upper() if dt else ""})
        if cols:
            table_col_info[ident] = cols

    # Only consider columns that look like join keys for the suffix matching
    # but also include all columns for fuzzy matching
    def _key_cols(cols: list[dict[str, str]]) -> list[dict[str, str]]:
        return [c for c in cols if any(c["name"].endswith(s) for s in _JOIN_KEY_SUFFIXES)]

    hints: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    idents = list(table_col_info.keys())

    for i, ident_a in enumerate(idents):
        for ident_b in idents[i + 1:]:
            if (ident_a, ident_b) in existing_pairs:
                continue

            _a, _b = sorted((ident_a, ident_b))
            pair_key = (_a, _b)
            if pair_key in seen_pairs:
                continue

            cols_a = table_col_info[ident_a]
            cols_b = table_col_info[ident_b]
            key_a = _key_cols(cols_a)
            key_b = _key_cols(cols_b)

            candidate_columns: list[dict[str, str]] = []
            all_types_compatible = True

            # 1) Exact name match on key-suffix columns
            names_b = {c["name"]: c for c in cols_b}
            for ca in key_a:
                cb = names_b.get(ca["name"])
                if cb:
                    compat = _types_compatible(ca["data_type"], cb["data_type"])
                    if not compat:
                        all_types_compatible = False
                    candidate_columns.append({
                        "left_col": ca["name"],
                        "right_col": cb["name"],
                        "reason": f"exact name match (suffix key){'' if compat else ' [TYPE MISMATCH]'}",
                    })

            # 2) Fuzzy name match on key-suffix columns (only if no exact match)
            exact_left = {c["left_col"] for c in candidate_columns}
            for ca in key_a:
                if ca["name"] in exact_left:
                    continue
                for cb in key_b:
                    if ca["name"] == cb["name"]:
                        continue
                    if _is_fuzzy_match(ca["name"], cb["name"]):
                        compat = _types_compatible(ca["data_type"], cb["data_type"])
                        if not compat:
                            all_types_compatible = False
                        candidate_columns.append({
                            "left_col": ca["name"],
                            "right_col": cb["name"],
                            "reason": f"fuzzy name match (stem/substring){'' if compat else ' [TYPE MISMATCH]'}",
                        })

            if not candidate_columns:
                continue

            seen_pairs.add(pair_key)
            hints.append({
                "left_table": ident_a,
                "right_table": ident_b,
                "candidate_columns": candidate_columns,
                "type_compatible": all_types_compatible,
            })

    # 3) Eval-feedback enrichment: soft signal clusters may reference
    # table pairs that heuristics missed (e.g., wrong join column, SCD filters).
    if soft_signal_clusters:
        feedback_pairs = _extract_table_pairs_from_clusters(soft_signal_clusters)
        ident_lower = {ident.lower(): ident for ident in table_col_info}

        for pair in feedback_pairs:
            t1_lower, t2_lower = pair
            t1_orig = ident_lower.get(t1_lower)
            t2_orig = ident_lower.get(t2_lower)
            if not t1_orig or not t2_orig:
                for k, v in ident_lower.items():
                    if t1_lower in k or k.endswith(t1_lower.rsplit(".", 1)[-1]):
                        t1_orig = t1_orig or v
                    if t2_lower in k or k.endswith(t2_lower.rsplit(".", 1)[-1]):
                        t2_orig = t2_orig or v
            if not t1_orig or not t2_orig:
                continue
            _a, _b = sorted((t1_orig, t2_orig))
            if (_a, _b) in seen_pairs or (_a, _b) in existing_pairs:
                continue

            cols_a = table_col_info.get(t1_orig, [])
            cols_b = table_col_info.get(t2_orig, [])
            names_b_map = {c["name"]: c for c in cols_b}
            cands: list[dict[str, str]] = []
            for ca in cols_a:
                cb = names_b_map.get(ca["name"])
                if cb:
                    cands.append({
                        "left_col": ca["name"],
                        "right_col": cb["name"],
                        "reason": "eval feedback: shared column name",
                    })
            if cands:
                seen_pairs.add((_a, _b))
                hints.append({
                    "left_table": t1_orig,
                    "right_table": t2_orig,
                    "candidate_columns": cands,
                    "type_compatible": True,
                    "source": "eval_feedback",
                })
        logger.info(
            "Join discovery: %d feedback pairs from %d soft signal clusters",
            len(feedback_pairs), len(soft_signal_clusters),
        )

    logger.info(
        "Join discovery: found %d hint pairs (%d existing specs)",
        len(hints), len(existing_pairs) // 2,
    )
    return hints


# ── Proactive Join Discovery (execution-proven) ──────────────────────

_FACT_PREFIXES = ("fact_", "fct_")

_JOIN_FQN_RE = re.compile(
    r"\bJOIN\s+"
    r"((?:`[^`]+`(?:\.`[^`]+`){1,2})"              # backtick-quoted 2- or 3-part
    r"|(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){1,2}))"    # unquoted 2- or 3-part
    r"(?:\s+(?:AS\s+)?(\w+))?"
    r"\s*ON\s+(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|;|\Z)",
    re.I | re.S,
)

_SQL_FROM_TABLE_RE = re.compile(
    r"\bFROM\s+"
    r"((?:`[^`]+`(?:\.`[^`]+`){1,2})"              # backtick-quoted 2- or 3-part
    r"|(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){1,2}))",   # unquoted 2- or 3-part
    re.I,
)


def _convert_fk_to_candidates(
    fk_rows: list[dict],
    short_to_fqn: dict[str, str] | None = None,
) -> list[dict]:
    """Convert FK constraint dicts into the join candidate format.

    Each FK dict (from ``get_foreign_keys_for_tables_rest`` or its Spark
    fallback) has ``child_table``, ``child_columns``, ``parent_table``,
    ``parent_columns``, ``constraint_name``.

    Returns candidates compatible with the pipeline used by
    ``_corroborate_with_uc_metadata`` and ``_build_join_specs_from_proven``,
    with an extra ``fk_constraint: True`` flag so downstream logic can
    recognise their authoritative provenance.
    """
    candidates: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    for fk in fk_rows:
        child_fqn = fk.get("child_table", "")
        parent_fqn = fk.get("parent_table", "")
        child_cols = fk.get("child_columns", [])
        parent_cols = fk.get("parent_columns", [])
        if not (child_fqn and parent_fqn and child_cols and parent_cols):
            continue
        if len(child_cols) != len(parent_cols):
            continue

        child_short = _short_name(child_fqn).lower()
        parent_short = _short_name(parent_fqn).lower()

        on_parts = [
            f"`{child_short}`.`{cc}` = `{parent_short}`.`{pc}`"
            for cc, pc in zip(child_cols, parent_cols)
        ]
        on_condition = " AND ".join(on_parts)

        _pk_a, _pk_b = sorted((child_fqn, parent_fqn))
        pair_key = (_pk_a, _pk_b)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        candidates.append({
            "left_table": child_fqn,
            "right_table": parent_fqn,
            "on_condition": on_condition,
            "frequency": 0,
            "agreed": False,
            "source_questions": [],
            "fk_constraint": True,
            "constraint_name": fk.get("constraint_name", ""),
        })

    logger.info(
        "FK→candidates: converted %d FK constraints into %d join candidates",
        len(fk_rows), len(candidates),
    )
    return candidates


def _extract_proven_joins(
    rows: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Extract execution-validated join paths from baseline eval rows.

    Considers rows where the arbiter verdict is positive (``both_correct``,
    ``genie_correct``, or ``ground_truth_correct``).  Parses JOIN…ON clauses
    from both Genie SQL and ground-truth SQL, resolves short table names to
    FQN identifiers, and returns deduplicated candidates sorted by frequency.
    """
    _POSITIVE_VERDICTS = {"both_correct", "genie_correct", "ground_truth_correct"}

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", [])

    short_to_fqn: dict[str, str] = {}
    _ambiguous_shorts: set[str] = set()
    for t in list(tables) + list(mvs):
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier", "") or t.get("name", "")
        if ident:
            short = _short_name(ident).lower().strip("`")
            if short in short_to_fqn and short_to_fqn[short] != ident:
                _ambiguous_shorts.add(short)
            short_to_fqn[short] = ident
            fqn_lower = ident.lower().strip("`")
            short_to_fqn[fqn_lower] = ident

    logger.info(
        "Join extraction: short_to_fqn has %d entries, %d ambiguous shorts",
        len(short_to_fqn), len(_ambiguous_shorts),
    )

    candidates: dict[tuple[str, str], dict] = {}
    _diag_positive = 0
    _diag_has_join = 0
    _diag_no_from = 0
    _diag_no_resolve = 0

    for row in rows:
        arbiter = (
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or row.get("arbiter")
            or ""
        )
        if str(arbiter).lower() not in _POSITIVE_VERDICTS:
            continue

        _diag_positive += 1
        qid = _row_qid(row)

        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}

        gt_sql = (
            (_req.get("expected_sql", "") if isinstance(_req, dict) else "")
            or row.get("inputs/expected_sql", "")
            or ""
        )
        genie_sql = (
            (_resp.get("response", "") if isinstance(_resp, dict) else "")
            or row.get("outputs/response", "")
            or ""
        )

        for sql, source_label in [(gt_sql, "gt"), (genie_sql, "genie")]:
            if not sql:
                continue

            join_matches = list(_JOIN_FQN_RE.finditer(sql))
            if not join_matches:
                continue

            _diag_has_join += 1

            from_m = _SQL_FROM_TABLE_RE.search(sql)
            from_table_raw = from_m.group(1).replace("`", "").lower() if from_m else ""
            from_fqn = ""
            if from_table_raw:
                from_fqn = short_to_fqn.get(from_table_raw, "")
                if not from_fqn:
                    from_short = _short_name(from_table_raw).lower()
                    if from_short not in _ambiguous_shorts:
                        from_fqn = short_to_fqn.get(from_short, "")
                    elif "." in from_table_raw and from_table_raw.count(".") >= 2:
                        from_fqn = from_table_raw

            if not from_fqn:
                _diag_no_from += 1
                logger.debug(
                    "Join extraction [%s/%s]: no FROM table resolved "
                    "(raw=%r), skipping %d JOINs",
                    qid, source_label, from_table_raw, len(join_matches),
                )
                continue

            for m in join_matches:
                joined_table_raw = m.group(1).replace("`", "").lower()
                alias = (m.group(2) or "").lower()
                on_clause = m.group(3).strip()

                joined_fqn = short_to_fqn.get(joined_table_raw, "")
                if not joined_fqn:
                    joined_short = _short_name(joined_table_raw).lower()
                    if joined_short not in _ambiguous_shorts:
                        joined_fqn = short_to_fqn.get(joined_short, "")
                    elif "." in joined_table_raw and joined_table_raw.count(".") >= 2:
                        joined_fqn = joined_table_raw

                if not joined_fqn:
                    if "." in joined_table_raw and joined_table_raw.count(".") >= 2:
                        joined_fqn = joined_table_raw
                    else:
                        _diag_no_resolve += 1
                        logger.debug(
                            "Join extraction [%s/%s]: cannot resolve "
                            "joined table %r to FQN",
                            qid, source_label, joined_table_raw,
                        )
                        continue

                _pk_l, _pk_r = sorted((from_fqn, joined_fqn))
                pair_key = (_pk_l, _pk_r)
                if pair_key[0] == pair_key[1]:
                    continue

                if pair_key not in candidates:
                    candidates[pair_key] = {
                        "left_table": pair_key[0],
                        "right_table": pair_key[1],
                        "on_conditions": {},
                        "frequency": 0,
                        "source_questions": [],
                        "from_gt": set(),
                        "from_genie": set(),
                    }

                entry = candidates[pair_key]
                entry["frequency"] += 1
                if qid not in entry["source_questions"]:
                    entry["source_questions"].append(qid)
                entry[f"from_{source_label}"].add(qid)

                on_norm = re.sub(r"\s+", " ", on_clause).strip()
                if on_norm:
                    entry["on_conditions"][on_norm] = entry["on_conditions"].get(on_norm, 0) + 1

    result: list[dict] = []
    for pair_key, entry in candidates.items():
        gt_qs = entry.pop("from_gt")
        genie_qs = entry.pop("from_genie")
        agreed_qs = gt_qs & genie_qs
        entry["agreed"] = len(agreed_qs) > 0

        best_condition = ""
        if entry["on_conditions"]:
            best_condition = max(entry["on_conditions"], key=entry["on_conditions"].get)
        entry["on_condition"] = best_condition
        del entry["on_conditions"]

        result.append(entry)

    result.sort(key=lambda x: (-int(x.get("agreed", False)), -x["frequency"]))

    logger.info(
        "Proactive join discovery: %d candidates from %d rows "
        "(positive_verdicts=%d, sql_with_join=%d, "
        "no_from_resolved=%d, no_joined_resolved=%d)",
        len(result), len(rows),
        _diag_positive, _diag_has_join,
        _diag_no_from, _diag_no_resolve,
    )
    for cand in result:
        logger.info(
            "  candidate: %s <-> %s  freq=%d agreed=%s on=%s",
            cand["left_table"], cand["right_table"],
            cand["frequency"], cand["agreed"],
            cand.get("on_condition", "")[:80],
        )
    return result


def _corroborate_with_uc_metadata(
    candidates: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Filter proven join candidates by UC column type compatibility.

    Rejects candidates whose join columns have known incompatible types.
    Candidates with unknown types pass through (benefit of the doubt for
    execution-proven joins).

    Builds the type lookup using both short-name and FQN keys to avoid
    mismatches in multi-catalog environments.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    col_types: dict[tuple[str, str], str] = {}
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = _short_name(ident).lower()
        fqn_lower = ident.lower()
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            dt = cc.get("data_type", "")
            if col_name and dt:
                col_types[(short, col_name)] = str(dt).upper()
                col_types[(fqn_lower, col_name)] = str(dt).upper()

    validated: list[dict] = []
    for cand in candidates:
        on_cond = cand.get("on_condition", "")
        if not on_cond:
            validated.append(cand)
            continue

        pattern = r"`?(\w+)`?\s*\.\s*`?(\w+)`?\s*=\s*`?(\w+)`?\s*\.\s*`?(\w+)`?"
        match = re.search(pattern, on_cond)
        if not match:
            validated.append(cand)
            continue

        alias_l, col_l = match.group(1).lower(), match.group(2).lower()
        alias_r, col_r = match.group(3).lower(), match.group(4).lower()

        left_fqn = cand.get("left_table", "").lower()
        right_fqn = cand.get("right_table", "").lower()
        type_l = (
            col_types.get((left_fqn, col_l), "")
            or col_types.get((alias_l, col_l), "")
        )
        type_r = (
            col_types.get((right_fqn, col_r), "")
            or col_types.get((alias_r, col_r), "")
        )

        if type_l and type_r and not _types_compatible(type_l, type_r):
            logger.info(
                "Proactive join: rejecting %s <-> %s — type mismatch %s(%s) vs %s(%s)",
                cand["left_table"], cand["right_table"],
                col_l, type_l, col_r, type_r,
            )
            continue

        cand["type_compatible"] = True
        validated.append(cand)

    logger.info(
        "Proactive join: %d/%d candidates passed UC type check",
        len(validated), len(candidates),
    )
    return validated


def _build_join_specs_from_proven(
    candidates: list[dict],
    metadata_snapshot: dict,
) -> list[dict]:
    """Convert validated candidates into proper Genie API join_spec dicts.

    Assigns relationship types heuristically: fact→dim gets MANY_TO_ONE,
    everything else defaults to MANY_TO_ONE as it is the most common
    star-schema pattern.
    """
    from genie_space_optimizer.common.genie_schema import ensure_join_spec_fields
    from genie_space_optimizer.optimization.applier import _validate_join_spec_entry

    specs: list[dict] = []
    for cand in candidates:
        left_fqn = cand["left_table"]
        right_fqn = cand["right_table"]
        on_condition = cand.get("on_condition", "")

        left_short = _short_name(left_fqn).lower()
        right_short = _short_name(right_fqn).lower()

        left_is_fact = any(left_short.startswith(p) for p in _FACT_PREFIXES)
        right_is_fact = any(right_short.startswith(p) for p in _FACT_PREFIXES)

        if left_is_fact and not right_is_fact:
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"
        elif right_is_fact and not left_is_fact:
            left_fqn, right_fqn = right_fqn, left_fqn
            left_short, right_short = right_short, left_short
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"
        else:
            rt = "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"

        sql_parts = []
        if on_condition:
            equijoin_only = _extract_equijoin_predicates(on_condition)
            if equijoin_only:
                on_condition = equijoin_only
            normalized = re.sub(
                r"`?\w+`?\.",
                lambda m: m.group(0),
                on_condition,
            )
            has_backticks = "`" in normalized
            if not has_backticks:
                normalized = re.sub(
                    r"(\w+)\.(\w+)",
                    r"`\1`.`\2`",
                    normalized,
                )
            sql_parts.append(normalized)
        sql_parts.append(f"--rt={rt}--")

        spec = {
            "left": {"identifier": left_fqn, "alias": left_short},
            "right": {"identifier": right_fqn, "alias": right_short},
            "sql": sql_parts,
        }
        spec = ensure_join_spec_fields(spec)

        if not _validate_join_spec_entry(spec):
            logger.info(
                "Proactive join: spec rejected by validation — %s <-> %s",
                left_fqn, right_fqn,
            )
            continue

        valid, reason = validate_join_spec_types(spec, metadata_snapshot)
        if not valid:
            logger.info(
                "Proactive join: spec rejected by type check — %s <-> %s: %s",
                left_fqn, right_fqn, reason,
            )
            continue

        spec["_proactive_metadata"] = {
            "frequency": cand.get("frequency", 0),
            "agreed": cand.get("agreed", False),
            "source_questions": cand.get("source_questions", []),
        }
        specs.append(spec)

    specs.sort(
        key=lambda s: (
            -int(s.get("_proactive_metadata", {}).get("agreed", False)),
            -s.get("_proactive_metadata", {}).get("frequency", 0),
        )
    )

    logger.info(
        "Proactive join: built %d valid join specs from %d candidates",
        len(specs), len(candidates),
    )
    return specs


# ═══════════════════════════════════════════════════════════════════════
# 3. ASI Extraction
# ═══════════════════════════════════════════════════════════════════════


def read_asi_from_uc(
    spark: Any,
    mlflow_run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Query ``genie_eval_asi_results`` Delta table via Spark."""
    table = f"{catalog}.{schema}.genie_eval_asi_results"
    try:
        df = spark.sql(
            f"""
            SELECT run_id, iteration, question_id, judge, value,
                   failure_type, severity, confidence, blame_set,
                   counterfactual_fix, wrong_clause, expected_value,
                   actual_value, missing_metadata, ambiguity_detected
            FROM {table}
            WHERE run_id = '{mlflow_run_id}'
            ORDER BY question_id, judge
            """
        )
        rows: list[dict] = []
        for r in df.collect():
            row_dict = r.asDict()
            if row_dict.get("blame_set"):
                try:
                    row_dict["blame_set"] = json.loads(row_dict["blame_set"])
                except (json.JSONDecodeError, TypeError):
                    row_dict["blame_set"] = [row_dict["blame_set"]]
            rows.append(row_dict)
        return rows
    except Exception:
        logger.exception("read_asi_from_uc failed")
        return []


def _extract_asi_from_assessments(assessments: list) -> list[dict]:
    """Parse ASI metadata from MLflow assessments list."""
    feedbacks: list[dict] = []
    for a in assessments:
        if not isinstance(a, dict):
            continue
        meta = a.get("metadata", {})
        if not isinstance(meta, dict):
            continue
        feedbacks.append(
            {
                "value": a.get("value", ""),
                "judge": a.get("name", ""),
                "question_id": a.get("question_id", ""),
                "failure_type": meta.get("failure_type", ""),
                "blame_set": meta.get("blame_set", []),
                "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                "confidence": float(meta.get("confidence", 0.5)),
                "asi_severity": meta.get("severity", ""),
                "asi_wrong_clause": meta.get("wrong_clause", ""),
                "asi_expected_value": meta.get("expected_value", ""),
                "asi_actual_value": meta.get("actual_value", ""),
                "asi_missing_metadata": meta.get("missing_metadata", ""),
                "asi_ambiguity_detected": meta.get("ambiguity_detected", False),
            }
        )
    return feedbacks


def _extract_judge_feedbacks_from_eval(
    eval_results: dict,
    catalog: str = "",
    schema: str = "",
    warehouse_id: str = "",
) -> list[dict]:
    """Extract judge feedback dicts from eval results using UC-first priority chain."""
    direct = eval_results.get("judge_feedbacks") or eval_results.get("feedbacks")
    if isinstance(direct, list) and direct:
        return direct

    rows = (
        eval_results.get("eval_results")
        or eval_results.get("rows")
        or eval_results.get("table")
    )
    if not isinstance(rows, list):
        return []

    feedbacks: list[dict] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for col, val in list(row.items()):
            if col.endswith("/value") and str(val).lower() in ("no", "false"):
                judge = col.replace("/value", "")
                meta_col = f"{judge}/metadata"
                meta = row.get(meta_col, {})
                if isinstance(meta, dict) and meta.get("failure_type"):
                    feedbacks.append(
                        {
                            "value": val,
                            "judge": judge,
                            "failure_type": meta.get("failure_type", ""),
                            "blame_set": meta.get("blame_set", []),
                            "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                            "confidence": float(meta.get("confidence", 0.7)),
                            "question_id": _row_qid(row, fallback=f"q{i}"),
                            "feedback_id": f"r{i}_{judge}",
                        }
                    )
                    continue

            if col.startswith("feedback/") and "no" in str(val).lower():
                judge = col.replace("feedback/", "")
                rationale = row.get(f"rationale/{judge}", row.get("rationale", ""))
                feedbacks.append(
                    {
                        "value": val,
                        "blame_set": _infer_blame_from_rationale(rationale),
                        "counterfactual_fix": [],
                        "feedback_id": f"r{i}_{judge}",
                        "question_id": row.get(
                            "inputs/question", row.get("question", f"q{i}")
                        ),
                        "confidence": 0.7,
                    }
                )
    return feedbacks


def _infer_blame_from_rationale(rationale: str, metadata_snapshot: dict | None = None) -> list[str]:
    """Infer blame_set from judge rationale for grouping."""
    r = (rationale or "").lower()
    blame: list[str] = []
    if "table" in r:
        blame.append("tables")
    if "column" in r:
        blame.append("column_metadata")
    if "join" in r:
        blame.append("joins")
    if "filter" in r:
        blame.append("filters")
    if "instruction" in r:
        blame.append("instructions")
    return blame if blame else ["_ungrouped"]


# ═══════════════════════════════════════════════════════════════════════
# 4. LLM-powered Proposal Generation
# ═══════════════════════════════════════════════════════════════════════


def _extract_metadata_for_blame(
    metadata_snapshot: dict, blame_set: Any
) -> str:
    """Extract relevant metadata sections for the blamed objects.

    Handles fully-qualified names (``catalog.schema.table``) by matching
    on the last dotted component as well as the full string.
    """
    if not blame_set or not metadata_snapshot:
        return "(no metadata available)"

    blame_items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
    blame_lower = set()
    for b in blame_items:
        bl = b.lower().strip()
        blame_lower.add(bl)
        if "." in bl:
            blame_lower.add(bl.rsplit(".", 1)[-1])

    sections: list[str] = []
    matched_tables: set[str] = set()

    for table in metadata_snapshot.get("tables", []):
        table_name = table.get("name") or table.get("identifier", "")
        tn_lower = table_name.lower()
        short_name = tn_lower.rsplit(".", 1)[-1] if "." in tn_lower else tn_lower
        if any(b in tn_lower or b == short_name for b in blame_lower):
            if table_name in matched_tables:
                continue
            matched_tables.add(table_name)
            sections.append(f"Table: {table_name}")
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                desc = col.get("description", "")
                if isinstance(desc, list):
                    desc = " ".join(desc)
                sections.append(f"  Column: {col_name} — {desc}")

    for b in blame_lower:
        for table in metadata_snapshot.get("tables", []):
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                if b == col_name.lower():
                    desc = col.get("description", "")
                    if isinstance(desc, list):
                        desc = " ".join(desc)
                    tname = table.get("name") or table.get("identifier", "")
                    sections.append(f"Column {tname}.{col_name}: {desc}")

    from genie_space_optimizer.optimization.applier import _get_general_instructions
    instructions = _get_general_instructions(metadata_snapshot)
    if instructions and any("instruction" in b for b in blame_lower):
        sections.append(f"Instructions: {instructions[:500]}")

    return "\n".join(sections) if sections else "(blamed objects not found in metadata)"


def _format_full_schema_context(
    metadata_snapshot: dict,
    filter_tables: set[str] | None = None,
) -> str:
    """Build a full schema summary of all tables, columns, descriptions, and synonyms.

    Gives the LLM complete visibility into the Genie Space structure so it can
    make informed decisions about which columns need descriptions vs. which
    should inherit from Unity Catalog, and which synonyms already exist.

    If *filter_tables* is provided, only tables whose identifier (lowercased)
    is in the set are included — useful for scoping join discovery prompts.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    from genie_space_optimizer.optimization.structured_metadata import (
        deduplicate_structured_description,
    )

    lines: list[str] = []
    for tbl in tables:
        if filter_tables is not None:
            tbl_id = (tbl.get("identifier", "") or "").lower()
            if tbl_id not in filter_tables:
                continue
        identifier = tbl.get("identifier", "")
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(tbl_desc)
        tbl_desc = deduplicate_structured_description(tbl_desc)
        lines.append(f"### Table: {identifier}")
        if tbl_desc:
            lines.append(f"  Description: {tbl_desc}")
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            if isinstance(desc, list):
                desc = "\n".join(desc) if desc else ""
            desc = deduplicate_structured_description(desc) if desc else ""
            uc_comment = cc.get("uc_comment", "")
            if not desc and uc_comment:
                desc = uc_comment
            syns = cc.get("synonyms", [])
            type_part = f" ({data_type})" if data_type else ""
            desc_part = f" -- {desc}" if desc else " -- (from UC)"
            syn_part = f" | synonyms: {syns}" if syns else ""
            lines.append(f"  - `{col_name}`{type_part}{desc_part}{syn_part}")
    return "\n".join(lines) if lines else "(no schema available)"


def _format_schema_index(metadata_snapshot: dict) -> str:
    """Compact table-of-contents for the triage strategist.

    Each table gets a single line with column names and types — no descriptions,
    no synonyms. Keeps the prompt small while giving full schema awareness.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    lines: list[str] = []
    for tbl in tables:
        identifier = tbl.get("identifier", "")
        cols = tbl.get("column_configs", [])
        col_parts: list[str] = []
        for cc in cols:
            cname = cc.get("column_name", "")
            dtype = cc.get("data_type", "")
            col_parts.append(f"{cname}:{dtype}" if dtype else cname)
        col_preview = ", ".join(col_parts[:12])
        suffix = f", ... +{len(cols) - 12} more" if len(cols) > 12 else ""
        lines.append(f"- {identifier} ({len(cols)} cols: {col_preview}{suffix})")
    return "\n".join(lines) if lines else "(no schema available)"


def _build_identifier_allowlist(
    metadata_snapshot: dict,
    uc_columns: list[dict] | None = None,
) -> dict[str, Any]:
    """Extract an authoritative allowlist of all valid identifiers from metadata.

    Merges Genie Config (tables, metric views, functions, column_configs)
    with UC column metadata to produce a single source of truth that LLM
    prompts and static validators can reference.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables_list = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    funcs_list = metadata_snapshot.get("functions", []) or ds.get("functions", []) or []
    mvs_list = ds.get("metric_views", []) or metadata_snapshot.get("metric_views", []) or []

    table_ids: list[str] = []
    tables_short: set[str] = set()
    columns_by_table: dict[str, list[tuple[str, str]]] = {}
    columns_flat: set[str] = set()

    uc_type_lookup: dict[tuple[str, str], str] = {}
    if uc_columns:
        for row in uc_columns:
            if not isinstance(row, dict):
                continue
            tbl = str(row.get("table_name") or "").strip().lower()
            col = str(row.get("column_name") or "").strip().lower()
            dtype = str(row.get("data_type") or "").strip().upper()
            if tbl and col:
                uc_type_lookup[(tbl, col)] = dtype

    for tbl in tables_list:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        if not ident:
            continue
        table_ids.append(ident)
        short = ident.rsplit(".", 1)[-1].lower()
        tables_short.add(short)

        col_entries: list[tuple[str, str]] = []
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            if not isinstance(cc, dict):
                continue
            col_name = cc.get("column_name") or cc.get("name") or ""
            dtype = cc.get("data_type") or ""
            if not dtype:
                dtype = uc_type_lookup.get((short, col_name.lower()), "")
            col_entries.append((col_name, dtype.upper() if dtype else ""))
            if col_name:
                columns_flat.add(f"{short}.{col_name}".lower())
                columns_flat.add(col_name.lower())
        columns_by_table[short] = col_entries

    func_ids: list[str] = []
    funcs_short: set[str] = set()
    for fn in funcs_list:
        if isinstance(fn, dict):
            name = fn.get("identifier", "") or fn.get("name", "")
        else:
            name = str(fn)
        if name:
            func_ids.append(name)
            funcs_short.add(name.rsplit(".", 1)[-1].lower())

    mv_ids: list[str] = []
    for mv in mvs_list:
        if isinstance(mv, dict):
            name = mv.get("identifier", "") or mv.get("name", "")
        else:
            name = str(mv)
        if name:
            mv_ids.append(name)

    return {
        "tables": table_ids,
        "tables_short": tables_short,
        "columns": columns_by_table,
        "columns_flat": columns_flat,
        "functions": func_ids,
        "functions_short": funcs_short,
        "metric_views": mv_ids,
    }


def _format_identifier_allowlist(allowlist: dict[str, Any]) -> str:
    """Render the identifier allowlist as a prompt-ready string."""
    sections: list[str] = []

    if allowlist.get("tables"):
        lines = ["VALID TABLES (use ONLY these in FROM/JOIN):"]
        for t in allowlist["tables"]:
            lines.append(f"- {t}")
        sections.append("\n".join(lines))

    if allowlist.get("columns"):
        lines = ["VALID COLUMNS BY TABLE (use ONLY these column names):"]
        for tbl_short, cols in sorted(allowlist["columns"].items()):
            if not cols:
                continue
            col_parts = []
            for col_name, dtype in cols:
                col_parts.append(f"{col_name} ({dtype})" if dtype else col_name)
            lines.append(f"{tbl_short}: {', '.join(col_parts)}")
        sections.append("\n".join(lines))

    if allowlist.get("functions"):
        lines = ["VALID FUNCTIONS (use ONLY these):"]
        for fn in allowlist["functions"]:
            lines.append(f"- {fn}")
        sections.append("\n".join(lines))

    if allowlist.get("metric_views"):
        lines = ["VALID METRIC VIEWS:"]
        for mv in allowlist["metric_views"]:
            lines.append(f"- {mv}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections) if sections else "(no assets configured)"


_SQL_TABLE_REF_RE = re.compile(
    r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`(?:\.`[^`]+`)*"
    r"|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)"
    r"(?:\s*\()?",
    re.IGNORECASE,
)


def _validate_sql_identifiers(
    sql: str,
    allowlist: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Deterministic cross-check of SQL table/column refs against the allowlist.

    Returns ``(is_valid, violations)`` where *violations* is a list of
    human-readable strings describing each unrecognized identifier.
    This does NOT require a SparkSession — purely regex-based.
    """
    violations: list[str] = []
    if not sql or not allowlist:
        return True, violations

    tables_full = {t.lower() for t in (allowlist.get("tables") or [])}
    tables_short = {s.lower() for s in (allowlist.get("tables_short") or set())}
    cols_flat = {c.lower() for c in (allowlist.get("columns_flat") or set())}

    for mv in allowlist.get("metric_views") or []:
        mv_lower = mv.lower()
        tables_full.add(mv_lower)
        tables_short.add(mv_lower.rsplit(".", 1)[-1])

    for m in _SQL_TABLE_REF_RE.finditer(sql):
        ref = m.group(1).replace("`", "").strip()
        ref_lower = ref.lower()
        leaf = ref_lower.rsplit(".", 1)[-1]
        if ref_lower not in tables_full and leaf not in tables_short:
            violations.append(f"Unknown table: {ref}")

    sql_upper = sql.upper()
    for kw in ("SELECT", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "ON"):
        idx = sql_upper.find(kw)
        if idx < 0:
            continue
        end = len(sql)
        for stop_kw in ("FROM", "JOIN", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION"):
            si = sql_upper.find(stop_kw, idx + len(kw))
            if 0 < si < end and stop_kw != kw:
                end = si
        clause = sql[idx + len(kw):end]
        for col_match in re.finditer(
            r"(?<![:\w])([A-Za-z_]\w*)\s*(?:\.([A-Za-z_]\w*))?",
            clause,
        ):
            part1 = col_match.group(1).lower()
            part2 = (col_match.group(2) or "").lower()
            if part2:
                candidate = f"{part1}.{part2}"
                if candidate in cols_flat:
                    continue
                if part2 in cols_flat:
                    continue
            else:
                if part1 in cols_flat or part1 in tables_short:
                    continue
                if part1 in _SQL_KEYWORDS:
                    continue

    return (len(violations) == 0, violations)


_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "is", "null",
    "as", "on", "join", "left", "right", "inner", "outer", "cross", "full",
    "group", "by", "order", "asc", "desc", "having", "limit", "offset",
    "union", "all", "distinct", "case", "when", "then", "else", "end",
    "between", "like", "exists", "count", "sum", "avg", "min", "max",
    "cast", "coalesce", "nullif", "true", "false", "insert", "update",
    "delete", "into", "values", "set", "create", "alter", "drop", "table",
    "view", "index", "with", "recursive", "over", "partition", "row_number",
    "rank", "dense_rank", "lag", "lead", "first_value", "last_value",
    "date", "timestamp", "string", "int", "integer", "bigint", "decimal",
    "float", "double", "boolean", "array", "map", "struct", "measure",
    "current_date", "current_timestamp", "extract", "year", "month", "day",
    "hour", "minute", "second", "interval", "trim", "upper", "lower",
    "substring", "concat", "length", "replace", "round", "floor", "ceil",
    "abs", "if", "ifnull", "isnull", "nvl", "to_date", "date_format",
    "datediff", "dateadd", "months_between", "trunc", "try_cast",
})


def _format_compact_cluster_summaries(clusters: list[dict]) -> str:
    """One-liner per cluster for the triage strategist — no SQL diffs."""
    if not clusters:
        return "(No failure clusters.)"

    lines: list[str] = []
    for cluster in clusters:
        cid = cluster.get("cluster_id", "?")
        rc = cluster.get("root_cause", "unknown")
        qids = cluster.get("question_ids", [])
        blame = cluster.get("asi_blame_set")
        if isinstance(blame, str) and blame:
            blame_parts = [b.strip() for b in blame.split("|")][:5]
        elif isinstance(blame, list):
            blame_parts = [str(b) for b in blame[:5]]
        else:
            blame_parts = []
        fixes = cluster.get("asi_counterfactual_fixes", [])
        fix_str = "; ".join(str(f)[:120] for f in fixes[:2]) if fixes else ""

        parts = [f"{cid}: {rc} ({len(qids)} questions)"]
        if blame_parts:
            parts.append(f"blamed=[{', '.join(blame_parts)}]")
        if fix_str:
            parts.append(f'fixes=["{fix_str}"]')

        qtext_samples: list[str] = []
        for qt in cluster.get("question_traces", [])[:2]:
            qt_text = qt.get("question_text", "")[:100]
            if qt_text:
                qtext_samples.append(qt_text)
        for sc in cluster.get("sql_contexts", [])[:2]:
            qt_text = sc.get("question", "")[:100]
            if qt_text and qt_text not in qtext_samples:
                qtext_samples.append(qt_text)
        if qtext_samples:
            parts.append(f"sample_qs=[{'; '.join(qtext_samples[:2])}]")

        lines.append(" | ".join(parts))
    return "\n".join(lines)


def _format_structured_column_context(
    metadata_snapshot: dict,
    blame_set: Any,
    lever: int,
) -> str:
    """Build structured column metadata with editability markers for the LLM.

    Shows each relevant column's current structured sections with [EDITABLE]
    or [LOCKED] markers based on lever ownership.  Falls back to all tables
    when blame_set is empty.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        classify_column,
        entity_type_for_column,
        extract_synonyms_section,
        merge_synonyms,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []

    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
        for b in items:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    lines: list[str] = []
    columns_shown = 0
    max_columns = 40

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        lines.append(f"### Table: {identifier}")

        for cc in tbl.get("column_configs", []):
            if columns_shown >= max_columns:
                break
            col_name = cc.get("column_name", "")
            if not col_name:
                continue

            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            syns = cc.get("synonyms", [])
            uc_comment = cc.get("uc_comment", "")

            desc_text = desc
            if isinstance(desc_text, list):
                desc_text = "\n".join(desc_text)
            if not desc_text and uc_comment:
                desc_text = uc_comment

            sections = parse_structured_description(desc_text)

            if syns:
                existing_syn = extract_synonyms_section(sections)
                all_syns = merge_synonyms(existing_syn, syns)
                from genie_space_optimizer.optimization.structured_metadata import (
                    format_synonyms_section,
                )

                sections["synonyms"] = format_synonyms_section(all_syns)

            etype = entity_type_for_column(
                col_name, data_type, is_in_metric_view=is_mv,
            )
            kind = classify_column(col_name, data_type, is_in_metric_view=is_mv)
            template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])

            lines.append(f"  Column: `{col_name}` ({data_type or 'unknown'}) [type: {kind}]")
            for sk in template_sections:
                label = SECTION_LABELS[sk]
                value = sections.get(sk, "").strip()
                marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
                lines.append(
                    f"    {marker} **{label}:** {value if value else '(empty)'}"
                )
            preamble = sections.get("_preamble", "").strip()
            if preamble:
                lines.append(f"    [Legacy text]: {preamble}")

            _profile = metadata_snapshot.get("_data_profile", {})
            _tbl_profile = (
                _profile.get(identifier, {})
                or _profile.get(identifier.lower(), {})
            )
            _col_profile = _tbl_profile.get("columns", {}).get(col_name, {})
            if _col_profile.get("distinct_values"):
                lines.append(f"    Data values: {_col_profile['distinct_values']}")
            elif _col_profile.get("min") is not None:
                lines.append(
                    f"    Data range: [{_col_profile['min']}, {_col_profile['max']}]"
                )

            lines.append("")
            columns_shown += 1

        if columns_shown >= max_columns:
            lines.append("  ... (additional columns omitted for brevity)")
            break

    return "\n".join(lines) if lines else "(no structured column metadata available)"


def _format_structured_table_context(
    metadata_snapshot: dict,
    blame_set: Any,
    lever: int,
) -> str:
    """Build structured table-level metadata with editability markers for the LLM.

    Shows each relevant table's current structured sections (Purpose, Best For,
    Grain, SCD, Relationships) with [EDITABLE]/[LOCKED] markers based on lever
    ownership.
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
        for b in items:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    lines: list[str] = []

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        etype = "mv_table" if is_mv else "table"
        template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])

        desc = tbl.get("description", [])
        desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
        sections = parse_structured_description(desc_text)

        lines.append(f"### Table: {identifier} (entity_type: {etype})")
        for sk in template_sections:
            label = SECTION_LABELS[sk]
            value = sections.get(sk, "").strip()
            marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
            lines.append(f"  {marker} **{label}:** {value if value else '(empty)'}")
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            lines.append(f"  [Legacy text]: {preamble}")
        lines.append("")

    return "\n".join(lines) if lines else "(no structured table metadata available)"


def _format_structured_function_context(
    metadata_snapshot: dict,
    lever: int,
) -> str:
    """Build structured function metadata with editability markers for the LLM.

    Shows each function's current metadata in structured sections (Purpose,
    Best For, Use Instead Of, Parameters, Example).
    """
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    if not funcs:
        return "(no functions in this Genie Space)"

    owned_sections = LEVER_SECTION_OWNERSHIP.get(lever, set())
    template_sections = ENTITY_TYPE_TEMPLATES.get("function", [])
    lines: list[str] = []

    for fn in funcs:
        name = fn.get("name") or fn.get("identifier", "")
        comment = fn.get("comment") or fn.get("description") or ""
        if isinstance(comment, list):
            comment = "\n".join(comment)
        sections = parse_structured_description(comment)

        lines.append(f"### Function: {name}")
        for sk in template_sections:
            label = SECTION_LABELS[sk]
            value = sections.get(sk, "").strip()
            marker = "[EDITABLE]" if sk in owned_sections else "[LOCKED]"
            lines.append(f"  {marker} **{label}:** {value if value else '(empty)'}")
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            lines.append(f"  [Legacy text]: {preamble}")
        lines.append("")

    return "\n".join(lines)


def _describe_patch_type(patch_type: str) -> str:
    """Human-readable description of a patch type."""
    pt_info = PATCH_TYPES.get(patch_type)
    if pt_info:
        return (
            f"{patch_type}: scope={pt_info['scope']}, "
            f"risk={pt_info['risk_level']}, affects={pt_info['affects']}"
        )
    return patch_type


def _format_sql_diffs(
    cluster: dict,
    *,
    max_sql_chars: int = 0,
    max_sample_chars: int = 500,
) -> str:
    """Build a human-readable summary of SQL diffs for the LLM prompt.

    When *max_sql_chars* > 0, individual SQL blocks are truncated to that
    length to keep overall prompt size manageable.  When *max_sample_chars*
    > 0, ground-truth and Genie result samples from the comparison dict are
    included (truncated to *max_sample_chars*).
    """
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return "(no SQL context available)"

    def _trunc(sql: str) -> str:
        if max_sql_chars > 0 and len(sql) > max_sql_chars:
            return sql[:max_sql_chars] + " ... (truncated)"
        return sql

    lines: list[str] = []
    for idx, ctx in enumerate(sql_contexts[:3], 1):
        q = ctx.get("question", "")
        exp = _trunc(ctx.get("expected_sql", ""))
        gen = _trunc(ctx.get("generated_sql", ""))
        comp = ctx.get("comparison", {})
        lines.append(f"### Question {idx}: {q}")
        lines.append(f"**Expected SQL:**\n```sql\n{exp}\n```")
        lines.append(f"**Generated SQL:**\n```sql\n{gen}\n```")
        if isinstance(comp, dict) and comp.get("error"):
            lines.append(f"**Error:** {comp['error']}")
        elif isinstance(comp, dict) and not comp.get("match"):
            match_type = comp.get("match_type", "unknown")
            lines.append(f"**Mismatch type:** {match_type}")
        if max_sample_chars > 0 and isinstance(comp, dict):
            gt_sample = comp.get("gt_sample")
            if gt_sample:
                lines.append(
                    f"**Expected Result (sample):**\n```\n{str(gt_sample)[:max_sample_chars]}\n```"
                )
            genie_sample = comp.get("genie_sample")
            if genie_sample:
                lines.append(
                    f"**Genie Result (sample):**\n```\n{str(genie_sample)[:max_sample_chars]}\n```"
                )
        lines.append("")
    cf = cluster.get("asi_counterfactual_fixes", [])
    if cf:
        lines.append("**Suggested fixes from judges:**")
        for fix in cf[:3]:
            lines.append(f"- {fix}")
    return "\n".join(lines)


def _format_blamed_column_values(
    clusters: list[dict],
    data_profile: dict,
    *,
    max_columns: int = 20,
) -> str:
    """Build a compact value-hint section for columns that appear in blame sets.

    Extracts blamed column and table references from failure clusters, looks
    them up in *data_profile*, and renders their distinct values or ranges.
    """
    if not data_profile:
        return "(no data profile available)"

    blamed_refs: set[str] = set()
    for cluster in clusters:
        blame = cluster.get("asi_blame_set")
        if isinstance(blame, str) and blame:
            blamed_refs.update(b.strip().lower() for b in blame.split("|") if b.strip())
        elif isinstance(blame, list):
            blamed_refs.update(str(b).strip().lower() for b in blame if b)

    if not blamed_refs:
        return "(no blamed columns identified)"

    profile_lower: dict[str, dict] = {}
    for table_fqn, tinfo in data_profile.items():
        tkey = table_fqn.lower()
        short_name = tkey.split(".")[-1] if "." in tkey else tkey
        for col_name, cinfo in tinfo.get("columns", {}).items():
            col_lower = col_name.lower()
            profile_lower[f"{tkey}.{col_lower}"] = cinfo
            profile_lower[f"{short_name}.{col_lower}"] = cinfo
            profile_lower[col_lower] = cinfo

    lines: list[str] = []
    matched = 0
    for ref in sorted(blamed_refs):
        if matched >= max_columns:
            break
        cinfo = profile_lower.get(ref)
        if not cinfo:
            continue
        matched += 1
        card = cinfo.get("cardinality", "?")
        parts = [f"cardinality={card}"]
        vals = cinfo.get("distinct_values")
        if vals:
            parts.append(f"values={vals}")
        minv = cinfo.get("min")
        maxv = cinfo.get("max")
        if minv is not None:
            parts.append(f"range=[{minv}, {maxv}]")
        lines.append(f"- {ref}: {', '.join(parts)}")

    return "\n".join(lines) if lines else "(no matching column profiles found)"


def _derive_blame_from_sql(cluster: dict) -> list[str] | None:
    """Derive blamed table/column references from SQL diffs when ASI blame_set is empty."""
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return None
    blamed: set[str] = set()
    for ctx in sql_contexts[:3]:
        exp_tables = _extract_sql_tables(ctx.get("expected_sql", ""))
        gen_tables = _extract_sql_tables(ctx.get("generated_sql", ""))
        blamed.update(exp_tables | gen_tables)
    return sorted(blamed)[:5] if blamed else None


def _format_existing_example_sqls(metadata_snapshot: dict) -> str:
    """Format existing example_question_sqls for inclusion in the Lever 6 prompt."""
    example_sqls = metadata_snapshot.get("example_question_sqls", [])
    if not example_sqls:
        return "(none)"
    lines: list[str] = []
    for ex in example_sqls[:20]:
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = sql[0] if sql else ""
        if not q:
            continue
        entry = f"- Q: {q}\n  SQL: {sql[:200]}"
        params = ex.get("parameters", [])
        if params:
            param_strs = [
                f"{p.get('name', '?')}:{p.get('type_hint', 'STRING')}"
                for p in params if isinstance(p, dict)
            ]
            entry += f"\n  Params: {', '.join(param_strs)}"
        guidance = ex.get("usage_guidance", [])
        if guidance:
            g = guidance[0] if isinstance(guidance, list) else str(guidance)
            entry += f"\n  Guidance: {g[:150]}"
        lines.append(entry)
    return "\n".join(lines) if lines else "(none)"


def _format_eval_summary(clusters: list[dict]) -> str:
    """Produce a compact summary of all evaluation clusters for the holistic prompt."""
    if not clusters:
        return "No failure clusters from evaluation (all questions passed)."

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]
    total_questions = sum(len(c.get("question_ids", [])) for c in clusters)
    root_causes: dict[str, int] = {}
    judges: dict[str, int] = {}
    for c in clusters:
        rc = c.get("root_cause", "unknown")
        root_causes[rc] = root_causes.get(rc, 0) + len(c.get("question_ids", []))
        judge = c.get("affected_judge", "unknown")
        judges[judge] = judges.get(judge, 0) + len(c.get("question_ids", []))

    lines = [
        f"Total clusters: {len(clusters)} (hard failures: {len(hard)}, soft signals: {len(soft)})",
        f"Total affected questions: {total_questions}",
        "",
        "Failures by root cause:",
    ]
    for rc, count in sorted(root_causes.items(), key=lambda x: -x[1]):
        lines.append(f"  - {rc}: {count} questions")
    lines.append("")
    lines.append("Failures by judge:")
    for judge, count in sorted(judges.items(), key=lambda x: -x[1]):
        lines.append(f"  - {judge}: {count} questions")
    return "\n".join(lines)


def _format_lever_summary(lever_changes: list[dict] | None) -> str:
    """Format what levers 1-4 did for the holistic lever 5 prompt."""
    if not lever_changes:
        return "(No changes applied by earlier levers in this iteration.)"

    lines: list[str] = []
    for lc in lever_changes:
        lever_name = lc.get("lever_name", f"Lever {lc.get('lever', '?')}")
        delta = lc.get("accuracy_delta", 0)
        patches = lc.get("patches", [])
        delta_str = f"+{delta:.1f}%" if delta >= 0 else f"{delta:.1f}%"
        lines.append(f"### {lever_name} (accuracy change: {delta_str})")
        for p in patches[:10]:
            change = p.get("change", "")
            ptype = p.get("patch_type", "")
            lines.append(f"  - [{ptype}] {change}")
        if len(patches) > 10:
            lines.append(f"  ... and {len(patches) - 10} more patches")
        lines.append("")
    return "\n".join(lines) if lines else "(No changes applied.)"


def _normalize_blame(raw: Any) -> list[str]:
    """Normalize a blame_set value to a flat list of strings."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw:
        return [b.strip() for b in raw.split("|") if b.strip()]
    return []


def _format_cluster_briefs(
    clusters: list[dict],
    top_n: int = 5,
    max_sql_chars: int = 0,
) -> str:
    """Format cluster data for the holistic prompt.

    Hard-failure clusters (top N) get SQL diffs; remaining get one-line
    summaries.  Soft-signal clusters are rendered under a separate header.

    When *max_sql_chars* > 0 each SQL block in the diff is truncated to that
    length — useful for keeping the strategist prompt within budget.
    """
    if not clusters:
        return "(No failure clusters.)"

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]

    sorted_hard = sorted(hard, key=lambda c: len(c.get("question_ids", [])), reverse=True)
    lines: list[str] = []

    for idx, cluster in enumerate(sorted_hard[:top_n], 1):
        rc = cluster.get("root_cause", "unknown")
        q_ids = cluster.get("question_ids", [])
        judge = cluster.get("affected_judge", "unknown")
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        cid = cluster.get("cluster_id", f"C{idx:03d}")
        lines.append(f"### {cid}: {rc} ({len(q_ids)} questions, judge: {judge})")
        if blame:
            lines.append(f"Blamed objects: {', '.join(blame[:5])}")
        lines.append(_format_sql_diffs(cluster, max_sql_chars=max_sql_chars))
        lines.append("")

    remaining_hard = sorted_hard[top_n:]
    if remaining_hard:
        lines.append(f"### Additional hard-failure clusters ({len(remaining_hard)} more):")
        for cluster in remaining_hard:
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = _normalize_blame(cluster.get("asi_blame_set"))
            blame_str = f" blamed=[{', '.join(blame[:3])}]" if blame else ""
            lines.append(
                f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
            )

    if soft:
        sorted_soft = sorted(soft, key=lambda c: len(c.get("question_ids", [])), reverse=True)
        lines.append("")
        lines.append("### Correct-but-Suboptimal Patterns (arbiter: correct, individual judges: failed)")
        lines.append("These queries returned correct results but used suboptimal approaches.")
        lines.append("Use these to inform best-practice guidance, NOT to fix failures.")
        lines.append("")
        for idx, cluster in enumerate(sorted_soft[:top_n], 1):
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = _normalize_blame(cluster.get("asi_blame_set"))
            cid = cluster.get("cluster_id", f"S{idx:03d}")
            lines.append(f"#### {cid}: {rc} ({len(q_ids)} questions, judge: {judge})")
            if blame:
                lines.append(f"Blamed objects: {', '.join(blame[:5])}")
            lines.append(_format_sql_diffs(cluster, max_sql_chars=max_sql_chars))
            lines.append("")
        remaining_soft = sorted_soft[top_n:]
        if remaining_soft:
            lines.append(f"#### Additional soft-signal clusters ({len(remaining_soft)} more):")
            for cluster in remaining_soft:
                rc = cluster.get("root_cause", "unknown")
                q_ids = cluster.get("question_ids", [])
                judge = cluster.get("affected_judge", "unknown")
                blame = _normalize_blame(cluster.get("asi_blame_set"))
                blame_str = f" blamed=[{', '.join(blame[:3])}]" if blame else ""
                lines.append(
                    f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
                )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Structured-JSON context builders (Phase 7)
# ---------------------------------------------------------------------------

def _build_cluster_data(clusters: list[dict], *, top_n: int = 5) -> list[dict]:
    """Convert failure clusters into structured dicts for JSON context."""
    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    sorted_hard = sorted(hard, key=lambda c: len(c.get("question_ids", [])), reverse=True)
    result: list[dict] = []
    for cluster in sorted_hard[:top_n]:
        questions: list[dict] = []
        for ctx in cluster.get("sql_contexts", [])[:3]:
            comp = ctx.get("comparison", {}) or {}
            q_entry: dict[str, Any] = {
                "question": ctx.get("question", ""),
                "expected_sql": ctx.get("expected_sql", ""),
                "generated_sql": ctx.get("generated_sql", ""),
            }
            if isinstance(comp, dict):
                if comp.get("error"):
                    q_entry["error"] = comp["error"]
                elif not comp.get("match"):
                    q_entry["mismatch_type"] = comp.get("match_type", "unknown")
                gt_sample = comp.get("gt_sample")
                if gt_sample:
                    q_entry["expected_sample"] = str(gt_sample)[:500]
                genie_sample = comp.get("genie_sample")
                if genie_sample:
                    q_entry["genie_sample"] = str(genie_sample)[:500]
            questions.append(q_entry)
        entry: dict[str, Any] = {
            "cluster_id": cluster.get("cluster_id", "?"),
            "root_cause": cluster.get("root_cause", "unknown"),
            "judge": cluster.get("affected_judge", "unknown"),
            "question_count": len(cluster.get("question_ids", [])),
            "blamed_objects": _normalize_blame(cluster.get("asi_blame_set")),
            "questions": questions,
        }
        cf = cluster.get("asi_counterfactual_fixes", [])
        if cf:
            entry["suggested_fixes"] = [str(f)[:200] for f in cf[:5]]
        result.append(entry)
    if len(sorted_hard) > top_n:
        for cluster in sorted_hard[top_n:]:
            result.append({
                "cluster_id": cluster.get("cluster_id", "?"),
                "root_cause": cluster.get("root_cause", "unknown"),
                "judge": cluster.get("affected_judge", "unknown"),
                "question_count": len(cluster.get("question_ids", [])),
                "blamed_objects": _normalize_blame(cluster.get("asi_blame_set")),
                "summary_only": True,
            })
    return result


def _build_soft_signal_data(soft_clusters: list[dict]) -> list[dict]:
    """Convert soft-signal clusters into structured dicts."""
    if not soft_clusters:
        return []
    _info_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    filtered: list[dict] = []
    for sc in soft_clusters:
        judges_in = {
            fj.get("judge", "")
            for qt in sc.get("question_traces", [])
            for fj in qt.get("failed_judges", [])
        }
        if judges_in and judges_in <= _info_judges:
            continue
        filtered.append(sc)
    result: list[dict] = []
    for sc in filtered[:10]:
        entry: dict[str, Any] = {
            "cluster_id": sc.get("cluster_id", "?"),
            "root_cause": sc.get("root_cause", "unknown"),
            "question_count": len(sc.get("question_ids", [])),
        }
        traces: list[dict] = []
        for qt in sc.get("question_traces", [])[:2]:
            t: dict[str, Any] = {"question": qt.get("question_text", "")[:120]}
            judges_detail = []
            for fj in qt.get("failed_judges", []):
                judges_detail.append({
                    "judge": fj.get("judge", "?"),
                    "root_cause": fj.get("resolved_root_cause", "?"),
                    "rationale": fj.get("rationale_snippet", "")[:150],
                })
            if judges_detail:
                t["failed_judges"] = judges_detail
            traces.append(t)
        if traces:
            entry["traces"] = traces
        result.append(entry)
    return result


def _build_schema_data(
    metadata_snapshot: dict,
    filter_tables: set[str] | None = None,
) -> list[dict]:
    """Build schema context as structured list of table dicts."""
    from genie_space_optimizer.optimization.structured_metadata import (
        deduplicate_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    result: list[dict] = []
    for tbl in list(tables) + list(mvs):
        identifier = tbl.get("identifier", "")
        if filter_tables is not None:
            tbl_id = (identifier or "").lower()
            if tbl_id not in filter_tables:
                continue
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = "\n".join(tbl_desc)
        tbl_desc = deduplicate_structured_description(tbl_desc)
        columns: list[dict] = []
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            if isinstance(desc, list):
                desc = "\n".join(desc) if desc else ""
            desc = deduplicate_structured_description(desc) if desc else ""
            uc_comment = cc.get("uc_comment", "")
            if not desc and uc_comment:
                desc = uc_comment
            col_entry: dict[str, Any] = {"name": col_name}
            if data_type:
                col_entry["type"] = data_type
            if desc:
                col_entry["description"] = desc
            syns = cc.get("synonyms", [])
            if syns:
                col_entry["synonyms"] = syns
            columns.append(col_entry)
        entry: dict[str, Any] = {"table": identifier}
        if tbl_desc:
            entry["description"] = tbl_desc
        if columns:
            entry["columns"] = columns
        result.append(entry)
    return result


def _build_structured_table_data(
    metadata_snapshot: dict,
    blame_set: list[str] | None,
) -> list[dict]:
    """Build structured table metadata as dicts with editability info."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        for b in blame_set:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(1, set())
    result: list[dict] = []

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        etype = "mv_table" if is_mv else "table"
        template_sections = ENTITY_TYPE_TEMPLATES.get(etype, [])
        desc = tbl.get("description", [])
        desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
        sections = parse_structured_description(desc_text)

        tbl_entry: dict[str, Any] = {
            "table": identifier,
            "entity_type": etype,
            "sections": {},
        }
        for sk in template_sections:
            value = sections.get(sk, "").strip()
            editable = sk in owned_sections
            tbl_entry["sections"][SECTION_LABELS[sk]] = {
                "value": value or None,
                "editable": editable,
            }
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            tbl_entry["legacy_text"] = preamble
        result.append(tbl_entry)

    return result


def _build_structured_column_data(
    metadata_snapshot: dict,
    blame_set: list[str] | None,
) -> list[dict]:
    """Build structured column metadata as dicts with editability info."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        classify_column,
        entity_type_for_column,
        extract_synonyms_section,
        format_synonyms_section,
        merge_synonyms,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])
    mvs = ds.get("metric_views", []) or []
    mv_identifiers = {
        (m.get("identifier") or "").rsplit(".", 1)[-1].lower()
        for m in mvs
    }

    blame_lower: set[str] = set()
    if blame_set:
        for b in blame_set:
            bl = b.lower().strip()
            blame_lower.add(bl)
            if "." in bl:
                blame_lower.add(bl.rsplit(".", 1)[-1])

    owned_sections = LEVER_SECTION_OWNERSHIP.get(1, set()) | LEVER_SECTION_OWNERSHIP.get(2, set())
    result: list[dict] = []
    columns_shown = 0
    max_columns = 40

    for tbl in tables:
        identifier = tbl.get("identifier", "")
        short_name = identifier.rsplit(".", 1)[-1].lower() if identifier else ""
        is_mv = short_name in mv_identifiers

        if blame_lower:
            tbl_match = short_name in blame_lower or identifier.lower() in blame_lower
            col_match = any(
                (cc.get("column_name") or "").lower() in blame_lower
                for cc in tbl.get("column_configs", [])
            )
            if not tbl_match and not col_match:
                continue

        tbl_columns: list[dict] = []
        for cc in tbl.get("column_configs", []):
            if columns_shown >= max_columns:
                break
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            syns = cc.get("synonyms", [])
            uc_comment = cc.get("uc_comment", "")
            desc_text = "\n".join(desc) if isinstance(desc, list) else str(desc or "")
            if not desc_text and uc_comment:
                desc_text = uc_comment
            sections = parse_structured_description(desc_text)
            if syns:
                existing_syn = extract_synonyms_section(sections)
                all_syns = merge_synonyms(existing_syn, syns)
                sections["synonyms"] = format_synonyms_section(all_syns)
            etype = entity_type_for_column(col_name, data_type, is_in_metric_view=is_mv)
            kind = classify_column(col_name, data_type, is_in_metric_view=is_mv)
            template_secs = ENTITY_TYPE_TEMPLATES.get(etype, [])
            col_entry: dict[str, Any] = {
                "name": col_name,
                "type": data_type or "unknown",
                "classification": kind,
                "sections": {},
            }
            for sk in template_secs:
                value = sections.get(sk, "").strip()
                editable = sk in owned_sections
                col_entry["sections"][SECTION_LABELS[sk]] = {
                    "value": value or None,
                    "editable": editable,
                }
            preamble = sections.get("_preamble", "").strip()
            if preamble:
                col_entry["legacy_text"] = preamble
            _profile = metadata_snapshot.get("_data_profile", {})
            _tbl_profile = _profile.get(identifier, {}) or _profile.get(identifier.lower(), {})
            _col_profile = _tbl_profile.get("columns", {}).get(col_name, {})
            if _col_profile.get("distinct_values"):
                col_entry["data_values"] = _col_profile["distinct_values"]
            elif _col_profile.get("min") is not None:
                col_entry["data_range"] = [_col_profile["min"], _col_profile["max"]]
            tbl_columns.append(col_entry)
            columns_shown += 1

        if tbl_columns:
            result.append({"table": identifier, "columns": tbl_columns})
        if columns_shown >= max_columns:
            break

    return result


def _build_structured_function_data(metadata_snapshot: dict) -> list[dict]:
    """Build structured function metadata as dicts."""
    from genie_space_optimizer.optimization.structured_metadata import (
        ENTITY_TYPE_TEMPLATES,
        LEVER_SECTION_OWNERSHIP,
        SECTION_LABELS,
        parse_structured_description,
    )

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    if not funcs:
        return []

    owned_sections = LEVER_SECTION_OWNERSHIP.get(3, set())
    template_sections = ENTITY_TYPE_TEMPLATES.get("function", [])
    result: list[dict] = []

    for fn in funcs:
        name = fn.get("name") or fn.get("identifier", "")
        comment = fn.get("comment") or fn.get("description") or ""
        if isinstance(comment, list):
            comment = "\n".join(comment)
        sections = parse_structured_description(comment)
        fn_entry: dict[str, Any] = {"name": name, "sections": {}}
        for sk in template_sections:
            value = sections.get(sk, "").strip()
            editable = sk in owned_sections
            fn_entry["sections"][SECTION_LABELS[sk]] = {
                "value": value or None,
                "editable": editable,
            }
        preamble = sections.get("_preamble", "").strip()
        if preamble:
            fn_entry["legacy_text"] = preamble
        result.append(fn_entry)
    return result


def _build_join_specs_data(metadata_snapshot: dict) -> list[dict]:
    """Build join specifications as structured dicts with clean relationship types."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    inst = metadata_snapshot.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    specs = (
        metadata_snapshot.get("join_specs", [])
        or inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )
    if not specs:
        return []
    result: list[dict] = []
    for js in specs:
        left = js.get("left", {})
        right = js.get("right", {})
        sql_raw = js.get("sql", "")

        condition_text = ""
        relationship = None
        if isinstance(sql_raw, list):
            conditions = [s for s in sql_raw if not str(s).startswith("--rt=")]
            rt_parts = [s for s in sql_raw if str(s).startswith("--rt=")]
            condition_text = " AND ".join(str(c) for c in conditions)
            if rt_parts:
                rt_str = str(rt_parts[0]).strip("-").removeprefix("rt=")
                relationship = rt_str.replace("FROM_RELATIONSHIP_TYPE_", "").replace("_", " ").lower()
        else:
            condition_text = str(sql_raw)[:200]

        entry: dict[str, Any] = {
            "left_table": left.get("identifier", "?"),
            "right_table": right.get("identifier", "?"),
            "condition": condition_text[:200],
        }
        if relationship:
            entry["relationship"] = relationship
        result.append(entry)
    return result


def _build_example_sqls_data(metadata_snapshot: dict) -> list[dict]:
    """Build existing example SQL as structured dicts."""
    example_sqls = metadata_snapshot.get("example_question_sqls", [])
    if not example_sqls:
        return []
    result: list[dict] = []
    for ex in example_sqls[:20]:
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = sql[0] if sql else ""
        if not q:
            continue
        entry: dict[str, Any] = {"question": q, "sql": sql[:200]}
        params = ex.get("parameters", [])
        if params:
            entry["parameters"] = [
                {"name": p.get("name", "?"), "type": p.get("type_hint", "STRING")}
                for p in params if isinstance(p, dict)
            ]
        guidance = ex.get("usage_guidance", [])
        if guidance:
            g = guidance[0] if isinstance(guidance, list) else str(guidance)
            entry["guidance"] = g[:150]
        result.append(entry)
    return result


def _build_blamed_values_data(
    clusters: list[dict],
    data_profile: dict,
    *,
    max_columns: int = 20,
) -> dict[str, dict]:
    """Build blamed column value profiles as structured dict."""
    if not data_profile:
        return {}

    blamed_refs: set[str] = set()
    for cluster in clusters:
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        for b in blame:
            blamed_refs.add(b.strip().lower())

    if not blamed_refs:
        return {}

    profile_lower: dict[str, dict] = {}
    for table_fqn, tinfo in data_profile.items():
        tkey = table_fqn.lower()
        short_name = tkey.split(".")[-1] if "." in tkey else tkey
        for col_name, cinfo in tinfo.get("columns", {}).items():
            col_lower = col_name.lower()
            profile_lower[f"{tkey}.{col_lower}"] = cinfo
            profile_lower[f"{short_name}.{col_lower}"] = cinfo
            profile_lower[col_lower] = cinfo

    result: dict[str, dict] = {}
    matched = 0
    for ref in sorted(blamed_refs):
        if matched >= max_columns:
            break
        cinfo = profile_lower.get(ref)
        if not cinfo:
            continue
        matched += 1
        entry: dict[str, Any] = {}
        card = cinfo.get("cardinality")
        if card is not None:
            entry["cardinality"] = card
        vals = cinfo.get("distinct_values")
        if vals:
            entry["values"] = vals
        minv = cinfo.get("min")
        maxv = cinfo.get("max")
        if minv is not None:
            entry["range"] = [minv, maxv]
        result[ref] = entry

    return result


def _extract_tables_from_clusters(clusters: list[dict]) -> set[str] | None:
    """Extract relevant table identifiers from SQL diffs and blame sets.

    Returns a lowercased set of fully-qualified or short table names, or
    ``None`` if nothing could be extracted (which disables filtering).
    """
    tables: set[str] = set()
    for cluster in clusters:
        blame = _normalize_blame(cluster.get("asi_blame_set"))
        for b in blame:
            bl = b.lower().strip()
            if ":table" in bl:
                tables.add(bl.split(":")[0])
            else:
                tables.add(bl)
        for ctx in cluster.get("sql_contexts", [])[:3]:
            for key in ("expected_sql", "generated_sql"):
                for m in re.finditer(r'\b(\w+\.\w+\.\w+)\b', ctx.get(key, "")):
                    tables.add(m.group(1).lower())
    return tables if tables else None


def _truncate_context_to_budget(context: dict, budget_tokens: int) -> dict:
    """Truncate context dict to fit within token budget.

    Removes entries from lowest-priority sections first:
    1. Remove soft_signal_clusters entries
    2. Remove non-blamed schema table entries
    3. Trim column lists per table in schema
    4. Trim example_sqls
    5. Trim blamed_column_values
    Never touches failure_clusters or priority_analysis.
    """
    import copy as _copy

    def _estimate(d: dict) -> int:
        return _estimate_tokens(json.dumps(d, default=str))

    current = _estimate(context)
    if current <= budget_tokens:
        return context

    result = _copy.deepcopy(context)

    if current > budget_tokens and result.get("soft_signal_clusters"):
        result["soft_signal_clusters"] = result["soft_signal_clusters"][:3]
        current = _estimate(result)

    if current > budget_tokens and result.get("soft_signal_clusters"):
        result["soft_signal_clusters"] = []
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        half = max(3, len(result["schema"]) // 2)
        result["schema"] = result["schema"][:half]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        for tbl in result["schema"]:
            if isinstance(tbl.get("columns"), list) and len(tbl["columns"]) > 10:
                tbl["columns"] = tbl["columns"][:10]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("existing_example_sqls"), list):
        result["existing_example_sqls"] = result["existing_example_sqls"][:5]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("blamed_column_values"), dict):
        items = list(result["blamed_column_values"].items())
        result["blamed_column_values"] = dict(items[:10])
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("structured_metadata"), dict):
        sm = result["structured_metadata"]
        if isinstance(sm.get("functions"), list):
            sm["functions"] = sm["functions"][:3]
        current = _estimate(result)

    if current > budget_tokens and isinstance(result.get("schema"), list):
        result["schema"] = result["schema"][:3]
        current = _estimate(result)

    return result


def _build_context_data(
    *,
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    reflection_buffer: list[dict],
    priority_ranking: list[dict],
    blame_set: list[str] | None,
    success_summary: str,
    reflection_text: str,
    persistence_text: str,
    proven_patterns_text: str,
    suggestions_text: str,
) -> dict:
    """Assemble all context sections as a single Python dict for JSON serialization."""
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    relevant_tables = _extract_tables_from_clusters(clusters + soft_signal_clusters)

    return {
        "progress_summary": success_summary,
        "priority_analysis": [
            {
                "rank": c.get("rank", "?"),
                "cluster_id": c.get("cluster_id", "?"),
                "root_cause": c.get("root_cause", "?"),
                "judge": c.get("affected_judge", "?"),
                "questions": len(c.get("question_ids", [])),
                "impact": c.get("impact_score", 0),
            }
            for c in priority_ranking[:10]
        ],
        "reflection_history": reflection_text,
        "proven_patterns": proven_patterns_text,
        "persistent_failures": persistence_text,
        "human_suggestions": suggestions_text or None,
        "schema": _build_schema_data(metadata_snapshot, filter_tables=relevant_tables),
        "failure_clusters": _build_cluster_data(clusters),
        "soft_signal_clusters": _build_soft_signal_data(soft_signal_clusters),
        "structured_metadata": {
            "tables": _build_structured_table_data(metadata_snapshot, blame_set),
            "columns": _build_structured_column_data(metadata_snapshot, blame_set),
            "functions": _build_structured_function_data(metadata_snapshot),
        },
        "join_specifications": _build_join_specs_data(metadata_snapshot),
        "current_instructions": _get_general_instructions(metadata_snapshot) or None,
        "existing_example_sqls": _build_example_sqls_data(metadata_snapshot),
        "blamed_column_values": _build_blamed_values_data(
            clusters, metadata_snapshot.get("_data_profile", {}),
        ),
    }


_SQL_PATTERN_ROOT_CAUSES = frozenset({
    "wrong_table", "wrong_join", "missing_filter", "missing_aggregation",
    "wrong_aggregation", "wrong_measure", "select_star", "tvf_parameter_error",
})


def _resolve_lever5_llm_result(
    llm_result: dict, original_patch_type: str, cluster: dict | None = None,
) -> tuple[str, dict]:
    """Interpret the instruction_type returned by the Lever 6 LLM and resolve
    the actual patch_type and extra fields to merge into the proposal.

    When the cluster's root_cause is a clear SQL-pattern issue (e.g.
    ``wrong_join``, ``missing_filter``) and the cluster has SQL context with
    a valid expected SQL, force ``add_example_sql`` to provide a concrete
    pattern rather than a verbose text instruction.

    Returns ``(resolved_patch_type, extra_fields)``.
    """
    instruction_type = llm_result.get("instruction_type", "text_instruction")

    if instruction_type == "example_sql":
        return "add_example_sql", {
            "example_question": llm_result.get("example_question", ""),
            "example_sql": llm_result.get("example_sql", ""),
            "parameters": llm_result.get("parameters", []),
            "usage_guidance": llm_result.get("usage_guidance", ""),
        }

    if instruction_type == "sql_expression":
        logger.info(
            "Lever 6 LLM recommended sql_expression (handled by levers 1-5): "
            "table=%s column=%s — falling back to example_sql",
            llm_result.get("target_table", ""),
            llm_result.get("target_column", ""),
        )
        return original_patch_type, {}

    if cluster and cluster.get("root_cause") in _SQL_PATTERN_ROOT_CAUSES:
        sql_ctxs = cluster.get("sql_contexts", [])
        representative = next(
            (sc for sc in sql_ctxs if sc.get("expected_sql") and sc.get("question")),
            None,
        )
        if representative:
            logger.info(
                "Forcing add_example_sql for SQL-pattern root cause '%s' "
                "(LLM returned text_instruction)",
                cluster["root_cause"],
            )
            return "add_example_sql", {
                "example_question": representative["question"],
                "example_sql": representative["expected_sql"],
                "parameters": [],
                "usage_guidance": llm_result.get("rationale", ""),
                "forced_from_sql_pattern": True,
            }

    if original_patch_type == "add_example_sql":
        logger.warning(
            "Lever 6 LLM returned text_instruction for a routing failure "
            "(original_patch_type=add_example_sql). Example SQL is preferred "
            "for routing issues. Keeping text_instruction but marking as downgraded."
        )

    raw_text = llm_result.get("instruction_text", "")
    return "add_instruction", {
        "new_text": _sanitize_plaintext_instructions(raw_text) if raw_text else "",
        "downgraded_from_example_sql": original_patch_type == "add_example_sql",
    }


def _call_llm_for_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    patch_type: str,
    lever: int,
) -> dict:
    """Call Databricks Claude Opus 4.6 to generate proposal text.

    Returns ``{"proposed_value": str, "rationale": str}``.
    For lever 5 the response may also contain ``instruction_type``,
    ``example_question``, ``example_sql``, ``target_table``, etc.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    prompt_map = {
        1: LEVER_1_2_COLUMN_PROMPT,
        2: LEVER_1_2_COLUMN_PROMPT,
        4: LEVER_4_JOIN_SPEC_PROMPT,
        5: LEVER_5_INSTRUCTION_PROMPT,
    }
    prompt_template = prompt_map.get(lever, PROPOSAL_GENERATION_PROMPT)

    current_dict_count = sum(
        1
        for t in metadata_snapshot.get("tables", [])
        for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )

    sql_diffs = _format_sql_diffs(cluster)
    blame = cluster.get("asi_blame_set")
    if not blame:
        blame = _derive_blame_from_sql(cluster)

    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst_lookup = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst_lookup, dict):
        _inst_lookup = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    _join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst_lookup.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "failure_type": cluster.get("asi_failure_type", cluster.get("root_cause", "")),
        "blame_set": blame or "",
        "affected_questions": cluster.get("question_ids", []),
        "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
        "severity": "major",
        "current_metadata": _extract_metadata_for_blame(
            metadata_snapshot, blame
        ),
        "patch_type_description": _describe_patch_type(patch_type),
        "failures_context": json.dumps(cluster, default=str),
        "sql_diffs": sql_diffs,
        "current_join_specs": json.dumps(_join_specs, default=str),
        "table_relationships": json.dumps(
            [t.get("relationships", []) for t in _tables],
            default=str,
        ),
        "current_column_configs": json.dumps(
            metadata_snapshot.get("column_configs", {}), default=str
        ),
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "string_column_count": metadata_snapshot.get("string_column_count", 0),
        "max_value_dictionary_cols": MAX_VALUE_DICTIONARY_COLUMNS,
        "current_dictionary_count": current_dict_count,
        "current_instructions": _get_general_instructions(metadata_snapshot),
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(
            0,
            24500 - len(_get_general_instructions(metadata_snapshot)),
        ),
        "table_names": [
            t.get("name") or t.get("identifier", "")
            for t in _tables
        ],
        "mv_names": [
            m.get("name") or m.get("identifier", "")
            for m in _mvs
        ],
        "tvf_names": [
            f.get("name") or f.get("identifier", "")
            for f in _funcs
        ],
    }

    if lever in (1, 2):
        format_kwargs["structured_column_context"] = _format_structured_column_context(
            metadata_snapshot, blame, lever,
        )
        format_kwargs["structured_table_context"] = _format_structured_table_context(
            metadata_snapshot, blame, lever,
        )
        format_kwargs["full_schema_context"] = "(See structured table/column metadata above for relevant schema.)"
        format_kwargs.pop("current_column_configs", None)

    format_kwargs = _truncate_to_budget(
        format_kwargs, prompt_template,
        priority_keys=["full_schema_context", "current_column_configs", "table_relationships", "failures_context", "sql_diffs"],
    )

    prompt = format_mlflow_template(prompt_template, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _tmpl_name = {1: "LEVER_1_2_COLUMN", 2: "LEVER_1_2_COLUMN", 4: "LEVER_4_JOIN_SPEC", 5: "LEVER_5_INSTRUCTION"}.get(lever, "PROPOSAL_GENERATION")
    _link_prompt_to_trace(_tmpl_name.lower())
    _W = 78
    _hdr = f"┌─── LLM Call [{_tmpl_name}] " + "─" * max(0, _W - 18 - len(_tmpl_name))
    _ftr = "└" + "─" * (_W - 1)

    _cid = cluster.get("cluster_id", "?")
    _root = cluster.get("root_cause", "?")
    _q_traces = cluster.get("question_traces", [])
    _ctxts = cluster.get("sql_contexts", [])

    _judge_lines = []
    for qt in _q_traces[:5]:
        for jt in qt.get("failed_judges", []):
            snip = (jt.get("rationale_snippet") or "")[:120].replace("\n", " ")
            _judge_lines.append(f"│   {qt['question_id'][:40]} / {jt['judge']}: \"{snip}\"")

    _sql_diff_lines = []
    if _ctxts:
        ctx = _ctxts[0]
        exp_snip = (ctx.get("expected_sql") or "")[:200].replace("\n", " ")
        gen_snip = (ctx.get("generated_sql") or "")[:200].replace("\n", " ")
        if exp_snip or gen_snip:
            _sql_diff_lines.append(f"│   Expected:  {exp_snip}")
            _sql_diff_lines.append(f"│   Generated: {gen_snip}")

    _cfix_lines = []
    for cf in cluster.get("asi_counterfactual_fixes", [])[:3]:
        _cfix_lines.append(f"│   \"{str(cf)[:120]}\"")

    _extra = (
        f"│ {'Cluster:':<24s} {_cid}\n"
        f"│ {'Root cause:':<24s} {_root}\n"
    )
    if _judge_lines:
        _extra += "│\n│ --- Judge Feedback Driving This Patch ---\n" + "\n".join(_judge_lines) + "\n"
    if _sql_diff_lines:
        _extra += "│\n│ --- SQL Diff (sample) ---\n" + "\n".join(_sql_diff_lines) + "\n"
    if _cfix_lines:
        _extra += "│\n│ --- Counterfactual Fixes ---\n" + "\n".join(_cfix_lines) + "\n"

    print(
        f"\n{_hdr}\n"
        f"│ {'Patch type:':<24s} {patch_type}\n"
        f"│ {'Failure type:':<24s} {format_kwargs.get('failure_type', '?')}\n"
        f"│ {'Blame set:':<24s} {format_kwargs.get('blame_set', '?')}\n"
        f"│ {'Questions:':<24s} {len(format_kwargs.get('affected_questions', []))}\n"
        f"{_extra}"
        f"│ {'Prompt length:':<24s} {len(prompt):,} chars\n│"
    )

    import time

    from genie_space_optimizer.optimization.evaluation import _extract_json

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            _wc = _ws_with_timeout(None)
            response = _wc.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content=(
                            "You are a JSON-only responder. Your ENTIRE response must be a single "
                            "valid JSON object. Do not include any analysis, markdown, commentary, "
                            "or explanation outside the JSON. Start your response with '{' and end "
                            "with '}'."
                        ),
                    ),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt),
                ],
                temperature=LLM_TEMPERATURE,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError("LLM response had no choices")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content:
                raise ValueError("LLM response content is empty")
            text = str(content).strip()
            parsed = _extract_json(text)
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} OK -- parsed JSON\n"
                f"│ {'Proposed value:':<24s} {str(parsed.get('proposed_value', ''))[:300]}\n"
                f"│ {'Rationale:':<24s} {str(parsed.get('rationale', ''))[:300]}\n"
                f"│ {'Result:':<24s} OK\n"
                f"{_ftr}"
            )
            return parsed
        except json.JSONDecodeError:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON (retrying...)")
                time.sleep(2**attempt)
                continue
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON -- retries exhausted\n"
                f"│ Raw text (500 chars): {text[:500]}\n"
                f"│\n"
                f"│ {'Result:':<24s} FALLBACK (raw text kept as proposed_value)\n"
                f"{_ftr}"
            )
            return {"proposed_value": text, "rationale": "LLM response was not valid JSON after all retries"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error (retrying...)")
                time.sleep(2**attempt)
            else:
                logger.exception("LLM [%s] call failed after %d retries", _tmpl_name, LLM_MAX_RETRIES)
                print(
                    f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error -- retries exhausted\n"
                    f"│ {'Result:':<24s} FAILED\n"
                    f"{_ftr}"
                )
                return {
                    "proposed_value": "",
                    "rationale": "LLM call failed",
                }
    return {
        "proposed_value": "",
        "rationale": "LLM call failed",
    }


def _format_discovery_hints(
    hints: list[dict],
    join_overlaps: list[dict] | None = None,
) -> str:
    """Format discovery hints into a human-readable string for the LLM prompt.

    When *join_overlaps* are available (from preflight data profiling), each
    hint is annotated with the FK overlap ratio for the relevant table pair.
    """
    if not hints:
        return "(no heuristic hints)"

    overlap_index: dict[tuple[str, str], dict] = {}
    for ov in (join_overlaps or []):
        key = (ov.get("left_table", "").lower(), ov.get("right_table", "").lower())
        overlap_index[key] = ov
        overlap_index[(key[1], key[0])] = ov

    lines: list[str] = []
    for idx, h in enumerate(hints, 1):
        lt = h.get("left_table", "")
        rt = h.get("right_table", "")
        compat = h.get("type_compatible", True)
        lines.append(f"### Hint {idx}: {lt} ↔ {rt}")
        if not compat:
            lines.append("  ⚠️  Some candidate columns have mismatched types")

        ov = overlap_index.get((lt.lower(), rt.lower()))
        if ov:
            ratio = ov.get("overlap_ratio", 0)
            fk_col = ov.get("fk_column", "?")
            pk_col = ov.get("pk_column", "?")
            pct = f"{ratio * 100:.0f}%"
            lines.append(
                f"  📊 FK overlap: {fk_col} → {pk_col} = {pct} "
                f"({ov.get('left_distinct', '?')} distinct FK values)"
            )

        for cc in h.get("candidate_columns", []):
            lines.append(
                f"  - {cc.get('left_col', '?')} ↔ {cc.get('right_col', '?')} "
                f"({cc.get('reason', 'unknown')})"
            )
    return "\n".join(lines)


_JOIN_PROSE_SPLIT_RE = re.compile(
    r",\s*(?:MANY_TO_|ONE_TO_|Use this|This join|Always|Note:)",
    re.IGNORECASE,
)
_SENTENCE_BOUNDARY_RE = re.compile(r"\.\s+[A-Z]")


_EQUIJOIN_PREDICATE_RE = re.compile(
    r"`?[\w.]+`?\s*\.\s*`?\w+`?\s*=\s*`?[\w.]+`?\s*\.\s*`?\w+`?",
)


def _extract_equijoin_predicates(sql_str: str) -> str:
    """Keep only ``table.col = table.col`` predicates from a join SQL string.

    The Genie API ``join_specs[].sql`` field only accepts equijoin predicates
    (column equality between two tables).  LLMs and execution-proven SQL
    sometimes include filter predicates such as
    ``AND dim_property.is_current = true`` which the API rejects.

    This function extracts all ``a.x = b.y`` predicates and returns them
    joined by `` AND ``, discarding everything else.
    """
    predicates = _EQUIJOIN_PREDICATE_RE.findall(sql_str)
    return " AND ".join(predicates)


def _sanitize_join_sql(sql_str: str) -> str:
    """Strip prose, cardinality labels, and non-equijoin predicates.

    LLMs sometimes embed descriptive text like ``MANY_TO_ONE. Use this join
    to connect...`` after the actual ON-clause expression.  This function
    first strips prose, then extracts only equijoin predicates that the
    Genie API accepts.
    """
    cleaned = _JOIN_PROSE_SPLIT_RE.split(sql_str, maxsplit=1)[0]
    cleaned = _SENTENCE_BOUNDARY_RE.split(cleaned, maxsplit=1)[0]
    cleaned = cleaned.strip().rstrip(",").rstrip(".")
    equijoin_only = _extract_equijoin_predicates(cleaned)
    return equijoin_only if equijoin_only else cleaned


def _call_llm_for_join_discovery(
    metadata_snapshot: dict,
    hints: list[dict],
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Call the LLM with the discovery prompt to validate and refine join hints.

    Returns a list of ``{"join_spec": {...}, "rationale": str}`` dicts.
    """
    from genie_space_optimizer.optimization.evaluation import _extract_json

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _inst_js = metadata_snapshot.get("instructions", {})
    if not isinstance(_inst_js, dict):
        _inst_js = {}
    _join_specs = (
        metadata_snapshot.get("join_specs", [])
        or _inst_js.get("join_specs", [])
        or ds.get("join_specs", [])
    )

    hint_tables: set[str] = set()
    for h in hints:
        for k in ("left_table", "right_table", "table", "source_table", "target_table"):
            v = h.get(k)
            if v and isinstance(v, str):
                hint_tables.add(v.lower())

    if hint_tables:
        scoped_schema = _format_full_schema_context(metadata_snapshot, filter_tables=hint_tables)
    else:
        scoped_schema = _format_full_schema_context(metadata_snapshot)

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "full_schema_context": scoped_schema,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "current_join_specs": json.dumps(_join_specs, default=str),
        "discovery_hints": _format_discovery_hints(
            hints,
            join_overlaps=metadata_snapshot.get("_join_overlaps"),
        ),
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, LEVER_4_JOIN_DISCOVERY_PROMPT,
        priority_keys=["full_schema_context", "current_join_specs"],
    )

    prompt = format_mlflow_template(LEVER_4_JOIN_DISCOVERY_PROMPT, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _link_prompt_to_trace("lever_4_join_discovery")

    logger.info(
        "\n"
        "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] INPUT ────────────────────────────────\n"
        "│ Hints: %d\n"
        "│ Existing join specs: %d\n"
        "│ Prompt length: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(hints), len(_join_specs), len(prompt),
    )

    import time

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads()."
    )

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            wc = _ws_with_timeout(w)
            response = wc.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=system_msg),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt),
                ],
                temperature=LLM_TEMPERATURE,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError("LLM response had no choices")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content:
                raise ValueError("LLM response content is empty")
            text = str(content).strip()
            result = _extract_json(text)
            specs = result.get("join_specs", [])
            rationale = result.get("rationale", "")
            for s in specs:
                if isinstance(s, dict) and "sql" in s:
                    s["sql"] = [
                        _sanitize_join_sql(part) for part in s["sql"]
                        if isinstance(part, str)
                    ]
            out = [
                {"join_spec": s, "rationale": rationale}
                for s in specs
                if isinstance(s, dict)
            ]
            logger.info(
                "\n"
                "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] RESPONSE ─────────────────────────\n"
                "│ Join specs returned: %d\n"
                "│ Rationale: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                len(out), str(rationale)[:300],
            )
            return out
        except json.JSONDecodeError:
            logger.warning(
                "JOIN_DISCOVERY non-JSON response (attempt %d/%d): %.500s",
                attempt + 1, LLM_MAX_RETRIES, text,
            )
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return []
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception(
                    "\n"
                    "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] ERROR ────────────────────────────\n"
                    "│ Prompt len: %d chars | Retries: %d\n"
                    "└─────────────────────────────────────────────────────────────────────────",
                    len(prompt), LLM_MAX_RETRIES,
                )
                return []
    return []


def _sanitize_plaintext_instructions(text: str) -> str:
    """Strip residual Markdown from instruction text for plain-text display."""
    text = re.sub(r'^```[a-z]*\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^---+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\*\*\*+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^___+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(
        r'^#{1,6}\s+(.+)$',
        lambda m: m.group(1).upper().rstrip() + ':',
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


_SECTION_HEADER_RE = re.compile(
    r'^([A-Z][A-Z /]+):[ \t]*$', re.MULTILINE,
)
_KNOWN_SECTIONS = set(INSTRUCTION_SECTION_ORDER)

_INLINE_SECTION_NAMES: list[str] = sorted(
    INSTRUCTION_SECTION_ORDER, key=lambda s: len(s), reverse=True
)
_INLINE_SECTION_RE = re.compile(
    r'(?:^|\s)('
    + '|'.join(re.escape(s) for s in _INLINE_SECTION_NAMES)
    + r'):\s*'
)


def _parse_sections(text: str) -> tuple[dict[str, list[str]], list[str]]:
    """Parse structured plain-text into {SECTION_HEADER: [lines]} and preamble lines."""
    lines = text.splitlines()
    sections: dict[str, list[str]] = {}
    preamble: list[str] = []
    current: str | None = None

    for line in lines:
        m = _SECTION_HEADER_RE.match(line)
        if m and m.group(1) in _KNOWN_SECTIONS:
            current = m.group(1)
            if current not in sections:
                sections[current] = []
        elif current is not None:
            sections[current].append(line)
        else:
            preamble.append(line)

    for key in sections:
        while sections[key] and not sections[key][-1].strip():
            sections[key].pop()

    return sections, preamble


def _merge_structured_instructions(
    existing: str,
    contributions: list[str],
    global_guidance: str = "",
) -> str:
    """Merge instruction fragments into a single structured document.

    Parses ``existing`` and each contribution by ALL-CAPS section header,
    deduplicates bullets within each section, then reassembles in
    ``INSTRUCTION_SECTION_ORDER``.  Unrecognized content goes into CONSTRAINTS.
    """
    merged: dict[str, list[str]] = {s: [] for s in INSTRUCTION_SECTION_ORDER}

    existing_sections, existing_preamble = _parse_sections(
        _sanitize_plaintext_instructions(existing) if existing else ""
    )
    for section, lines in existing_sections.items():
        if section in merged:
            merged[section].extend(lines)

    if existing_preamble:
        non_blank = [l for l in existing_preamble if l.strip()]
        if non_blank:
            if not merged["PURPOSE"]:
                merged["PURPOSE"].extend(non_blank)
            else:
                merged["CONSTRAINTS"].extend(non_blank)

    for fragment in contributions:
        sanitized = _sanitize_plaintext_instructions(fragment) if fragment else ""
        frag_sections, frag_preamble = _parse_sections(sanitized)
        for section, lines in frag_sections.items():
            if section in merged:
                merged[section].extend(lines)
            else:
                merged["CONSTRAINTS"].extend(lines)
        if frag_preamble:
            non_blank = [l for l in frag_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    if global_guidance:
        sanitized_g = _sanitize_plaintext_instructions(global_guidance)
        g_sections, g_preamble = _parse_sections(sanitized_g)
        for section, lines in g_sections.items():
            if section in merged:
                merged[section].extend(lines)
        if g_preamble:
            non_blank = [l for l in g_preamble if l.strip()]
            if non_blank:
                merged["CONSTRAINTS"].extend(non_blank)

    for section in merged:
        seen: set[str] = set()
        deduped: list[str] = []
        for line in merged[section]:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped not in seen:
                seen.add(stripped)
                deduped.append(line)
        merged[section] = deduped

    parts: list[str] = []
    for section in INSTRUCTION_SECTION_ORDER:
        if merged[section]:
            parts.append(f"{section}:")
            for line in merged[section]:
                stripped = line.strip()
                if not stripped:
                    continue
                if not stripped.startswith("- "):
                    stripped = f"- {stripped}"
                parts.append(stripped)
            parts.append("")

    result = "\n".join(parts).strip()
    return _sanitize_plaintext_instructions(result)


def normalize_instructions(text: str) -> str:
    """Parse text into canonical structured sections and reassemble."""
    return _merge_structured_instructions(existing=text, contributions=[], global_guidance="")


def _repair_truncated_holistic_json(text: str) -> dict:
    """Extract instruction_text and example_sql_proposals from a truncated JSON.

    When the LLM output exceeds max_tokens, the JSON is cut off mid-string.
    Attempts to salvage both ``instruction_text`` and ``example_sql_proposals``.
    """
    instruction_text = ""
    example_sql_proposals: list[dict] = []

    m = re.search(r'"instruction_text"\s*:\s*"', text)
    if m:
        start = m.end()
        i = start
        while i < len(text):
            ch = text[i]
            if ch == "\\" and i + 1 < len(text):
                i += 2
                continue
            if ch == '"':
                break
            i += 1
        else:
            i = len(text)
        raw = text[start:i]
        try:
            instruction_text = json.loads(f'"{raw}"')
        except (json.JSONDecodeError, ValueError):
            instruction_text = raw.replace('\\"', '"').replace("\\n", "\n")

    m_ex = re.search(r'"example_sql_proposals"\s*:\s*\[', text)
    if m_ex:
        bracket_start = m_ex.end() - 1
        depth = 0
        for j in range(bracket_start, len(text)):
            if text[j] == "[":
                depth += 1
            elif text[j] == "]":
                depth -= 1
            if depth == 0:
                try:
                    example_sql_proposals = json.loads(text[bracket_start : j + 1])
                except json.JSONDecodeError:
                    pass
                break

    if not instruction_text and not example_sql_proposals:
        raise json.JSONDecodeError("No instruction_text or example_sql_proposals found", text, 0)

    logger.warning(
        "Repaired truncated holistic JSON — %d chars instruction, %d example SQL proposals",
        len(instruction_text), len(example_sql_proposals),
    )
    return {
        "instruction_text": instruction_text,
        "example_sql_proposals": example_sql_proposals,
        "rationale": "Recovered from truncated JSON response",
    }


def _call_llm_for_holistic_instructions(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
) -> dict:
    """Single LLM call to synthesize ALL evaluation learnings into holistic instructions.

    Returns ``{"instruction_text": str, "example_sql_proposals": list, "rationale": str}``.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])

    config = metadata_snapshot.get("config") or {}
    space_desc = config.get("description") or ""
    if isinstance(space_desc, list):
        space_desc = "\n".join(space_desc)
    if not space_desc:
        space_desc = "(No description set for this Genie Space.)"

    current_instructions = _get_general_instructions(metadata_snapshot)
    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    resolved_ids: set[str] = set()
    for lc in (lever_changes or []):
        for cid in (lc.get("cluster_ids", []) or []):
            if lc.get("status") in ("applied", "success"):
                resolved_ids.add(str(cid))

    unresolved = [
        c for c in all_clusters
        if str(c.get("cluster_id", "")) not in resolved_ids
    ]
    focus_clusters = unresolved if unresolved else all_clusters

    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "space_description": space_desc,
        "eval_summary": _format_eval_summary(focus_clusters),
        "cluster_briefs": _format_cluster_briefs(focus_clusters, top_n=5),
        "lever_summary": _format_lever_summary(lever_changes),
        "current_instructions": current_instructions or "(No current instructions.)",
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(0, 24500 - 500),
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "table_names": [t.get("name") or t.get("identifier", "") for t in _tables],
        "mv_names": [m.get("name") or m.get("identifier", "") for m in _mvs],
        "tvf_names": [f.get("name") or f.get("identifier", "") for f in _funcs],
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, LEVER_5_HOLISTIC_PROMPT,
        priority_keys=["existing_example_sqls", "lever_summary", "cluster_briefs", "eval_summary"],
    )

    prompt = format_mlflow_template(LEVER_5_HOLISTIC_PROMPT, **format_kwargs)

    from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
    _link_prompt_to_trace("lever_5_holistic")

    _W = 78
    _hdr = "┌─── LLM Call [LEVER_5_HOLISTIC] " + "─" * max(0, _W - 32)
    _ftr = "└" + "─" * (_W - 1)

    logger.info(
        "\n%s\n│ Clusters: %d\n│ Lever changes: %d\n│ Prompt length: %d chars\n%s",
        _hdr, len(focus_clusters), len(lever_changes or []), len(prompt), _ftr,
    )

    import time

    from genie_space_optimizer.optimization.evaluation import _extract_json

    holistic_system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'instruction_text' string field."
    )

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            wc = _ws_with_timeout(w)
            response = wc.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=holistic_system_msg),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt),
                ],
                temperature=LLM_TEMPERATURE,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError("LLM response had no choices")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content:
                raise ValueError("LLM response content is empty")
            text = str(content).strip()
            try:
                result = _extract_json(text)
            except json.JSONDecodeError:
                result = _repair_truncated_holistic_json(text)

            instruction_text = result.get("instruction_text", "")
            if instruction_text:
                instruction_text = _sanitize_plaintext_instructions(instruction_text)
            example_proposals = result.get("example_sql_proposals", [])
            rationale = result.get("rationale", "")

            if instruction_text and len(instruction_text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Holistic instruction text exceeds %d chars (%d), truncating",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(instruction_text),
                )
                instruction_text = instruction_text[:MAX_HOLISTIC_INSTRUCTION_CHARS]

            logger.info(
                "\n┌─── LLM Response [LEVER_5_HOLISTIC] ──────────────────────────────────\n"
                "│ Instruction text: %d chars\n"
                "│ Example SQL proposals: %d\n"
                "│ Rationale: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                len(instruction_text), len(example_proposals), str(rationale)[:300],
            )
            return {
                "instruction_text": instruction_text,
                "example_sql_proposals": example_proposals if isinstance(example_proposals, list) else [],
                "rationale": rationale,
            }
        except json.JSONDecodeError:
            logger.warning(
                "Holistic LLM response was not valid JSON (attempt %d): %.500s",
                attempt + 1, text,
            )
            if attempt >= LLM_MAX_RETRIES - 1:
                m_regex = re.search(r'"instruction_text"\s*:\s*"(.{50,}?)"', text, re.DOTALL)
                if m_regex:
                    recovered = m_regex.group(1).replace('\\"', '"').replace("\\n", "\n")
                    logger.info("Last-ditch regex recovered %d chars of instruction_text", len(recovered))
                    return {"instruction_text": recovered, "example_sql_proposals": [], "rationale": "Regex recovery"}
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "JSON parse failed"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception(
                    "Holistic LLM call failed after %d retries (prompt len: %d)",
                    LLM_MAX_RETRIES, len(prompt),
                )
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "LLM call failed"}
    return {"instruction_text": "", "example_sql_proposals": [], "rationale": "All retries exhausted"}


# ═══════════════════════════════════════════════════════════════════════
# Phase 1 — Holistic Strategist
# ═══════════════════════════════════════════════════════════════════════

_EMPTY_STRATEGY: dict[str, Any] = {
    "action_groups": [],
    "global_instruction_rewrite": "",
    "rationale": "",
}


def _format_soft_signal_summary(soft_clusters: list[dict]) -> str:
    """Compact summary of soft-signal clusters for the strategist prompt."""
    if not soft_clusters:
        return "(No soft signals.)"
    _info_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
    filtered: list[dict] = []
    for sc in soft_clusters:
        judges_in_cluster = {
            fj.get("judge", "")
            for qt in sc.get("question_traces", [])
            for fj in qt.get("failed_judges", [])
        }
        if judges_in_cluster and judges_in_cluster <= _info_judges:
            continue
        filtered.append(sc)
    if not filtered:
        return "(No actionable soft signals.)"
    lines: list[str] = []
    for sc in filtered[:10]:
        cid = sc.get("cluster_id", "?")
        rc = sc.get("root_cause", "unknown")
        qids = sc.get("question_ids", [])
        lines.append(f"- {cid}: root_cause={rc}, questions={len(qids)}")
        for qt in sc.get("question_traces", [])[:2]:
            qtext = qt.get("question_text", "")[:120]
            lines.append(f"    Q: {qtext}")
            for fj in qt.get("failed_judges", []):
                lines.append(
                    f"    Judge {fj.get('judge','?')}: {fj.get('resolved_root_cause','?')} "
                    f"— {fj.get('rationale_snippet','')[:150]}"
                )
    return "\n".join(lines) if lines else "(No soft signals.)"


def _format_join_specs_context(metadata_snapshot: dict) -> str:
    """Format current join specs for the strategist prompt."""
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    inst = metadata_snapshot.get("instructions", {})
    if not isinstance(inst, dict):
        inst = {}
    specs = (
        metadata_snapshot.get("join_specs", [])
        or inst.get("join_specs", [])
        or ds.get("join_specs", [])
    )
    if not specs:
        return "(No join specifications configured.)"
    lines: list[str] = []
    for js in specs:
        left = js.get("left", {})
        right = js.get("right", {})
        sql = js.get("sql", "")
        lines.append(f"- {left.get('identifier','?')} <-> {right.get('identifier','?')}: {sql[:200]}")
    return "\n".join(lines)


def _repair_truncated_strategy_json(text: str) -> dict:
    """Extract action_groups from a truncated strategy JSON response.

    Attempts bracket-matching on the ``action_groups`` array first, then
    falls back to extracting ``global_instruction_rewrite`` if present.
    """
    result: dict[str, Any] = {**_EMPTY_STRATEGY, "rationale": "Recovered from truncated JSON"}

    m_ag = re.search(r'"action_groups"\s*:\s*\[', text)
    if m_ag:
        bracket_start = m_ag.end() - 1
        depth = 0
        for i in range(bracket_start, len(text)):
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
            if depth == 0:
                try:
                    result["action_groups"] = json.loads(text[bracket_start : i + 1])
                except json.JSONDecodeError:
                    pass
                break

    m_gi = re.search(r'"global_instruction_rewrite"\s*:\s*"', text)
    if m_gi:
        start = m_gi.end()
        j = start
        while j < len(text):
            ch = text[j]
            if ch == "\\" and j + 1 < len(text):
                j += 2
                continue
            if ch == '"':
                break
            j += 1
        else:
            j = len(text)
        raw = text[start:j]
        try:
            result["global_instruction_rewrite"] = json.loads(f'"{raw}"')
        except (json.JSONDecodeError, ValueError):
            result["global_instruction_rewrite"] = raw.replace('\\"', '"').replace("\\n", "\n")

    if not result["action_groups"] and not result["global_instruction_rewrite"]:
        raise json.JSONDecodeError("Could not extract strategy fields", text, 0)

    logger.warning(
        "Repaired truncated strategy JSON — %d action groups, %d chars instruction",
        len(result["action_groups"]),
        len(result.get("global_instruction_rewrite", "")),
    )
    return result


def _normalize_instruction_rewrite(raw: Any) -> dict | str:
    """Normalize ``global_instruction_rewrite`` from an LLM response.

    The prompt schema asks for a JSON object (``{section: text}``), but
    the LLM may return a plain string, a list, or ``None``.  This helper
    ensures downstream code always receives either a validated ``dict``
    or a sanitized ``str``.

    When the LLM returns a string that contains recognizable section
    headers (e.g. ``QUERY RULES: - bullet ...``), the function attempts
    to parse it into the preferred dict form so that downstream code
    follows the safer section-level merge path.
    """
    if isinstance(raw, dict):
        valid_keys = set(INSTRUCTION_SECTION_ORDER)
        return {k: str(v) for k, v in raw.items() if k in valid_keys and v is not None}
    if isinstance(raw, list):
        raw = "\n".join(str(item) for item in raw)
    if isinstance(raw, str) and raw.strip():
        sanitized = _sanitize_plaintext_instructions(raw)
        if len(sanitized) > MAX_HOLISTIC_INSTRUCTION_CHARS:
            sanitized = sanitized[:MAX_HOLISTIC_INSTRUCTION_CHARS]
        parsed = _try_parse_string_as_section_dict(sanitized)
        if parsed is not None:
            return parsed
        return sanitized
    return ""


def _try_parse_string_as_section_dict(text: str) -> dict[str, str] | None:
    """Attempt to parse a plain-text string into a ``{section: content}`` dict.

    First tries ``_parse_sections`` (which requires headers on their own line).
    If that fails, applies ``_INLINE_SECTION_RE`` to reformat inline
    ``"SECTION: content"`` into newline-separated form and retries.

    Returns ``None`` if no known section headers are found.
    """
    sections, preamble = _parse_sections(text)
    if not sections:
        reformatted = _INLINE_SECTION_RE.sub(r'\n\1:\n', text).strip()
        if reformatted != text:
            sections, preamble = _parse_sections(reformatted)

    if not sections:
        return None

    valid_keys = set(INSTRUCTION_SECTION_ORDER)
    result: dict[str, str] = {}
    for k, lines in sections.items():
        if k in valid_keys:
            result[k] = "\n".join(lines)
    if preamble:
        non_blank = [ln for ln in preamble if ln.strip()]
        if non_blank:
            existing = result.get("CONSTRAINTS", "")
            result["CONSTRAINTS"] = (
                (existing + "\n" if existing else "") + "\n".join(non_blank)
            )
    if result:
        logger.info(
            "Coerced string instruction_rewrite to dict with %d section(s): %s",
            len(result), sorted(result.keys()),
        )
        return result
    return None


def _preview_instruction_rewrite(rewrite: dict | str, max_chars: int = 200) -> str:
    """Human-readable preview of an instruction rewrite for logging."""
    if isinstance(rewrite, dict):
        parts = [
            f"{k}: {v[:60]}..." if len(str(v)) > 60 else f"{k}: {v}"
            for k, v in rewrite.items() if v
        ]
        preview = "; ".join(parts)
        return preview[:max_chars] + ("..." if len(preview) > max_chars else "")
    return str(rewrite)[:max_chars]


def _call_llm_for_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Monolithic strategist fallback — used only when triage returns 0 AGs.

    Sends the full STRATEGIST_PROMPT with compressed context (top-5 clusters,
    SQL truncated to 300 chars) to stay within timeout bounds.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    _blame_items: list[str] = []
    for c in clusters:
        _blame_items.extend(_normalize_blame(c.get("asi_blame_set")))
    blame_set: list[str] | None = list(dict.fromkeys(_blame_items)) if _blame_items else None

    format_kwargs: dict[str, Any] = {
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "cluster_briefs": _format_cluster_briefs(clusters, top_n=5, max_sql_chars=300),
        "soft_signal_summary": _format_soft_signal_summary(soft_signal_clusters),
        "structured_table_context": _format_structured_table_context(
            metadata_snapshot, blame_set, lever=1,
        ),
        "structured_column_context": _format_structured_column_context(
            metadata_snapshot, blame_set, lever=1,
        ),
        "structured_function_context": _format_structured_function_context(
            metadata_snapshot, lever=3,
        ),
        "current_join_specs": _format_join_specs_context(metadata_snapshot),
        "current_instructions": (
            _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
        ),
        "existing_example_sqls": _format_existing_example_sqls(metadata_snapshot),
        "blamed_column_values": _format_blamed_column_values(
            clusters, metadata_snapshot.get("_data_profile", {}),
        ),
        "instruction_char_budget": max(0, 24500 - 500),
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_PROMPT,
        priority_keys=["blamed_column_values", "full_schema_context", "existing_example_sqls",
                        "soft_signal_summary", "structured_function_context",
                        "structured_column_context", "cluster_briefs"],
    )

    prompt = format_mlflow_template(STRATEGIST_PROMPT, **format_kwargs)

    _W = 78
    _hdr = "┌─── LLM Call [STRATEGIST] " + "─" * max(0, _W - 27)
    _ftr = "└" + "─" * (_W - 1)
    logger.info(
        "\n%s\n│ Hard clusters: %d\n│ Soft clusters: %d\n│ Prompt length: %d chars\n%s",
        _hdr, len(clusters), len(soft_signal_clusters), len(prompt), _ftr,
    )
    print(
        f"\n{'=' * _W}\n"
        f"  PHASE 1: HOLISTIC STRATEGIST\n"
        f"  Hard clusters: {len(clusters)} | Soft clusters: {len(soft_signal_clusters)}\n"
        f"  Prompt: {len(prompt):,} chars\n"
        f"{'=' * _W}"
    )

    _link_prompt_to_trace("strategist")

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'action_groups' array."
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name="monolithic_strategy_fallback",
        )
    except Exception:
        logger.exception(
            "Strategist LLM call failed after retries (prompt len: %d)", len(prompt),
        )
        return {**_EMPTY_STRATEGY, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Strategist LLM response was not valid JSON: %.500s", text)
            return {**_EMPTY_STRATEGY, "rationale": "JSON parse failed"}

    action_groups = result.get("action_groups", [])
    if not isinstance(action_groups, list):
        action_groups = []
    global_rewrite = _normalize_instruction_rewrite(result.get("global_instruction_rewrite"))
    rationale = result.get("rationale", "")

    _rewrite_preview = _preview_instruction_rewrite(global_rewrite)
    _rewrite_desc = (
        f"{len(global_rewrite)} sections" if isinstance(global_rewrite, dict)
        else f"{len(global_rewrite)} chars"
    )
    logger.info(
        "\n┌─── LLM Response [STRATEGIST] ────────────────────────────────────────\n"
        "│ Action groups: %d\n"
        "│ Global instruction rewrite: %s\n"
        "│ Rationale: %s\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(action_groups), _rewrite_desc, str(rationale)[:300],
    )
    print(
        f"\n  Strategy produced {len(action_groups)} action group(s), "
        f"{_rewrite_desc} instruction rewrite"
    )
    for i, ag in enumerate(action_groups):
        levers = sorted(ag.get("lever_directives", {}).keys())
        qs = ag.get("affected_questions", [])
        print(
            f"    AG{i + 1}: {ag.get('root_cause_summary', '?')[:80]}"
            f" | levers={levers} | questions={len(qs)}"
        )

    return {
        "action_groups": action_groups,
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }


# ── Adaptive Strategist (single-call, one AG per iteration) ─────────────


def _call_llm_for_adaptive_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    reflection_buffer: list[dict],
    priority_ranking: list[dict],
    tried_patches: set[tuple[str, str]],
    w: WorkspaceClient | None = None,
    *,
    total_benchmarks: int = 0,
    passing_benchmarks: int = 0,
    verdict_history: dict | None = None,
    skill_exemplars: list[dict] | None = None,
    human_suggestions: list[dict] | None = None,
) -> dict:
    """Single-call strategist that produces exactly ONE action group.

    Combines schema context, failure clusters, SQL diffs, a priority
    ranking, and a reflection buffer into one prompt.  Designed for the
    adaptive lever loop where this call is made every iteration with
    fresh evaluation results.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    _blame_items: list[str] = []
    for c in clusters:
        _blame_items.extend(_normalize_blame(c.get("asi_blame_set")))
    blame_set: list[str] | None = list(dict.fromkeys(_blame_items)) if _blame_items else None

    # ── Build priority ranking text ──────────────────────────────────
    ranking_lines: list[str] = []
    for c in priority_ranking[:10]:
        ranking_lines.append(
            f"  Rank {c.get('rank', '?')}: "
            f"[{c.get('cluster_id', '?')}] {c.get('root_cause', '?')} "
            f"(judge={c.get('affected_judge', '?')}, "
            f"questions={len(c.get('question_ids', []))}, "
            f"impact={c.get('impact_score', 0):.1f})"
        )
    ranking_text = "\n".join(ranking_lines) if ranking_lines else "(no clusters)"

    # ── Build success summary ────────────────────────────────────────
    failing = total_benchmarks - passing_benchmarks
    success_summary = (
        f"{passing_benchmarks} of {total_benchmarks} benchmarks pass all judges. "
        f"{failing} failures remain."
        if total_benchmarks > 0
        else "(benchmark counts not available)"
    )

    # ── Build reflection text ────────────────────────────────────────
    reflection_text = format_reflection_buffer(reflection_buffer)

    # ── Build per-question persistence summary ────────────────────────
    from genie_space_optimizer.optimization.harness import (
        _build_question_persistence_summary,
    )
    persistence_text, _persistence_structured = _build_question_persistence_summary(
        verdict_history or {}, reflection_buffer,
    )

    # ── Build proven patterns text ────────────────────────────────────
    _proven_lines: list[str] = []
    for _ex in (skill_exemplars or []):
        _proven_lines.append(
            f"- Root cause: {_ex.get('root_cause', '?')[:80]} | "
            f"Levers: {', '.join(str(l) for l in _ex.get('lever_pattern', []))} | "
            f"Patches: {', '.join(str(x) for x in _ex.get('patch_types', [])[:4] if x is not None)} | "
            f"Gain: {_ex.get('accuracy_gain', 0):+.1f}%"
        )
    proven_patterns_text = (
        "\n".join(_proven_lines)
        if _proven_lines
        else "(No accepted patterns yet. This is informed by prior successful iterations.)"
    )

    # ── Build human reviewer suggestions text ──────────────────────────
    suggestions_text = ""
    if human_suggestions:
        _TYPE_LABELS = {
            "column_description": "Column descriptions",
            "business_rule": "Business rules",
            "join_condition": "Join conditions",
            "instruction": "Instructions",
            "general": "General suggestions",
        }
        grouped: dict[str, list[str]] = {}
        for s in human_suggestions:
            ttype = s.get("target_type", "general")
            target = s.get("target_identifier", "")
            suggestion = s.get("suggestion", "")
            if not suggestion:
                for item in s.get("suggestions", []):
                    grouped.setdefault("general", []).append(str(item))
                continue
            label = f"{target}: {suggestion}" if target else suggestion
            grouped.setdefault(ttype, []).append(label)
        lines_hs: list[str] = ["Human reviewer suggestions from prior review:"]
        for ttype, type_label in _TYPE_LABELS.items():
            items = grouped.get(ttype)
            if not items:
                continue
            lines_hs.append(f"\n{type_label}:")
            for item in items:
                lines_hs.append(f"- {item}")
        suggestions_text = "\n".join(lines_hs)

    # ── Build structured context ────────────────────────────────────
    context_data = _build_context_data(
        clusters=clusters,
        soft_signal_clusters=soft_signal_clusters,
        metadata_snapshot=metadata_snapshot,
        reflection_buffer=reflection_buffer,
        priority_ranking=priority_ranking,
        blame_set=blame_set,
        success_summary=success_summary,
        reflection_text=reflection_text,
        persistence_text=persistence_text,
        proven_patterns_text=proven_patterns_text,
        suggestions_text=suggestions_text,
    )
    context_data = _truncate_context_to_budget(context_data, PROMPT_TOKEN_BUDGET)
    context_json = json.dumps(context_data, indent=2, default=str)

    format_kwargs: dict[str, Any] = {
        "context_json": context_json,
        "identifier_allowlist": _format_identifier_allowlist(
            _build_identifier_allowlist(metadata_snapshot)
        ),
        "instruction_char_budget": max(0, 24500 - 500),
    }

    prompt = format_mlflow_template(ADAPTIVE_STRATEGIST_PROMPT, **format_kwargs)

    _W = 78
    _iter_label = len(reflection_buffer) + 1
    logger.info(
        "\n┌─── LLM Call [ADAPTIVE STRATEGIST] %s\n"
        "│ Clusters: %d hard, %d soft\n"
        "│ Reflections: %d\n"
        "│ Prompt: %d chars\n"
        "└%s",
        "─" * max(0, _W - 37),
        len(clusters),
        len(soft_signal_clusters),
        len(reflection_buffer),
        len(prompt),
        "─" * (_W - 1),
    )
    _input_lines = [
        f"\n{'=' * _W}",
        f"  STRATEGIST INPUT (Iteration {_iter_label})",
        f"{'=' * _W}",
        f"|  {'Success Summary:':<28s} {success_summary}",
        f"|  {'Clusters:':<28s} {len(clusters)} hard, {len(soft_signal_clusters)} soft",
        f"|  {'Reflections:':<28s} {len(reflection_buffer)}",
        f"|  {'Prompt:':<28s} {len(prompt):,} chars",
    ]
    _ranking_preview = ranking_text.split("\n")[:5]
    if _ranking_preview:
        _input_lines.append(f"|  {'Priority Ranking (top 5):':<28s}")
        for _rp in _ranking_preview:
            _input_lines.append(f"|    {_rp.strip()}")
    _refl_preview = reflection_text[:500]
    if _refl_preview and _refl_preview != "(No prior iterations. This is the first attempt after baseline evaluation.)":
        _input_lines.append(f"|  {'Reflection Buffer:':<28s}")
        for _rl in _refl_preview.split("\n")[:10]:
            _input_lines.append(f"|    {_rl}")
        if len(reflection_text) > 500:
            _input_lines.append(f"|    ... ({len(reflection_text) - 500} more chars)")
    _persist_preview = persistence_text[:500]
    if _persist_preview and "No" not in _persist_preview[:30]:
        _input_lines.append(f"|  {'Persistence Summary:':<28s}")
        for _pl in _persist_preview.split("\n")[:8]:
            _input_lines.append(f"|    {_pl}")
    if _proven_lines:
        _input_lines.append(f"|  {'Proven Patterns:':<28s} {len(_proven_lines)}")
        for _pp in _proven_lines[:3]:
            _input_lines.append(f"|    {_pp}")
    _input_lines.append(f"{'=' * _W}")
    print("\n".join(_input_lines))

    _link_prompt_to_trace("adaptive_strategist")

    system_msg = (
        "You are a JSON API. You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, analysis, or markdown outside the JSON. "
        "Your entire response must be parseable by json.loads(). "
        "The JSON must contain an 'action_groups' array with EXACTLY one entry."
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt, span_name="adaptive_strategy",
        )
    except Exception:
        logger.exception("Adaptive strategist LLM call failed after retries")
        return {**_EMPTY_STRATEGY, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Adaptive strategist response not valid JSON: %.500s", text)
            return {**_EMPTY_STRATEGY, "rationale": "JSON parse failed"}

    action_groups = result.get("action_groups", [])
    if not isinstance(action_groups, list):
        action_groups = []
    global_rewrite = _normalize_instruction_rewrite(result.get("global_instruction_rewrite"))
    rationale = result.get("rationale", "")

    _rewrite_preview = _preview_instruction_rewrite(global_rewrite)
    _rewrite_desc = (
        f"{len(global_rewrite)} sections" if isinstance(global_rewrite, dict)
        else f"{len(global_rewrite)} chars"
    )
    logger.info(
        "\n┌─── LLM Response [ADAPTIVE STRATEGIST] ──────────────────────────────\n"
        "│ Action groups: %d\n"
        "│ Global instruction rewrite: %s\n"
        "│ Rationale: %s\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(action_groups),
        _rewrite_desc,
        str(rationale)[:300],
    )
    _out_lines = [
        f"\n{'=' * _W}",
        f"  STRATEGIST OUTPUT (Iteration {_iter_label})",
        f"{'=' * _W}",
    ]
    if action_groups:
        ag = action_groups[0]
        levers = sorted(ag.get("lever_directives", {}).keys())
        qs = ag.get("affected_questions", [])
        _out_lines.append(f"|  {'AG:':<28s} {ag.get('root_cause_summary', '?')[:100]}")
        _out_lines.append(f"|  {'Levers:':<28s} {', '.join(levers)}")
        _out_lines.append(f"|  {'Affected Questions:':<28s} {len(qs)} — {', '.join(qs[:5])}")
        _out_lines.append(f"|  {'Escalation:':<28s} {ag.get('escalation', 'none') or 'none'}")
        _out_lines.append(f"|  {'Rationale:':<28s} {str(rationale)[:200]}")
        if global_rewrite:
            _out_lines.append(f"|  {'Instruction Rewrite:':<28s} {_rewrite_preview}...")
    else:
        _out_lines.append("|  No action group produced")
        if rationale:
            _out_lines.append(f"|  {'Rationale:':<28s} {str(rationale)[:200]}")
    _out_lines.append(f"{'=' * _W}")
    print("\n".join(_out_lines))

    return {
        "action_groups": action_groups[:1],
        "global_instruction_rewrite": global_rewrite,
        "rationale": rationale,
    }


# ── Phase 1a: Triage ────────────────────────────────────────────────────

_EMPTY_TRIAGE: dict[str, Any] = {
    "action_groups": [],
    "global_instruction_guidance": "",
    "rationale": "",
}


def _call_llm_for_triage(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Phase 1a: lightweight triage call that sees ALL clusters compactly.

    Returns action group *skeletons* with ``levers_needed``, ``focus_tables``,
    and ``focus_columns`` — no actual lever directives yet.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    schema_index = _format_schema_index(metadata_snapshot)
    cluster_summaries = _format_compact_cluster_summaries(clusters)
    soft_summary = _format_soft_signal_summary(soft_signal_clusters)
    join_summary = _format_join_specs_context(metadata_snapshot)
    current_instr = _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
    instruction_summary = current_instr[:1500]
    if len(current_instr) > 1500:
        instruction_summary += f" ... ({len(current_instr) - 1500} chars omitted)"

    format_kwargs: dict[str, Any] = {
        "schema_index": schema_index,
        "cluster_summaries": cluster_summaries,
        "soft_signal_summary": soft_summary,
        "current_join_summary": join_summary,
        "instruction_summary": instruction_summary,
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_TRIAGE_PROMPT,
        priority_keys=["soft_signal_summary", "instruction_summary", "schema_index", "cluster_summaries"],
    )

    prompt = format_mlflow_template(STRATEGIST_TRIAGE_PROMPT, **format_kwargs)

    _W = 78
    logger.info(
        "\n┌─── LLM Call [TRIAGE] %s\n│ Clusters: %d hard, %d soft\n│ Prompt: %d chars\n└%s",
        "─" * max(0, _W - 23), len(clusters), len(soft_signal_clusters), len(prompt), "─" * (_W - 1),
    )
    print(
        f"\n{'=' * _W}\n"
        f"  PHASE 1a: TRIAGE STRATEGIST\n"
        f"  Clusters: {len(clusters)} hard, {len(soft_signal_clusters)} soft\n"
        f"  Prompt: {len(prompt):,} chars\n"
        f"{'=' * _W}"
    )

    _link_prompt_to_trace("strategist_triage")

    system_msg = (
        "You are a JSON API. Respond with ONLY a valid JSON object. "
        "No explanation or markdown outside the JSON. "
        "The JSON must contain an 'action_groups' array."
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt, span_name="phase_1a_triage",
        )
    except Exception:
        logger.exception("Triage LLM call failed after retries (prompt len: %d)", len(prompt))
        return {**_EMPTY_TRIAGE, "rationale": "LLM call failed"}

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("Triage LLM response was not valid JSON: %.500s", text)
            return {**_EMPTY_TRIAGE, "rationale": "JSON parse failed"}

    ags = result.get("action_groups", [])
    if not isinstance(ags, list):
        ags = []

    logger.info("Triage produced %d action group skeleton(s)", len(ags))
    print(f"\n  Triage produced {len(ags)} action group skeleton(s)")
    for i, ag in enumerate(ags):
        levers = ag.get("levers_needed", [])
        ft = ag.get("focus_tables", [])
        fc = ag.get("focus_columns", [])
        print(
            f"    AG{i + 1}: {ag.get('root_cause_summary', '?')[:80]}"
            f" | levers={levers} | tables={len(ft)} | cols={len(fc)}"
        )

    return {
        "action_groups": ags,
        "global_instruction_guidance": result.get("global_instruction_guidance", ""),
        "rationale": result.get("rationale", ""),
    }


# ── Phase 1b: AG Detail ─────────────────────────────────────────────────

def _call_llm_for_ag_detail(
    ag_skeleton: dict,
    clusters: list[dict],
    metadata_snapshot: dict,
    instruction_char_budget: int = 4000,
    w: WorkspaceClient | None = None,
) -> dict:
    """Phase 1b: produce full lever_directives for one action group skeleton.

    Receives the skeleton from triage plus *only* the relevant clusters and
    metadata scoped to ``focus_tables``/``focus_columns``.
    """
    from genie_space_optimizer.optimization.applier import _get_general_instructions
    from genie_space_optimizer.optimization.evaluation import (
        _extract_json,
        _link_prompt_to_trace,
    )

    ag_id = ag_skeleton.get("id", "AG?")
    source_cids = set(ag_skeleton.get("source_cluster_ids", []))
    relevant_clusters = [c for c in clusters if c.get("cluster_id") in source_cids]
    if not relevant_clusters:
        relevant_clusters = clusters[:3]

    sql_diffs_parts: list[str] = []
    for cluster in relevant_clusters:
        cid = cluster.get("cluster_id", "?")
        rc = cluster.get("root_cause", "unknown")
        sql_diffs_parts.append(f"### Cluster {cid}: {rc}")
        sql_diffs_parts.append(_format_sql_diffs(cluster))
        sql_diffs_parts.append("")
    sql_diffs_text = "\n".join(sql_diffs_parts) if sql_diffs_parts else "(no SQL context)"

    focus_tables = ag_skeleton.get("focus_tables", [])
    focus_columns = ag_skeleton.get("focus_columns", [])
    blame_set = list(focus_tables) + [c.split(".")[-1] for c in focus_columns if "." in c]
    if not blame_set:
        for c in relevant_clusters:
            b = c.get("asi_blame_set")
            if isinstance(b, str) and b:
                blame_set.extend(b.split("|"))
            elif isinstance(b, list):
                blame_set.extend(str(x) for x in b)

    levers_needed = ag_skeleton.get("levers_needed", [1, 5])

    structured_table_ctx = _format_structured_table_context(
        metadata_snapshot, blame_set or None, lever=1,
    )
    structured_col_ctx = _format_structured_column_context(
        metadata_snapshot, blame_set or None, lever=1,
    )
    structured_fn_ctx = ""
    if 3 in levers_needed:
        structured_fn_ctx = _format_structured_function_context(metadata_snapshot, lever=3)

    join_specs = _format_join_specs_context(metadata_snapshot)
    current_instr = _get_general_instructions(metadata_snapshot) or "(No current instructions.)"
    example_sqls = _format_existing_example_sqls(metadata_snapshot)

    skeleton_json = json.dumps(ag_skeleton, indent=2, default=str)
    _allowlist = _build_identifier_allowlist(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "action_group_skeleton": skeleton_json,
        "sql_diffs": sql_diffs_text,
        "identifier_allowlist": _format_identifier_allowlist(_allowlist),
        "structured_table_context": structured_table_ctx,
        "structured_column_context": structured_col_ctx,
        "structured_function_context": structured_fn_ctx,
        "current_join_specs": join_specs,
        "current_instructions": current_instr,
        "existing_example_sqls": example_sqls,
        "instruction_char_budget": instruction_char_budget,
    }

    format_kwargs = _truncate_to_budget(
        format_kwargs, STRATEGIST_DETAIL_PROMPT,
        priority_keys=["existing_example_sqls", "structured_function_context", "current_instructions", "sql_diffs"],
    )

    prompt = format_mlflow_template(STRATEGIST_DETAIL_PROMPT, **format_kwargs)

    logger.info(
        "\n┌─── LLM Call [AG DETAIL: %s] ────────────────────────────────────\n"
        "│ Clusters: %d | Levers: %s | Prompt: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────",
        ag_id, len(relevant_clusters), levers_needed, len(prompt),
    )
    print(
        f"\n  Phase 1b: Detailing {ag_id} "
        f"({len(relevant_clusters)} clusters, levers={levers_needed}, "
        f"prompt={len(prompt):,} chars)"
    )

    _link_prompt_to_trace("strategist_detail")

    _EMPTY_DETAIL: dict[str, Any] = {
        "lever_directives": {}, "coordination_notes": "",
        "instruction_contribution": "", "proposals": [],
    }

    system_msg = (
        "You are a JSON API. Respond with ONLY a valid JSON object. "
        "No explanation or markdown outside the JSON. "
        "The JSON must contain a 'lever_directives' object."
    )

    try:
        text, _response = _traced_llm_call(
            w, system_msg, prompt,
            span_name=f"phase_1b_detail_{ag_id}",
        )
    except Exception:
        logger.exception("AG detail LLM call failed after retries for %s", ag_id)
        return dict(_EMPTY_DETAIL)

    try:
        result = _extract_json(text)
    except json.JSONDecodeError:
        try:
            result = _repair_truncated_strategy_json(text)
        except json.JSONDecodeError:
            logger.warning("AG detail LLM response not valid JSON: %.500s", text)
            return dict(_EMPTY_DETAIL)

    lever_dirs = result.get("lever_directives", {})
    if not isinstance(lever_dirs, dict):
        lever_dirs = {}
    coord = result.get("coordination_notes", "")
    raw_instr = result.get("instruction_contribution", "")
    if isinstance(raw_instr, dict):
        instr_contrib = raw_instr
        instr_len = sum(len(str(v)) for v in raw_instr.values())
    else:
        instr_contrib = str(raw_instr) if raw_instr else ""
        instr_len = len(instr_contrib)
    proposals = result.get("proposals", [])
    if not isinstance(proposals, list):
        proposals = []

    logger.info(
        "AG %s detail: %d lever directives, coordination=%d chars, instruction=%d chars, proposals=%d",
        ag_id, len(lever_dirs), len(coord), instr_len, len(proposals),
    )
    print(
        f"    {ag_id} detail: levers={sorted(lever_dirs.keys())}, "
        f"coordination={len(coord)} chars, instruction={instr_len} chars, "
        f"proposals={len(proposals)}"
    )

    return {
        "lever_directives": lever_dirs,
        "coordination_notes": coord,
        "instruction_contribution": instr_contrib,
        "proposals": proposals,
    }


def _generate_holistic_strategy(
    clusters: list[dict],
    soft_signal_clusters: list[dict],
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
) -> dict:
    """Two-phase progressive strategist.

    Phase 1a — Triage: compact summaries of ALL clusters, produces action
    group skeletons with ``levers_needed`` and ``focus_objects``.

    Phase 1b — Detail: for each skeleton, full SQL diffs and structured
    metadata (scoped to focus objects) produce concrete ``lever_directives``.

    Falls back to the monolithic ``_call_llm_for_strategy`` if triage
    returns 0 action groups (LLM failure safety net).

    Returns a strategy dict with ``action_groups``, ``global_instruction_rewrite``,
    and ``rationale`` — identical shape to what harness.py expects.
    """
    import mlflow
    from mlflow.entities import SpanType

    from genie_space_optimizer.optimization.applier import _get_general_instructions

    hard = [c for c in clusters if c.get("cluster_id")]
    soft = [c for c in soft_signal_clusters if c.get("cluster_id")]

    if not hard and not soft:
        logger.info("No clusters to strategize on — returning empty strategy")
        return {**_EMPTY_STRATEGY, "rationale": "No clusters available"}

    with mlflow.start_span(name="generate_holistic_strategy", span_type=SpanType.CHAIN) as span:
        span.set_inputs({
            "hard_clusters": len(hard),
            "soft_clusters": len(soft),
        })

        # ── Phase 1a: Triage (CHAT_MODEL span created inside _call_llm_for_triage)
        triage_result = _call_llm_for_triage(
            clusters=hard,
            soft_signal_clusters=soft,
            metadata_snapshot=metadata_snapshot,
            w=w,
        )
        triage_ags = triage_result.get("action_groups", [])

        if not triage_ags:
            logger.warning(
                "Triage returned 0 action groups — falling back to monolithic strategist"
            )
            print("\n  Triage returned 0 AGs — falling back to monolithic call")
            fallback = _call_llm_for_strategy(
                clusters=hard,
                soft_signal_clusters=soft,
                metadata_snapshot=metadata_snapshot,
                w=w,
            )
            ags = fallback.get("action_groups", [])
            for i, ag in enumerate(ags):
                if "id" not in ag:
                    ag["id"] = f"AG{i + 1}"
                if "priority" not in ag:
                    ag["priority"] = i + 1
            span.set_outputs({
                "action_groups_count": len(ags),
                "mode": "monolithic_fallback",
                "global_instruction_rewrite_len": len(fallback.get("global_instruction_rewrite", "")),
            })
            return fallback

        # ── Phase 1b: Detail per AG (CHAT_MODEL spans created inside _call_llm_for_ag_detail)
        per_ag_budget = max(2000, 24000 // max(len(triage_ags), 1))
        final_ags: list[dict] = []
        str_contributions: list[str] = []
        dict_contributions: list[dict[str, str]] = []

        for i, skeleton in enumerate(triage_ags):
            if "id" not in skeleton:
                skeleton["id"] = f"AG{i + 1}"
            if "priority" not in skeleton:
                skeleton["priority"] = i + 1

            detail = _call_llm_for_ag_detail(
                ag_skeleton=skeleton,
                clusters=hard,
                metadata_snapshot=metadata_snapshot,
                instruction_char_budget=per_ag_budget,
                w=w,
            )

            lever_dirs = detail.get("lever_directives", {})
            coord_notes = detail.get("coordination_notes", "")
            instr_contrib = detail.get("instruction_contribution", "")
            proposals = detail.get("proposals", [])

            assembled_ag: dict[str, Any] = {
                "id": skeleton["id"],
                "root_cause_summary": skeleton.get("root_cause_summary", ""),
                "source_cluster_ids": skeleton.get("source_cluster_ids", []),
                "affected_questions": skeleton.get("affected_questions", []),
                "priority": skeleton.get("priority", i + 1),
                "lever_directives": lever_dirs,
                "coordination_notes": coord_notes,
                "proposals": proposals,
            }
            final_ags.append(assembled_ag)

            if isinstance(instr_contrib, dict) and instr_contrib:
                dict_contributions.append(instr_contrib)
            elif isinstance(instr_contrib, str) and instr_contrib.strip():
                str_contributions.append(instr_contrib)

        # ── Merge instruction contributions (structure-aware) ────────
        global_guidance = triage_result.get("global_instruction_guidance", "")
        existing_instr = _get_general_instructions(metadata_snapshot)

        if dict_contributions:
            existing_sections, _ = _parse_sections(
                _sanitize_plaintext_instructions(existing_instr) if existing_instr else ""
            )
            merged_sections: dict[str, list[str]] = {
                s: existing_sections.get(s, []) for s in INSTRUCTION_SECTION_ORDER
            }
            valid_keys = set(INSTRUCTION_SECTION_ORDER)
            for dc in dict_contributions:
                for key, value in dc.items():
                    if key not in valid_keys:
                        continue
                    if value == "":
                        merged_sections[key] = []
                    elif isinstance(value, str):
                        merged_sections[key] = [
                            ln for ln in value.splitlines() if ln.strip()
                        ]
            parts: list[str] = []
            for section in INSTRUCTION_SECTION_ORDER:
                lines = merged_sections[section]
                if not lines:
                    continue
                parts.append(f"{section}:")
                for ln in lines:
                    s = ln.strip()
                    if not s:
                        continue
                    if not s.startswith("- "):
                        s = f"- {s}"
                    parts.append(s)
                parts.append("")
            global_rewrite = _sanitize_plaintext_instructions("\n".join(parts).strip())
        elif str_contributions or global_guidance:
            global_rewrite = _merge_structured_instructions(
                existing=existing_instr,
                contributions=str_contributions,
                global_guidance=global_guidance,
            )
        else:
            global_rewrite = ""

        if global_rewrite and len(global_rewrite) > MAX_HOLISTIC_INSTRUCTION_CHARS:
            global_rewrite = global_rewrite[:MAX_HOLISTIC_INSTRUCTION_CHARS]
            logger.warning(
                "Merged instruction rewrite truncated to %d chars",
                MAX_HOLISTIC_INSTRUCTION_CHARS,
            )

        strategy: dict[str, Any] = {
            "action_groups": final_ags,
            "global_instruction_rewrite": global_rewrite,
            "rationale": triage_result.get("rationale", ""),
        }

        span.set_outputs({
            "action_groups_count": len(final_ags),
            "mode": "two_phase_progressive",
            "global_instruction_rewrite_len": len(global_rewrite),
            "rationale": str(strategy.get("rationale", ""))[:300],
        })

        print(
            f"\n  Strategy complete: {len(final_ags)} action group(s), "
            f"{len(global_rewrite)} chars instruction rewrite"
        )

    return strategy


def validate_join_spec_types(
    join_spec: dict,
    metadata_snapshot: dict,
) -> tuple[bool, str]:
    """Validate that join columns have compatible data types.

    Parses the ``sql`` array to extract column names from the join condition,
    looks up their ``data_type`` in the enriched metadata, and checks
    compatibility.

    Returns ``(valid, reason)`` — if valid is False, *reason* explains why.
    """
    sql_parts = join_spec.get("sql", [])
    if not sql_parts:
        return True, "no sql to validate"

    condition = sql_parts[0] if isinstance(sql_parts, list) else str(sql_parts)

    left_obj = join_spec.get("left", {})
    right_obj = join_spec.get("right", {})
    left_alias = left_obj.get("alias", "") if isinstance(left_obj, dict) else ""
    right_alias = right_obj.get("alias", "") if isinstance(right_obj, dict) else ""
    left_ident = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
    right_ident = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""

    # Parse "= " join conditions: `left_alias`.`col` = `right_alias`.`col`
    pattern = r"`([^`]+)`\s*\.\s*`([^`]+)`\s*=\s*`([^`]+)`\s*\.\s*`([^`]+)`"
    match = re.search(pattern, condition)
    if not match:
        return True, "could not parse join condition columns"

    cond_left_alias, cond_left_col = match.group(1), match.group(2)
    cond_right_alias, cond_right_col = match.group(3), match.group(4)

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    col_types: dict[tuple[str, str], str] = {}
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = _short_name(ident).lower()
        fqn_lower = ident.lower()
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            dt = cc.get("data_type", "")
            if col_name and dt:
                col_types[(short, col_name)] = str(dt).upper()
                col_types[(fqn_lower, col_name)] = str(dt).upper()

    left_type = (
        col_types.get((left_ident.lower(), cond_left_col.lower()), "")
        or col_types.get((cond_left_alias.lower(), cond_left_col.lower()), "")
    )
    right_type = (
        col_types.get((right_ident.lower(), cond_right_col.lower()), "")
        or col_types.get((cond_right_alias.lower(), cond_right_col.lower()), "")
    )

    if not left_type or not right_type:
        return True, "type info unavailable, skipping validation"

    if _types_compatible(left_type, right_type):
        return True, f"types compatible: {left_type} ↔ {right_type}"

    return False, (
        f"incompatible types: {cond_left_alias}.{cond_left_col} ({left_type}) "
        f"↔ {cond_right_alias}.{cond_right_col} ({right_type})"
    )


def _is_generic_counterfactual(fix: str) -> bool:
    """Return True if the counterfactual_fix is too vague to drive optimization."""
    if not fix:
        return True
    lower = fix.strip().lower()
    if any(lower.startswith(p) for p in GENERIC_FIX_PREFIXES):
        has_specific_ref = any(
            tok in lower
            for tok in (".", "_", "column", "table ", "tvf", "function ")
            if len(tok) > 1
        )
        if not has_specific_ref:
            return True
    return False


def _describe_fix(cluster: dict) -> str:
    """Describe the fix for a cluster, preferring specific ASI fixes."""
    asi_fixes = [f for f in cluster.get("asi_counterfactual_fixes", []) if f]
    specific_fixes = [f for f in asi_fixes if not _is_generic_counterfactual(f)]
    if specific_fixes:
        return specific_fixes[0]
    if cluster.get("root_cause") == "repeatability_issue":
        dominant_asset = cluster.get("asi_blame_set") or cluster.get(
            "dominant_asset", "TABLE"
        )
        base = REPEATABILITY_FIX_BY_ASSET.get(
            dominant_asset, REPEATABILITY_FIX_BY_ASSET["TABLE"]
        )
        return f"{base} (affects {len(cluster['question_ids'])} questions)"
    wrong_clause = cluster.get("asi_wrong_clause") or ""
    blame = cluster.get("asi_blame_set") or ""
    if wrong_clause and blame:
        return (
            f"Fix {cluster['root_cause']}: wrong clause '{wrong_clause}' "
            f"in {blame} affecting {len(cluster['question_ids'])} questions."
        )
    return (
        f"Fix {cluster['root_cause']} affecting "
        f"{len(cluster['question_ids'])} questions. "
        f"Judge: {cluster['affected_judge']}."
    )


def _deduplicate_clusters(clusters: list[dict]) -> list[dict]:
    """Merge clusters with identical (root_cause, question_ids) into one representative.

    Keeps the cluster with the highest confidence and tracks all merged judge
    names so attribution is not lost.
    """
    seen: dict[tuple, dict] = {}
    for c in clusters:
        key = (c.get("root_cause", ""), tuple(sorted(c.get("question_ids", []))))
        if key not in seen or c.get("confidence", 0) > seen[key].get("confidence", 0):
            merged = dict(c)
            merged.setdefault("merged_judges", [])
            seen[key] = merged
        seen[key]["merged_judges"].append(c.get("affected_judge", ""))
    return list(seen.values())


_INSTRUCTION_CONTENT_PATTERNS = re.compile(
    r"(?i)(ROUTING\s*RULES|MUST\s+follow|MUST\s+use|TVF\s+ROUTING|"
    r"METRIC\s+VIEW|MEASURE\s*\(|GROUP\s+BY|ORDER\s+BY|WHERE\s+.*=|"
    r"SELECT\s+\*\s+FROM|CRITICAL\s+ROUTING|enable_format_assistance|"
    r"enable_entity_matching)",
)


def _detect_instruction_content_in_description(
    metadata_snapshot: dict,
) -> list[dict]:
    """Check if the Genie Space description contains instruction-like content.

    Returns a list of ``update_description`` proposals that strip the
    instruction content, keeping only the user-facing summary paragraph.
    """
    config = metadata_snapshot.get("config") or {}
    desc = config.get("description") or ""
    if isinstance(desc, list):
        desc = "\n".join(desc)
    if not desc or not _INSTRUCTION_CONTENT_PATTERNS.search(desc):
        return []

    paragraphs = desc.split("\n\n")
    summary_parts: list[str] = []
    for para in paragraphs:
        if _INSTRUCTION_CONTENT_PATTERNS.search(para):
            break
        summary_parts.append(para.strip())
    clean_desc = "\n\n".join(summary_parts).strip()
    if not clean_desc or clean_desc == desc.strip():
        return []

    logger.info(
        "Description contains instruction-like content (%d chars). "
        "Proposing cleanup to %d chars.",
        len(desc),
        len(clean_desc),
    )
    return [
        {
            "patch_type": "update_description",
            "scope": "genie_config",
            "target": "genie_space",
            "proposed_value": clean_desc,
            "old_value": desc,
            "rationale": (
                "Description contained LLM-facing routing rules and SQL patterns. "
                "Stripped to user-facing summary only."
            ),
            "lever": 5,
            "net_impact": 1,
            "question_ids": [],
            "asi": {
                "failure_type": "missing_instruction",
                "severity": "minor",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
        }
    ]


def _validate_lever5_proposals(
    proposals: list[dict],
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Filter out empty, generic, over-length, or hallucinated Lever 5 proposals."""
    from genie_space_optimizer.common.config import MAX_INSTRUCTION_TEXT_CHARS
    from genie_space_optimizer.common.genie_schema import count_instruction_slots, MAX_INSTRUCTION_SLOTS

    _tables = metadata_snapshot.get("tables") or []
    _funcs = metadata_snapshot.get("functions") or []
    _mvs = metadata_snapshot.get("metric_views") or []
    known_assets: set[str] = set()
    for t in _tables:
        name = (t.get("name") or t.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for f in _funcs:
        name = (f.get("name") or f.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for m in _mvs:
        name = (m.get("name") or m.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    known_assets.discard("")

    id_allowlist = _build_identifier_allowlist(metadata_snapshot)

    existing_eqs_raw = (
        (metadata_snapshot.get("config") or metadata_snapshot).get("example_question_sqls")
        or metadata_snapshot.get("example_question_sqls")
        or []
    )
    existing_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_questions.add(q)

    seen_new_questions: set[str] = set()

    current_slots = count_instruction_slots(metadata_snapshot)
    remaining_budget = MAX_INSTRUCTION_SLOTS - current_slots
    added_slots = 0

    valid: list[dict] = []
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype not in ("add_instruction", "add_example_sql", "rewrite_instruction"):
            valid.append(p)
            continue

        if ptype == "rewrite_instruction":
            text = (p.get("proposed_value") or "").strip()
            if not text:
                logger.info("Rejecting empty rewrite_instruction proposal")
                continue
            if len(text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Rejecting rewrite_instruction exceeding %d chars (%d chars)",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(text),
                )
                continue
            _MIN_INSTRUCTION_LEN = 50
            if len(text) < _MIN_INSTRUCTION_LEN:
                logger.warning(
                    "Rejecting rewrite_instruction below minimum length (%d < %d chars)",
                    len(text), _MIN_INSTRUCTION_LEN,
                )
                continue
            found_sections = [
                s for s in INSTRUCTION_SECTION_ORDER
                if re.search(rf'^{re.escape(s)}:', text, re.MULTILINE)
            ]
            if not found_sections:
                logger.warning(
                    "rewrite_instruction has no recognized structured sections "
                    "(expected ALL-CAPS headers like PURPOSE:, ASSET ROUTING:, etc.). "
                    "Content will still be accepted but may lack structure."
                )
            valid.append(p)
            continue

        if ptype == "add_instruction":
            text = (p.get("proposed_value") or p.get("new_text") or "").strip()
            if not text:
                logger.info("Rejecting empty add_instruction proposal")
                continue
            if len(text) > MAX_INSTRUCTION_TEXT_CHARS:
                logger.warning(
                    "Rejecting add_instruction proposal exceeding %d chars (%d chars)",
                    MAX_INSTRUCTION_TEXT_CHARS,
                    len(text),
                )
                continue
            text_lower = text.lower()
            if known_assets and not any(a in text_lower for a in known_assets):
                logger.warning(
                    "Rejecting generic add_instruction (no known asset referenced): %.100s...",
                    text,
                )
                continue

        if ptype == "add_example_sql":
            eq = (p.get("example_question") or "").strip()
            es = (p.get("example_sql") or "").strip()
            if not eq or not es:
                logger.info("Rejecting empty add_example_sql proposal")
                continue

            if added_slots >= remaining_budget:
                logger.warning(
                    "Dropping add_example_sql — slot budget exhausted (%d/%d)",
                    current_slots + added_slots, MAX_INSTRUCTION_SLOTS,
                )
                continue

            eq_norm = eq.lower().strip()
            if eq_norm in existing_questions:
                logger.info("Rejecting add_example_sql duplicate of existing config: %.80s", eq)
                continue
            if any(_ngram_similarity(eq_norm, eq_existing) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                   for eq_existing in existing_questions):
                logger.info("Rejecting add_example_sql fuzzy-duplicate of existing config: %.80s", eq)
                continue
            if eq_norm in seen_new_questions:
                logger.info("Rejecting add_example_sql duplicate within batch: %.80s", eq)
                continue
            if any(_ngram_similarity(eq_norm, seen) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                   for seen in seen_new_questions):
                logger.info("Rejecting add_example_sql fuzzy-duplicate within batch: %.80s", eq)
                continue
            seen_new_questions.add(eq_norm)

            sql_ok, violations = _validate_sql_identifiers(es, id_allowlist)
            if not sql_ok:
                logger.warning(
                    "Rejecting add_example_sql with hallucinated identifiers: %s — %.120s",
                    violations, es,
                )
                continue

            if spark is not None:
                try:
                    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql
                    is_valid, err = validate_ground_truth_sql(
                        es, spark, catalog=catalog, gold_schema=gold_schema,
                        execute=True,
                        parameters=p.get("parameters"),
                        w=w, warehouse_id=warehouse_id,
                    )
                    if not is_valid:
                        logger.warning(
                            "Rejecting add_example_sql that failed validation: %s — %.120s",
                            err, es,
                        )
                        continue
                except Exception:
                    logger.debug("Example SQL execution validation skipped (error)", exc_info=True)

            added_slots += 1

        valid.append(p)

    rejected = len(proposals) - len(valid)
    if rejected:
        logger.info(
            "Lever 5 proposal validation: %d rejected, %d kept", rejected, len(valid)
        )
    return valid


def _mine_benchmark_example_sqls(
    benchmarks: list[dict],
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Mine benchmarks with ``expected_sql`` as ready-made example SQL proposals.

    Skips benchmarks whose question already exists in the config's
    ``example_question_sqls``.  Validates each via
    ``validate_ground_truth_sql(..., execute=True)`` so only SQL that
    actually runs and returns rows is proposed.

    Respects the 100-slot instruction budget — stops mining once the
    remaining slot budget is exhausted.
    """
    from genie_space_optimizer.common.genie_schema import count_instruction_slots, MAX_INSTRUCTION_SLOTS

    current_slots = count_instruction_slots(metadata_snapshot)
    remaining_budget = max(0, MAX_INSTRUCTION_SLOTS - current_slots)

    if remaining_budget <= 0:
        logger.info(
            "Benchmark mining: skipping — slot budget already exhausted (%d/%d)",
            current_slots, MAX_INSTRUCTION_SLOTS,
        )
        print(
            f"\n-- BENCHMARK EXAMPLE SQL MINING {'─' * 40}\n"
            f"  |  Slot budget exhausted:      {current_slots}/{MAX_INSTRUCTION_SLOTS} — skipping\n"
            + "─" * 72
        )
        return []

    existing_eqs_raw = (
        (metadata_snapshot.get("config") or metadata_snapshot).get("example_question_sqls")
        or metadata_snapshot.get("example_question_sqls")
        or []
    )
    existing_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_questions.add(q)

    proposals: list[dict] = []
    skipped_no_sql = 0
    skipped_dup = 0
    skipped_invalid = 0

    for b in benchmarks:
        sql = (b.get("expected_sql") or "").strip()
        question = (b.get("question") or "").strip()
        if not sql or not question:
            skipped_no_sql += 1
            continue

        q_norm = question.lower().strip()
        if q_norm in existing_questions or any(
            _ngram_similarity(q_norm, eq) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
            for eq in existing_questions
        ):
            skipped_dup += 1
            continue
        existing_questions.add(q_norm)

        if spark is not None:
            try:
                from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql
                is_valid, err = validate_ground_truth_sql(
                    sql, spark, catalog=catalog, gold_schema=gold_schema,
                    execute=True,
                    parameters=b.get("parameters"),
                    w=w, warehouse_id=warehouse_id,
                )
                if not is_valid:
                    logger.info(
                        "Benchmark mining: skipping invalid SQL for '%.60s': %s",
                        question, err,
                    )
                    skipped_invalid += 1
                    if skipped_invalid <= 3:
                        print(f"  |    [skip] '{question[:50]}': {err[:120]}")
                    continue
                try:
                    test_rows = spark.sql(f"SELECT * FROM ({sql}) LIMIT 1").collect()
                    if not test_rows:
                        logger.info(
                            "Benchmark mining: skipping 0-row result for '%.60s'",
                            question,
                        )
                        skipped_invalid += 1
                        continue
                except Exception:
                    pass
            except Exception as _val_exc:
                logger.debug("Benchmark mining: validation error, skipping", exc_info=True)
                skipped_invalid += 1
                if skipped_invalid <= 3:
                    print(f"  |    [skip] '{question[:50]}': {type(_val_exc).__name__}: {str(_val_exc)[:100]}")
                continue

        if len(proposals) >= remaining_budget:
            logger.info(
                "Benchmark mining: stopping — slot budget exhausted (%d/%d used)",
                current_slots + len(proposals), MAX_INSTRUCTION_SLOTS,
            )
            break

        proposals.append({
            "proposal_id": f"P_BM_{len(proposals) + 1:03d}",
            "cluster_id": f"BENCHMARK_EX_{len(proposals) + 1:03d}",
            "lever": 5,
            "scope": "genie_config",
            "patch_type": "add_example_sql",
            "change_description": f"Benchmark-mined example SQL: {question[:80]}",
            "proposed_value": question,
            "example_question": question,
            "example_sql": sql,
            "parameters": b.get("parameters", []) or [],
            "usage_guidance": f"Mined from benchmark ground truth for: {question[:100]}",
            "rationale": "Pre-validated benchmark SQL mined as example SQL",
            "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
            "confidence": 0.95,
            "questions_fixed": 1,
            "questions_at_risk": 0,
            "net_impact": 0.95,
            "asi": {
                "failure_type": "asset_routing_error",
                "blame_set": [],
                "severity": "minor",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
            "provenance": {
                "cluster_id": f"BENCHMARK_EX_{len(proposals):03d}",
                "root_cause": "benchmark_mining",
                "originating_questions": [question],
                "lever": 5,
                "lever_name": "Benchmark Example SQL Mining",
                "patch_type": "add_example_sql",
            },
        })

    logger.info(
        "Benchmark mining: %d proposals, %d skipped (no sql=%d, dup=%d, invalid=%d)",
        len(proposals), skipped_no_sql + skipped_dup + skipped_invalid,
        skipped_no_sql, skipped_dup, skipped_invalid,
    )
    print(
        f"\n-- BENCHMARK EXAMPLE SQL MINING {'─' * 40}\n"
        f"  |  Benchmarks scanned:         {len(benchmarks)}\n"
        f"  |  Slot budget:                {current_slots}/{MAX_INSTRUCTION_SLOTS} "
        f"(remaining: {remaining_budget})\n"
        f"  |  Skipped (no SQL):           {skipped_no_sql}\n"
        f"  |  Skipped (duplicate):        {skipped_dup}\n"
        f"  |  Skipped (invalid SQL):      {skipped_invalid}\n"
        f"  |  Proposals generated:        {len(proposals)}\n"
        + "─" * 72
    )
    return proposals


def _ngram_similarity(a: str, b: str, n: int = 3) -> float:
    """Compute Jaccard similarity over character n-grams."""
    if not a or not b:
        return 0.0
    a_lower, b_lower = a.lower(), b.lower()
    a_ngrams = {a_lower[i : i + n] for i in range(len(a_lower) - n + 1)}
    b_ngrams = {b_lower[i : i + n] for i in range(len(b_lower) - n + 1)}
    if not a_ngrams or not b_ngrams:
        return 0.0
    return len(a_ngrams & b_ngrams) / len(a_ngrams | b_ngrams)


def _filter_no_op_proposals(proposals: list[dict], metadata_snapshot: dict) -> list[dict]:
    """Remove proposals that don't meaningfully change the current metadata.

    For ``update_column_description`` / ``update_description``: drops the
    proposal if the proposed value is essentially identical to the existing
    column description (after normalizing whitespace).
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    existing_descs: dict[tuple[str, str], str] = {}
    for tbl in tables:
        tbl_name = (tbl.get("name") or tbl.get("identifier") or "").lower()
        short_name = tbl_name.rsplit(".", 1)[-1] if "." in tbl_name else tbl_name
        for col in tbl.get("columns", tbl.get("column_configs", [])):
            col_name = (col.get("name") or col.get("column_name") or "").lower()
            raw_desc = col.get("description") or col.get("comment") or ""
            if isinstance(raw_desc, list):
                raw_desc = " ".join(str(x) for x in raw_desc)
            desc = re.sub(r"\s+", " ", str(raw_desc).strip().lower())
            if desc:
                existing_descs[(tbl_name, col_name)] = desc
                existing_descs[(short_name, col_name)] = desc

    existing_eqs_raw = (
        (metadata_snapshot.get("config") or metadata_snapshot).get("example_question_sqls")
        or metadata_snapshot.get("example_question_sqls")
        or []
    )
    existing_eq_questions: set[str] = set()
    for e in existing_eqs_raw:
        if isinstance(e, dict):
            q = e.get("question", "")
            if isinstance(q, list):
                q = " ".join(q)
            q = q.lower().strip()
            if q:
                existing_eq_questions.add(q)

    kept: list[dict] = []
    dropped = 0
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = (p.get("table") or p.get("target_table") or "").lower()
            col = (p.get("column") or p.get("target_column") or "").lower()
            proposed = re.sub(r"\s+", " ", (p.get("proposed_value") or "").strip().lower())
            short_tbl = tbl.rsplit(".", 1)[-1] if "." in tbl else tbl
            current = existing_descs.get((tbl, col)) or existing_descs.get((short_tbl, col)) or ""
            if current and proposed and _ngram_similarity(current, proposed) > 0.97:
                dropped += 1
                continue
        if ptype == "add_example_sql":
            eq = (p.get("example_question") or "").lower().strip()
            if eq and (
                eq in existing_eq_questions
                or any(_ngram_similarity(eq, existing) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD
                       for existing in existing_eq_questions)
            ):
                logger.info("Filtering no-op/near-duplicate add_example_sql: %.80s", eq)
                dropped += 1
                continue
        kept.append(p)

    if dropped:
        logger.info(
            "Filtered %d no-op proposals (description unchanged from current metadata)",
            dropped,
        )
    return kept


def _deduplicate_proposals(proposals: list[dict]) -> list[dict]:
    """Remove duplicate proposals using type-aware deduplication.

    - ``update_column_description`` / ``add_column_synonym``: dedup by (table, column).
    - ``add_instruction``: merge near-duplicates (ngram similarity > 0.7).
    - ``add_example_sql``: dedup by normalized SQL text.
    - Others: dedup by exact (patch_type, proposed_value).
    """
    out: list[dict] = []

    col_desc_best: dict[tuple[str, str], int] = {}
    instruction_entries: list[tuple[int, dict]] = []
    example_sql_seen: dict[str, int] = {}
    exact_seen: dict[tuple[str, str], int] = {}

    for p in proposals:
        ptype = p.get("patch_type", "")
        val = (p.get("proposed_value") or "").strip()
        impact = p.get("net_impact", 0)

        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = p.get("table", p.get("target_table", ""))
            col = p.get("column", p.get("target_column", ""))
            key = (tbl, col)
            if key in col_desc_best:
                existing_idx = col_desc_best[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            col_desc_best[key] = len(out)
            out.append(p)

        elif ptype == "add_instruction" and val:
            merged = False
            for existing_idx, existing_p in instruction_entries:
                existing_val = (existing_p.get("proposed_value") or "").strip()
                if _ngram_similarity(val, existing_val) > 0.7:
                    if impact > out[existing_idx].get("net_impact", 0):
                        out[existing_idx] = p
                        instruction_entries = [
                            (ei, ep) if ei != existing_idx else (ei, p)
                            for ei, ep in instruction_entries
                        ]
                    merged = True
                    break
            if not merged:
                instruction_entries.append((len(out), p))
                out.append(p)

        elif ptype == "add_example_sql" and val:
            normalized = re.sub(r"\s+", " ", val.lower()).strip()
            if normalized in example_sql_seen:
                existing_idx = example_sql_seen[normalized]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            fuzzy_match_idx = None
            for seen_norm, seen_idx in example_sql_seen.items():
                if _ngram_similarity(normalized, seen_norm) > _EXAMPLE_SQL_SIMILARITY_THRESHOLD:
                    fuzzy_match_idx = seen_idx
                    break
            if fuzzy_match_idx is not None:
                if impact > out[fuzzy_match_idx].get("net_impact", 0):
                    out[fuzzy_match_idx] = p
                continue
            example_sql_seen[normalized] = len(out)
            out.append(p)

        else:
            key = (ptype, val)
            if key in exact_seen:
                existing_idx = exact_seen[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            exact_seen[key] = len(out)
            out.append(p)

    return out


def _merge_overlapping_instructions(proposals: list[dict]) -> list[dict]:
    """Merge ``add_instruction`` proposals that share >70% keyword overlap.

    Keeps all non-instruction proposals intact. For instructions, groups those
    with high overlap and concatenates into a single combined instruction.
    """
    instructions: list[dict] = []
    others: list[dict] = []
    for p in proposals:
        if p.get("patch_type") == "add_instruction":
            instructions.append(p)
        else:
            others.append(p)

    if len(instructions) <= 1:
        return proposals

    def _keywords(text: str) -> set[str]:
        return {w.lower() for w in re.findall(r"\b\w{3,}\b", text)}

    merged: list[dict] = []
    used: set[int] = set()

    for i, p1 in enumerate(instructions):
        if i in used:
            continue
        group = [p1]
        kw1 = _keywords(p1.get("proposed_value", ""))
        for j, p2 in enumerate(instructions):
            if j <= i or j in used:
                continue
            kw2 = _keywords(p2.get("proposed_value", ""))
            if kw1 and kw2:
                overlap = len(kw1 & kw2) / len(kw1 | kw2)
                if overlap > 0.7:
                    group.append(p2)
                    used.add(j)
        used.add(i)

        if len(group) == 1:
            merged.append(group[0])
        else:
            best = max(group, key=lambda g: g.get("net_impact", 0))
            total_questions = sum(g.get("questions_fixed", 0) for g in group)
            best = dict(best)
            best["questions_fixed"] = total_questions
            best["merged_count"] = len(group)
            merged.append(best)
            logger.info(
                "Merged %d overlapping instructions into one (kept best net_impact=%.2f)",
                len(group), best.get("net_impact", 0),
            )

    return others + merged


_LEVER_NAMES = {0: "Proactive Enrichment", 1: "Tables & Columns", 2: "Metric Views", 3: "Table-Valued Functions", 4: "Join Specifications", 5: "Genie Space Instructions"}


def _build_provenance(cluster: dict, lever: int, patch_type: str) -> dict:
    """Build a provenance dict from a cluster's question_traces."""
    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "root_cause": cluster.get("root_cause", "other"),
        "originating_questions": cluster.get("question_traces", []),
        "lever": lever,
        "lever_name": _LEVER_NAMES.get(lever, f"Lever {lever}"),
        "patch_type": patch_type,
    }


def generate_proposals_from_strategy(
    strategy: dict,
    action_group: dict,
    metadata_snapshot: dict,
    target_lever: int,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
) -> list[dict]:
    """Generate proposals for a single lever guided by the holistic strategy.

    Each lever acts as an *executor*: it receives the strategist's directives
    for its action group and generates the concrete patch proposals accordingly.
    """
    import mlflow

    proposals: list[dict] = []
    ag_id = action_group.get("id", "AG?")
    directives = action_group.get("lever_directives", {})
    lever_key = str(target_lever)
    lever_dir = directives.get(lever_key, {})

    if not lever_dir and target_lever != 5:
        return proposals

    scope = _resolve_scope(target_lever, apply_mode)
    coordination_notes = action_group.get("coordination_notes", "")
    root_cause = action_group.get("root_cause_summary", "")
    affected_qs = action_group.get("affected_questions", [])
    q_fixed = len(affected_qs)
    source_clusters = action_group.get("source_cluster_ids", [])

    provenance_base = {
        "cluster_id": ag_id,
        "root_cause": root_cause,
        "originating_questions": [],
        "lever": target_lever,
        "lever_name": _LEVER_NAMES.get(target_lever, f"Lever {target_lever}"),
        "patch_type": "",
    }

    from mlflow.entities import SpanType as _SpanType

    with mlflow.start_span(
        name=f"generate_proposals_lever_{target_lever}_ag_{ag_id}",
        span_type=_SpanType.CHAIN,
    ) as span:
        span.set_inputs({
            "action_group_id": ag_id,
            "target_lever": target_lever,
            "root_cause": root_cause[:200],
            "affected_questions": len(affected_qs),
            "directives_keys": list(lever_dir.keys()) if isinstance(lever_dir, dict) else [],
        })

        # ── Lever 1 / 2: table + column metadata ────────────────────────
        if target_lever in (1, 2):
            for tbl_entry in lever_dir.get("tables", []):
                if not isinstance(tbl_entry, dict):
                    continue
                tbl = tbl_entry.get("table", "")
                tbl_sections = tbl_entry.get("sections", {})
                tbl_etype = tbl_entry.get("entity_type", "table")
                if tbl and isinstance(tbl_sections, dict) and tbl_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": target_lever,
                        "scope": scope,
                        "patch_type": "update_description",
                        "change_description": f"[{ag_id}] Update table {tbl} sections={list(tbl_sections.keys())}",
                        "proposed_value": "",
                        "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(target_lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_description"},
                        "table": tbl,
                        "table_sections": tbl_sections,
                        "table_entity_type": tbl_etype,
                    })

            for col_entry in lever_dir.get("columns", []):
                if not isinstance(col_entry, dict):
                    continue
                tbl = col_entry.get("table", "")
                col = col_entry.get("column", "")
                col_sections = col_entry.get("sections", {})
                col_etype = col_entry.get("entity_type", "")
                if tbl and col and isinstance(col_sections, dict) and col_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": target_lever,
                        "scope": scope,
                        "patch_type": "update_column_description",
                        "change_description": f"[{ag_id}] Update {tbl}.{col} sections={list(col_sections.keys())}",
                        "proposed_value": "",
                        "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(target_lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_column_description"},
                        "table": tbl,
                        "column": col,
                        "column_sections": col_sections,
                        "column_entity_type": col_etype,
                    })

        # ── Lever 3: functions ───────────────────────────────────────────
        elif target_lever == 3:
            for fn_entry in lever_dir.get("functions", []):
                if not isinstance(fn_entry, dict):
                    continue
                fn_name = fn_entry.get("function", "")
                fn_sections = fn_entry.get("sections", {})
                if fn_name and isinstance(fn_sections, dict) and fn_sections:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": 3,
                        "scope": scope,
                        "patch_type": "update_function_description",
                        "change_description": f"[{ag_id}] Update function {fn_name} sections={list(fn_sections.keys())}",
                        "proposed_value": "",
                        "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(3, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.85,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.85, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "update_function_description"},
                        "function": fn_name,
                        "function_sections": fn_sections,
                    })

        # ── Lever 4: join specs ──────────────────────────────────────────
        elif target_lever == 4:
            for js_entry in lever_dir.get("join_specs", []):
                if not isinstance(js_entry, dict):
                    continue
                left_table = js_entry.get("left_table", "")
                right_table = js_entry.get("right_table", "")
                guidance = js_entry.get("join_guidance", "")
                if left_table and right_table:
                    sanitized_guidance = _sanitize_join_sql(guidance) if guidance else ""
                    join_spec = ensure_join_spec_fields({
                        "left": {"identifier": left_table},
                        "right": {"identifier": right_table},
                        "sql": [sanitized_guidance] if sanitized_guidance else [],
                    })
                    valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                    if not valid:
                        logger.info("[%s] Join spec rejected (type mismatch): %s", ag_id, reason)
                        continue
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": ag_id,
                        "lever": 4,
                        "scope": "genie_config",
                        "patch_type": "add_join_spec",
                        "change_description": f"[{ag_id}] Join: {left_table} ↔ {right_table}",
                        "proposed_value": "",
                        "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                        "join_spec": join_spec,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.8,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": max(q_fixed * 0.8, 1.0),
                        "asi": {
                            "failure_type": root_cause,
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "add_join_spec"},
                    })

            discovery_hints = discover_join_candidates(metadata_snapshot)
            if discovery_hints:
                discovery_specs = _call_llm_for_join_discovery(
                    metadata_snapshot, discovery_hints, w=w,
                )
                for spec_result in discovery_specs:
                    join_spec = spec_result.get("join_spec")
                    if not isinstance(join_spec, dict):
                        continue
                    join_spec = ensure_join_spec_fields(join_spec)
                    spec_result["join_spec"] = join_spec
                    valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                    if not valid:
                        logger.info("[%s] Discovery join rejected: %s", ag_id, reason)
                        continue
                    left_obj = join_spec.get("left", {})
                    right_obj = join_spec.get("right", {})
                    left_id = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
                    right_id = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
                    sql_parts = join_spec.get("sql", [])
                    condition = sql_parts[0] if sql_parts else ""
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_DISC_{len(proposals) + 1:03d}",
                        "lever": 4,
                        "scope": "genie_config",
                        "patch_type": "add_join_spec",
                        "change_description": f"[{ag_id}] Discover join: {condition}" if condition else f"[{ag_id}] Join: {left_id} ↔ {right_id}",
                        "proposed_value": "",
                        "rationale": spec_result.get("rationale", "LLM-assisted join discovery"),
                        "join_spec": join_spec,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.7,
                        "questions_fixed": 0,
                        "questions_at_risk": 0,
                        "net_impact": 0.35,
                        "asi": {
                            "failure_type": "missing_join_spec",
                            "blame_set": [],
                            "severity": "minor",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                    })

        # ── Lever 5: instructions + example SQL ──────────────────────────
        elif target_lever == 5:
            l5_dir = lever_dir or {}
            instruction_sections = l5_dir.get("instruction_sections")
            instruction_guidance = (l5_dir.get("instruction_guidance") or "").strip()

            example_sqls_list = l5_dir.get("example_sqls", [])
            if not example_sqls_list:
                legacy = l5_dir.get("example_sql")
                if isinstance(legacy, dict):
                    example_sqls_list = [legacy]
            if not isinstance(example_sqls_list, list):
                example_sqls_list = [example_sqls_list] if isinstance(example_sqls_list, dict) else []

            if isinstance(instruction_sections, dict) and instruction_sections:
                from genie_space_optimizer.optimization.applier import _get_general_instructions

                valid_keys = set(INSTRUCTION_SECTION_ORDER)
                invalid = [k for k in instruction_sections if k not in valid_keys]
                if invalid:
                    logger.warning("Ignoring unknown instruction sections: %s", invalid)
                    instruction_sections = {
                        k: v for k, v in instruction_sections.items() if k in valid_keys
                    }

                current_instructions = _get_general_instructions(metadata_snapshot)
                existing_sections, _ = _parse_sections(
                    _sanitize_plaintext_instructions(current_instructions) if current_instructions else ""
                )
                merged_secs: dict[str, list[str]] = {
                    s: list(existing_sections.get(s, []))
                    for s in INSTRUCTION_SECTION_ORDER
                }
                for key, value in instruction_sections.items():
                    if not isinstance(value, str):
                        continue
                    if value == "":
                        merged_secs[key] = []
                    else:
                        merged_secs[key] = [ln for ln in value.splitlines() if ln.strip()]

                parts: list[str] = []
                for section in INSTRUCTION_SECTION_ORDER:
                    lines = merged_secs[section]
                    if not lines:
                        continue
                    parts.append(f"{section}:")
                    for ln in lines:
                        s = ln.strip()
                        if not s:
                            continue
                        if not s.startswith("- "):
                            s = f"- {s}"
                        parts.append(s)
                    parts.append("")
                merged_text = _sanitize_plaintext_instructions("\n".join(parts).strip())

                if (
                    current_instructions
                    and merged_text
                    and len(current_instructions.strip()) > 50
                    and len(merged_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
                ):
                    logger.warning(
                        "Instruction rewrite would lose content (%d -> %d chars) "
                        "— force-merging existing instructions",
                        len(current_instructions), len(merged_text),
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": ag_id,
                    "lever": 5,
                    "scope": "genie_config",
                    "patch_type": "rewrite_instruction",
                    "change_description": f"[{ag_id}] Instruction rewrite ({len(merged_text)} chars)",
                    "proposed_value": merged_text,
                    "old_value": current_instructions,
                    "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                    "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.85,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": max(q_fixed * 0.85, 1.0),
                    "asi": {
                        "failure_type": "missing_instruction",
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "provenance": {**provenance_base, "patch_type": "rewrite_instruction"},
                })

            elif instruction_guidance:
                from genie_space_optimizer.optimization.applier import _get_general_instructions

                current_instructions = _get_general_instructions(metadata_snapshot)
                merged_text = _merge_structured_instructions(
                    existing=current_instructions,
                    contributions=[instruction_guidance],
                )
                if (
                    current_instructions
                    and merged_text
                    and len(current_instructions.strip()) > 50
                    and len(merged_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
                ):
                    logger.warning(
                        "Instruction rewrite would lose content (%d -> %d chars) "
                        "— force-merging existing instructions",
                        len(current_instructions), len(merged_text),
                    )
                    merged_text = _merge_structured_instructions(
                        existing=current_instructions,
                        contributions=[merged_text],
                    )
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": ag_id,
                    "lever": 5,
                    "scope": "genie_config",
                    "patch_type": "rewrite_instruction",
                    "change_description": f"[{ag_id}] Instruction rewrite ({len(merged_text)} chars)",
                    "proposed_value": merged_text,
                    "old_value": current_instructions,
                    "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                    "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.85,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": max(q_fixed * 0.85, 1.0),
                    "asi": {
                        "failure_type": "missing_instruction",
                        "blame_set": source_clusters,
                        "severity": "major",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                    "provenance": {**provenance_base, "patch_type": "rewrite_instruction"},
                })

            for ex_idx, example_sql_dir in enumerate(example_sqls_list):
                if not isinstance(example_sql_dir, dict):
                    continue
                eq = (example_sql_dir.get("question") or "").strip()
                es = (example_sql_dir.get("sql_sketch") or "").strip()
                if eq and es:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_EX{ex_idx + 1}",
                        "lever": 5,
                        "scope": "genie_config",
                        "patch_type": "add_example_sql",
                        "change_description": f"[{ag_id}] Example SQL {ex_idx + 1}: {eq[:80]}",
                        "proposed_value": eq,
                        "example_question": eq,
                        "example_sql": es,
                        "parameters": example_sql_dir.get("parameters", []),
                        "usage_guidance": example_sql_dir.get("usage_guidance", ""),
                        "rationale": f"Strategy: {root_cause}. {coordination_notes}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.8,
                        "questions_fixed": 1,
                        "questions_at_risk": 0,
                        "net_impact": 0.8,
                        "asi": {
                            "failure_type": "asset_routing_error",
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "add_example_sql"},
                    })

        # ── Example SQL from any lever ────────────────────────────────────
        if target_lever != 5 and isinstance(lever_dir, dict):
            ex_sqls = lever_dir.get("example_sqls", [])
            if not ex_sqls:
                legacy_ex = lever_dir.get("example_sql")
                if isinstance(legacy_ex, dict):
                    ex_sqls = [legacy_ex]
            if not isinstance(ex_sqls, list):
                ex_sqls = [ex_sqls] if isinstance(ex_sqls, dict) else []
            for ex_idx, ex_sql in enumerate(ex_sqls):
                if not isinstance(ex_sql, dict):
                    continue
                eq = (ex_sql.get("question") or "").strip()
                es = (ex_sql.get("sql_sketch") or "").strip()
                if eq and es:
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": f"{ag_id}_L{target_lever}_EX{ex_idx + 1}",
                        "lever": 5,
                        "scope": "genie_config",
                        "patch_type": "add_example_sql",
                        "change_description": f"[{ag_id}] Lever {target_lever} example SQL {ex_idx + 1}: {eq[:80]}",
                        "proposed_value": eq,
                        "example_question": eq,
                        "example_sql": es,
                        "parameters": ex_sql.get("parameters", []),
                        "usage_guidance": ex_sql.get("usage_guidance", ""),
                        "rationale": f"Example SQL from lever {target_lever}: {root_cause}",
                        "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                        "confidence": 0.75,
                        "questions_fixed": 1,
                        "questions_at_risk": 0,
                        "net_impact": 0.75,
                        "asi": {
                            "failure_type": "asset_routing_error",
                            "blame_set": source_clusters,
                            "severity": "major",
                            "counterfactual_fixes": [],
                            "ambiguity_detected": False,
                        },
                        "provenance": {**provenance_base, "patch_type": "add_example_sql"},
                    })

        proposals = _validate_lever5_proposals(
            proposals, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        )
        proposals = _deduplicate_proposals(proposals)
        proposals = _filter_no_op_proposals(proposals, metadata_snapshot)
        proposals.sort(key=lambda p: p.get("net_impact", 0), reverse=True)

        span.set_outputs({
            "proposal_count": len(proposals),
            "proposal_types": [p.get("patch_type", "?") for p in proposals],
            "tables_affected": sorted({p.get("table", "") for p in proposals if p.get("table")}),
        })
        logger.info(
            "[%s] Lever %d generated %d proposal(s) from strategy directives",
            ag_id, target_lever, len(proposals),
        )

    return proposals


def generate_metadata_proposals(
    clusters: list[dict],
    metadata_snapshot: dict,
    target_lever: int | None = None,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    failed_levers: set[int] | None = None,
    lever_changes: list[dict] | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    benchmarks: list[dict] | None = None,
    warehouse_id: str = "",
) -> list[dict]:
    """Generate metadata change proposals from failure clusters.

    For each cluster, maps to a lever, calls the LLM to generate a concrete
    ``proposed_value``, resolves scope, and scores by net_impact.

    When *target_lever* is 5, uses **holistic mode**: a single LLM call
    synthesizes ALL evaluation learnings into a coherent instruction document
    (``rewrite_instruction``) plus targeted example SQL proposals. The
    *lever_changes* parameter provides context about what levers 1-4 already
    fixed in this iteration.

    When *failed_levers* is provided and *target_lever* is 5, clusters whose
    natural lever is in *failed_levers* are also included so that lever 5
    (instructions / example SQL) can act as a catch-all.
    """
    failed_levers = failed_levers or set()
    _pre_dedup = len(clusters)
    clusters = _deduplicate_clusters(clusters)
    if len(clusters) < _pre_dedup:
        logger.info(
            "Cluster dedup: %d -> %d (merged %d duplicates)",
            _pre_dedup, len(clusters), _pre_dedup - len(clusters),
        )
    proposals: list[dict] = []
    if target_lever == 5:
        desc_proposals = _detect_instruction_content_in_description(metadata_snapshot)
        proposals.extend(desc_proposals)

    # ── Holistic path for lever 5 ─────────────────────────────────────
    if target_lever == 5:
        all_lever5_clusters: list[dict] = []
        for cluster in clusters:
            natural_lever = _map_to_lever(
                cluster["root_cause"],
                asi_failure_type=cluster.get("asi_failure_type"),
                blame_set=cluster.get("asi_blame_set"),
                judge=cluster.get("affected_judge"),
            )
            if natural_lever == 5 or natural_lever in failed_levers:
                all_lever5_clusters.append(cluster)

        holistic_result = _call_llm_for_holistic_instructions(
            all_lever5_clusters if all_lever5_clusters else clusters,
            metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
        )

        instruction_text = holistic_result.get("instruction_text", "")
        if instruction_text:
            instruction_text = _sanitize_plaintext_instructions(instruction_text)
        example_proposals = holistic_result.get("example_sql_proposals", [])
        rationale = holistic_result.get("rationale", "")

        from genie_space_optimizer.optimization.applier import _get_general_instructions

        current_instructions = _get_general_instructions(metadata_snapshot)

        if instruction_text:
            if (
                current_instructions
                and len(current_instructions.strip()) > 50
                and len(instruction_text.strip()) < len(current_instructions.strip()) * _INSTR_LOSS_THRESHOLD
            ):
                logger.warning(
                    "Holistic instruction rewrite would lose content (%d -> %d chars) "
                    "— force-merging existing instructions",
                    len(current_instructions), len(instruction_text),
                )
                instruction_text = _merge_structured_instructions(
                    existing=current_instructions,
                    contributions=[instruction_text],
                )
            total_q = sum(len(c.get("question_ids", [])) for c in all_lever5_clusters)
            holistic_traces = []
            for c in (all_lever5_clusters or clusters):
                holistic_traces.extend(c.get("question_traces", []))
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": "HOLISTIC_L5",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "rewrite_instruction",
                "change_description": f"Holistic instruction rewrite ({len(instruction_text)} chars)",
                "proposed_value": instruction_text,
                "old_value": current_instructions,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.8,
                "questions_fixed": total_q,
                "questions_at_risk": 0,
                "net_impact": max(total_q * 0.8, 1.0),
                "asi": {
                    "failure_type": "missing_instruction",
                    "blame_set": [],
                    "severity": "major",
                    "counterfactual_fixes": [],
                    "ambiguity_detected": False,
                },
                "provenance": {
                    "cluster_id": "HOLISTIC_L5",
                    "root_cause": "missing_instruction",
                    "originating_questions": holistic_traces,
                    "lever": 5,
                    "lever_name": "Genie Space Instructions",
                    "patch_type": "rewrite_instruction",
                },
            })

        for idx, ex in enumerate(example_proposals):
            if not isinstance(ex, dict):
                continue
            eq = (ex.get("example_question") or "").strip()
            es = (ex.get("example_sql") or "").strip()
            if not eq or not es:
                continue
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": f"HOLISTIC_EX_{idx + 1:03d}",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "add_example_sql",
                "change_description": f"Example SQL: {eq[:80]}",
                "proposed_value": eq,
                "example_question": eq,
                "example_sql": es,
                "parameters": ex.get("parameters", []),
                "usage_guidance": ex.get("usage_guidance", ""),
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.75,
                "questions_fixed": 1,
                "questions_at_risk": 0,
                "net_impact": 0.75,
                "asi": {
                    "failure_type": "asset_routing_error",
                    "blame_set": [],
                    "severity": "major",
                    "counterfactual_fixes": [],
                    "ambiguity_detected": False,
                },
            })

        if benchmarks:
            mined = _mine_benchmark_example_sqls(
                benchmarks, metadata_snapshot,
                spark=spark, catalog=catalog, gold_schema=gold_schema,
                w=w, warehouse_id=warehouse_id,
            )
            if mined:
                logger.info("Benchmark mining added %d example SQL proposals", len(mined))
                proposals.extend(mined)

        proposals = _validate_lever5_proposals(
            proposals, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        )
        _pre_dedup_p = len(proposals)
        proposals = _deduplicate_proposals(proposals)
        if len(proposals) < _pre_dedup_p:
            logger.info(
                "Proposal dedup: %d -> %d (removed %d duplicates)",
                _pre_dedup_p, len(proposals), _pre_dedup_p - len(proposals),
            )
        proposals.sort(key=lambda p: p["net_impact"], reverse=True)
        return proposals

    # ── Standard per-cluster path (levers 1-4) ───────────────────────
    _MAX_CLUSTERS_PER_LEVER = 3
    eligible_clusters: list[tuple[dict, int]] = []
    for cluster in clusters:
        natural_lever = _map_to_lever(
            cluster["root_cause"],
            asi_failure_type=cluster.get("asi_failure_type"),
            blame_set=cluster.get("asi_blame_set"),
            judge=cluster.get("affected_judge"),
        )
        lever = natural_lever
        if target_lever is not None and lever != target_lever:
            continue
        eligible_clusters.append((cluster, lever))

    eligible_clusters.sort(key=lambda x: len(x[0]["question_ids"]), reverse=True)
    if len(eligible_clusters) > _MAX_CLUSTERS_PER_LEVER:
        logger.info(
            "Capping clusters for lever %s: %d -> %d (keeping top by question count)",
            target_lever, len(eligible_clusters), _MAX_CLUSTERS_PER_LEVER,
        )
        eligible_clusters = eligible_clusters[:_MAX_CLUSTERS_PER_LEVER]

    for cluster, lever in eligible_clusters:

        failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get(
                (failure_type, 1), "add_instruction"
            ),
        )

        llm_result = _call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever)

        extra_fields: dict = {}

        if lever == 4 and isinstance(llm_result.get("join_spec"), dict):
            llm_result["join_spec"] = ensure_join_spec_fields(llm_result["join_spec"])
            valid, reason = validate_join_spec_types(
                llm_result["join_spec"], metadata_snapshot
            )
            if not valid:
                logger.info(
                    "Reactive join proposal rejected (type mismatch): %s", reason
                )
                continue
            extra_fields["join_spec"] = llm_result["join_spec"]

        scope = _resolve_scope(lever, apply_mode)
        q_fixed = len(cluster["question_ids"])
        confidence = cluster["confidence"]
        total_objects = max(len(metadata_snapshot.get("tables", [])), 1)
        blast_radius = 1
        net_impact = q_fixed * confidence - 0.1 * (blast_radius / total_objects)
        rationale = llm_result.get("rationale", "")

        asi_block = {
            "failure_type": failure_type,
            "blame_set": cluster.get("asi_blame_set") or [],
            "severity": "major",
            "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
            "ambiguity_detected": cluster.get("root_cause") == "repeatability_issue",
        }

        if lever in (1, 2) and isinstance(llm_result.get("changes"), list):
            for change in llm_result["changes"]:
                if not isinstance(change, dict):
                    continue
                tbl = change.get("table", "")
                col = change.get("column", "")
                if not tbl or not col:
                    continue

                sections = change.get("sections")
                entity_type = change.get("entity_type", "")

                if isinstance(sections, dict) and sections:
                    section_keys = list(sections.keys())
                    change_desc = f"Update {tbl}.{col} sections={section_keys}"
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": cluster["cluster_id"],
                        "lever": lever,
                        "scope": scope,
                        "patch_type": patch_type,
                        "change_description": change_desc,
                        "proposed_value": "",
                        "rationale": rationale,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": confidence,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": net_impact,
                        "asi": asi_block,
                        "provenance": _build_provenance(cluster, lever, patch_type),
                        "table": tbl,
                        "column": col,
                        "column_sections": sections,
                        "column_entity_type": entity_type,
                        **extra_fields,
                    })
                else:
                    desc = change.get("description")
                    syns = change.get("synonyms")
                    change_desc = f"Update {tbl}.{col}"
                    if desc:
                        change_desc += f" description={desc}"
                    if syns:
                        change_desc += f" synonyms={syns}"
                    proposals.append({
                        "proposal_id": f"P{len(proposals) + 1:03d}",
                        "cluster_id": cluster["cluster_id"],
                        "lever": lever,
                        "scope": scope,
                        "patch_type": patch_type,
                        "change_description": change_desc,
                        "proposed_value": desc[0] if isinstance(desc, list) and desc else "",
                        "rationale": rationale,
                        "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                        "confidence": confidence,
                        "questions_fixed": q_fixed,
                        "questions_at_risk": 0,
                        "net_impact": net_impact,
                        "asi": asi_block,
                        "provenance": _build_provenance(cluster, lever, patch_type),
                        "table": tbl,
                        "column": col,
                        "column_description": desc,
                        "column_synonyms": syns,
                        **extra_fields,
                    })

            for tbl_change in llm_result.get("table_changes") or []:
                if not isinstance(tbl_change, dict):
                    continue
                tbl = tbl_change.get("table", "")
                if not tbl:
                    continue
                tbl_sections = tbl_change.get("sections")
                if not isinstance(tbl_sections, dict) or not tbl_sections:
                    continue
                tbl_etype = tbl_change.get("entity_type", "table")
                change_desc = f"Update table {tbl} sections={list(tbl_sections.keys())}"
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": cluster["cluster_id"],
                    "lever": lever,
                    "scope": scope,
                    "patch_type": "update_description",
                    "change_description": change_desc,
                    "proposed_value": "",
                    "rationale": rationale,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                    "confidence": confidence,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": net_impact,
                    "asi": asi_block,
                    "provenance": _build_provenance(cluster, lever, "update_description"),
                    "table": tbl,
                    "table_sections": tbl_sections,
                    "table_entity_type": tbl_etype,
                    **extra_fields,
                })
        else:
            proposed_value = (
                llm_result.get("proposed_value")
                or llm_result.get("instruction_text")
                or llm_result.get("example_question")
                or ""
            )

            proposal = {
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": cluster["cluster_id"],
                "lever": lever,
                "scope": scope,
                "patch_type": patch_type,
                "change_description": proposed_value or _describe_fix(cluster),
                "proposed_value": proposed_value,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                "confidence": confidence,
                "questions_fixed": q_fixed,
                "questions_at_risk": 0,
                "net_impact": net_impact,
                "asi": asi_block,
                "provenance": _build_provenance(cluster, lever, patch_type),
                **extra_fields,
            }
            proposals.append(proposal)

    if target_lever == 4:
        soft_clusters_for_joins = [
            c for c in clusters
            if c.get("source") == "soft_signal" or c.get("is_soft_signal")
        ]
        discovery_hints = discover_join_candidates(
            metadata_snapshot,
            soft_signal_clusters=soft_clusters_for_joins or None,
        )
        if discovery_hints:
            discovery_specs = _call_llm_for_join_discovery(
                metadata_snapshot, discovery_hints, w=w,
            )
            for spec_result in discovery_specs:
                join_spec = spec_result.get("join_spec")
                if not isinstance(join_spec, dict):
                    continue
                join_spec = ensure_join_spec_fields(join_spec)
                spec_result["join_spec"] = join_spec

                valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                if not valid:
                    logger.info("Discovery join rejected (type mismatch): %s", reason)
                    continue

                left_obj = join_spec.get("left", {})
                right_obj = join_spec.get("right", {})
                left_id = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
                right_id = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
                sql_parts = join_spec.get("sql", [])
                condition = sql_parts[0] if sql_parts else ""

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": f"JOIN_DISC_{len(proposals) + 1:03d}",
                    "lever": 4,
                    "scope": "genie_config",
                    "patch_type": "add_join_spec",
                    "change_description": f"Add join: {condition}" if condition else f"Add join: {left_id} ↔ {right_id}",
                    "proposed_value": "",
                    "rationale": spec_result.get("rationale", "LLM-assisted join discovery"),
                    "join_spec": join_spec,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.7,
                    "questions_fixed": 0,
                    "questions_at_risk": 0,
                    "net_impact": 0.35,
                    "asi": {
                        "failure_type": "missing_join_spec",
                        "blame_set": [],
                        "severity": "minor",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                })

    proposals = _validate_lever5_proposals(
        proposals, metadata_snapshot,
        spark=spark, catalog=catalog, gold_schema=gold_schema,
        w=w, warehouse_id=warehouse_id,
    )
    _pre_dedup_p = len(proposals)
    proposals = _deduplicate_proposals(proposals)
    if len(proposals) < _pre_dedup_p:
        logger.info(
            "Proposal dedup: %d -> %d (removed %d duplicates)",
            _pre_dedup_p, len(proposals), _pre_dedup_p - len(proposals),
        )
    proposals = _merge_overlapping_instructions(proposals)
    proposals = _filter_no_op_proposals(proposals, metadata_snapshot)
    proposals.sort(key=lambda p: p["net_impact"], reverse=True)

    return proposals


def propose_patch_set_from_asi(
    asi_rows: list[dict],
    metadata_snapshot: dict,
    lever: int | None = None,
) -> list[dict]:
    """Generate proposals directly from ASI records.

    Wraps ASI rows into cluster-like structures and delegates to
    ``generate_metadata_proposals``.
    """
    blame_groups: dict[tuple, list] = defaultdict(list)
    for row in asi_rows:
        if not isinstance(row, dict):
            continue
        ft = row.get("failure_type", row.get("asi_failure_type", "other"))
        bs = row.get("blame_set", [])
        if isinstance(bs, list):
            key = (ft, tuple(sorted(bs)))
        else:
            key = (ft, (str(bs),))
        blame_groups[key].append(row)

    clusters: list[dict] = []
    for (ft, bs_tuple), rows in blame_groups.items():
        if len(rows) < 1:
            continue
        clusters.append(
            {
                "cluster_id": f"ASI_C{len(clusters) + 1:03d}",
                "root_cause": ft,
                "question_ids": [
                    r.get("question_id", f"q{i}") for i, r in enumerate(rows)
                ],
                "affected_judge": rows[0].get("judge", "unknown"),
                "confidence": sum(
                    float(r.get("confidence", 0.5)) for r in rows
                )
                / max(len(rows), 1),
                "asi_failure_type": ft,
                "asi_blame_set": list(bs_tuple) if bs_tuple else None,
                "asi_counterfactual_fixes": [
                    r.get("counterfactual_fix", "")
                    for r in rows
                    if r.get("counterfactual_fix")
                ],
            }
        )

    return generate_metadata_proposals(
        clusters, metadata_snapshot, target_lever=lever
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Scoring (pure)
# ═══════════════════════════════════════════════════════════════════════


def score_patch_set(proposals: list[dict], metadata_snapshot: dict) -> float:
    """Score a patch set by expected impact.

    ``questions_blamed * avg_confidence - 0.1 * (blast_objects / total_objects)``
    """
    if not proposals:
        return 0.0

    total_objects = max(
        len(metadata_snapshot.get("tables", [])) if metadata_snapshot else 1, 1
    )

    all_targets: set[str] = set()
    questions_total = 0
    confidences: list[float] = []

    for p in proposals:
        target = p.get("target_object") or p.get("object_id") or p.get("target", "")
        if target:
            all_targets.add(target)
        questions_total += p.get("questions_fixed", 0)
        confidences.append(float(p.get("confidence", 0.5)))

    avg_confidence = sum(confidences) / max(len(confidences), 1)
    blast = len(all_targets)
    return questions_total * avg_confidence - 0.1 * (blast / total_objects)


# ═══════════════════════════════════════════════════════════════════════
# 6. Conflict Detection & Batching (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_conflicts_and_batch(proposals: list[dict]) -> list[list[dict]]:
    """Group proposals into conflict-free batches.

    Within each lever group, checks ``CONFLICT_RULES``.  Starts a new batch
    when a conflict is found.
    """
    conflict_set = set()
    for a, b in CONFLICT_RULES:
        conflict_set.add((a, b))
        conflict_set.add((b, a))

    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_types: set[str] = set()

    for p in proposals:
        p_type = p.get("patch_type") or p.get("type", "")
        has_conflict = any((p_type, existing) in conflict_set for existing in current_types)
        if has_conflict:
            batches.append(current_batch)
            current_batch = [p]
            current_types = {p_type}
        else:
            current_batch.append(p)
            current_types.add(p_type)

    if current_batch:
        batches.append(current_batch)
    return batches


# ═══════════════════════════════════════════════════════════════════════
# 7. Regression Detection (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_regressions(
    current_scores: dict[str, float],
    previous_scores: dict[str, float],
    threshold: float = REGRESSION_THRESHOLD,
    skip_judges: set[str] | None = None,
) -> list[dict]:
    """Detect if any metric dropped more than ``threshold`` percentage points.

    Parameters
    ----------
    skip_judges : set[str] | None
        Judge names to exclude from regression checking.  Use this for
        informational judges whose convergence threshold is 0.0 (e.g.
        ``response_quality``) — they should not block progress.
    """
    regressions: list[dict] = []
    for key in previous_scores:
        if skip_judges and key in skip_judges:
            continue
        prev_val = previous_scores.get(key, 0)
        curr_val = current_scores.get(key, 0)
        if curr_val < prev_val - threshold:
            regressions.append(
                {
                    "judge": key,
                    "previous": prev_val,
                    "current": curr_val,
                    "drop": prev_val - curr_val,
                }
            )
    return regressions


# ═══════════════════════════════════════════════════════════════════════
# 8. Validation (pure)
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(
    patches: list[dict], metadata_snapshot: dict | None = None
) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Checks: known types, conflict rules, blast radius (max 5 objects),
    and optionally validates targets exist in metadata_snapshot.
    """
    errors: list[str] = []
    valid_types = set(PATCH_TYPES.keys())

    for i, p in enumerate(patches):
        pt = p.get("type") if isinstance(p, dict) else None
        if pt and pt not in valid_types:
            errors.append(f"Patch {i}: unknown type '{pt}'")

    targets: set[str] = set()
    for p in patches:
        if not isinstance(p, dict):
            continue
        obj = p.get("target_object") or p.get("object_id") or p.get("target") or p.get("table")
        if obj:
            targets.add(obj)

    if len(targets) > MAX_PATCH_OBJECTS:
        errors.append(
            f"Too many target objects: {len(targets)} (max {MAX_PATCH_OBJECTS})"
        )

    types_in_set = {p.get("type") for p in patches if isinstance(p, dict)}
    for a, b in CONFLICT_RULES:
        if a in types_in_set and b in types_in_set:
            errors.append(f"Conflicting patch types: {a} and {b}")

    if metadata_snapshot:
        known_tables = {
            t.get("name") or t.get("identifier", "")
            for t in metadata_snapshot.get("tables", [])
        }
        known_columns: set[str] = set()
        for t in metadata_snapshot.get("tables", []):
            for c in t.get("columns", t.get("column_configs", [])):
                known_columns.add(c.get("name") or c.get("column_name", ""))

        for i, p in enumerate(patches):
            if not isinstance(p, dict):
                continue
            tgt_table = p.get("table", "")
            tgt_col = p.get("column", "")
            if tgt_table and known_tables and tgt_table not in known_tables:
                errors.append(f"Patch {i}: table '{tgt_table}' not found in metadata")
            if tgt_col and known_columns and tgt_col not in known_columns:
                errors.append(f"Patch {i}: column '{tgt_col}' not found in metadata")

    return (len(errors) == 0, errors)
