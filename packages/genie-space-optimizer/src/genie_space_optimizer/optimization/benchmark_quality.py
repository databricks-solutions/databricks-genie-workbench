"""Canonical benchmark-quality review for the GSO v2 QC task.

The official Genie Benchmark API treats the published benchmark corpus as the
source of truth.  This module therefore reviews the corpus *before* it reaches
the native evaluator.  It combines deterministic SQL/data checks with a
context-grounded LLM review of question quality and question-to-SQL alignment.

The output is deliberately JSON-shaped so it can be stored directly in the
``benchmark_qc`` artifact and rendered by both Workbench frontends.
"""

from __future__ import annotations

import json
import logging
import math
import re
from typing import Any

from genie_space_optimizer.common.config import (
    BENCHMARK_QUALITY_REVIEW_PROMPT,
    format_mlflow_template,
)
from genie_space_optimizer.optimization.benchmarks import (
    validate_benchmarks,
    validate_gt_returns_results,
    validate_predicate_values,
)

logger = logging.getLogger(__name__)

QUALITY_REVIEW_VERSION = "benchmark_quality_v3"
QUALITY_ERROR_CONFIDENCE = 0.75

QUESTION_QUALITY = "question_quality"
QUESTION_SQL_ALIGNMENT = "question_sql_alignment"
SQL_VALIDITY = "sql_validity"
DATA_VALIDITY = "data_validity"
REVIEW_SYSTEM = "review_system"
CORPUS_COVERAGE = "corpus_coverage"

REQUIRED_QUESTION_FORMATS = (
    "aggregation",
    "lookup",
    "comparison",
    "time_filter",
    "top_n",
)

_NON_ACTIONABLE_WARNING_CODES = frozenset(
    {
        "GT_EXECUTION_NOT_RUN",
        "REVIEW_NOT_RUN",
        "VALUE_ACCESS_GAP",
        "VOLATILE_TIME_REFERENCE",
    }
)

_QUESTION_QUALITY_CODES = frozenset(
    {
        "AMBIGUOUS_METRIC",
        "AMBIGUOUS_TIME_SCOPE",
        "AMBIGUOUS_GRAIN",
        "UNANSWERABLE_FROM_SPACE",
        "IMPLEMENTATION_HINT",
        "UNNATURAL_PHRASING",
        "CONTEXT_DEPENDENT_QUESTION",
        "FLAKY_BENCHMARK",
        "WEAK_BUT_ANSWERABLE",
    }
)
_ALIGNMENT_CODES = frozenset(
    {
        "EXTRA_FILTER",
        "EXTRA_COLUMNS",
        "MISSING_FILTER",
        "MISSING_AGGREGATION",
        "WRONG_METRIC",
        "WRONG_TIME_SCOPE",
        "WRONG_GRAIN",
        "WRONG_JOIN",
        "WRONG_INTERPRETATION",
        "RESULT_SHAPE_MISMATCH",
        "UNORDERED_LIMIT",
        "NONDETERMINISTIC_SQL",
        "VOLATILE_TIME_REFERENCE",
    }
)

_SQL_NON_CODE_RE = re.compile(
    r"/\*.*?\*/|--[^\n]*|'(?:''|[^'])*'|`(?:``|[^`])*`|\"(?:\"\"|[^\"])*\"",
    re.DOTALL,
)
_NONDETERMINISTIC_SQL_RE = re.compile(
    r"\b(?:rand|random|randn|uuid|monotonically_increasing_id)\s*\(",
    re.IGNORECASE,
)
_NONDETERMINISTIC_SAMPLE_RE = re.compile(
    r"\btablesample\s*\(|\bsample\s+(?:bernoulli|system)\b",
    re.IGNORECASE,
)
_VOLATILE_TIME_RE = re.compile(
    r"\b(?:current_date|current_timestamp|localtimestamp|now|curdate)\b",
    re.IGNORECASE,
)


def _question_id(benchmark: dict, index: int) -> str:
    return str(
        benchmark.get("_quality_question_id")
        or benchmark.get("id")
        or benchmark.get("question_id")
        or benchmark.get("space_question_id")
        or f"benchmark_{index + 1:03d}"
    )


def _finding(
    *,
    question_id: str,
    question: str,
    source: str,
    category: str,
    code: str,
    severity: str,
    explanation: str,
    expected_sql: str = "",
    confidence: float = 1.0,
    evidence: Any = None,
    proposed_question: str | None = None,
    proposed_sql: str | None = None,
    recommended_action: str | None = None,
) -> dict[str, Any]:
    try:
        bounded_confidence = float(confidence)
    except (TypeError, ValueError):
        bounded_confidence = 0.0
    if not math.isfinite(bounded_confidence):
        bounded_confidence = 0.0
    proposed_question_value = (
        proposed_question.strip()
        if isinstance(proposed_question, str) and proposed_question.strip()
        else None
    )
    proposed_sql_value = (
        proposed_sql.strip()
        if isinstance(proposed_sql, str) and proposed_sql.strip()
        else None
    )
    return {
        "question_id": question_id,
        "question": question,
        "source": source or "unknown",
        "category": category,
        "code": code,
        "severity": "error" if severity == "error" else "warning",
        "confidence": max(0.0, min(1.0, bounded_confidence)),
        "explanation": explanation[:1000],
        "evidence": evidence,
        "before": {"question": question, "sql": expected_sql} if question or expected_sql else None,
        "proposed_question": proposed_question_value,
        "proposed_sql": proposed_sql_value,
        "recommended_action": recommended_action,
    }


def _sql_tokens_with_depth(sql: str) -> list[tuple[str, int]]:
    """Return SQL syntax tokens without literals, comments, or quoted identifiers."""
    cleaned = _sql_code_only(sql)
    depth = 0
    tokens: list[tuple[str, int]] = []
    for match in re.finditer(r"[A-Za-z_]+|[()]", cleaned):
        token = match.group(0)
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(0, depth - 1)
        else:
            tokens.append((token.upper(), depth))
    return tokens


def _sql_code_only(sql: str) -> str:
    """Mask SQL regions whose words cannot be clauses or function calls."""
    return _SQL_NON_CODE_RE.sub(" ", str(sql or ""))


def _has_unordered_limit(sql: str) -> bool:
    tokens = _sql_tokens_with_depth(sql)
    for limit_index, (token, depth) in enumerate(tokens):
        if token != "LIMIT":
            continue
        select_index = -1
        for index in range(limit_index - 1, -1, -1):
            prior, prior_depth = tokens[index]
            if prior_depth == depth and prior == "SELECT":
                select_index = index
                break
        same_scope = [
            word
            for word, word_depth in tokens[select_index + 1 : limit_index]
            if word_depth == depth
        ]
        if not any(
            same_scope[index : index + 2] == ["ORDER", "BY"]
            for index in range(max(0, len(same_scope) - 1))
        ):
            return True
    return False


def _deterministic_sql_findings(
    benchmark: dict,
    *,
    question_id: str,
) -> list[dict[str, Any]]:
    """Detect ground-truth SQL shapes that are unstable across executions."""
    sql = str(benchmark.get("expected_sql") or "")
    if not sql.strip():
        return []
    question = str(benchmark.get("question") or "")
    source = _source(benchmark)
    findings: list[dict[str, Any]] = []

    if _has_unordered_limit(sql):
        findings.append(
            _finding(
                question_id=question_id,
                question=question,
                source=source,
                category=SQL_VALIDITY,
                code="UNORDERED_LIMIT",
                severity="error",
                explanation=(
                    "Ground-truth SQL uses LIMIT without ORDER BY in the same "
                    "query scope, so the selected rows are unstable."
                ),
                expected_sql=sql,
                evidence={"expected_sql": sql},
                recommended_action="repair_benchmark_sql",
            )
        )

    cleaned = _sql_code_only(sql)
    if _NONDETERMINISTIC_SQL_RE.search(cleaned) or _NONDETERMINISTIC_SAMPLE_RE.search(cleaned):
        findings.append(
            _finding(
                question_id=question_id,
                question=question,
                source=source,
                category=SQL_VALIDITY,
                code="NONDETERMINISTIC_SQL",
                severity="error",
                explanation=(
                    "Ground-truth SQL uses randomization or sampling and cannot "
                    "produce a stable benchmark result."
                ),
                expected_sql=sql,
                evidence={"expected_sql": sql},
                recommended_action="repair_benchmark_sql",
            )
        )
    if _VOLATILE_TIME_RE.search(cleaned):
        findings.append(
            _finding(
                question_id=question_id,
                question=question,
                source=source,
                category=SQL_VALIDITY,
                code="VOLATILE_TIME_REFERENCE",
                severity="warning",
                explanation=(
                    "Ground-truth SQL depends on the execution clock. Prefer "
                    "explicit date boundaries unless a moving window is intentional."
                ),
                expected_sql=sql,
                evidence={"expected_sql": sql},
                recommended_action="review_temporal_ground_truth",
            )
        )
    return findings


def _parsed_space(config: dict) -> dict:
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return parsed
    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return serialized
    return config


def _value_matching_columns(config: dict) -> dict[tuple[str, str], bool]:
    """Index whether a Genie column exposes example/value matching support."""
    parsed = _parsed_space(config)
    data_sources = parsed.get("data_sources")
    if not isinstance(data_sources, dict):
        return {}
    indexed: dict[tuple[str, str], bool] = {}
    for source_key in ("tables", "metric_views"):
        for asset in data_sources.get(source_key) or []:
            if not isinstance(asset, dict):
                continue
            identifier = str(asset.get("identifier") or asset.get("name") or "")
            normalized = identifier.replace("`", "").lower()
            leaf = normalized.rsplit(".", 1)[-1]
            for column in asset.get("column_configs") or []:
                if not isinstance(column, dict):
                    continue
                name = str(column.get("column_name") or column.get("name") or "").lower()
                if not name:
                    continue
                enabled = bool(
                    column.get("enable_entity_matching")
                    or column.get("build_value_dictionary")
                    or column.get("enable_format_assistance")
                    or column.get("get_example_values")
                )
                indexed[(normalized, name)] = enabled
                indexed[(leaf, name)] = enabled
    return indexed


def _literal_is_explicit_in_question(literal: str, question: str) -> bool:
    literal_tokens = re.findall(r"[a-z0-9]+", str(literal).lower())
    question_tokens = re.findall(r"[a-z0-9]+", str(question).lower())
    if not literal_tokens:
        return True
    width = len(literal_tokens)
    return any(
        question_tokens[index : index + width] == literal_tokens
        for index in range(len(question_tokens) - width + 1)
    )


def _value_access_findings(
    benchmark: dict,
    *,
    question_id: str,
    config: dict,
) -> list[dict[str, Any]]:
    """Flag business-value translations Genie is not configured to recognize."""
    from genie_space_optimizer.optimization.benchmarks import (
        _extract_predicates,
        _extract_table_aliases,
    )

    parsed = _parsed_space(config)
    data_sources = parsed.get("data_sources")
    if not isinstance(data_sources, dict):
        return []
    configured_assets: set[str] = set()
    for source_key in ("tables", "metric_views"):
        for asset in data_sources.get(source_key) or []:
            if not isinstance(asset, dict):
                continue
            identifier = str(asset.get("identifier") or asset.get("name") or "")
            normalized = identifier.replace("`", "").lower()
            if normalized:
                configured_assets.add(normalized)
                configured_assets.add(normalized.rsplit(".", 1)[-1])
    if not configured_assets:
        return []
    matching = _value_matching_columns(config)
    sql = str(benchmark.get("expected_sql") or "")
    question = str(benchmark.get("question") or "")
    aliases = _extract_table_aliases(sql)
    unique_tables = list(dict.fromkeys(aliases.values()))
    findings: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for predicate in _extract_predicates(sql):
        alias = str(predicate.get("table_alias") or "").lower()
        table = aliases.get(alias, "") if alias else ""
        if not table and len(unique_tables) == 1:
            table = unique_tables[0]
        normalized_table = str(table).replace("`", "").lower()
        leaf = normalized_table.rsplit(".", 1)[-1]
        column = str(predicate.get("column") or "").lower()
        if not normalized_table or not column:
            continue
        if normalized_table not in configured_assets and leaf not in configured_assets:
            continue
        enabled = matching.get(
            (normalized_table, column),
            matching.get((leaf, column), False),
        )
        if enabled:
            continue
        implicit_values = [
            str(value)
            for value in predicate.get("values") or []
            if not _literal_is_explicit_in_question(str(value), question)
        ]
        key = (normalized_table, column)
        if not implicit_values or key in seen:
            continue
        seen.add(key)
        findings.append(
            _finding(
                question_id=question_id,
                question=question,
                source=_source(benchmark),
                category=DATA_VALIDITY,
                code="VALUE_ACCESS_GAP",
                severity="warning",
                explanation=(
                    "The ground truth translates the user's wording to stored "
                    f"values on {leaf}.{column}, but that column has neither "
                    "example-value nor entity-matching support enabled."
                ),
                expected_sql=sql,
                evidence={
                    "table": normalized_table,
                    "column": column,
                    "implicit_sql_values": implicit_values,
                },
                recommended_action="enable_value_matching",
            )
        )
    return findings


def review_benchmark_format_coverage(benchmarks: list[dict]) -> dict[str, Any]:
    """Summarize required user-question formats in the final corpus."""
    counts = {question_format: 0 for question_format in REQUIRED_QUESTION_FORMATS}
    category_map = {
        "aggregation": "aggregation",
        "detail": "lookup",
        "list": "lookup",
        "comparison": "comparison",
        "time-series": "time_filter",
        "ranking": "top_n",
    }
    for benchmark in benchmarks:
        detected: set[str] = set()
        category = str(benchmark.get("category") or "").strip().lower()
        if category in category_map:
            detected.add(category_map[category])
        question = str(benchmark.get("question") or "").lower()
        sql = str(benchmark.get("expected_sql") or "").lower()
        if re.search(r"\b(total|sum|average|avg|count|how many|by each|per)\b", question) or re.search(
            r"\b(sum|avg|count|min|max)\s*\(", sql,
        ):
            detected.add("aggregation")
        if re.search(r"\b(what is|which|show me|find|status of|details? for)\b", question):
            detected.add("lookup")
        if re.search(r"\b(compare|versus|vs\.?|difference|higher than|lower than)\b", question):
            detected.add("comparison")
        if re.search(
            r"\b(ytd|year.to.date|last|this (?:week|month|quarter|year)|between|since|before|after)\b",
            question,
        ) or re.search(r"\b(date|timestamp|interval|year|month|quarter)\b", sql):
            detected.add("time_filter")
        if re.search(r"\b(top|bottom|highest|lowest|rank(?:ed|ing)?)\b", question) or (
            "limit" in sql and "order by" in sql
        ):
            detected.add("top_n")
        for question_format in detected:
            counts[question_format] += 1

    missing = [name for name, count in counts.items() if count == 0]
    findings = []
    if missing:
        findings.append(
            _finding(
                question_id="__corpus__",
                question="",
                source="benchmark_corpus",
                category=CORPUS_COVERAGE,
                code="MISSING_QUESTION_FORMAT",
                severity="warning",
                explanation=(
                    "The final benchmark corpus does not cover all required user "
                    f"question formats: {', '.join(missing)}."
                ),
                evidence={"missing_formats": missing, "format_counts": counts},
                recommended_action="add_benchmark_coverage",
            )
        )
    return {
        "required_formats": list(REQUIRED_QUESTION_FORMATS),
        "counts": counts,
        "missing": missing,
        "findings": findings,
    }


def _source(benchmark: dict) -> str:
    return str(benchmark.get("source") or benchmark.get("provenance") or "unknown")


def _is_generated_benchmark(benchmark: dict) -> bool:
    """Return whether GSO, rather than a user, authored the question text."""
    source = str(benchmark.get("source") or "").strip().lower()
    provenance = str(benchmark.get("provenance") or "").strip().lower()
    return source == "llm_generated" or provenance in {
        "synthetic",
        "coverage_gap_fill",
    }


def build_actionable_warning_repair(
    benchmark: dict,
    benchmark_result: dict,
) -> tuple[dict | None, dict | None]:
    """Build one coherent repair candidate for a warning disposition.

    The quality model emits the same top-level proposal on each issue for a
    question.  A proposal becomes actionable only when repair policy is
    enabled by the caller, every non-empty proposed value agrees, and the
    proposal changes the current benchmark.  Review-system warnings remain
    advisory because they describe missing evidence rather than benchmark
    content.

    The returned change record is audit metadata; this helper does not mutate
    the input benchmark or authorize a live Genie Agent update by itself.
    """
    if benchmark_result.get("disposition") != "warning":
        return None, None

    findings = [
        finding
        for finding in benchmark_result.get("findings", [])
        if isinstance(finding, dict)
        and finding.get("severity") == "warning"
        and finding.get("category") != REVIEW_SYSTEM
        and str(finding.get("code") or "").upper()
        not in _NON_ACTIONABLE_WARNING_CODES
    ]
    proposed_questions = {
        str(finding.get("proposed_question") or "").strip()
        for finding in findings
        if str(finding.get("proposed_question") or "").strip()
    }
    proposed_sqls = {
        str(finding.get("proposed_sql") or "").strip()
        for finding in findings
        if str(finding.get("proposed_sql") or "").strip()
    }
    if len(proposed_questions) > 1 or len(proposed_sqls) > 1:
        logger.warning(
            "Ignoring conflicting benchmark warning proposals for question_id=%s",
            benchmark_result.get("question_id") or _question_id(benchmark, 0),
        )
        return None, None

    before_question = str(benchmark.get("question") or "").strip()
    before_sql = str(benchmark.get("expected_sql") or "").strip()
    after_question = next(iter(proposed_questions), before_question)
    after_sql = next(iter(proposed_sqls), before_sql)
    if after_question == before_question and after_sql == before_sql:
        return None, None

    candidate = dict(benchmark)
    candidate["question"] = after_question
    candidate["expected_sql"] = after_sql
    change = {
        "question_id": str(
            benchmark.get("space_question_id")
            or benchmark.get("id")
            or benchmark.get("question_id")
            or benchmark_result.get("question_id")
            or ""
        ),
        "before_question": before_question,
        "after_question": after_question,
        "before_sql": before_sql,
        "after_sql": after_sql,
        "reason": "benchmark_quality_warning_repair",
        "codes": sorted(
            {
                str(finding.get("code") or "UNKNOWN")
                for finding in findings
                if finding.get("proposed_question") or finding.get("proposed_sql")
            }
        ),
    }
    return candidate, change


def _strip_json_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return text


def _call_quality_llm(prompt: str) -> str:
    from genie_space_optimizer.optimization.llm_client import call_llm

    raw, _response = call_llm(
        None,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    return raw


def _llm_review(
    benchmarks: list[dict],
    *,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    batch_size: int,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Return semantic findings plus ids whose review did not complete."""
    from genie_space_optimizer.optimization.benchmarking import _build_schema_contexts

    contexts = _build_schema_contexts(config, uc_columns, uc_routines)
    contexts.setdefault("example_sql_questions_context", "(none)")
    findings: list[dict[str, Any]] = []
    incomplete: set[str] = set()

    indexed = [(_question_id(b, i), b) for i, b in enumerate(benchmarks)]
    for start in range(0, len(indexed), batch_size):
        batch = indexed[start : start + batch_size]
        payload = [
            {
                "question_id": qid,
                "question": str(b.get("question") or ""),
                "expected_sql": str(b.get("expected_sql") or ""),
                "source": _source(b),
            }
            for qid, b in batch
        ]
        prompt = format_mlflow_template(
            BENCHMARK_QUALITY_REVIEW_PROMPT,
            benchmarks_json=json.dumps(payload, indent=2),
            **contexts,
        )
        expected = {qid: b for qid, b in batch}
        try:
            parsed = json.loads(_strip_json_fence(_call_quality_llm(prompt)))
            if isinstance(parsed, dict):
                parsed = parsed.get("reviews", [])
            if not isinstance(parsed, list):
                raise ValueError("quality review response is not a JSON array")

            returned: dict[str, dict] = {}
            duplicates: set[str] = set()
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                qid = str(item.get("question_id") or "")
                if qid in returned:
                    duplicates.add(qid)
                returned[qid] = item

            for qid, benchmark in batch:
                review = returned.get(qid)
                if review is None or qid in duplicates:
                    incomplete.add(qid)
                    continue
                try:
                    confidence = float(review.get("confidence", 0.8))
                except (TypeError, ValueError):
                    confidence = 0.8
                if not math.isfinite(confidence):
                    confidence = 0.0
                malformed_issue = False
                for issue in review.get("issues") or []:
                    if not isinstance(issue, dict):
                        malformed_issue = True
                        continue
                    category = str(issue.get("category") or "").lower()
                    code = str(issue.get("code") or "").upper()
                    if category == QUESTION_QUALITY:
                        if code not in _QUESTION_QUALITY_CODES:
                            malformed_issue = True
                            continue
                    elif category == QUESTION_SQL_ALIGNMENT:
                        if code not in _ALIGNMENT_CODES:
                            malformed_issue = True
                            continue
                    else:
                        malformed_issue = True
                        continue
                    severity = str(issue.get("severity") or "warning").lower()
                    if code == "WEAK_BUT_ANSWERABLE":
                        severity = "warning"
                    elif code == "IMPLEMENTATION_HINT":
                        # Generated questions with confident implementation leakage
                        # are unsafe evaluation rows and can be replaced with a new
                        # benchmark. User-authored wording is immutable: retain it as
                        # an advisory warning for the owner instead of auto-pruning or
                        # rewriting it. Low-confidence findings are warnings for both.
                        severity = (
                            "error"
                            if confidence >= QUALITY_ERROR_CONFIDENCE
                            and _is_generated_benchmark(benchmark)
                            else "warning"
                        )
                    elif severity == "error" and confidence < QUALITY_ERROR_CONFIDENCE:
                        severity = "warning"
                    findings.append(
                        _finding(
                            question_id=qid,
                            question=str(benchmark.get("question") or ""),
                            source=_source(benchmark),
                            category=category,
                            code=code,
                            severity=severity,
                            confidence=confidence,
                            explanation=str(issue.get("explanation") or code),
                            expected_sql=str(benchmark.get("expected_sql") or ""),
                            evidence=issue.get("evidence"),
                            proposed_question=review.get("proposed_question"),
                            proposed_sql=review.get("proposed_sql"),
                        )
                    )
                if malformed_issue:
                    incomplete.add(qid)
        except Exception as exc:
            logger.warning(
                "Benchmark quality review failed for batch starting at %d: %s",
                start,
                exc,
            )
            incomplete.update(expected)

    return findings, incomplete


def review_benchmark_quality(
    benchmarks: list[dict],
    spark: Any,
    *,
    catalog: str = "",
    schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
    config: dict | None = None,
    uc_columns: list[dict] | None = None,
    deterministic_uc_columns: list[dict] | None = None,
    uc_routines: list[dict] | None = None,
    batch_size: int = 10,
) -> dict[str, Any]:
    """Review a benchmark corpus and partition it into eligible/excluded rows.

    Error findings exclude a row. Warning findings keep it eligible. An LLM
    infrastructure failure is represented explicitly as ``REVIEW_NOT_RUN`` and
    remains eligible so a transient model outage cannot silently shrink the
    corpus; callers can use ``review_status=degraded`` to gate rollout policy.
    """
    config = config or {}
    uc_columns = uc_columns or []
    deterministic_uc_columns = deterministic_uc_columns or uc_columns
    uc_routines = uc_routines or []
    findings: list[dict[str, Any]] = []
    indexed = [(_question_id(b, i), b) for i, b in enumerate(benchmarks)]

    sql_results = validate_benchmarks(
        benchmarks,
        spark,
        catalog=catalog,
        gold_schema=schema,
        w=w,
        warehouse_id=warehouse_id,
        config=config,
    )
    sql_valid_ids: set[str] = set()
    for (qid, benchmark), result in zip(indexed, sql_results):
        question = str(benchmark.get("question") or "")
        sql = str(benchmark.get("expected_sql") or "")
        if not question.strip():
            findings.append(
                _finding(
                    question_id=qid,
                    question=question,
                    source=_source(benchmark),
                    category=QUESTION_QUALITY,
                    code="EMPTY_QUESTION",
                    severity="error",
                    explanation="Benchmark question is empty.",
                    expected_sql=sql,
                )
            )
        if not sql.strip():
            findings.append(
                _finding(
                    question_id=qid,
                    question=question,
                    source=_source(benchmark),
                    category=DATA_VALIDITY,
                    code="MISSING_EXPECTED_SQL",
                    severity="error",
                    explanation="Benchmark has no ground-truth SQL.",
                    expected_sql=sql,
                )
            )
        elif result.get("valid"):
            stability_findings = _deterministic_sql_findings(
                benchmark,
                question_id=qid,
            )
            findings.extend(stability_findings)
            if not any(
                finding.get("severity") == "error"
                for finding in stability_findings
            ):
                sql_valid_ids.add(qid)
        else:
            from genie_space_optimizer.optimization.benchmarking import (
                _classify_sql_validation_error,
            )

            error = str(result.get("error") or "SQL validation failed")
            findings.append(
                _finding(
                    question_id=qid,
                    question=question,
                    source=_source(benchmark),
                    category=SQL_VALIDITY,
                    code=_classify_sql_validation_error(error).upper(),
                    severity="error",
                    explanation=error,
                    expected_sql=sql,
                    evidence={"expected_sql": sql},
                )
            )

    if sql_valid_ids:
        from genie_space_optimizer.optimization.benchmarking import (
            _build_metadata_allowlist,
            _enforce_metadata_constraints,
            _guard_mv_select_star,
            effective_metric_view_identifiers_with_catalog,
        )

        allowlist = _build_metadata_allowlist(
            config=config,
            # Prompt columns and deterministic validation columns are separate
            # contracts. Omitted inventory columns remain valid here without
            # being copied into the semantic-review prompt below.
            uc_columns=deterministic_uc_columns,
            uc_routines=uc_routines,
        )
        metric_views = effective_metric_view_identifiers_with_catalog(config)
        for qid, benchmark in indexed:
            if qid not in sql_valid_ids:
                continue
            sql = str(benchmark.get("expected_sql") or "")
            metadata_ok = True
            reason_code = "ok"
            reason_message = ""
            if allowlist.get("assets"):
                metadata_ok, reason_code, reason_message = _enforce_metadata_constraints(
                    benchmark=benchmark,
                    sql=sql,
                    allowlist=allowlist,
                    catalog=catalog,
                    schema=schema,
                )
            star_ok, star_reason = _guard_mv_select_star(sql, metric_views)
            if not star_ok:
                metadata_ok = False
                reason_code = "mv_select_star"
                reason_message = star_reason
            if not metadata_ok:
                sql_valid_ids.discard(qid)
                findings.append(
                    _finding(
                        question_id=qid,
                        question=str(benchmark.get("question") or ""),
                        source=_source(benchmark),
                        category=SQL_VALIDITY,
                        code=reason_code.upper(),
                        severity="error",
                        explanation=reason_message,
                        expected_sql=sql,
                        evidence={"expected_sql": sql},
                    )
                )

    reviewable_pairs: list[tuple[str, dict]] = []
    for qid, benchmark in indexed:
        if qid not in sql_valid_ids:
            continue
        stable = dict(benchmark)
        stable["_quality_question_id"] = qid
        reviewable_pairs.append((qid, stable))
    reviewable_ids = [qid for qid, _ in reviewable_pairs]
    reviewable = [benchmark for _, benchmark in reviewable_pairs]

    data_profile = config.get("_data_profile")
    if isinstance(data_profile, dict) and data_profile and reviewable:
        predicate_results = validate_predicate_values(reviewable, data_profile)
        for qid, benchmark, result in zip(reviewable_ids, reviewable, predicate_results):
            if result.get("valid", True):
                continue
            proposed_sql = str(benchmark.get("expected_sql") or "")
            for mismatch in result.get("mismatches") or []:
                suggestion = mismatch.get("suggestion")
                literal = mismatch.get("literal")
                if suggestion and literal:
                    proposed_sql = proposed_sql.replace(f"'{literal}'", f"'{suggestion}'")
            safe_proposal = proposed_sql != str(benchmark.get("expected_sql") or "")
            findings.append(
                _finding(
                    question_id=qid,
                    question=str(benchmark.get("question") or ""),
                    source=_source(benchmark),
                    category=DATA_VALIDITY,
                    code="DATA_VALUE_MISMATCH",
                    severity="warning" if safe_proposal else "error",
                    explanation="Ground-truth SQL uses filter values not found in the data profile.",
                    expected_sql=str(benchmark.get("expected_sql") or ""),
                    evidence=result.get("mismatches"),
                    proposed_sql=proposed_sql if safe_proposal else None,
                    recommended_action="repair_benchmark_sql",
                )
            )

    for qid, benchmark in reviewable_pairs:
        findings.extend(
            _value_access_findings(
                benchmark,
                question_id=qid,
                config=config,
            )
        )

    if reviewable:
        execution_results = validate_gt_returns_results(
            reviewable,
            spark,
            w=w,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
        )
        for qid, benchmark, result in zip(reviewable_ids, reviewable, execution_results):
            if not result.get("has_results", True) and result.get("error") is None:
                findings.append(
                    _finding(
                        question_id=qid,
                        question=str(benchmark.get("question") or ""),
                        source=_source(benchmark),
                        category=DATA_VALIDITY,
                        code="GT_RETURNS_NO_ROWS",
                        severity="error",
                        explanation="Ground-truth SQL returned no rows.",
                        expected_sql=str(benchmark.get("expected_sql") or ""),
                        evidence={"row_count": result.get("row_count", 0)},
                    )
                )
            elif result.get("error"):
                findings.append(
                    _finding(
                        question_id=qid,
                        question=str(benchmark.get("question") or ""),
                        source=_source(benchmark),
                        category=REVIEW_SYSTEM,
                        code="GT_EXECUTION_NOT_RUN",
                        severity="warning",
                        explanation=str(result.get("error")),
                        expected_sql=str(benchmark.get("expected_sql") or ""),
                    )
                )

    semantic_findings, incomplete_ids = _llm_review(
        reviewable,
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
        batch_size=batch_size,
    ) if reviewable else ([], set())
    findings.extend(semantic_findings)
    by_qid = {qid: b for qid, b in indexed}
    for qid in sorted(incomplete_ids):
        benchmark = by_qid.get(qid, {})
        findings.append(
            _finding(
                question_id=qid,
                question=str(benchmark.get("question") or ""),
                source=_source(benchmark),
                category=REVIEW_SYSTEM,
                code="REVIEW_NOT_RUN",
                severity="warning",
                confidence=0.0,
                explanation="Semantic benchmark review did not complete for this question.",
                expected_sql=str(benchmark.get("expected_sql") or ""),
            )
        )

    findings_by_id: dict[str, list[dict[str, Any]]] = {qid: [] for qid, _ in indexed}
    for finding in findings:
        findings_by_id.setdefault(str(finding["question_id"]), []).append(finding)

    accepted: list[dict] = []
    excluded: list[dict] = []
    benchmark_results: list[dict[str, Any]] = []
    for qid, benchmark in indexed:
        row_findings = findings_by_id.get(qid, [])
        has_error = any(f.get("severity") == "error" for f in row_findings)
        disposition = "excluded" if has_error else ("warning" if row_findings else "passed")
        (excluded if has_error else accepted).append(benchmark)
        benchmark_results.append(
            {
                "question_id": qid,
                "question": str(benchmark.get("question") or ""),
                "source": _source(benchmark),
                "disposition": disposition,
                "findings": row_findings,
            }
        )

    reviewed = max(len(reviewable) - len(incomplete_ids), 0)
    semantic_coverage = reviewed / len(reviewable) if reviewable else 1.0
    return {
        "version": QUALITY_REVIEW_VERSION,
        "review_status": "degraded" if incomplete_ids else "complete",
        "semantic_review_coverage": round(semantic_coverage, 4),
        "accepted": accepted,
        "excluded": excluded,
        "findings": findings,
        "benchmark_results": benchmark_results,
        "counts": {
            "total": len(benchmarks),
            "trusted": sum(1 for r in benchmark_results if r["disposition"] == "passed"),
            "warnings": sum(1 for r in benchmark_results if r["disposition"] == "warning"),
            "excluded": sum(1 for r in benchmark_results if r["disposition"] == "excluded"),
            "review_not_run": len(incomplete_ids),
        },
    }
