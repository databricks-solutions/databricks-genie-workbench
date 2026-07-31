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

QUALITY_REVIEW_VERSION = "benchmark_quality_v2"
QUALITY_ERROR_CONFIDENCE = 0.75

QUESTION_QUALITY = "question_quality"
QUESTION_SQL_ALIGNMENT = "question_sql_alignment"
SQL_VALIDITY = "sql_validity"
DATA_VALIDITY = "data_validity"
REVIEW_SYSTEM = "review_system"

_NON_ACTIONABLE_WARNING_CODES = frozenset(
    {
        "GT_EXECUTION_NOT_RUN",
        "REVIEW_NOT_RUN",
    }
)

_QUESTION_QUALITY_CODES = frozenset(
    {
        "AMBIGUOUS_METRIC",
        "AMBIGUOUS_TIME_SCOPE",
        "AMBIGUOUS_GRAIN",
        "UNANSWERABLE_FROM_SPACE",
        "IMPLEMENTATION_HINT",
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
    }
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
            findings.append(
                _finding(
                    question_id=qid,
                    question=str(benchmark.get("question") or ""),
                    source=_source(benchmark),
                    category=DATA_VALIDITY,
                    code="DATA_VALUE_MISMATCH",
                    severity="error",
                    explanation="Ground-truth SQL uses filter values not found in the data profile.",
                    expected_sql=str(benchmark.get("expected_sql") or ""),
                    evidence=result.get("mismatches"),
                    proposed_sql=proposed_sql,
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
