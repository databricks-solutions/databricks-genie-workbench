"""Genie benchmark-eval taxonomy mapping for MLflow visualization.

This module is deliberately additive: it maps existing GSO judge outputs to
Genie product eval reason codes without changing scorer values, thresholds,
RCA routing, or optimizer behavior.
"""

from __future__ import annotations

from typing import Any

MAPPING_VERSION = "gso_genie_eval_taxonomy_v1"
LOW_CONFIDENCE_THRESHOLD = 0.5

GENIE_DETERMINISTIC_REASON_RATIONALES: dict[str, str] = {
    "EMPTY_RESULT": (
        "Genie's generated SQL results were empty for this benchmark question."
    ),
    "RESULT_MISSING_ROWS": (
        "Genie's generated SQL response is missing rows from the provided ground truth SQL."
    ),
    "RESULT_EXTRA_ROWS": (
        "Genie's generated SQL response has more rows than the provided ground truth SQL."
    ),
    "RESULT_MISSING_COLUMNS": (
        "Genie's generated SQL response is missing columns from the provided ground truth SQL."
    ),
    "RESULT_EXTRA_COLUMNS": (
        "Genie's generated SQL response has more columns than the provided ground truth SQL."
    ),
    "SINGLE_CELL_DIFFERENCE": (
        "A single value result was produced but differs from the ground truth result."
    ),
    "EMPTY_GOOD_SQL": "The benchmark SQL returned an empty result.",
    "COLUMN_TYPE_DIFFERENCE": (
        "The values between the results match but the column type is different."
    ),
}

GENIE_LLM_REASON_RATIONALES: dict[str, str] = {
    "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER": (
        "Genie's generated SQL is missing a WHERE clause condition or has incorrect "
        "filter logic that excludes or includes wrong data."
    ),
    "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT": (
        "Genie's generated SQL returns only some of the requested data or columns, "
        "missing parts of what the ground truth SQL returns."
    ),
    "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST": (
        "Genie's generated SQL fundamentally misunderstands what the user is asking "
        "for, addressing the wrong question or goal."
    ),
    "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC": (
        "Genie's generated SQL fails to apply specified instructions or business "
        "logic that should be followed."
    ),
    "LLM_JUDGE_INCORRECT_METRIC_CALCULATION": (
        "Genie's generated SQL uses incorrect logic or makes wrong assumptions when "
        "calculating metrics."
    ),
    "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE": (
        "Genie's generated SQL references wrong tables or columns, or uses fields "
        "that do not match the ground truth SQL's intent."
    ),
    "LLM_JUDGE_INCORRECT_FUNCTION_USAGE": (
        "Genie's generated SQL uses SQL functions incorrectly or inappropriately, "
        "including wrong parameters or the wrong function for the task."
    ),
    "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN": (
        "Genie's generated SQL is missing necessary joins between tables or has "
        "incorrect join conditions or join types that produce wrong results."
    ),
    "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION": (
        "Genie's generated SQL is missing GROUP BY clauses or has incorrect grouping "
        "that does not match the requested aggregation level."
    ),
    "LLM_JUDGE_FORMATTING_ERROR": (
        "Genie's generated SQL output has incorrect formatting, ordering, or "
        "presentation issues that do not match expectations."
    ),
    "LLM_JUDGE_OTHER": (
        "The judge identified an error that does not fall into another Genie eval category."
    ),
}

GENIE_REASON_RATIONALES: dict[str, str] = {
    **GENIE_DETERMINISTIC_REASON_RATIONALES,
    **GENIE_LLM_REASON_RATIONALES,
}

JUDGE_FAILURE_TO_GENIE_REASON: dict[tuple[str, str], str] = {
    ("schema_accuracy", "wrong_table"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("schema_accuracy", "wrong_column"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("schema_accuracy", "missing_column"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("schema_accuracy", "wrong_join"): "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    ("schema_accuracy", "missing_join"): "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    ("schema_accuracy", "missing_join_spec"): "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    ("schema_accuracy", "wrong_join_spec"): "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    ("schema_accuracy", "incorrect_function_usage"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("schema_accuracy", "wrong_function"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("schema_accuracy", "tvf_parameter_error"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("logical_accuracy", "missing_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("logical_accuracy", "wrong_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("logical_accuracy", "wrong_filter_condition"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("logical_accuracy", "missing_temporal_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("logical_accuracy", "wrong_aggregation"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("logical_accuracy", "missing_aggregation"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("logical_accuracy", "wrong_groupby"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("logical_accuracy", "wrong_grouping"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("logical_accuracy", "wrong_measure"): "LLM_JUDGE_INCORRECT_METRIC_CALCULATION",
    ("logical_accuracy", "different_metric"): "LLM_JUDGE_INCORRECT_METRIC_CALCULATION",
    ("logical_accuracy", "incorrect_function_usage"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("logical_accuracy", "wrong_function"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("logical_accuracy", "tvf_parameter_error"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("logical_accuracy", "wrong_orderby"): "LLM_JUDGE_FORMATTING_ERROR",
    ("logical_accuracy", "formatting_error"): "LLM_JUDGE_FORMATTING_ERROR",
    ("logical_accuracy", "missing_instruction"): "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    ("logical_accuracy", "business_logic_missing"): "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    ("semantic_equivalence", "different_metric"): "LLM_JUDGE_INCORRECT_METRIC_CALCULATION",
    ("semantic_equivalence", "different_grain"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("semantic_equivalence", "different_scope"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("semantic_equivalence", "misinterpreted_request"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("completeness", "partial_answer"): "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT",
    ("completeness", "missing_column"): "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT",
    ("completeness", "missing_dimension"): "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT",
    ("completeness", "missing_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("completeness", "missing_temporal_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("completeness", "missing_aggregation"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("completeness", "missing_instruction"): "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    ("completeness", "business_logic_missing"): "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    ("response_quality", "inaccurate_description"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("response_quality", "unsupported_claim"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("response_quality", "misleading_summary"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("response_quality", "misinterpreted_request"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("response_quality", "formatting_error"): "LLM_JUDGE_FORMATTING_ERROR",
    ("asset_routing", "asset_routing_error"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("asset_routing", "wrong_function"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("asset_routing", "incorrect_function_usage"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("asset_routing", "tvf_parameter_error"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("arbiter", "wrong_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("arbiter", "missing_filter"): "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    ("arbiter", "wrong_aggregation"): "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    ("arbiter", "wrong_measure"): "LLM_JUDGE_INCORRECT_METRIC_CALCULATION",
    ("arbiter", "wrong_table"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("arbiter", "wrong_column"): "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    ("arbiter", "wrong_join"): "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    ("arbiter", "missing_instruction"): "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    ("arbiter", "misinterpreted_request"): "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    ("arbiter", "formatting_error"): "LLM_JUDGE_FORMATTING_ERROR",
    ("arbiter", "incorrect_function_usage"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("repeatability", "repeatability_issue"): "LLM_JUDGE_OTHER",
    ("syntax_validity", "no_genie_sql"): "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT",
    ("syntax_validity", "unbalanced_identifier_quoting"): "LLM_JUDGE_OTHER",
    ("syntax_validity", "incorrect_function_usage"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    ("syntax_validity", "tvf_parameter_error"): "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
}


def _normalise(value: Any) -> str:
    return str(value or "").strip().lower()


def _reason_family(reason: str | None, *, judge_name: str = "") -> str:
    if judge_name == "result_correctness":
        return "deterministic"
    if not reason:
        return "none"
    if reason in GENIE_DETERMINISTIC_REASON_RATIONALES:
        return "deterministic"
    if reason in GENIE_LLM_REASON_RATIONALES:
        return "llm_judge"
    return "unmapped"


def _classify_result_correctness_reason(comparison: dict[str, Any]) -> str | None:
    cmp = comparison or {}
    match_type = _normalise(cmp.get("match_type"))
    gt_rows = int(cmp.get("gt_rows") or 0)
    genie_rows = int(cmp.get("genie_rows") or 0)
    gt_columns = set(str(c) for c in (cmp.get("gt_columns") or []))
    genie_columns = set(str(c) for c in (cmp.get("genie_columns") or []))

    if match_type == "both_empty" or (gt_rows == 0 and genie_rows == 0):
        return None
    if gt_rows == 0 and genie_rows > 0:
        return "EMPTY_GOOD_SQL"
    if gt_rows > 0 and genie_rows == 0:
        return "EMPTY_RESULT"
    if gt_columns and genie_columns and genie_columns < gt_columns:
        return "RESULT_MISSING_COLUMNS"
    if gt_columns and genie_columns and genie_columns > gt_columns:
        return "RESULT_EXTRA_COLUMNS"
    if gt_rows > genie_rows:
        return "RESULT_MISSING_ROWS"
    if gt_rows < genie_rows:
        return "RESULT_EXTRA_ROWS"
    if cmp.get("column_type_difference"):
        return "COLUMN_TYPE_DIFFERENCE"
    if gt_rows == 1 and genie_rows == 1 and len(gt_columns) == 1 and len(genie_columns) == 1:
        return "SINGLE_CELL_DIFFERENCE"
    return None


def build_genie_equivalent_eval(
    *,
    judge_name: str,
    value: str,
    failure_type: str | None = None,
    confidence: float | None = None,
    comparison: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return Genie-equivalent eval metadata for an existing GSO judge result."""
    raw_value = _normalise(value)
    raw_failure = _normalise(failure_type)
    mapping_confidence = float(confidence if confidence is not None else 1.0)

    if raw_value in {"yes", "true", "1", "both_correct", "genie_correct"}:
        reason = None
        assessment = "GOOD"
        unmapped = False
    elif judge_name == "result_correctness":
        reason = _classify_result_correctness_reason(comparison or {})
        unmapped = reason is None
        assessment = "NEEDS_REVIEW" if unmapped or mapping_confidence < LOW_CONFIDENCE_THRESHOLD else "BAD"
        if reason is None:
            reason = "LLM_JUDGE_OTHER"
    else:
        reason = JUDGE_FAILURE_TO_GENIE_REASON.get((judge_name, raw_failure))
        unmapped = reason is None
        if reason is None:
            reason = "LLM_JUDGE_OTHER"
        assessment = "NEEDS_REVIEW" if unmapped or mapping_confidence < LOW_CONFIDENCE_THRESHOLD else "BAD"

    reasons = [] if assessment == "GOOD" else [reason]
    reason_rationales = {
        r: GENIE_REASON_RATIONALES[r]
        for r in reasons
        if r in GENIE_REASON_RATIONALES
    }
    return {
        "assessment": assessment,
        "primary_assessment_reason": reason if reasons else None,
        "assessment_reasons": reasons,
        "reason_family": _reason_family(reason if reasons else None, judge_name=judge_name),
        "reason_rationales": reason_rationales,
        "mapped_from": {
            "judge": judge_name,
            "failure_type": failure_type or "",
        },
        "mapping_confidence": mapping_confidence,
        "unmapped": unmapped,
        "mapping_version": MAPPING_VERSION,
    }


def with_genie_equivalent_eval(
    metadata: dict[str, Any] | None,
    *,
    judge_name: str,
    value: str,
    failure_type: str | None = None,
    confidence: float | None = None,
    comparison: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a copy of metadata with additive Genie-equivalent eval details."""
    enriched = dict(metadata or {})
    resolved_failure_type = failure_type
    if resolved_failure_type is None:
        resolved_failure_type = str(enriched.get("failure_type") or "")
    resolved_confidence = confidence
    if resolved_confidence is None and enriched.get("confidence") is not None:
        try:
            resolved_confidence = float(enriched["confidence"])
        except (TypeError, ValueError):
            resolved_confidence = None
    enriched["genie_equivalent_eval"] = build_genie_equivalent_eval(
        judge_name=judge_name,
        value=value,
        failure_type=resolved_failure_type,
        confidence=resolved_confidence,
        comparison=comparison,
    )
    return enriched


def format_genie_eval_summary(genie_equivalent_eval: dict[str, Any] | None) -> str:
    """Return a compact summary line for MLflow rationale rendering."""
    if not genie_equivalent_eval:
        return ""
    assessment = genie_equivalent_eval.get("assessment", "NEEDS_REVIEW")
    reason = genie_equivalent_eval.get("primary_assessment_reason")
    if reason:
        return f"Genie equivalent eval: {assessment} / {reason}"
    return f"Genie equivalent eval: {assessment}"
