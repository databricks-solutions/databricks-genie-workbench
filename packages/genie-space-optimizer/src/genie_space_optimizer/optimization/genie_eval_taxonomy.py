"""Reason-code descriptions for official Genie benchmark evaluations."""

from __future__ import annotations


MAPPING_VERSION = "gso_genie_eval_taxonomy_v1"

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
