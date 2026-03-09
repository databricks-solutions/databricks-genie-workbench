"""Result comparator for auto-labeling benchmark questions.

Uses a two-phase approach:
1. Quick heuristic pre-check for obvious exact/value matches (no LLM needed)
2. LLM-based semantic comparison for everything else — considers SQL intent,
   column name similarity, value equivalence, and overall correctness
"""

import json
import logging
import math

from backend.models import ComparisonDiscrepancy, ComparisonResult

logger = logging.getLogger(__name__)


def _normalize_value(val: object) -> object:
    """Normalize a single cell value for comparison."""
    if val is None:
        return None
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return round(val, 6)
    if isinstance(val, str):
        return val.strip().lower()
    return val


def _normalize_columns(columns: list[dict]) -> list[str]:
    """Extract and lowercase column names."""
    return [c.get("name", "").lower() for c in columns]


def _normalize_rows(data: list[list]) -> list[tuple]:
    """Normalize and sort rows for deterministic comparison."""
    normalized = [
        tuple(_normalize_value(cell) for cell in row)
        for row in data
    ]
    normalized.sort(key=lambda r: tuple(str(v) for v in r))
    return normalized


def _quick_heuristic_check(
    genie_result: dict,
    expected_result: dict,
) -> ComparisonResult | None:
    """Fast heuristic check for obvious matches. Returns None if inconclusive.

    Only returns a result for clear exact/value matches where LLM is unnecessary.
    """
    genie_error = genie_result.get("error")
    expected_error = expected_result.get("error")

    if genie_error or expected_error:
        return None  # Let LLM assess error cases

    genie_cols = genie_result.get("columns", [])
    expected_cols = expected_result.get("columns", [])
    genie_data = genie_result.get("data", [])
    expected_data = expected_result.get("data", [])

    genie_col_names = _normalize_columns(genie_cols)
    expected_col_names = _normalize_columns(expected_cols)

    genie_rows = _normalize_rows(genie_data)
    expected_rows = _normalize_rows(expected_data)

    # Exact match: same columns, same rows
    if genie_col_names == expected_col_names and genie_rows == expected_rows:
        return ComparisonResult(
            match_type="exact",
            confidence=1.0,
            auto_label=True,
            discrepancies=[],
            summary="Results are an exact match — same columns and data.",
        )

    # Same column set, reorder and check
    if set(genie_col_names) == set(expected_col_names) and genie_rows and expected_rows:
        col_mapping = [genie_col_names.index(c) for c in expected_col_names]
        reordered = [tuple(row[i] for i in col_mapping) for row in genie_rows]
        reordered.sort(key=lambda r: tuple(str(v) for v in r))
        if reordered == expected_rows:
            return ComparisonResult(
                match_type="value_match",
                confidence=0.95,
                auto_label=True,
                discrepancies=[ComparisonDiscrepancy(
                    type="column_order",
                    detail="Column order differs but all data matches.",
                )],
                summary="Data matches exactly — column order is different but values are identical.",
            )

    return None  # Inconclusive — need LLM


def _build_comparison_prompt(
    question: str | None,
    genie_sql: str | None,
    expected_sql: str | None,
    genie_result: dict,
    expected_result: dict,
) -> str:
    """Build the LLM prompt for semantic result comparison."""

    # Format results as compact tables for the prompt
    def _format_result(result: dict) -> str:
        if result.get("error"):
            return f"ERROR: {result['error']}"
        cols = [c.get("name", "?") for c in result.get("columns", [])]
        data = result.get("data", [])
        if not cols and not data:
            return "Empty result set"
        lines = [" | ".join(cols)]
        lines.append("-" * len(lines[0]))
        for row in data[:20]:  # Cap at 20 rows for prompt size
            lines.append(" | ".join(str(v) for v in row))
        if len(data) > 20:
            lines.append(f"... ({len(data)} total rows)")
        return "\n".join(lines)

    question_section = f"## Question\n{question}\n" if question else ""
    genie_sql_section = f"## Genie's SQL\n```sql\n{genie_sql}\n```\n" if genie_sql else ""
    expected_sql_section = f"## Expected SQL\n```sql\n{expected_sql}\n```\n" if expected_sql else ""

    return f"""You are comparing two SQL query results to determine if Genie answered a benchmark question correctly.

{question_section}
{genie_sql_section}
{expected_sql_section}
## Genie's Result
{_format_result(genie_result)}

## Expected Result
{_format_result(expected_result)}

## Instructions
Determine if Genie's answer is semantically correct. Be pragmatic, not pedantic:

- **Column names don't matter** — "total_units_sold" and "total_units" are the same thing if the values match
- **Column order doesn't matter** — focus on the data, not column position
- **Row order doesn't matter** — same rows in different order is a match
- **Minor rounding differences** (< 1%) in numeric values are acceptable
- **NULL vs empty string** — treat as equivalent
- **Extra columns** in Genie's output are OK if all expected data is present
- **Missing columns** matter — if expected data is absent, that's a problem
- **SQL approach differences** are fine if they produce equivalent results (e.g., different JOINs, subquery vs CTE)
- If one query errored and the other didn't, that's incorrect
- If both returned data, compare the actual values — the SQL approach doesn't matter

Classify as:
- **correct**: The results are semantically equivalent — Genie answered the question right
- **partially_correct**: Most of the answer is right but with notable differences (e.g., 90%+ values match, or one extra/missing row)
- **incorrect**: The results are meaningfully different — wrong values, wrong rows, or wrong interpretation

Output JSON:
{{
  "match_type": "correct" | "partially_correct" | "incorrect",
  "confidence": 0.0-1.0,
  "summary": "One sentence explaining your assessment",
  "discrepancies": [
    {{"type": "...", "detail": "..."}}
  ]
}}

For discrepancy types use: "value_diff", "missing_column", "extra_rows", "missing_rows", "sql_error", "different_interpretation", "rounding", "column_alias".
Only include discrepancies for actual issues — omit trivial differences like column aliases."""

    return prompt


def compare_results(
    genie_result: dict,
    expected_result: dict,
    genie_sql: str | None = None,
    expected_sql: str | None = None,
    question: str | None = None,
) -> ComparisonResult:
    """Compare Genie vs expected SQL results with semantic analysis.

    Uses a fast heuristic for obvious matches, falls back to LLM for nuanced cases.

    Args:
        genie_result: Dict with columns, data, row_count, truncated, error
        expected_result: Dict with same shape
        genie_sql: The SQL Genie generated (optional, improves LLM analysis)
        expected_sql: The expected benchmark SQL (optional)
        question: The benchmark question text (optional)

    Returns:
        ComparisonResult with match_type, confidence, discrepancies, and summary
    """
    # Phase 1: Quick heuristic for obvious exact/value matches
    quick = _quick_heuristic_check(genie_result, expected_result)
    if quick is not None:
        return quick

    # Phase 2: LLM-based semantic comparison
    try:
        from backend.services.llm_utils import call_serving_endpoint, get_llm_model, parse_json_from_llm_response

        prompt = _build_comparison_prompt(
            question=question,
            genie_sql=genie_sql,
            expected_sql=expected_sql,
            genie_result=genie_result,
            expected_result=expected_result,
        )

        content = call_serving_endpoint(
            messages=[{"role": "user", "content": prompt}],
            model=get_llm_model(),
            max_tokens=1024,
        )

        result = parse_json_from_llm_response(content)

        # Map LLM match types to our model
        match_type = result.get("match_type", "incorrect")
        confidence = float(result.get("confidence", 0.5))
        summary = result.get("summary", "")

        # Map to auto_label
        if match_type == "correct":
            auto_label = True
            display_match_type = "correct"
        elif match_type == "partially_correct":
            auto_label = confidence >= 0.8
            display_match_type = "partial"
        else:
            auto_label = False
            display_match_type = "mismatch"

        discrepancies = [
            ComparisonDiscrepancy(type=d.get("type", "other"), detail=d.get("detail", ""))
            for d in result.get("discrepancies", [])
        ]

        return ComparisonResult(
            match_type=display_match_type,
            confidence=confidence,
            auto_label=auto_label,
            discrepancies=discrepancies,
            summary=summary,
        )

    except Exception as e:
        logger.warning(f"LLM comparison failed, falling back to heuristic: {e}")
        return _heuristic_fallback(genie_result, expected_result)


def _heuristic_fallback(
    genie_result: dict,
    expected_result: dict,
) -> ComparisonResult:
    """Fallback heuristic when LLM is unavailable."""
    genie_error = genie_result.get("error")
    expected_error = expected_result.get("error")

    if genie_error:
        return ComparisonResult(
            match_type="mismatch",
            confidence=1.0,
            auto_label=False,
            discrepancies=[ComparisonDiscrepancy(type="sql_error", detail=f"Genie query error: {genie_error}")],
            summary="Genie's query returned an error.",
        )
    if expected_error:
        return ComparisonResult(
            match_type="mismatch",
            confidence=0.5,
            auto_label=False,
            discrepancies=[ComparisonDiscrepancy(type="sql_error", detail=f"Expected query error: {expected_error}")],
            summary="Expected query returned an error — manual review needed.",
        )

    genie_data = genie_result.get("data", [])
    expected_data = expected_result.get("data", [])
    genie_rows = _normalize_rows(genie_data)
    expected_rows = _normalize_rows(expected_data)

    # Try positional value comparison ignoring column names
    if len(genie_rows) == len(expected_rows) and genie_rows == expected_rows:
        return ComparisonResult(
            match_type="correct",
            confidence=0.9,
            auto_label=True,
            discrepancies=[],
            summary="Data values match (column names may differ).",
        )

    # Compute overlap
    genie_set = set(genie_rows)
    expected_set = set(expected_rows)
    if expected_set:
        overlap = len(genie_set & expected_set) / len(expected_set)
    else:
        overlap = 1.0 if not genie_set else 0.0

    if overlap >= 0.9:
        return ComparisonResult(
            match_type="partial",
            confidence=overlap,
            auto_label=True,
            discrepancies=[ComparisonDiscrepancy(
                type="value_diff",
                detail=f"{int(overlap * 100)}% row overlap",
            )],
            summary=f"Results mostly match — {int(overlap * 100)}% of expected rows found.",
        )

    return ComparisonResult(
        match_type="mismatch",
        confidence=0.0,
        auto_label=False,
        discrepancies=[ComparisonDiscrepancy(
            type="value_diff",
            detail=f"Genie returned {len(genie_rows)} rows, expected {len(expected_rows)}",
        )],
        summary=f"Results differ — Genie returned {len(genie_rows)} rows vs {len(expected_rows)} expected.",
    )
