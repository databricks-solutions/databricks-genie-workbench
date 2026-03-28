"""
Scorer assembly — 9 judges for Genie Space evaluation.

Scorers that depend on runtime context (``spark``, ``WorkspaceClient``)
expose ``_make_*`` factory functions.  Use ``make_all_scorers()`` to get
a fully bound ``all_scorers`` list ready for ``mlflow.genai.evaluate()``.

Stateless scorers (``asset_routing_scorer``, ``result_correctness_scorer``)
are importable directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from genie_space_optimizer.optimization.evaluation import build_temporal_note

from .asset_routing import asset_routing_scorer
from .repeatability import repeatability_scorer
from .result_correctness import result_correctness_scorer

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession


def build_scorer_context(
    *,
    question: str,
    genie_sql: str,
    gt_sql: str,
    cmp: dict,
    include_column_note: bool = False,
) -> str:
    """Build the common context block shared by all SQL-comparison judges.

    Returns a multi-line string with comparison summary, empty-data notes,
    column subset notes, and temporal notes — ready to append to any
    judge-specific instructions.
    """
    parts: list[str] = []

    parts.append(f"User question: {question}")
    parts.append(f"Expected SQL: {gt_sql}")
    parts.append(f"Generated SQL: {genie_sql}")

    if cmp:
        parts.append(
            f"\nResult comparison (GT vs Genie): match={cmp.get('match')}, "
            f"match_type={cmp.get('match_type', 'unknown')}, "
            f"gt_rows={cmp.get('gt_rows', '?')}, genie_rows={cmp.get('genie_rows', '?')}\n"
            "NOTE: If results match (match=True), the queries are functionally equivalent "
            "even if SQL syntax differs. Weight this heavily in your assessment."
        )

    if include_column_note:
        gt_cols = set(cmp.get("gt_columns") or [])
        genie_cols = set(cmp.get("genie_columns") or [])
        extra_gt_cols = gt_cols - genie_cols
        if extra_gt_cols and genie_cols <= gt_cols:
            parts.append(
                f"\nCOLUMN NOTE: The Expected SQL returns {len(extra_gt_cols)} extra column(s) "
                f"not in Generated SQL: {sorted(extra_gt_cols)}. "
                "All Generated SQL columns exist in Expected SQL — this is a column SUBSET. "
                "Evaluate against the USER'S QUESTION only. If those extra columns were NOT "
                "requested by the user, the queries are still equivalent/complete."
            )

    gt_rows = cmp.get("gt_rows", -1)
    genie_rows = cmp.get("genie_rows", -1)
    if gt_rows == 0 and genie_rows > 0:
        parts.append(
            "\nCRITICAL: The Expected SQL returned 0 rows while the Generated SQL "
            "returned data. This likely means the Expected SQL has overly restrictive "
            "filters NOT present in the user's question. Evaluate the Generated SQL "
            "SOLELY against the user's question. If it correctly answers what the "
            "user asked, it should PASS."
        )
    elif gt_rows == 0:
        parts.append(
            "\nIMPORTANT: Both queries returned 0 rows — the underlying dataset "
            "has no matching data. Minor SQL differences have no practical impact. "
            "Lean toward PASS if structurally sound."
        )

    temporal = build_temporal_note(cmp)
    if temporal:
        parts.append(temporal)

    return "\n".join(parts)


def make_all_scorers(
    w: WorkspaceClient,
    spark: SparkSession,
    catalog: str,
    schema: str,
    loaded_prompts: dict[str, str] | None = None,
    instruction_context: str = "",
) -> list:
    """Assemble all 9 scorers with bound runtime context.

    Returns an ordered list suitable for passing to
    ``mlflow.genai.evaluate(scorers=...)``.
    """
    from .arbiter import _make_arbiter_scorer
    from .completeness import _make_completeness_judge
    from .logical_accuracy import _make_logical_accuracy_judge
    from .response_quality import _make_response_quality_judge
    from .schema_accuracy import _make_schema_accuracy_judge
    from .semantic_equivalence import _make_semantic_equivalence_judge
    from .syntax_validity import _make_syntax_validity_scorer

    return [
        _make_syntax_validity_scorer(spark, catalog, schema),
        _make_schema_accuracy_judge(w, catalog, schema),
        _make_logical_accuracy_judge(w, catalog, schema),
        _make_semantic_equivalence_judge(w, catalog, schema),
        _make_completeness_judge(w, catalog, schema),
        _make_response_quality_judge(w, catalog, schema),
        asset_routing_scorer,
        result_correctness_scorer,
        _make_arbiter_scorer(w, catalog, schema, loaded_prompts, instruction_context=instruction_context),
    ]


def make_repeatability_scorers() -> list:
    """Return scorers for repeatability-only evaluation runs.

    Uses the repeatability scorer plus result_correctness (to track whether
    the answer is still correct) and asset_routing.
    """
    return [
        repeatability_scorer,
        result_correctness_scorer,
        asset_routing_scorer,
    ]


__all__ = [
    "asset_routing_scorer",
    "build_scorer_context",
    "repeatability_scorer",
    "result_correctness_scorer",
    "make_all_scorers",
    "make_repeatability_scorers",
]
