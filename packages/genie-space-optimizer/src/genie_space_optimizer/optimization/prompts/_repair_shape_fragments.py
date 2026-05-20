"""Plan 9 Task 2 — RepairShape-keyed prompt fragment registry.

Replaces the pre-Plan-9 archetype.prompt_template interpolation.
Each RepairShape enum member maps to a short natural-language
description of the shape that gets fed into the L5b / L6 generator
prompt. RepairShape.OTHER is the free-form structural rewrite
safety net — the ultimate fallback after catalog removal.

When a new RepairShape is added to repair_intent.py, an entry MUST
be added here in the same commit. Pinned by
test_repair_shape_fragments_registry.test_every_repair_shape_has_a_fragment.

Adding new entries is the path to expressing new repair patterns
without inventing new archetypes — the LLM picks the shape; the
renderer threads the fragment; no catalog gate.
"""
from __future__ import annotations

from typing import Union

from genie_space_optimizer.optimization.repair_intent import RepairShape


REPAIR_SHAPE_FRAGMENTS: dict[RepairShape, str] = {
    RepairShape.TOP_N_BY_METRIC: (
        "Produce a Top-N query: aggregate a numeric column by a "
        "categorical dimension, ORDER BY the aggregate DESC, LIMIT N. "
        "Use the target_objects you emitted to pick the table and "
        "the metric / dimension columns. Do NOT reproduce any benchmark "
        "text; invent a concrete but reasonable question."
    ),
    RepairShape.ORDERED_LIST_BY_METRIC: (
        "Produce a cardinality-preserving ordered-list query for a plural "
        "ranking question. Aggregate a numeric column by a categorical "
        "dimension and ORDER BY the aggregate DESC. Do not filter to "
        "rank = 1 or use SELECT TOP 1 — the question is about ranking, "
        "not about picking a single row. Do NOT reproduce any benchmark "
        "text; invent a concrete but reasonable question."
    ),
    RepairShape.RANK_WITHIN_GROUP: (
        "Rank rows within each group using ROW_NUMBER() or RANK() OVER "
        "(PARTITION BY dim ORDER BY metric DESC). Use the target_objects "
        "to pick the partition dimension and the ranking metric column."
    ),
    RepairShape.PERIOD_OVER_PERIOD: (
        "Compare a metric across two time windows (e.g. this month vs "
        "last month, day vs MTD, current quarter vs prior quarter). "
        "Use DATE_TRUNC or a simple range predicate; pick the time "
        "column from your target_objects. Provide a clear "
        "business-meaningful question."
    ),
    RepairShape.FILTER_COMPOSE: (
        "Compose a named reusable filter as an SQL snippet. Example: "
        "is_active_customer := status = 'active' AND deleted_at IS NULL. "
        "Use the target_objects to pick the columns the filter "
        "references; the filter snippet name should be descriptive."
    ),
    RepairShape.FILTER_REMOVE: (
        "Remove a defensive or overly-narrow filter that excludes rows "
        "the question actually needs. Emit a corrective example SQL "
        "that demonstrates the correct (broader) filter, or emit an "
        "instruction snippet that documents the filter must be removed. "
        "Use target_objects to identify the column(s) the existing "
        "filter wrongly constrains."
    ),
    RepairShape.JOIN_DISCOVERY: (
        "Demonstrate the correct join between two related entities. "
        "Use the foreign-key column names from your target_objects "
        "explicitly (e.g. child.parent_id = parent.id) and pick the "
        "right join type (INNER vs LEFT). Project a small handful of "
        "columns from both sides so the relationship is unambiguous."
    ),
    RepairShape.SQL_EXPRESSION: (
        "Emit a named SQL expression (`add_sql_snippet_expression`) "
        "that computes a derived value from existing columns. Use "
        "target_objects to anchor the expression to the correct "
        "table and columns. The expression should be reusable across "
        "multiple example SQLs."
    ),
    RepairShape.COLUMN_DESCRIPTION: (
        "Add or refine a column description to disambiguate two "
        "columns the LLM is confusing (e.g. prefix-similar columns "
        "like is_prior_year_same_day vs is_one_day_prior_year_same_day). "
        "Use target_objects to pick the COLUMN-kind identifier; the "
        "description should explain when to use this column vs the "
        "confusable alternative."
    ),
    RepairShape.METRIC_VIEW_REFINEMENT: (
        "Refine a metric view: add a missing dimension or measure, "
        "or rename a measure to clarify its semantics. Use "
        "target_objects with asset_kind=metric_view; the columns "
        "list should enumerate the measures/dimensions the refinement "
        "affects."
    ),
    RepairShape.INSTRUCTION: (
        "Emit a natural-language instruction that documents a rule "
        "the LLM keeps violating (e.g. 'always GROUP BY all "
        "non-aggregated SELECT columns'). Instructions do not require "
        "target_objects — leave the array empty."
    ),
    RepairShape.OTHER: (
        # ULTIMATE SAFETY NET — Plan 9's free-form structural rewrite
        # fragment. Fires when the LLM picks RepairShape.OTHER (e.g.
        # because the repair pattern is novel and does not fit any
        # named shape). The fragment instructs the LLM to emit a
        # self-contained example SQL or snippet, justify why no named
        # shape fits, and ground every column reference in
        # target_objects. After catalog removal (T10), this is the
        # only deterministic fallback left.
        "Free-form structural rewrite: emit an example SQL or SQL "
        "snippet that solves the cluster's failure pattern using "
        "ONLY the assets and columns named in your target_objects. "
        "Your rationale MUST explain why none of the named "
        "RepairShape values fit this repair. Ground every column "
        "reference in target_objects; do not invent identifiers. "
        "Keep the SQL self-contained and runnable against the "
        "schema you were shown."
    ),
}


def fragment_for(repair_shape: Union[RepairShape, str]) -> str:
    """Return the prompt fragment for the given RepairShape.

    Accepts RepairShape enum value or raw string (for replay of
    pre-Plan-9 traces). Unknown strings fall back to the OTHER
    fragment.
    """
    if isinstance(repair_shape, str):
        try:
            repair_shape = RepairShape(repair_shape)
        except ValueError:
            return REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]
    return REPAIR_SHAPE_FRAGMENTS.get(
        repair_shape,
        REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER],
    )
