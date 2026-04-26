"""Structural archetype library for example_sql synthesis (Bug #4, P3.1).

The optimizer's example_sqls are the single biggest hint to Genie's query
generator. Historically those came from ``_mine_benchmark_example_sqls``
which copied expected_sql verbatim — textbook leakage. Replacement is
structural synthesis: pick an archetype that matches the failure mode,
fill in schema-appropriate placeholders, and let the LLM produce an
ORIGINAL example query of that shape.

Each Archetype declares:

* ``name`` — stable identifier for routing + observability.
* ``applicable_root_causes`` — cluster root causes (as emitted by
  ``cluster_failures``) where this archetype helps. Matching is all-or-
  nothing; order in the list does not matter.
* ``required_schema_traits`` — light schema checks (presence of a numeric
  column, a date column, etc.) gating applicability.
* ``prompt_template`` — a short natural-language description of the shape
  fed to the synthesis LLM. No raw benchmark text; the AFS + archetype
  together are the full context.
* ``output_shape`` — structural contract the 5-gate validator uses to
  check that the LLM honored the archetype.
* ``patch_type`` — where the synthesized output ends up. Most are
  ``add_example_sql``; a handful (``filter_compose``) route to
  ``add_sql_snippet_filter``.

``pick_archetype`` is deterministic: given the same AFS + schema, it
always returns the same archetype. No LLM involvement in routing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class Archetype:
    name: str
    applicable_root_causes: frozenset[str]
    required_schema_traits: frozenset[str]
    prompt_template: str
    output_shape: dict[str, Any]
    patch_type: str = "add_example_sql"
    # Pre-flight synthesis driver (``optimization/preflight_synthesis.py``)
    # has no failure cluster — it works from schema alone. A few shapes
    # (cohort retention, event sequences, self-joins, funnels) need a
    # failure signal to produce grounded examples; we exclude them from
    # pre-flight by flipping this flag. The reactive AFS path in
    # ``optimization/synthesis.py`` is unaffected.
    preflight_eligible: bool = True

    def matches(self, root_cause: str, schema_traits: set[str]) -> bool:
        if self.applicable_root_causes and root_cause not in self.applicable_root_causes:
            return False
        if self.required_schema_traits and not self.required_schema_traits.issubset(
            schema_traits
        ):
            return False
        return True


# ── Schema trait extraction ────────────────────────────────────────────


def _col_type(col: dict) -> str:
    """Canonical column-type reader.

    Production ``serialized_space`` columns store the type under
    ``data_type``; older / test fixtures use ``type_text`` or plain
    ``type``. Reading only one of those silently collapses trait
    detection to an empty set (which then drags the preflight planner
    down to a single eligible archetype). Centralising the fallback
    order here keeps every caller consistent.
    """
    return str(
        col.get("data_type")
        or col.get("type_text")
        or col.get("type")
        or ""
    ).lower()


def schema_traits(metadata_snapshot: dict) -> set[str]:
    """Lightweight classification of schema capabilities used for archetype
    gating. Intentionally coarse; any genuine signal comes from the AFS +
    blame_set, not from these traits.

    Accepts both snapshot shapes we see in the codebase:

    * Production ``serialized_space`` shape (harness passes
      ``config["_parsed_space"]``): tables live under
      ``data_sources.tables`` and metric views under
      ``data_sources.metric_views``.
    * Legacy / test fixtures: tables at the top level under ``tables``.

    Prefers ``data_sources.*`` when present; falls back to top-level
    keys. Silently returning empty traits here used to collapse the
    preflight planner to a single archetype (``filter_compose``) and
    caused the ``Generated candidates: 2`` regression.
    """
    traits: set[str] = set()
    ds = metadata_snapshot.get("data_sources") or {}
    if not isinstance(ds, dict):
        ds = {}

    tables_raw = ds.get("tables") or metadata_snapshot.get("tables") or []
    metric_views_raw = (
        ds.get("metric_views") or metadata_snapshot.get("metric_views") or []
    )

    tables = tables_raw if isinstance(tables_raw, list) else []

    for t in tables:
        if not isinstance(t, dict):
            continue
        for col in t.get("column_configs", []) or []:
            if not isinstance(col, dict):
                continue
            col_type = _col_type(col)
            if any(x in col_type for x in ("int", "double", "decimal", "long", "float", "numeric")):
                traits.add("has_numeric")
            if any(x in col_type for x in ("date", "timestamp")):
                traits.add("has_date")
            if "string" in col_type or "varchar" in col_type:
                traits.add("has_categorical")
    if len(tables) >= 2:
        traits.add("has_joinable")
    if metric_views_raw:
        traits.add("has_metric_view")
    else:
        # PR 14: detect MVs that Genie serialized under ``data_sources.tables``
        # by their measure-typed column configs. Without this, schemas
        # whose only MV is mis-classified as a table miss the
        # ``has_metric_view`` trait, which blocks the period_over_period
        # / ratio_by_dimension archetypes.
        for t in tables:
            if not isinstance(t, dict):
                continue
            for col in t.get("column_configs", []) or []:
                if not isinstance(col, dict):
                    continue
                if (
                    str(col.get("column_type", "")).lower() == "measure"
                    or col.get("is_measure")
                ):
                    traits.add("has_metric_view")
                    break
            if "has_metric_view" in traits:
                break

    # PR 19: catalog-detection fallback. Genie's serialized payload
    # sometimes omits both ``data_sources.metric_views`` AND the
    # ``column_type='measure'`` / ``is_measure`` flags, leaving real MVs
    # invisible to the two branches above. The harness populates
    # ``_metric_view_yaml`` from a ``DESCRIBE TABLE EXTENDED ... AS
    # JSON`` probe; consult it as a third source of truth so the trait
    # fires whenever any of the three detection paths sees an MV.
    if "has_metric_view" not in traits:
        cache = metadata_snapshot.get("_metric_view_yaml") or {}
        if isinstance(cache, dict) and cache:
            traits.add("has_metric_view")

    # PR 30 — final fallback: consult the unified ``_asset_semantics``
    # contract. Catches MVs where every legacy signal was reclassified
    # in profiling but the semantics builder still produced a metric_view
    # entry from structural catalog signals.
    if "has_metric_view" not in traits:
        try:
            from genie_space_optimizer.common.asset_semantics import (
                metric_view_identifiers as _sem_mv_idents,
            )
            if _sem_mv_idents(metadata_snapshot):
                traits.add("has_metric_view")
        except Exception:
            pass
    return traits


# ── Archetype catalog ──────────────────────────────────────────────────

_ROOT_CAUSES_AGG = frozenset({
    "missing_aggregation", "wrong_aggregation", "wrong_measure", "select_star",
})
_ROOT_CAUSES_FILTER = frozenset({
    "missing_filter", "wrong_filter", "wrong_filter_condition",
    "value_format_mismatch", "temporal_filter_missing",
})
_ROOT_CAUSES_JOIN = frozenset({
    "wrong_join", "wrong_join_spec", "missing_join", "missing_join_spec",
    "wrong_table",
})
_ROOT_CAUSES_RANKING = frozenset({
    "missing_limit", "wrong_ordering", "ranking_missing",
})
_ROOT_CAUSES_TIME = frozenset({
    "temporal_filter_missing", "missing_filter", "wrong_filter",
})


ARCHETYPES: list[Archetype] = [
    # Safety net: always eligible regardless of trait detection. Guarantees
    # the pre-flight planner has at least one archetype to emit even if
    # ``schema_traits`` returns an empty set (stale metadata, blank types,
    # etc.). Produces shape-valid, zero-filter SELECT queries that should
    # never EMPTY_RESULT on a non-empty table.
    Archetype(
        name="simple_enumerate",
        applicable_root_causes=frozenset(),
        required_schema_traits=frozenset(),
        prompt_template=(
            "Produce a straightforward enumerate-or-list query: "
            "SELECT a few columns FROM a single table, optionally with "
            "ORDER BY and LIMIT. No WHERE filters, no joins, no aggregation. "
            "Question should be a clean 'show me N <rows> from <asset>' style."
        ),
        output_shape={"requires_constructs": ["SELECT", "LIMIT"]},
        patch_type="add_example_sql",
    ),
    Archetype(
        name="top_n_by_metric",
        applicable_root_causes=(
            _ROOT_CAUSES_RANKING | _ROOT_CAUSES_AGG | frozenset({
                "plural_top_n_collapse",
            })
        ),
        required_schema_traits=frozenset({"has_numeric"}),
        prompt_template=(
            "Produce a Top-N query: aggregate a numeric column by a "
            "categorical dimension, ORDER BY the aggregate DESC, LIMIT N. "
            "Do NOT reproduce any benchmark text; invent a concrete but "
            "reasonable question."
        ),
        output_shape={
            "requires_constructs": ["SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"],
        },
    ),
    Archetype(
        name="group_by_all_projected_keys",
        applicable_root_causes=frozenset({
            "granularity_drop", "wrong_grouping",
        }),
        required_schema_traits=frozenset({"has_numeric", "has_categorical"}),
        prompt_template=(
            "Aggregate a numeric metric and report it for EVERY non-aggregated "
            "column in the SELECT list. The GROUP BY clause must enumerate ALL "
            "projected non-aggregated columns; do not drop any dimension that "
            "appears in SELECT. Demonstrate the rule on two or more grouping "
            "keys (e.g. region + time_window)."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY"]},
    ),
    Archetype(
        name="period_over_period",
        applicable_root_causes=(
            _ROOT_CAUSES_TIME | _ROOT_CAUSES_AGG | frozenset({
                "time_window_pivot",
            })
        ),
        required_schema_traits=frozenset({"has_numeric", "has_date"}),
        prompt_template=(
            "Compare a metric across two time windows (e.g. this month vs "
            "last month). Use DATE_TRUNC or a simple range predicate. "
            "Provide a clear business-meaningful question."
        ),
        output_shape={"requires_constructs": ["SELECT", "WHERE", "GROUP_BY"]},
    ),
    Archetype(
        name="correct_join_spec",
        applicable_root_causes=frozenset({
            "wrong_join_spec",
            "missing_join_spec",
            "wrong_join",
            "wrong_join_type",
        }),
        required_schema_traits=frozenset({"has_joinable"}),
        prompt_template=(
            "Demonstrate the correct join between two related entities. "
            "Use the foreign-key column names explicitly (e.g. "
            "child.parent_id = parent.id) and pick the right join type "
            "(INNER for required matches, LEFT for optional). Project a "
            "small handful of columns from both sides so the relationship "
            "is unambiguous."
        ),
        output_shape={"requires_constructs": ["SELECT", "JOIN"]},
        preflight_eligible=False,  # needs failure context to pick the join keys
    ),
    Archetype(
        name="cohort_retention",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_date", "has_joinable"}),
        prompt_template=(
            "Cohort retention: group users by their first activity month, "
            "then measure their activity in subsequent months."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY", "JOIN"]},
        preflight_eligible=False,  # needs failure context; too open-ended for schema-only synthesis
    ),
    Archetype(
        name="funnel_conversion",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_categorical"}),
        prompt_template=(
            "Compute a conversion funnel: count entities at each stage "
            "(viewed -> added -> purchased)."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY"]},
        preflight_eligible=False,  # stage detection needs domain vocabulary not in schema alone
    ),
    Archetype(
        name="ratio_by_dimension",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_numeric", "has_categorical"}),
        prompt_template=(
            "Compute a ratio metric (e.g. margin %, conversion %) by a "
            "categorical dimension. Use CASE + SUM / SUM or similar."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY"]},
    ),
    Archetype(
        name="running_total",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_numeric", "has_date"}),
        prompt_template=(
            "Running total over time: SUM(...) OVER (ORDER BY date)."
        ),
        output_shape={"requires_constructs": ["SELECT", "WINDOW"]},
    ),
    Archetype(
        name="rank_within_group",
        applicable_root_causes=(
            _ROOT_CAUSES_RANKING | frozenset({"plural_top_n_collapse"})
        ),
        required_schema_traits=frozenset({"has_numeric", "has_categorical"}),
        prompt_template=(
            "Rank rows within each group: ROW_NUMBER() or RANK() OVER "
            "(PARTITION BY dim ORDER BY metric DESC)."
        ),
        output_shape={"requires_constructs": ["SELECT", "WINDOW"]},
    ),
    Archetype(
        name="pct_change",
        applicable_root_causes=_ROOT_CAUSES_AGG | _ROOT_CAUSES_TIME,
        required_schema_traits=frozenset({"has_numeric", "has_date"}),
        prompt_template=(
            "Percent change period-over-period: (current - prior) / prior."
        ),
        output_shape={"requires_constructs": ["SELECT"]},
    ),
    Archetype(
        name="filter_compose",
        applicable_root_causes=_ROOT_CAUSES_FILTER,
        required_schema_traits=frozenset(),
        prompt_template=(
            "Compose a named reusable filter as an SQL snippet. Example: "
            "is_active_customer := status = 'active' AND deleted_at IS NULL."
        ),
        output_shape={"requires_constructs": ["WHERE"]},
        patch_type="add_sql_snippet_filter",
        # filter_compose emits a ``WHERE`` fragment, not a full SELECT; it is
        # the wrong shape for example-SQL synthesis. Keep it in the reactive
        # AFS path (where it still produces add_sql_snippet_filter patches)
        # but hide it from the schema-only preflight planner.
        preflight_eligible=False,
    ),
    Archetype(
        name="segment_compare",
        applicable_root_causes=_ROOT_CAUSES_AGG | _ROOT_CAUSES_FILTER,
        required_schema_traits=frozenset({"has_numeric", "has_categorical"}),
        prompt_template=(
            "Compare a metric between two segments (e.g. new vs returning "
            "customers). Use CASE inside SUM or separate CTEs."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY"]},
    ),
    Archetype(
        name="disambiguate_column",
        applicable_root_causes=frozenset({
            "column_disambiguation", "wrong_column",
        }),
        required_schema_traits=frozenset(),
        prompt_template=(
            "Pick the correct column among prefix-similar candidates. Phrase "
            "the example question so the answer hinges on choosing one of two "
            "columns that share a common name prefix (e.g. "
            "is_month_to_date_prior_year_same_day vs "
            "is_one_day_prior_year_same_day). Show the correct column in the "
            "SELECT list and explain the choice in the question."
        ),
        output_shape={"requires_constructs": ["SELECT"]},
        # Schema-only synthesis cannot know which prefix-pair is actually
        # confusing; the reactive cluster-driven path supplies that signal.
        preflight_eligible=False,
    ),
    Archetype(
        name="time_window_aggregate",
        applicable_root_causes=_ROOT_CAUSES_TIME | _ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_numeric", "has_date"}),
        prompt_template=(
            "Aggregate a numeric metric within a specified time window "
            "(e.g. trailing 30 days, current quarter)."
        ),
        output_shape={"requires_constructs": ["SELECT", "WHERE", "GROUP_BY"]},
    ),
    Archetype(
        name="self_join_hierarchy",
        applicable_root_causes=_ROOT_CAUSES_JOIN,
        required_schema_traits=frozenset({"has_joinable"}),
        prompt_template=(
            "Self-join to walk a hierarchy (e.g. employee -> manager). "
            "Use a CTE if the hierarchy can be multi-level."
        ),
        output_shape={"requires_constructs": ["SELECT", "JOIN"]},
        preflight_eligible=False,  # hierarchy detection requires FK awareness beyond trait inference
    ),
    Archetype(
        name="event_sequence",
        applicable_root_causes=_ROOT_CAUSES_JOIN | _ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_date"}),
        prompt_template=(
            "Find entities that performed event A before event B within a "
            "time window. Join on the entity, compare event timestamps."
        ),
        output_shape={"requires_constructs": ["SELECT", "JOIN", "WHERE"]},
        preflight_eligible=False,  # needs a concrete event vocabulary not derivable from schema alone
    ),
    Archetype(
        name="distinct_count_by_dim",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_categorical"}),
        prompt_template=(
            "COUNT(DISTINCT entity_id) grouped by a categorical dimension."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY"]},
    ),
    Archetype(
        name="pivot_wide",
        applicable_root_causes=_ROOT_CAUSES_AGG,
        required_schema_traits=frozenset({"has_numeric", "has_categorical"}),
        prompt_template=(
            "Pivot long-format data wide: one column per distinct category "
            "value using SUM + CASE WHEN."
        ),
        output_shape={"requires_constructs": ["SELECT", "GROUP_BY", "CASE"]},
    ),
]


def pick_archetype(
    cluster_afs: dict, metadata_snapshot: dict,
) -> Archetype | None:
    """Deterministic matcher. Returns the first Archetype in the catalog
    whose ``applicable_root_causes`` covers ``failure_type`` and whose
    ``required_schema_traits`` are all present in the snapshot.

    Selection is two-pass: archetypes that explicitly claim the failure
    root cause (non-empty ``applicable_root_causes``) win first, so the
    vocabulary reconciliation actually routes clusters to tailored
    shapes. Only when nothing in the catalog claims the cause do we
    fall through to safety-net archetypes (``applicable_root_causes``
    empty, e.g. ``simple_enumerate``). This preserves the
    ``simple_enumerate`` fallback for unknown root causes while
    preventing it from preempting every other archetype.
    """
    if not cluster_afs or not isinstance(cluster_afs, dict):
        return None
    failure_type = str(cluster_afs.get("failure_type") or "").strip()
    if not failure_type:
        return None
    traits = schema_traits(metadata_snapshot or {})
    for arch in ARCHETYPES:
        if arch.applicable_root_causes and arch.matches(failure_type, traits):
            return arch
    for arch in ARCHETYPES:
        if not arch.applicable_root_causes and arch.matches(failure_type, traits):
            return arch
    return None
