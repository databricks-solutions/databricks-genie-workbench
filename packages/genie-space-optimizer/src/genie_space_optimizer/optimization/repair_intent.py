"""Typed RepairIntent contract — Plan 1 Foundation.

A ``RepairIntent`` captures what kind of repair the optimizer is
attempting for a single failure cluster: the high-level ``RepairShape``
(the LLM's view in Plan 2) and the concrete ``PatchType`` (the
applier's view). It travels end-to-end through the pipeline: stamped
at synthesis time onto each proposal dict, surfaced as a typed carrier
on every stage I/O dataclass, and read by postmortem + downstream LLM
calls.

Today (Plan 1) the only producer is the deterministic
``intent_from_archetype`` adapter — it wraps the existing ``Archetype``
catalog so structural synthesis emits typed intents without any LLM
call. Plan 2 replaces the L5b synthesis path with an LLM call that
returns a ``RepairIntent`` directly (with ``RepairShape.OTHER`` as the
escape hatch for shapes outside the closed vocabulary). Plans 3 and 4
build further LLM-driven producers on top.

Design constraints:
  * ``RepairShape`` is closed-with-escape-hatch — the LLM picks from
    the canonical set or falls back to ``OTHER``. Pure semantic
    vocabulary, not tied to applier arms.
  * ``PatchType`` is closed and pinned to ``applier.py`` dispatch
    arms (see ``test_patch_type_covers_applier_dispatch_arms``).
    Adding a new applier arm requires adding a new ``PatchType``
    member in the same commit.
  * ``RepairIntent`` is frozen+slots+JsonRoundTrip so it can
    round-trip through MLflow Phase H capture and stage I/O
    boundaries.
"""

from __future__ import annotations

from enum import StrEnum


class RepairShape(StrEnum):
    """High-level repair category — the vocabulary the LLM picks from.

    Closed-with-escape-hatch: ``OTHER`` lets Plan 2's LLM propose a
    repair shape the catalog doesn't enumerate (e.g. a long-tail
    structural pattern). Downstream code that inspects ``repair_shape``
    must always handle ``OTHER`` — it is the deliberate fallback, not
    a bug indicator.
    """

    TOP_N_BY_METRIC = "top_n_by_metric"
    ORDERED_LIST_BY_METRIC = "ordered_list_by_metric"
    RANK_WITHIN_GROUP = "rank_within_group"
    PERIOD_OVER_PERIOD = "period_over_period"
    FILTER_COMPOSE = "filter_compose"
    FILTER_REMOVE = "filter_remove"
    JOIN_DISCOVERY = "join_discovery"
    SQL_EXPRESSION = "sql_expression"
    COLUMN_DESCRIPTION = "column_description"
    METRIC_VIEW_REFINEMENT = "metric_view_refinement"
    INSTRUCTION = "instruction"
    OTHER = "other"


class PatchType(StrEnum):
    """Closed set of patch types the applier dispatches on.

    Pinned to ``applier.py`` dispatch arms by
    ``test_patch_type_covers_applier_dispatch_arms``. If a new applier
    arm is added, a corresponding ``PatchType`` member MUST be added
    in the same commit or the test will fail.
    """

    # Instructions
    ADD_INSTRUCTION = "add_instruction"
    UPDATE_INSTRUCTION = "update_instruction"
    UPDATE_INSTRUCTION_SECTION = "update_instruction_section"
    REWRITE_INSTRUCTION = "rewrite_instruction"
    REMOVE_INSTRUCTION = "remove_instruction"
    # Example SQLs
    ADD_EXAMPLE_SQL = "add_example_sql"
    UPDATE_EXAMPLE_SQL = "update_example_sql"
    REMOVE_EXAMPLE_SQL = "remove_example_sql"
    # Descriptions
    ADD_DESCRIPTION = "add_description"
    UPDATE_DESCRIPTION = "update_description"
    ADD_COLUMN_DESCRIPTION = "add_column_description"
    UPDATE_COLUMN_DESCRIPTION = "update_column_description"
    ADD_TVF_DESCRIPTION = "add_tvf_description"
    # Columns
    HIDE_COLUMN = "hide_column"
    UNHIDE_COLUMN = "unhide_column"
    RENAME_COLUMN_ALIAS = "rename_column_alias"
    ADD_COLUMN_SYNONYM = "add_column_synonym"
    REMOVE_COLUMN_SYNONYM = "remove_column_synonym"
    # Tables
    ADD_TABLE = "add_table"
    REMOVE_TABLE = "remove_table"
    # Joins
    ADD_JOIN_SPEC = "add_join_spec"
    UPDATE_JOIN_SPEC = "update_join_spec"
    REMOVE_JOIN_SPEC = "remove_join_spec"
    # Filters
    ADD_DEFAULT_FILTER = "add_default_filter"
    REMOVE_DEFAULT_FILTER = "remove_default_filter"
    UPDATE_FILTER_CONDITION = "update_filter_condition"
    # Genie feature toggles
    ENABLE_EXAMPLE_VALUES = "enable_example_values"
    DISABLE_EXAMPLE_VALUES = "disable_example_values"
    ENABLE_VALUE_DICTIONARY = "enable_value_dictionary"
    DISABLE_VALUE_DICTIONARY = "disable_value_dictionary"
    # TVFs
    ADD_TVF = "add_tvf"
    REMOVE_TVF = "remove_tvf"
    ADD_TVF_PARAMETER = "add_tvf_parameter"
    REMOVE_TVF_PARAMETER = "remove_tvf_parameter"
    UPDATE_TVF_SQL = "update_tvf_sql"
    # Metric views
    ADD_MV_MEASURE = "add_mv_measure"
    UPDATE_MV_MEASURE = "update_mv_measure"
    REMOVE_MV_MEASURE = "remove_mv_measure"
    ADD_MV_DIMENSION = "add_mv_dimension"
    REMOVE_MV_DIMENSION = "remove_mv_dimension"
    UPDATE_MV_YAML = "update_mv_yaml"
    # SQL snippets
    ADD_SQL_SNIPPET_FILTER = "add_sql_snippet_filter"
    ADD_SQL_SNIPPET_EXPRESSION = "add_sql_snippet_expression"
    ADD_SQL_SNIPPET_MEASURE = "add_sql_snippet_measure"
