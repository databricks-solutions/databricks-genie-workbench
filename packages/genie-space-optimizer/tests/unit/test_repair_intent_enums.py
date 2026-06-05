"""Plan 1 Task 1 — RepairShape + PatchType enum contracts.

These enums are the closed-vocabulary surfaces Plan 2's LLM call binds
its output schema against. ``OTHER`` on RepairShape is the deliberate
escape hatch so the LLM can introduce a new shape without us pre-listing
it; ``PatchType`` is intentionally closed because every value must map to
an applier dispatch arm.
"""

from __future__ import annotations

from enum import StrEnum

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


def test_repair_shape_is_strenum() -> None:
    assert issubclass(RepairShape, StrEnum)


def test_patch_type_is_strenum() -> None:
    assert issubclass(PatchType, StrEnum)


def test_repair_shape_includes_other_escape_hatch() -> None:
    """OTHER is the deliberate escape hatch for Plan 2's LLM swap."""
    assert RepairShape("other") is RepairShape.OTHER


def test_repair_shape_has_canonical_members() -> None:
    """The closed vocabulary the LLM picks from in Plan 2."""
    expected = {
        "top_n_by_metric",
        "ordered_list_by_metric",
        "rank_within_group",
        "period_over_period",
        "filter_compose",
        "filter_remove",
        "join_discovery",
        "sql_expression",
        "column_description",
        "metric_view_refinement",
        "instruction",
        "other",
    }
    actual = {m.value for m in RepairShape}
    assert actual == expected, (
        f"RepairShape vocabulary drift. extra={actual - expected}, "
        f"missing={expected - actual}"
    )


def test_patch_type_covers_applier_dispatch_arms() -> None:
    """Every patch_type value must be one the applier dispatches on.

    If an applier arm is added or removed, PatchType MUST be updated
    in lockstep. This test pins the contract today.
    """
    expected = {
        # Instructions
        "add_instruction",
        "update_instruction",
        "update_instruction_section",
        "rewrite_instruction",
        "remove_instruction",
        # Example SQLs
        "add_example_sql",
        "add_example_sql_negative",
        "update_example_sql",
        "remove_example_sql",
        # Descriptions
        "add_description",
        "update_description",
        "add_column_description",
        "update_column_description",
        "add_tvf_description",
        # Columns
        "hide_column",
        "unhide_column",
        "rename_column_alias",
        "add_column_synonym",
        "remove_column_synonym",
        # Tables
        "add_table",
        "remove_table",
        # Joins
        "add_join_spec",
        "update_join_spec",
        "remove_join_spec",
        # Filters
        "add_default_filter",
        "remove_default_filter",
        "update_filter_condition",
        # Genie feature toggles
        "enable_example_values",
        "disable_example_values",
        "enable_value_dictionary",
        "disable_value_dictionary",
        # TVFs
        "add_tvf",
        "remove_tvf",
        "add_tvf_parameter",
        "remove_tvf_parameter",
        "update_tvf_sql",
        # Metric views
        "add_mv_measure",
        "update_mv_measure",
        "remove_mv_measure",
        "add_mv_dimension",
        "remove_mv_dimension",
        "update_mv_yaml",
        # SQL snippets
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
        "add_sql_snippet_measure",
    }
    actual = {m.value for m in PatchType}
    assert actual == expected, (
        f"PatchType vocabulary drift vs applier dispatch arms. "
        f"extra={actual - expected}, missing={expected - actual}"
    )


def test_patch_type_string_value_equals_member_value() -> None:
    """StrEnum guarantees the value IS the string. Sanity-check the
    invariant downstream code relies on."""
    assert PatchType.ADD_EXAMPLE_SQL == "add_example_sql"
    assert str(PatchType.ADD_INSTRUCTION) == "add_instruction"


def test_invalid_repair_shape_raises() -> None:
    with pytest.raises(ValueError):
        RepairShape("not_a_real_shape")


def test_invalid_patch_type_raises() -> None:
    with pytest.raises(ValueError):
        PatchType("not_a_real_patch_type")
