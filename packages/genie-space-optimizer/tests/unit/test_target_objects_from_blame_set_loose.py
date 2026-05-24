"""Trial 13g — ``_target_objects_from_blame_set`` accepts 1-2 part identifiers.

Pre-Trial-13g the parser silently dropped 2-part ``table.column`` and
1-part bare ``table`` blame entries, requiring 3-4 part FQNs. The
dc89 production replay run revealed the Stage 3 LLM consistently
emits 2-part identifiers grounded in Stage 1 evidence that uses
unqualified names — every proposal had ``target_objects=()`` and was
rejected by the Plan 12 survival contract.

This module locks in the loosened parser: 1, 2, 3, and 4-part inputs
all yield :class:`TargetObject` instances with the right grouping.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.synthesize import (
    _target_objects_from_blame_set,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
)


def test_two_part_table_column_yields_table_with_column() -> None:
    """``table.column`` → TABLE TargetObject with column appended."""
    result = _target_objects_from_blame_set(
        ["mv_7now_fact_sales.time_window"],
    )
    assert len(result) == 1
    obj = result[0]
    assert obj.asset_kind == AssetKind.TABLE
    assert obj.identifier == "mv_7now_fact_sales"
    assert obj.columns == ("time_window",)


def test_one_part_table_yields_table_with_empty_columns() -> None:
    """Bare table identifier (no dots) → TABLE TargetObject, no columns."""
    result = _target_objects_from_blame_set(["mv_fact_sales"])
    assert len(result) == 1
    obj = result[0]
    assert obj.asset_kind == AssetKind.TABLE
    assert obj.identifier == "mv_fact_sales"
    assert obj.columns == ()


def test_three_part_fqn_yields_table_without_column() -> None:
    """``catalog.schema.table`` → TABLE TargetObject, no columns
    (existing behaviour, regression-locked here)."""
    result = _target_objects_from_blame_set(
        ["main.sales.mv_fact_sales"],
    )
    assert len(result) == 1
    obj = result[0]
    assert obj.asset_kind == AssetKind.TABLE
    assert obj.identifier == "main.sales.mv_fact_sales"
    assert obj.columns == ()


def test_four_part_fqn_yields_table_grouped_with_column() -> None:
    """``catalog.schema.table.column`` → TABLE TargetObject grouped by
    table prefix with the column appended (existing behaviour)."""
    result = _target_objects_from_blame_set(
        ["main.sales.mv_fact_sales.cy_cust_count"],
    )
    assert len(result) == 1
    obj = result[0]
    assert obj.asset_kind == AssetKind.TABLE
    assert obj.identifier == "main.sales.mv_fact_sales"
    assert obj.columns == ("cy_cust_count",)


def test_mixed_shapes_dedup_by_table_identifier() -> None:
    """Multiple entries pointing at the same table_id (regardless of
    shape) collapse into one TargetObject with the union of columns
    in arrival order."""
    result = _target_objects_from_blame_set(
        [
            "mv_7now_fact_sales.time_window",
            "mv_7now_fact_sales.cy_cust_count",
            "mv_other.region",
            "mv_7now_fact_sales",  # dup table, no column
        ],
    )
    assert len(result) == 2
    by_id = {obj.identifier: obj for obj in result}
    assert by_id["mv_7now_fact_sales"].columns == (
        "time_window",
        "cy_cust_count",
    )
    assert by_id["mv_other"].columns == ("region",)


def test_whitespace_and_empty_entries_skipped() -> None:
    """Empty / whitespace-only entries are dropped silently."""
    result = _target_objects_from_blame_set(
        ["", "   ", "mv_fact.col", "\t"],
    )
    assert len(result) == 1
    assert result[0].identifier == "mv_fact"
    assert result[0].columns == ("col",)


def test_empty_input_returns_empty_tuple() -> None:
    """Empty blame_set in → empty tuple out (no crash)."""
    assert _target_objects_from_blame_set([]) == ()
    assert _target_objects_from_blame_set(()) == ()
