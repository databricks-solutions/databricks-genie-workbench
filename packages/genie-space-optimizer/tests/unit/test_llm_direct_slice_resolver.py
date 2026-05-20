"""Plan 9 Task 6 — llm_direct_slice_resolver.

Resolves a tuple of TargetObjects into a concrete AssetSlice
(tables, metric_view, columns, join_spec) by looking up each
identifier in metadata_snapshot. No archetype dependency.

Note: AssetSlice.columns is list[tuple[str, str]] = [(table_id,
column_name), ...] per AssetSlice's actual contract (see
preflight_synthesis.py:175-178). The resolver MUST produce that
shape so downstream consumers (existing per-lever generators)
keep working unchanged.
"""
import pytest

from genie_space_optimizer.optimization.llm_direct_slice_resolver import (
    resolve_target_objects_to_asset_slice,
    UnknownTargetObjectError,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_metadata_snapshot():
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "order_id", "type": "STRING"},
                        {"name": "product_id", "type": "STRING"},
                        {"name": "amount", "type": "DECIMAL"},
                    ],
                },
                {
                    "identifier": "main.sales.products",
                    "columns": [
                        {"name": "id", "type": "STRING"},
                        {"name": "name", "type": "STRING"},
                    ],
                },
            ],
            "metric_views": [
                {
                    "identifier": "main.sales.daily_orders_mv",
                    "columns": [
                        {"name": "order_count_total", "type": "BIGINT"},
                    ],
                },
            ],
        },
        "instructions": {
            "join_specs": [
                {
                    "left": {"identifier": "main.sales.orders"},
                    "right": {"identifier": "main.sales.products"},
                    "on": "orders.product_id = products.id",
                },
            ],
        },
    }


def test_resolve_single_table_target():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.orders",
            columns=("product_id", "amount"),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert len(slice_.tables) == 1
    assert slice_.tables[0]["identifier"] == "main.sales.orders"
    assert slice_.metric_view is None
    assert slice_.join_spec is None
    # Real AssetSlice.columns shape: list[tuple[str, str]]
    assert ("main.sales.orders", "product_id") in slice_.columns
    assert ("main.sales.orders", "amount") in slice_.columns


def test_resolve_two_tables_finds_join_spec():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.orders",
            columns=("product_id",),
        ),
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.products",
            columns=("id", "name"),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert len(slice_.tables) == 2
    assert slice_.join_spec is not None
    assert slice_.join_spec["on"] == "orders.product_id = products.id"


def test_resolve_metric_view_target():
    targets = (
        TargetObject(
            asset_kind=AssetKind.METRIC_VIEW,
            identifier="main.sales.daily_orders_mv",
            columns=("order_count_total",),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert slice_.metric_view is not None
    assert slice_.metric_view["identifier"] == "main.sales.daily_orders_mv"


def test_resolve_unknown_table_raises():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.does_not_exist",
            columns=(),
        ),
    )
    with pytest.raises(UnknownTargetObjectError):
        resolve_target_objects_to_asset_slice(
            targets, _make_metadata_snapshot(),
        )


def test_resolve_empty_targets_returns_empty_slice():
    """When LLM emits no target_objects (e.g. instruction-only
    repair), resolver returns an empty AssetSlice rather than
    raising. The caller decides whether the empty slice is OK."""
    slice_ = resolve_target_objects_to_asset_slice(
        (), _make_metadata_snapshot(),
    )
    assert slice_.tables == []
    assert slice_.metric_view is None
    assert slice_.columns == []
    assert slice_.join_spec is None


def test_resolve_column_kind_anchors_to_parent_table():
    """COLUMN-kind identifier is 'catalog.schema.table.column'; the
    resolver finds the parent table and emits a single column tuple."""
    targets = (
        TargetObject(
            asset_kind=AssetKind.COLUMN,
            identifier="main.sales.orders.amount",
            columns=(),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert ("main.sales.orders", "amount") in slice_.columns
