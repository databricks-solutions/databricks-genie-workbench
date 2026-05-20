"""Plan 9 Task 1 — TargetObject typed dataclass.

Verifies that TargetObject is frozen + slots + JsonRoundTrip; covers
table / metric_view / column asset kinds; rejects empty identifier;
rejects unknown asset_kind; round-trips through to_json / from_json.
"""
import pytest

from genie_space_optimizer.optimization.target_object_typed import (
    TargetObject,
    AssetKind,
)


def test_target_object_table_kind_is_immutable_and_round_trips():
    obj = TargetObject(
        asset_kind=AssetKind.TABLE,
        identifier="catalog.schema.orders",
        columns=("order_id", "amount"),
    )
    payload = obj.to_json()
    assert payload == {
        "asset_kind": "table",
        "identifier": "catalog.schema.orders",
        "columns": ["order_id", "amount"],
    }
    reconstructed = TargetObject.from_json(payload)
    assert reconstructed == obj
    with pytest.raises(Exception):
        obj.identifier = "different"  # frozen


def test_target_object_metric_view_kind():
    obj = TargetObject(
        asset_kind=AssetKind.METRIC_VIEW,
        identifier="catalog.schema.daily_orders_mv",
        columns=("order_count_total", "order_amount_sum"),
    )
    assert obj.asset_kind == AssetKind.METRIC_VIEW
    assert obj.to_json()["asset_kind"] == "metric_view"


def test_target_object_column_kind_with_no_columns_is_allowed():
    obj = TargetObject(
        asset_kind=AssetKind.COLUMN,
        identifier="catalog.schema.orders.customer_id",
        columns=(),
    )
    assert obj.columns == ()


def test_target_object_rejects_empty_identifier():
    with pytest.raises(ValueError, match="identifier"):
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="",
            columns=("col",),
        )


def test_target_object_rejects_unknown_asset_kind_via_from_json():
    with pytest.raises(ValueError):
        TargetObject.from_json({
            "asset_kind": "view",  # not in AssetKind
            "identifier": "x.y.z",
            "columns": [],
        })


def test_target_object_columns_are_tuple_immutable():
    obj = TargetObject(
        asset_kind=AssetKind.TABLE,
        identifier="x.y.orders",
        columns=("a", "b"),
    )
    assert isinstance(obj.columns, tuple)
