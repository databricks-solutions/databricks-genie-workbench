"""Plan 9 Task 1 — RepairIntent.target_objects field.

Verifies that RepairIntent gains a tuple of TargetObjects, defaults
to empty tuple for backward compatibility with pre-Plan-9 serialized
intents, and round-trips through from_json / to_json.
"""
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_intent(*, target_objects: tuple[TargetObject, ...] = ()) -> RepairIntent:
    return RepairIntent(
        intent_id="intent_test_001",
        intent_name="test_intent",
        intent_description="A test intent.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="Cluster needs top-N example.",
        confidence="high",
        source="test",
        cluster_id="cluster_test",
        target_qids=("q_001",),
        blame_set=("catalog.schema.orders",),
        rca_card_id="rca_test",
        ag_id="AG_TEST",
        target_objects=target_objects,
    )


def test_repair_intent_target_objects_defaults_empty():
    intent = _make_intent()
    assert intent.target_objects == ()


def test_repair_intent_target_objects_round_trips_via_json():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("order_id", "amount"),
        ),
        TargetObject(
            asset_kind=AssetKind.METRIC_VIEW,
            identifier="catalog.schema.daily_orders_mv",
            columns=("order_count_total",),
        ),
    )
    intent = _make_intent(target_objects=targets)
    payload = intent.to_json()
    assert "target_objects" in payload
    assert payload["target_objects"] == [
        {
            "asset_kind": "table",
            "identifier": "catalog.schema.orders",
            "columns": ["order_id", "amount"],
        },
        {
            "asset_kind": "metric_view",
            "identifier": "catalog.schema.daily_orders_mv",
            "columns": ["order_count_total"],
        },
    ]
    reconstructed = RepairIntent.from_json(payload)
    assert reconstructed.target_objects == targets


def test_repair_intent_from_json_backward_compatible_missing_target_objects():
    """Pre-Plan-9 serialized intents (no target_objects key) must
    still deserialize with target_objects=()."""
    payload = {
        "intent_id": "intent_legacy_001",
        "intent_name": "legacy",
        "intent_description": "Legacy intent.",
        "repair_shape": "top_n_by_metric",
        "patch_type": "add_example_sql",
        "rationale": "Legacy.",
        "confidence": "medium",
        "source": "legacy",
        "cluster_id": "c",
        "target_qids": ["q"],
        "blame_set": [],
        "rca_card_id": "r",
        "ag_id": "AG",
    }
    intent = RepairIntent.from_json(payload)
    assert intent.target_objects == ()
