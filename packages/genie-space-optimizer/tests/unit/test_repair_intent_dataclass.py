"""Plan 1 Task 2 — RepairIntent dataclass contract.

RepairIntent is the typed unit of repair the pipeline threads
end-to-end. It is frozen+slots+JsonRoundTrip so it can serialize
through MLflow Phase H capture, survive stage I/O boundaries, and be
reconstituted from postmortem JSON without bespoke parsers.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def _sample_intent(**overrides: object) -> RepairIntent:
    base = dict(
        intent_id="intent_H001_001",
        intent_name="top_n_by_metric",
        intent_description=(
            "Aggregate a numeric column by a categorical dimension, "
            "order DESC, limit N."
        ),
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="Cluster H001 root cause = missing_limit; top_n shape fits.",
        confidence="high",
        source="deterministic_archetype_adapter",
        cluster_id="H001",
        target_qids=("gs_009",),
        blame_set=("flights.carrier", "flights.delay_minutes"),
        rca_card_id="rca_H001_v1",
        ag_id="AG_H001_L5",
    )
    base.update(overrides)
    return RepairIntent(**base)  # type: ignore[arg-type]


def test_repair_intent_mixes_jsonroundtrip() -> None:
    assert issubclass(RepairIntent, JsonRoundTrip)


def test_repair_intent_is_frozen() -> None:
    intent = _sample_intent()
    with pytest.raises(FrozenInstanceError):
        intent.rationale = "mutated"  # type: ignore[misc]


def test_repair_intent_has_slots() -> None:
    """slots=True makes typo-assignment raise AttributeError."""
    intent = _sample_intent()
    with pytest.raises(AttributeError):
        object.__setattr__(intent, "not_a_real_field", True)


def test_required_fields_present() -> None:
    declared = {f.name for f in fields(RepairIntent)}
    required = {
        "intent_id",
        "intent_name",
        "intent_description",
        "repair_shape",
        "patch_type",
        "rationale",
        "confidence",
        "source",
        "cluster_id",
        "target_qids",
        "blame_set",
        "rca_card_id",
        "ag_id",
    }
    optional = {
        "applied_at_iter",
        "applied_signature",
        "acceptance_outcome",
        "rollback_reason",
    }
    assert required <= declared, f"missing required fields: {required - declared}"
    assert optional <= declared, f"missing optional fields: {optional - declared}"


def test_round_trip_preserves_all_fields() -> None:
    intent = _sample_intent(applied_at_iter=2, applied_signature="abc123")
    payload = intent.to_json()
    restored = RepairIntent.from_json(payload)
    assert restored == intent


def test_round_trip_enum_values_are_strings_in_json() -> None:
    """Enum values must serialise to their string form, not Python repr."""
    intent = _sample_intent()
    payload = intent.to_json()
    assert payload["repair_shape"] == "top_n_by_metric"
    assert payload["patch_type"] == "add_example_sql"


def test_round_trip_handles_optional_none_fields() -> None:
    intent = _sample_intent()
    payload = intent.to_json()
    restored = RepairIntent.from_json(payload)
    assert restored.acceptance_outcome is None
    assert restored.rollback_reason is None


def test_round_trip_tuples_stay_tuples() -> None:
    intent = _sample_intent()
    payload = intent.to_json()
    restored = RepairIntent.from_json(payload)
    assert isinstance(restored.target_qids, tuple)
    assert isinstance(restored.blame_set, tuple)


def test_confidence_must_be_literal_string() -> None:
    """confidence is typed as Literal['high', 'medium', 'low']; at
    runtime we accept the three strings only."""
    intent = _sample_intent(confidence="medium")
    assert intent.confidence == "medium"
    payload = intent.to_json()
    assert payload["confidence"] == "medium"
