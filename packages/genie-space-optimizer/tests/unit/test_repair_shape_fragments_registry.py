"""Plan 9 Task 2 — _repair_shape_fragments registry.

Verifies every RepairShape enum member maps to a non-empty prompt
fragment, the registry is complete (no missing shapes), and the
OTHER fragment is the free-form structural rewrite safety net.
"""
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
    REPAIR_SHAPE_FRAGMENTS,
    fragment_for,
)


def test_every_repair_shape_has_a_fragment():
    """Catalog-drift detector: when a new RepairShape is added,
    the registry MUST have an entry in the same commit."""
    missing = [
        shape for shape in RepairShape
        if shape not in REPAIR_SHAPE_FRAGMENTS
    ]
    assert missing == [], (
        f"Missing fragments for RepairShape members: {missing}. "
        f"Add entries to REPAIR_SHAPE_FRAGMENTS."
    )


def test_every_fragment_is_non_empty_and_str():
    for shape, fragment in REPAIR_SHAPE_FRAGMENTS.items():
        assert isinstance(fragment, str), shape
        assert fragment.strip(), shape


def test_fragment_for_returns_correct_fragment():
    for shape in RepairShape:
        assert fragment_for(shape) == REPAIR_SHAPE_FRAGMENTS[shape]


def test_fragment_for_unknown_string_falls_back_to_other():
    """When repair_shape is not a valid RepairShape value (legacy
    pre-Plan-9 traces), fragment_for falls back to the OTHER
    fragment rather than raising."""
    result = fragment_for("not_a_real_shape")
    assert result == REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]
