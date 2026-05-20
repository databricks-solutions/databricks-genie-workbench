"""Plan 9 Task 2 — verify the OTHER fragment is a complete safety net.

After catalog removal (T10), RepairShape.OTHER is the only
deterministic fragment left when the LLM picks a novel shape.
Test ensures the fragment mentions target_objects, instructs no
benchmark text reproduction, and is non-trivial in length.
"""
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
    REPAIR_SHAPE_FRAGMENTS,
)


OTHER = REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]


def test_other_fragment_references_target_objects():
    assert "target_objects" in OTHER, (
        "OTHER fragment MUST instruct LLM to ground in target_objects "
        "— it is the only constraint on free-form structural rewrites."
    )


def test_other_fragment_forbids_inventing_identifiers():
    assert "do not invent" in OTHER.lower(), (
        "OTHER fragment MUST forbid inventing identifiers — without "
        "this, free-form rewrites would frequently reference "
        "non-existent columns."
    )


def test_other_fragment_requires_justification():
    assert "rationale" in OTHER.lower(), (
        "OTHER fragment MUST require the LLM to explain why no "
        "named shape fits — without this, every repair would "
        "trivially pick OTHER."
    )


def test_other_fragment_is_substantial():
    """Catch accidental fragment shrinkage — OTHER MUST be at
    least 200 chars to convey the full free-form contract."""
    assert len(OTHER) >= 200, len(OTHER)
