"""Defect Plan 2 (2026-05-12) — extend classifier for ``no_applied_patches``.

The airline trial run ``31ecd96f-5d56-4b5a-af8e-38e9e5c549af`` produced five
consecutive ``skipped_no_applied_patches`` iterations that never entered the
forbidden-AG set. Root cause: ``classify_rollback_reason("no_applied_patches")``
fell to the ``OTHER`` catch-all, so ``_reflection_admitted_to_forbidden_set``
rejected the entry even though the reflection had a fully-populated
``(root_cause, blame_set, lever_set, signatures)`` identity tuple.

The fix maps ``"no_applied_patches"`` to ``RollbackClass.NO_ACTION`` so the
default-ON ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`` admission predicate picks it
up. Joins the existing ``no_proposals`` and ``ag_collision_with_forbidden_set``
producers on the same admission axis.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.rollback_class import (
    RollbackClass,
    classify_rollback_reason,
)


def test_no_applied_patches_maps_to_no_action() -> None:
    """The airline F6 producer must classify as NO_ACTION so the C13
    admission predicate picks it up."""
    assert (
        classify_rollback_reason("no_applied_patches")
        is RollbackClass.NO_ACTION
    )


def test_no_applied_patches_distinct_from_no_proposals() -> None:
    """Both reasons must classify as NO_ACTION but remain distinct
    strings — the strategist's reflection-text label uses the raw
    reason to distinguish blast-radius drops (no_applied_patches)
    from synthesis failure (no_proposals)."""
    assert (
        classify_rollback_reason("no_applied_patches")
        is RollbackClass.NO_ACTION
    )
    assert (
        classify_rollback_reason("no_proposals")
        is RollbackClass.NO_ACTION
    )


def test_unknown_no_applied_variant_still_returns_other() -> None:
    """Defense: only the exact string ``"no_applied_patches"`` is
    admitted. A typo'd variant must NOT silently broaden NO_ACTION."""
    assert (
        classify_rollback_reason("no_applied_patch")
        is RollbackClass.OTHER
    )
    assert (
        classify_rollback_reason("no-applied-patches")
        is RollbackClass.OTHER
    )
