"""Phase 0.5 — extend classifier for ``no_grounded_patches``.

The harness emits ``"no_grounded_patches"`` as the rollback-reason
string at ``harness.py:12297-12301`` when proposal grounding drops
every patch and no candidate state exists. Pre-Phase-0.5 it fell
through ``classify_rollback_reason`` to ``RollbackClass.OTHER``, so
``_reflection_admitted_to_forbidden_set`` rejected the entry and the
same AG re-emitted on subsequent iterations.

This file mirrors the convention set by
``test_rollback_classify_no_applied_patches.py`` (Defect Plan 2). The
fix maps ``"no_grounded_patches"`` to ``RollbackClass.NO_ACTION`` so
the default-ON ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`` admission
predicate picks it up. Joins the existing ``no_proposals``,
``ag_collision_with_forbidden_set``, and ``no_applied_patches``
producers on the same admission axis.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.rollback_class import (
    RollbackClass,
    classify_rollback_reason,
)


def test_no_grounded_patches_maps_to_no_action() -> None:
    """The harness:12299 producer must classify as NO_ACTION so the
    forbidden-AG admission predicate picks it up — closing the
    repetition pattern where the same AG re-emits across consecutive
    iterations with zero feedback."""
    assert (
        classify_rollback_reason("no_grounded_patches")
        is RollbackClass.NO_ACTION
    )


def test_no_grounded_patches_distinct_from_siblings_on_no_action_axis() -> None:
    """All four reasons map to NO_ACTION but remain distinct strings.
    Defect Plan 2 established that the strategist's reflection-text
    label uses the raw reason to distinguish blast-radius drops
    (no_applied_patches) from grounding drops (no_grounded_patches)
    from synthesis failure (no_proposals)."""
    for reason in (
        "no_proposals",
        "ag_collision_with_forbidden_set",
        "no_applied_patches",
        "no_grounded_patches",
    ):
        assert (
            classify_rollback_reason(reason) is RollbackClass.NO_ACTION
        ), reason


def test_unknown_no_grounded_variant_still_returns_other() -> None:
    """Defense: only the exact string ``"no_grounded_patches"`` is
    admitted. A typo'd variant must NOT silently broaden NO_ACTION
    (same shape as the no_applied_patches test in Defect Plan 2)."""
    for variant in (
        "no_grounded_patch",
        "no-grounded-patches",
        "no_grounded",
    ):
        assert (
            classify_rollback_reason(variant) is RollbackClass.OTHER
        ), variant


def test_format_rollback_reflection_labels_no_grounded_patches() -> None:
    """Defense-in-depth: the ``_format_rollback_reflection`` label_map
    must contain ``no_grounded_patches`` so the canonical label is
    locked via the lookup table, not via the else-branch
    split-on-colon fallback. This protects against future label-format
    drift (matching the Defect Plan 2 convention)."""
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness._format_rollback_reflection)
    assert '"no_grounded_patches": "no_grounded_patches"' in src, (
        "label_map must include no_grounded_patches as an explicit "
        "canonical-label entry — mirroring the no_applied_patches "
        "convention set by Defect Plan 2."
    )
