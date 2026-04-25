"""Phase 4.1: replacement_intent threads through update_sections.

Default behavior (``merge``) preserves the legacy guard that
concatenates new content onto old when the new value is much shorter,
to avoid silent destructive overwrites. ``replace`` opts out of the
guard so a focused strategist rewrite (or a ``rewrite_instruction``
split child) wins even if shorter.
"""

from __future__ import annotations

from genie_space_optimizer.optimization import structured_metadata as sm


_LONG_OLD_PURPOSE = (
    "Pre-aggregated executive sales report (ESR) materialized view "
    "combining store sales metrics with embedded fact aggregates. "
    "Provides daily, MTD, and YoY metrics across markets, zones, "
    "and regions for executive dashboards. Joins to mv_esr_dim_date "
    "for time filtering and to mv_esr_dim_location for geographic "
    "rollups."
)
_FOCUSED_NEW_PURPOSE = (
    "Pre-aggregated ESR materialized view; daily/MTD/YoY metrics."
)


def _existing_with_purpose(text: str) -> str:
    return f"PURPOSE:\n{text}"


def test_default_merge_concatenates_when_loss_threshold_crossed() -> None:
    """Backward compat: shrink-by-half triggers merge."""
    new_desc = sm.update_sections(
        current_description=_existing_with_purpose(_LONG_OLD_PURPOSE),
        updates={"purpose": _FOCUSED_NEW_PURPOSE},
        lever=1,
        entity_type="table",
        replacement_intent="merge",
    )
    rendered = "\n".join(new_desc) if isinstance(new_desc, list) else str(new_desc)
    # Old content is preserved (head of the long string).
    assert "executive sales report" in rendered.lower()
    # New content is appended.
    assert "daily/MTD/YoY metrics" in rendered


def test_replace_intent_keeps_only_new_content() -> None:
    new_desc = sm.update_sections(
        current_description=_existing_with_purpose(_LONG_OLD_PURPOSE),
        updates={"purpose": _FOCUSED_NEW_PURPOSE},
        lever=1,
        entity_type="table",
        replacement_intent="replace",
    )
    rendered = "\n".join(new_desc) if isinstance(new_desc, list) else str(new_desc)
    # Old verbose content is GONE.
    assert "executive dashboards" not in rendered
    assert "Joins to mv_esr_dim_date" not in rendered
    # New focused content is present.
    assert "daily/MTD/YoY metrics" in rendered


def test_default_replace_when_new_is_longer_or_close() -> None:
    """Loss-threshold guard does not fire when shrink is small or content grows."""
    # Must be >= 70% of the old length (315 chars) so the guard does
    # not trigger; pad with content that's clearly distinct.
    similar_new = (
        "Pre-aggregated executive sales report (ESR) materialized view "
        "combining store sales metrics with embedded fact aggregates "
        "for new use cases. Replaces the prior executive dashboard "
        "schema entirely; daily/MTD/YoY metrics are now defined at "
        "this level rather than at downstream views."
    )
    assert len(similar_new) >= len(_LONG_OLD_PURPOSE) * 0.7
    new_desc = sm.update_sections(
        current_description=_existing_with_purpose(_LONG_OLD_PURPOSE),
        updates={"purpose": similar_new},
        lever=1,
        entity_type="table",
        replacement_intent="merge",
    )
    rendered = "\n".join(new_desc) if isinstance(new_desc, list) else str(new_desc)
    # New value should win; the verbose ``Joins to`` tail of the OLD
    # purpose is gone (no merge fired).
    assert "Joins to mv_esr_dim_date" not in rendered
    # The new content's distinctive phrase is present.
    assert "Replaces the prior executive dashboard schema" in rendered
