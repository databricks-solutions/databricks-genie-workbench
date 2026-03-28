"""Lever metadata for integration callers."""

from __future__ import annotations

from genie_space_optimizer.common.config import DEFAULT_LEVER_ORDER, LEVER_NAMES

LEVER_DESCRIPTIONS: dict[int, str] = {
    1: "Update table descriptions, column descriptions, and synonyms",
    2: "Update metric view column descriptions",
    3: "Remove underperforming TVFs",
    4: "Add, update, or remove join relationships between tables",
    5: "Rewrite global routing instructions and add domain-specific guidance",
    6: "Add reusable SQL expressions (measures, filters, dimensions)",
}


def get_lever_info() -> list[dict]:
    """Return metadata for user-selectable levers (1-6).

    Lever 0 ("Proactive Enrichment") is a preparatory stage that always
    runs and is not exposed in the UI.
    """
    return [
        {
            "id": lever_id,
            "name": LEVER_NAMES[lever_id],
            "description": LEVER_DESCRIPTIONS.get(lever_id, ""),
        }
        for lever_id in DEFAULT_LEVER_ORDER
    ]


def get_default_lever_order() -> list[int]:
    """Return the default lever execution order: ``[1, 2, 3, 4, 5, 6]``."""
    return list(DEFAULT_LEVER_ORDER)
