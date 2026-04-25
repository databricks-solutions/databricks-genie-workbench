"""Phase 1.2: column-level allow-list lets Lever 1 update ``aggregation``.

The iteration-1 log showed ``Lever 1: skipping locked sections
['aggregation']`` because the flat ``LEVER_SECTION_OWNERSHIP`` map
conflated table-level sections with column-level fields. After the
fix, ``aggregation`` is allowed on column updates from Lever 1 but
remains absent from the Lever-1 table-level allow-list.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import structured_metadata as sm


def test_lever1_column_measure_allows_aggregation() -> None:
    """The exact case that fired in the iter-1 log: aggregation on a measure."""
    new_desc = sm.update_sections(
        current_description=None,
        updates={
            "definition": "Total USD sales for the day.",
            "aggregation": "SUM for total USD sales across stores.",
            "synonyms": "daily total sales",
        },
        lever=1,
        entity_type="column_measure",
    )
    rendered = "\n".join(new_desc) if isinstance(new_desc, list) else str(new_desc)
    assert "AGGREGATION:" in rendered
    assert "SUM for total USD sales" in rendered


def test_lever1_table_level_aggregation_still_locked() -> None:
    """Defense in depth: aggregation on table entity_type stays out."""
    with pytest.raises(sm.LeverOwnershipError):
        sm.update_sections(
            current_description=None,
            updates={"aggregation": "SUM"},
            lever=1,
            entity_type="table",
        )


def test_lever1_column_dimension_does_not_grant_aggregation() -> None:
    """Aggregation only makes sense on measure columns; dimensions still skip."""
    new_desc = sm.update_sections(
        current_description=None,
        updates={
            "definition": "Country code.",
            "values": "US, CA",
        },
        lever=1,
        entity_type="column_dim",
    )
    rendered = "\n".join(new_desc) if isinstance(new_desc, list) else str(new_desc)
    assert "DEFINITION:" in rendered
    assert "VALUES:" in rendered


def test_helper_resolves_column_vs_table_allowlist() -> None:
    table_allow = sm._allowed_sections_for_lever(1, "table")
    col_meas_allow = sm._allowed_sections_for_lever(1, "column_measure")
    assert "aggregation" not in table_allow
    assert "aggregation" in col_meas_allow
    # Lever 5 still owns nothing on either entity type.
    assert sm._allowed_sections_for_lever(5, "table") == set()
    assert sm._allowed_sections_for_lever(5, "column_measure") == set()
