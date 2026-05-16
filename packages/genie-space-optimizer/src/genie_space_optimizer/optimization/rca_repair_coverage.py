"""Typed RCA → Repair coverage matrix.

Every emittable ``RcaKind`` value (i.e. everything except
``RcaKind.UNKNOWN``) must have at least one ``(lever_family,
patch_type)`` repair pair listed here. The matrix is the single source
of truth — adding a new ``RcaKind`` requires adding an entry, enforced
by ``tests/unit/optimization/test_rca_repair_coverage.py``.

The pairs are ordered: the first entry is the *preferred* repair (the
lever the strategist tries first). Subsequent entries are fallbacks
the lever-loop rotates through when the preferred lever's proposal
generation returns empty (the C3 fix).
"""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind


RepairPair = tuple[int, str]


RCA_REPAIR_MATRIX: dict[RcaKind, tuple[RepairPair, ...]] = {
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: (
        (2, "update_column_description"),
        (5, "add_example_sql"),
        (1, "update_description"),
    ),
    RcaKind.MEASURE_SWAP: (
        (6, "add_sql_snippet_measure"),
        (2, "update_column_description"),
        (5, "add_example_sql"),
    ),
    RcaKind.CANONICAL_DIMENSION_MISSED: (
        (1, "update_column_description"),
        (5, "add_example_sql"),
    ),
    RcaKind.MISSING_REQUIRED_DIMENSION: (
        (1, "update_column_description"),
        (5, "add_example_sql"),
    ),
    RcaKind.EXTRA_DEFENSIVE_FILTER: (
        (6, "add_sql_snippet_filter"),
        (5, "add_example_sql"),
    ),
    RcaKind.JOIN_SPEC_MISSING_OR_WRONG: (
        (4, "add_join_spec"),
        (4, "update_join_spec"),
        (5, "add_example_sql"),
    ),
    RcaKind.FILTER_LOGIC_MISMATCH: (
        (6, "add_sql_snippet_filter"),
        (5, "add_example_sql"),
    ),
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: (
        (6, "add_sql_snippet_expression"),
        (5, "add_example_sql"),
        (1, "update_column_description"),
    ),
    RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: (
        (1, "add_column_synonym"),
        (1, "update_column_description"),
    ),
    RcaKind.SQL_EXPRESSION_MISSING: (
        (6, "add_sql_snippet_expression"),
        (5, "add_example_sql"),
    ),
    RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: (
        (5, "add_example_sql"),
    ),
    RcaKind.FUNCTION_OR_TVF_NOT_INVOKED: (
        (3, "add_tvf_description"),
        (5, "add_example_sql"),
    ),
    RcaKind.FUNCTION_ROUTING_MISMATCH: (
        (3, "add_tvf_description"),
        (5, "add_example_sql"),
    ),
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: (
        (6, "add_sql_snippet_expression"),
        (5, "add_example_sql"),
    ),
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: (
        (6, "add_sql_snippet_filter"),
        (5, "add_example_sql"),
    ),
    RcaKind.ASSET_TYPE_ROUTING_MISMATCH: (
        (3, "add_tvf_description"),
        (1, "update_description"),
        (5, "add_example_sql"),
    ),
    # UNKNOWN intentionally excluded — see _UNCOVERED_BY_DESIGN in the
    # coverage test.
}


def repairs_for(rca_kind: RcaKind) -> tuple[RepairPair, ...]:
    """Return the ordered tuple of (lever, patch_type) pairs for the
    given ``rca_kind``. Returns ``()`` for ``RcaKind.UNKNOWN``."""
    return RCA_REPAIR_MATRIX.get(rca_kind, ())


def assert_full_coverage(matrix: dict[RcaKind, tuple[RepairPair, ...]]) -> None:
    """Raise ``AssertionError`` listing any ``RcaKind`` value (other
    than ``UNKNOWN``) that is missing or maps to an empty tuple."""
    missing = [
        k.value for k in RcaKind
        if k is not RcaKind.UNKNOWN and not matrix.get(k)
    ]
    assert not missing, (
        f"RCA_REPAIR_MATRIX missing coverage for: {sorted(missing)}"
    )
