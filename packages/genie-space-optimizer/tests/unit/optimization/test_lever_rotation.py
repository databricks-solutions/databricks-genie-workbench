"""Unit tests for the RCA_REPAIR_MATRIX → lever rotation bridge."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind


def test_lever_set_for_measure_swap_is_6_2_5():
    """MEASURE_SWAP's matrix entry is ((6,...), (2,...), (5,...)).
    The lever set must be (6, 2, 5) — preserving the priority order
    and stripping the patch_type."""
    from genie_space_optimizer.optimization.lever_rotation import (
        lever_set_for_rca_kind,
    )
    assert lever_set_for_rca_kind(RcaKind.MEASURE_SWAP) == (6, 2, 5)


def test_lever_set_for_unknown_is_empty():
    from genie_space_optimizer.optimization.lever_rotation import (
        lever_set_for_rca_kind,
    )
    assert lever_set_for_rca_kind(RcaKind.UNKNOWN) == ()


def test_lever_set_for_example_sql_shape_needed_is_single_lever_5():
    """EXAMPLE_SQL_SHAPE_NEEDED has only one repair pair — (5, "add_example_sql")."""
    from genie_space_optimizer.optimization.lever_rotation import (
        lever_set_for_rca_kind,
    )
    assert lever_set_for_rca_kind(RcaKind.EXAMPLE_SQL_SHAPE_NEEDED) == (5,)


def test_lever_set_for_join_spec_collapses_duplicate_levers():
    """JOIN_SPEC_MISSING_OR_WRONG has two lever-4 patch types
    (add_join_spec, update_join_spec) then lever-5. The lever set
    collapses the dup so the rotation order is (4, 5)."""
    from genie_space_optimizer.optimization.lever_rotation import (
        lever_set_for_rca_kind,
    )
    assert lever_set_for_rca_kind(RcaKind.JOIN_SPEC_MISSING_OR_WRONG) == (4, 5)
