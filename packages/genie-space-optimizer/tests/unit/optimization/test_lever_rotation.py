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


def test_next_untried_with_empty_tried_returns_preferred():
    """First call (nothing tried yet) returns the preferred pair."""
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(RcaKind.MEASURE_SWAP, tried=frozenset())
    assert pair == (6, "add_sql_snippet_measure")


def test_next_untried_skips_already_tried_lever():
    """If lever 6 was tried, the next pair (lever 2) is returned."""
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(RcaKind.MEASURE_SWAP, tried=frozenset({6}))
    assert pair == (2, "update_column_description")


def test_next_untried_skips_multiple_already_tried():
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(RcaKind.MEASURE_SWAP, tried=frozenset({6, 2}))
    assert pair == (5, "add_example_sql")


def test_next_untried_returns_none_when_exhausted():
    """All 3 levers tried — no more pairs to rotate to."""
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(
        RcaKind.MEASURE_SWAP, tried=frozenset({6, 2, 5}),
    )
    assert pair is None


def test_next_untried_returns_none_for_unknown_rca():
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    assert next_untried_repair(RcaKind.UNKNOWN, tried=frozenset()) is None


def test_next_untried_preserves_first_pair_when_dup_levers():
    """JOIN_SPEC has two lever-4 pairs (add_join_spec, update_join_spec).
    With tried={}, the FIRST lever-4 pair is returned (add_join_spec)."""
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(
        RcaKind.JOIN_SPEC_MISSING_OR_WRONG, tried=frozenset(),
    )
    assert pair == (4, "add_join_spec")


def test_next_untried_skips_lever_4_advances_to_lever_5_when_4_tried():
    """When lever 4 is tried, BOTH lever-4 pairs (add_join_spec and
    update_join_spec) are skipped — rotation is at the LEVER level, not
    the patch-type level. The next untried is lever 5."""
    from genie_space_optimizer.optimization.lever_rotation import (
        next_untried_repair,
    )
    pair = next_untried_repair(
        RcaKind.JOIN_SPEC_MISSING_OR_WRONG, tried=frozenset({4}),
    )
    assert pair == (5, "add_example_sql")


def test_resolve_rca_kind_from_rca_card_kind_field():
    """When ``cluster["rca_card"]["rca_kind"]`` is set to a valid value,
    that's the authoritative source."""
    from genie_space_optimizer.optimization.lever_rotation import (
        resolve_rca_kind_for_cluster,
    )
    cluster = {
        "rca_card": {"rca_kind": "measure_swap"},
        "asi_failure_type": "different_metric",
        "root_cause": "wrong_aggregation",
    }
    assert resolve_rca_kind_for_cluster(cluster) == RcaKind.MEASURE_SWAP


def test_resolve_rca_kind_falls_back_to_asi_failure_type():
    """No rca_kind on the card — translate from asi_failure_type."""
    from genie_space_optimizer.optimization.lever_rotation import (
        resolve_rca_kind_for_cluster,
    )
    cluster = {
        "rca_card": {},
        "asi_failure_type": "wrong_aggregation",
        "root_cause": "wrong_aggregation",
    }
    assert resolve_rca_kind_for_cluster(cluster) == RcaKind.MEASURE_SWAP


def test_resolve_rca_kind_returns_unknown_for_empty_cluster():
    from genie_space_optimizer.optimization.lever_rotation import (
        resolve_rca_kind_for_cluster,
    )
    assert resolve_rca_kind_for_cluster({}) == RcaKind.UNKNOWN


def test_resolve_rca_kind_for_root_cause_only():
    """No rca_card, no asi_failure_type — use root_cause."""
    from genie_space_optimizer.optimization.lever_rotation import (
        resolve_rca_kind_for_cluster,
    )
    cluster = {"root_cause": "missing_join_spec"}
    assert resolve_rca_kind_for_cluster(cluster) == RcaKind.JOIN_SPEC_MISSING_OR_WRONG
