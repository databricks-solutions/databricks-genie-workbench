"""Coverage invariant — every emittable ``RcaKind`` must map to at
least one ``(lever, patch_type)`` pair. Trial-5 Run A's
``wrong_aggregation`` clusters reached the optimizer with no
patch-shape coverage; the loop wasted 4 iterations emitting empty
proposals."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_repair_coverage import (
    RCA_REPAIR_MATRIX,
    assert_full_coverage,
    repairs_for,
)


_UNCOVERED_BY_DESIGN: frozenset[RcaKind] = frozenset({RcaKind.UNKNOWN})


@pytest.mark.parametrize(
    "rca_kind",
    [k for k in RcaKind if k not in _UNCOVERED_BY_DESIGN],
)
def test_every_rca_kind_has_at_least_one_repair(rca_kind):
    pairs = repairs_for(rca_kind)
    assert pairs, (
        f"RcaKind.{rca_kind.name} has no repair pair in "
        f"RCA_REPAIR_MATRIX. Add an entry to rca_repair_coverage.py."
    )
    for lever, patch_type in pairs:
        assert isinstance(lever, int) and 1 <= lever <= 6, lever
        assert isinstance(patch_type, str) and patch_type, patch_type


def test_assert_full_coverage_raises_when_a_kind_is_missing():
    """The full-coverage assertion is what the lever-loop entry point
    can call at startup to fail fast if a future code change drops a
    kind from the matrix."""
    bad_matrix = {
        k: v
        for k, v in RCA_REPAIR_MATRIX.items()
        if k is not RcaKind.JOIN_SPEC_MISSING_OR_WRONG
    }
    with pytest.raises(AssertionError, match="join_spec_missing_or_wrong"):
        assert_full_coverage(bad_matrix)


def test_assert_full_coverage_passes_on_real_matrix():
    assert assert_full_coverage(RCA_REPAIR_MATRIX) is None


def test_every_lever_6_preferred_kind_has_non_lever_6_fallback():
    """When the preferred repair is a lever-6 SQL snippet (the most
    likely to decline because it requires the LLM to synthesize a
    correct snippet from the RCA card), the matrix MUST also list a
    non-lever-6 fallback. Otherwise rotation exhausts immediately and
    the loop stalemate-escalates without ever trying a different lever.
    """
    offenders: list[str] = []
    for rca_kind, pairs in RCA_REPAIR_MATRIX.items():
        if not pairs:
            continue
        preferred_lever = pairs[0][0]
        if preferred_lever != 6:
            continue
        non_six_levers = {lever for lever, _ in pairs[1:] if lever != 6}
        if not non_six_levers:
            offenders.append(rca_kind.value)

    assert not offenders, (
        f"RcaKinds with lever-6 preferred and no non-lever-6 fallback: "
        f"{sorted(offenders)}"
    )
