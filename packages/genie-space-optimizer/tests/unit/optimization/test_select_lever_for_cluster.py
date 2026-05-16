"""Unit tests for the harness selector that wedges in front of
``_map_to_lever`` to enable rotation."""

from __future__ import annotations


def _make_cluster(**overrides) -> dict:
    base = {
        "cluster_id": "C1",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "wrong_aggregation",
        "asi_blame_set": [],
        "affected_judge": "logical_accuracy",
        "rca_card": {"rca_kind": "measure_swap"},
    }
    base.update(overrides)
    return base


def test_selector_returns_preferred_lever_when_nothing_tried():
    """MEASURE_SWAP's preferred lever is 6. With no tried set, lever 6
    must be returned."""
    from genie_space_optimizer.optimization.harness import (
        _select_lever_for_cluster,
    )
    rotation_holder = {"tried": {}}
    assert _select_lever_for_cluster(_make_cluster(), rotation_holder) == 6


def test_selector_rotates_to_lever_2_after_lever_6_tried():
    from genie_space_optimizer.optimization.harness import (
        _select_lever_for_cluster,
    )
    rotation_holder = {"tried": {"C1": frozenset({6})}}
    assert _select_lever_for_cluster(_make_cluster(), rotation_holder) == 2


def test_selector_falls_back_to_legacy_map_on_unknown_rca_kind():
    """A cluster with no matrix coverage falls through to _map_to_lever.
    ``other`` root_cause + ``other`` failure_type resolve to RcaKind.UNKNOWN."""
    from genie_space_optimizer.optimization.harness import (
        _select_lever_for_cluster,
    )
    cluster = _make_cluster(
        rca_card={},
        asi_failure_type="other",
        root_cause="other",
        affected_judge="schema_accuracy",
    )
    rotation_holder = {"tried": {}}
    # _JUDGE_TO_LEVER["schema_accuracy"] == 1 — legacy fallback path.
    assert _select_lever_for_cluster(cluster, rotation_holder) == 1


def test_selector_falls_back_to_legacy_when_matrix_exhausted():
    """MEASURE_SWAP has 3 entries (6, 2, 5). With all 3 tried, the
    selector falls back to the legacy ``_map_to_lever`` result.
    ``wrong_aggregation`` legacy mapping == 6."""
    from genie_space_optimizer.optimization.harness import (
        _select_lever_for_cluster,
    )
    rotation_holder = {"tried": {"C1": frozenset({6, 2, 5})}}
    assert _select_lever_for_cluster(_make_cluster(), rotation_holder) == 6


def test_selector_does_not_mutate_rotation_holder():
    """The selector is a pure read of the holder — it must not bump
    counters as a side effect."""
    from genie_space_optimizer.optimization.harness import (
        _select_lever_for_cluster,
    )
    rotation_holder = {"tried": {"C1": frozenset({6})}}
    before = {k: dict(v) if hasattr(v, "items") else v for k, v in rotation_holder.items()}
    _select_lever_for_cluster(_make_cluster(), rotation_holder)
    assert rotation_holder == before
