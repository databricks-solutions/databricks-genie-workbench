"""Phase 1.3: ``_MAX_CLUSTERS_PER_LEVER`` lifted to distinct-root_causes floor.

The hard cap of 3 silently dropped a 4th cluster even when the cluster
had a distinct root_cause that deserved its own proposal. This test
asserts the new floor behavior at the source level: when 4 clusters
target the same lever and have 4 distinct root_causes, all 4 are
retained, with at least one keep per root_cause.
"""

from __future__ import annotations

from pathlib import Path


def test_max_clusters_floor_uses_distinct_root_causes() -> None:
    """Read the optimizer source and assert the cap formula is correct."""
    src_path = (
        Path(__file__).parents[2]
        / "src/genie_space_optimizer/optimization/optimizer.py"
    )
    src = src_path.read_text(encoding="utf-8")

    # The literal hard cap of 3 has been replaced by a min-floor.
    assert "_MAX_CLUSTERS_PER_LEVER = 3" not in src, (
        "Phase 1.3 expected the bare ``_MAX_CLUSTERS_PER_LEVER = 3`` "
        "to be removed. Use ``_MIN_CLUSTER_BUDGET`` with a "
        "distinct-root_causes floor instead."
    )
    # The new floor name + computation must be present.
    assert "_MIN_CLUSTER_BUDGET = 3" in src
    assert "_distinct_root_causes" in src
    assert "_max_clusters = max(_MIN_CLUSTER_BUDGET, len(_distinct_root_causes))" in src

    # The cap loop should preserve at least one cluster per distinct
    # root_cause before falling back to question-count ordering.
    assert "_seen_rc" in src
    assert "if _rc not in _seen_rc" in src
