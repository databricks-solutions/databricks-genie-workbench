"""I24 — if a run terminates with ag_collision_with_forbidden_set MORE
THAN ONCE on the same cluster, it's a quality failure (stale strategy
shipped to AG regenerator)."""
from genie_space_optimizer.optimization.invariants import (
    check_i24_ag_collision_quality_failure,
)


def test_violation_when_same_cluster_collides_twice():
    evidence = {
        "iteration_terminal_markers": [
            {
                "iteration": 1,
                "cluster_id": "H001",
                "terminal_reason": "ag_collision_with_forbidden_set",
            },
            {
                "iteration": 2,
                "cluster_id": "H001",
                "terminal_reason": "ag_collision_with_forbidden_set",
            },
        ],
    }
    violations = check_i24_ag_collision_quality_failure(evidence)
    assert len(violations) == 1
    assert violations[0]["cluster_id"] == "H001"
    assert violations[0]["collision_count"] == 2


def test_green_for_a_single_collision():
    """One collision is allowed (could be a transient AG-LLM hiccup); a
    second on the same cluster is the quality failure."""
    evidence = {
        "iteration_terminal_markers": [
            {
                "iteration": 1,
                "cluster_id": "H001",
                "terminal_reason": "ag_collision_with_forbidden_set",
            },
        ],
    }
    assert check_i24_ag_collision_quality_failure(evidence) == []


def test_green_when_no_collisions():
    evidence = {"iteration_terminal_markers": []}
    assert check_i24_ag_collision_quality_failure(evidence) == []


def test_collisions_on_different_clusters_dont_aggregate():
    """Two collisions on DIFFERENT clusters are not a violation;
    aggregation is per-cluster."""
    evidence = {
        "iteration_terminal_markers": [
            {
                "iteration": 1,
                "cluster_id": "H001",
                "terminal_reason": "ag_collision_with_forbidden_set",
            },
            {
                "iteration": 1,
                "cluster_id": "H002",
                "terminal_reason": "ag_collision_with_forbidden_set",
            },
        ],
    }
    assert check_i24_ag_collision_quality_failure(evidence) == []


def test_i24_in_high_tier():
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert "I24" in HIGH_TIER_INVARIANT_IDS
