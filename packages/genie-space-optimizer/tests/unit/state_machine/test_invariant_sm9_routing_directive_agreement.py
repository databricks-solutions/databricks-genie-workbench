"""SM9: ClusterMembershipRecord.effective_target_lever is the only source of directives_present."""
from genie_space_optimizer.optimization.state_machine.invariants_sm import (
    check_sm9_routing_directive_agreement,
)


def test_sm9_clean_when_lever_matches_directive():
    cluster_levers = {"H001": 6, "H002": 5}
    directives_by_cluster = {"H001": (6,), "H002": (5,)}
    assert check_sm9_routing_directive_agreement(
        cluster_levers=cluster_levers,
        directives_by_cluster=directives_by_cluster,
    ) == []


def test_sm9_violation_on_mismatch():
    cluster_levers = {"H001": 6}
    directives_by_cluster = {"H001": (1,)}  # legacy code wrote 1; routing said 6
    violations = check_sm9_routing_directive_agreement(
        cluster_levers=cluster_levers,
        directives_by_cluster=directives_by_cluster,
    )
    assert len(violations) == 1
    assert violations[0]["invariant"] == "SM9"
