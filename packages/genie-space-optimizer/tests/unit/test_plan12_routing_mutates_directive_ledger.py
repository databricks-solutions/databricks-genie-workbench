"""Plan 12 routing: when reroute_applied=True for an AG, the AG's
effective lever_directives must mutate, not just emit a marker.

Closes the gs_004 failure where routing said L1→L5 but the directive
consumer still ran L1 and emitted lever_not_proposal_generating.
"""
def test_reroute_l1_to_l5_mutates_ag_directives():
    from genie_space_optimizer.optimization.harness import (
        apply_plan12_reroute_to_ag,
    )

    ag = {
        "id": "AG_DECOMPOSED_H002",
        "lever_directives": {1: {"sql_expressions": []}},
        "target_lever": 1,
    }
    routing_decision = {
        "reroute_applied": True,
        "original_lever": 1,
        "new_lever": 5,
        "evidence_kind": "wrong_metric",
        "rationale": "Lever 1 is metadata-only; routing to Lever 5 example SQL.",
    }
    mutated = apply_plan12_reroute_to_ag(ag=ag, routing_decision=routing_decision)
    assert mutated["target_lever"] == 5
    assert 5 in mutated["lever_directives"]
    # Original L1 directives are preserved for audit but no longer authoritative.
    assert mutated.get("rerouted_from_lever") == 1


def test_no_reroute_leaves_ag_unchanged():
    from genie_space_optimizer.optimization.harness import (
        apply_plan12_reroute_to_ag,
    )

    ag = {
        "id": "AG_DECOMPOSED_H001",
        "lever_directives": {6: {"sql_expressions": ["foo"]}},
        "target_lever": 6,
    }
    routing_decision = {"reroute_applied": False, "evidence_kind": "ok"}
    mutated = apply_plan12_reroute_to_ag(ag=ag, routing_decision=routing_decision)
    assert mutated == ag
