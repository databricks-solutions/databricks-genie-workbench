"""I22 — every Stage 3 proposal_id must have exactly one matching
GSO_PATCH_OUTCOME_V1 marker."""
from genie_space_optimizer.optimization.invariants import (
    check_i22_patch_outcome_coverage,
)


def test_violation_when_proposal_has_no_outcome():
    evidence = {
        "plan11_stage3_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "cluster_id": "C001",
                "outcome": "synthesized",
                "proposal_ids": ["intent_001", "intent_002"],
            },
        ],
        "patch_outcome_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "intent_id": "intent_001",
                "outcome_kind": "applied",
            },
        ],
    }
    violations = check_i22_patch_outcome_coverage(evidence)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "I22"
    assert violations[0]["missing_intent_id"] == "intent_002"


def test_green_when_every_proposal_has_outcome():
    evidence = {
        "plan11_stage3_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "cluster_id": "C001",
                "outcome": "synthesized",
                "proposal_ids": ["intent_001", "intent_002"],
            },
        ],
        "patch_outcome_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "intent_id": "intent_001",
                "outcome_kind": "applied",
            },
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "intent_id": "intent_002",
                "outcome_kind": "validator_rejected",
            },
        ],
    }
    assert check_i22_patch_outcome_coverage(evidence) == []


def test_violation_when_outcome_double_emitted():
    evidence = {
        "plan11_stage3_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "cluster_id": "C001",
                "outcome": "synthesized",
                "proposal_ids": ["intent_001"],
            },
        ],
        "patch_outcome_markers": [
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "intent_id": "intent_001",
                "outcome_kind": "applied",
            },
            {
                "optimization_run_id": "run_x",
                "iteration": 2,
                "intent_id": "intent_001",
                "outcome_kind": "blast_radius_rejected",
            },
        ],
    }
    violations = check_i22_patch_outcome_coverage(evidence)
    assert len(violations) == 1
    assert "double" in violations[0]["message"].lower()


def test_silent_when_no_stage3_markers():
    evidence = {
        "plan11_stage3_markers": [],
        "patch_outcome_markers": [],
    }
    assert check_i22_patch_outcome_coverage(evidence) == []


def test_i22_in_high_tier():
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert "I22" in HIGH_TIER_INVARIANT_IDS
