"""I18 — Stage 3 synthesis coverage.

Every cluster from Stage 2 must have a Stage 3 synthesis marker.
"""
from genie_space_optimizer.optimization.invariants import (
    check_i18_stage3_synthesis_coverage,
)


def _build_evidence(stage2_markers, stage3_markers):
    return {
        "plan11_stage2_markers": stage2_markers,
        "plan11_stage3_markers": stage3_markers,
    }


def test_i18_violation_when_cluster_has_no_synthesis():
    evidence = _build_evidence(
        stage2_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "clustered",
             "cluster_ids": ["H001", "H002"]},
        ],
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "cluster_id": "H001",
             "outcome": "synthesized"},
            # H002 missing
        ],
    )
    violations = check_i18_stage3_synthesis_coverage(evidence)
    assert len(violations) == 1
    assert violations[0]["missing_cluster_id"] == "H002"


def test_i18_no_violation_when_all_clusters_covered():
    evidence = _build_evidence(
        stage2_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "clustered",
             "cluster_ids": ["H001"]},
        ],
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "cluster_id": "H001",
             "outcome": "synthesized"},
        ],
    )
    assert check_i18_stage3_synthesis_coverage(evidence) == []


def test_i18_silent_when_no_plan11_markers():
    assert check_i18_stage3_synthesis_coverage(
        {"plan11_stage2_markers": [], "plan11_stage3_markers": []}
    ) == []
