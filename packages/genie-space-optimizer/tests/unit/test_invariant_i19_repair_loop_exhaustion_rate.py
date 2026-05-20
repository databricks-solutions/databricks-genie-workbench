"""I19 — Repair loop exhaustion rate must not exceed 20%."""
from genie_space_optimizer.optimization.invariants import (
    check_i19_repair_loop_exhaustion_rate,
)


def _build_evidence(stage3_markers, repair_markers):
    return {
        "plan11_stage3_markers": stage3_markers,
        "plan11_repair_loop_markers": repair_markers,
    }


def test_i19_violation_above_threshold():
    # 3 proposals, 1 exhausted = 33% > 20%
    evidence = _build_evidence(
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1,
             "proposals_count": 3, "outcome": "synthesized"},
        ],
        repair_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "exhausted"},
        ],
    )
    violations = check_i19_repair_loop_exhaustion_rate(evidence)
    assert len(violations) == 1
    assert violations[0]["exhaustion_pct"] > 20.0


def test_i19_no_violation_below_threshold():
    # 10 proposals, 1 exhausted = 10% < 20%
    evidence = _build_evidence(
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1,
             "proposals_count": 10, "outcome": "synthesized"},
        ],
        repair_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "exhausted"},
        ],
    )
    assert check_i19_repair_loop_exhaustion_rate(evidence) == []


def test_i19_silent_when_no_plan11_markers():
    assert check_i19_repair_loop_exhaustion_rate(
        {"plan11_stage3_markers": [], "plan11_repair_loop_markers": []}
    ) == []
