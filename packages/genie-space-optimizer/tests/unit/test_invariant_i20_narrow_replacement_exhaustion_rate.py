"""I20 — Narrow replacement exhaustion rate must not exceed 20%."""
from genie_space_optimizer.optimization.invariants import (
    check_i20_narrow_replacement_exhaustion_rate,
)


def _build_evidence(stage3_markers, narrow_markers):
    return {
        "plan11_stage3_markers": stage3_markers,
        "plan11_narrow_replacement_markers": narrow_markers,
    }


def test_i20_violation_above_threshold():
    evidence = _build_evidence(
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1,
             "proposals_count": 3, "outcome": "synthesized"},
        ],
        narrow_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "exhausted"},
        ],
    )
    violations = check_i20_narrow_replacement_exhaustion_rate(evidence)
    assert len(violations) == 1


def test_i20_no_violation():
    evidence = _build_evidence(
        stage3_markers=[
            {"optimization_run_id": "r1", "iteration": 1,
             "proposals_count": 10, "outcome": "synthesized"},
        ],
        narrow_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "outcome": "exhausted"},
        ],
    )
    assert check_i20_narrow_replacement_exhaustion_rate(evidence) == []


def test_i20_silent_when_no_plan11_markers():
    assert check_i20_narrow_replacement_exhaustion_rate(
        {"plan11_stage3_markers": [], "plan11_narrow_replacement_markers": []}
    ) == []
