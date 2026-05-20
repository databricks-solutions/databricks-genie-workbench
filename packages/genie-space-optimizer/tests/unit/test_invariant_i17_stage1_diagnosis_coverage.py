"""I17 — Stage 1 diagnosis coverage.

Every failing QID in a Plan 11 iteration must have a
GSO_PLAN11_STAGE1_DIAGNOSIS_V1 marker. Missing marker = silent skip.
"""
from genie_space_optimizer.optimization.invariants import (
    check_i17_stage1_diagnosis_coverage,
)


def _build_evidence(stage1_markers, failing_qids_per_iter):
    return {
        "plan11_stage1_markers": stage1_markers,
        "failing_qids_per_iteration": failing_qids_per_iter,
    }


def test_i17_violation_when_qid_missing_marker():
    evidence = _build_evidence(
        stage1_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "qid": "gs_009",
             "outcome": "diagnosed"},
            # gs_024 missing
        ],
        failing_qids_per_iter={"r1:1": ["gs_009", "gs_024"]},
    )
    violations = check_i17_stage1_diagnosis_coverage(evidence)
    assert len(violations) == 1
    assert violations[0]["missing_qid"] == "gs_024"


def test_i17_no_violation_when_all_covered():
    evidence = _build_evidence(
        stage1_markers=[
            {"optimization_run_id": "r1", "iteration": 1, "qid": "gs_009",
             "outcome": "diagnosed"},
            {"optimization_run_id": "r1", "iteration": 1, "qid": "gs_024",
             "outcome": "declined"},
        ],
        failing_qids_per_iter={"r1:1": ["gs_009", "gs_024"]},
    )
    assert check_i17_stage1_diagnosis_coverage(evidence) == []


def test_i17_silent_when_no_plan11_markers():
    # Pre-Plan-11 evidence → should always pass (back-compat)
    evidence = _build_evidence(
        stage1_markers=[],
        failing_qids_per_iter={"r1:1": ["gs_009"]},
    )
    assert check_i17_stage1_diagnosis_coverage(evidence) == []
