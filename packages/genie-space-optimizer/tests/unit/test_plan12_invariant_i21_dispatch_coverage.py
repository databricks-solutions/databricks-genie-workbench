"""I21 — when plan11_llm_first=true and hard failures exist, a run
MUST contain either GSO_PLAN11_STAGE1_DIAGNOSIS_V1 markers OR an
explicit GSO_PLAN11_DISPATCH_DECISION_V1 marker."""
from genie_space_optimizer.optimization.invariants import (
    check_i21_plan11_dispatch_coverage,
)


def test_violation_when_flag_on_no_stage1_no_decision():
    evidence = {
        "plan11_flag_enabled": True,
        "hard_failures_present": True,
        "plan11_stage1_markers": [],
        "plan11_dispatch_decision_markers": [],
    }
    violations = check_i21_plan11_dispatch_coverage(evidence)
    assert len(violations) == 1
    assert violations[0]["invariant"] == "I21"


def test_green_when_stage1_marker_present():
    evidence = {
        "plan11_flag_enabled": True,
        "hard_failures_present": True,
        "plan11_stage1_markers": [{"qid": "gs_009", "outcome": "diagnosed"}],
        "plan11_dispatch_decision_markers": [],
    }
    assert check_i21_plan11_dispatch_coverage(evidence) == []


def test_green_when_dispatch_decision_marker_present():
    evidence = {
        "plan11_flag_enabled": True,
        "hard_failures_present": True,
        "plan11_stage1_markers": [],
        "plan11_dispatch_decision_markers": [
            {"outcome": "skipped", "skip_reason": "flag_disabled"},
        ],
    }
    assert check_i21_plan11_dispatch_coverage(evidence) == []


def test_silent_when_flag_off():
    evidence = {
        "plan11_flag_enabled": False,
        "hard_failures_present": True,
        "plan11_stage1_markers": [],
        "plan11_dispatch_decision_markers": [],
    }
    assert check_i21_plan11_dispatch_coverage(evidence) == []


def test_silent_when_no_hard_failures():
    evidence = {
        "plan11_flag_enabled": True,
        "hard_failures_present": False,
        "plan11_stage1_markers": [],
        "plan11_dispatch_decision_markers": [],
    }
    assert check_i21_plan11_dispatch_coverage(evidence) == []


def test_i21_in_high_tier_set():
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert "I21" in HIGH_TIER_INVARIANT_IDS
