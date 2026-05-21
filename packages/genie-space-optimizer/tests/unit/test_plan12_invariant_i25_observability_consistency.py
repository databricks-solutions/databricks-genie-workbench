"""I25 — observability aggregations must equal their authoritative
sources. Any mismatch >0 is a HIGH-tier violation."""
from genie_space_optimizer.optimization.invariants import (
    check_i25_observability_consistency,
)


def test_violation_when_phase_b_total_below_journey():
    evidence = {
        "phase_b_end_total_recorded": 0,
        "journey_records_count": 5,
        "iteration_summary_count_recorded": 0,
        "iteration_summary_count_from_journey": 0,
        "proposal_attempts_recorded": 0,
        "proposal_attempts_from_outcomes": 0,
        "run_summary_hard_count_recorded": 0,
        "run_summary_hard_count_from_eval": 0,
    }
    violations = check_i25_observability_consistency(evidence)
    assert any(v["field"] == "phase_b_end_total" for v in violations)


def test_violation_when_run_summary_hard_count_diverges():
    evidence = {
        "phase_b_end_total_recorded": 5,
        "journey_records_count": 5,
        "iteration_summary_count_recorded": 2,
        "iteration_summary_count_from_journey": 2,
        "proposal_attempts_recorded": 0,
        "proposal_attempts_from_outcomes": 0,
        "run_summary_hard_count_recorded": 0,
        "run_summary_hard_count_from_eval": 3,
    }
    violations = check_i25_observability_consistency(evidence)
    assert any(v["field"] == "run_summary_hard_count" for v in violations)


def test_green_when_all_aligned():
    evidence = {
        "phase_b_end_total_recorded": 5,
        "journey_records_count": 5,
        "iteration_summary_count_recorded": 2,
        "iteration_summary_count_from_journey": 2,
        "proposal_attempts_recorded": 3,
        "proposal_attempts_from_outcomes": 3,
        "run_summary_hard_count_recorded": 2,
        "run_summary_hard_count_from_eval": 2,
    }
    assert check_i25_observability_consistency(evidence) == []


def test_all_four_fields_individually_checked():
    """One violation per field that diverges."""
    evidence = {
        "phase_b_end_total_recorded": 1,
        "journey_records_count": 2,
        "iteration_summary_count_recorded": 1,
        "iteration_summary_count_from_journey": 2,
        "proposal_attempts_recorded": 1,
        "proposal_attempts_from_outcomes": 2,
        "run_summary_hard_count_recorded": 1,
        "run_summary_hard_count_from_eval": 2,
    }
    violations = check_i25_observability_consistency(evidence)
    fields = {v["field"] for v in violations}
    assert fields == {
        "phase_b_end_total",
        "iteration_summary_count",
        "proposal_attempts",
        "run_summary_hard_count",
    }


def test_silent_when_evidence_missing():
    """Pre-Plan-12 evidence (no recorded / source keys present) is
    treated as 0 == 0 across all fields → green."""
    assert check_i25_observability_consistency({}) == []


def test_i25_in_high_tier():
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert "I25" in HIGH_TIER_INVARIANT_IDS
