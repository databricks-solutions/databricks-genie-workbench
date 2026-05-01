"""Risk-lane banner: surfaces per-gate counters for the example-SQL
high-risk lane so operators can diagnose regressions."""
from genie_space_optimizer.optimization.harness import (
    _print_enrichment_risk_lane_banner,
)


def test_banner_lists_all_high_risk_gates(capsys):
    _print_enrichment_risk_lane_banner(
        candidates_in=60,
        firewall_blocked=12,
        firewall_warned=0,
        correctness_rejected=8,
        deterministic_safety_rejected=15,
        teaching_safety_rejected=10,
        smoke_test_rejected_batch=False,
        smoke_test_regressions=0,
        smoke_test_sample_size=0,
        applied=15,
    )
    out = capsys.readouterr().out
    assert "ENRICHMENT — HIGH-RISK LANE" in out
    assert "Candidates considered" in out
    assert "Firewall: blocked" in out
    assert "Correctness arbiter: rejected" in out
    assert "Deterministic safety: rejected" in out
    assert "Teaching-safety judge: rejected" in out
    assert "Smoke test" in out
    assert "Applied (gold standard)" in out


def test_banner_marks_smoke_test_rejection(capsys):
    _print_enrichment_risk_lane_banner(
        candidates_in=20,
        firewall_blocked=2,
        firewall_warned=0,
        correctness_rejected=3,
        deterministic_safety_rejected=4,
        teaching_safety_rejected=2,
        smoke_test_rejected_batch=True,
        smoke_test_regressions=2,
        smoke_test_sample_size=10,
        applied=0,
    )
    out = capsys.readouterr().out
    assert "REJECTED" in out
    assert "2/10" in out
