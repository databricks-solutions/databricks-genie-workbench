"""Unit tests for the canonical CandidateOutcome record."""
from __future__ import annotations

from genie_space_optimizer.optimization.candidate_outcome import CandidateOutcome


def _e943_payload() -> dict:
    """The clean-win full-eval payload from the e94376a3 postmortem."""
    return {
        "accepted": True,
        "iteration": 3,
        "ag_id": "AG_DECOMPOSED_H003",
        "reason_code": "accepted",
        "baseline_accuracy": 87.5,
        "candidate_accuracy": 100.0,
        "delta_pp": 12.5,
        "target_qids": ["airline_ticketing_and_fare_analysis_gs_009"],
        "target_fixed_qids": ["airline_ticketing_and_fare_analysis_gs_009"],
        "target_still_hard_qids": [],
        "regression_debt_qids": [],
    }


def test_from_full_eval_payload_clean_win():
    co = CandidateOutcome.from_full_eval_payload(
        _e943_payload(),
        selected_proposal_id="P_H003",
        acceptance_tier="accept_target_fixed",
        patches_applied=1,
        levers=[5],
    )
    assert co.accepted is True
    assert co.delta_pp == 12.5
    assert co.iteration == 3
    assert co.ag_id == "AG_DECOMPOSED_H003"
    assert co.target_fixed_qids == ("airline_ticketing_and_fare_analysis_gs_009",)
    assert co.selected_proposal_id == "P_H003"
    assert co.acceptance_tier == "accept_target_fixed"
    assert co.patches_applied == 1
    assert co.levers == (5,)
    assert co.is_clean_win is True
    assert co.has_target_debt is False


def test_clean_win_aggregate_kwargs_classify_improved():
    from genie_space_optimizer.optimization.state_machine.outcome import (
        classify_run_outcome_from_aggregates,
    )

    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    outcome = classify_run_outcome_from_aggregates(**co.to_aggregate_kwargs())
    assert outcome == "OPTIMIZER_IMPROVED"


def test_target_debt_detected():
    payload = _e943_payload()
    payload["target_qids"] = ["gs_009", "gs_024"]
    payload["target_fixed_qids"] = ["gs_009"]
    payload["target_still_hard_qids"] = ["gs_024"]
    co = CandidateOutcome.from_full_eval_payload(payload)
    assert co.has_target_debt is True
    assert co.is_clean_win is False


def test_target_debt_aggregate_classifies_target_debt():
    from genie_space_optimizer.optimization.state_machine.outcome import (
        classify_run_outcome_from_aggregates,
    )

    payload = _e943_payload()
    payload["target_qids"] = ["gs_009", "gs_024"]
    payload["target_fixed_qids"] = ["gs_009"]
    payload["target_still_hard_qids"] = ["gs_024"]
    co = CandidateOutcome.from_full_eval_payload(payload)
    outcome = classify_run_outcome_from_aggregates(**co.to_aggregate_kwargs())
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_not_accepted_is_not_clean_win():
    payload = _e943_payload()
    payload["accepted"] = False
    co = CandidateOutcome.from_full_eval_payload(payload)
    assert co.is_clean_win is False


def test_regression_debt_blocks_clean_win():
    payload = _e943_payload()
    payload["regression_debt_qids"] = ["gs_777"]
    co = CandidateOutcome.from_full_eval_payload(payload)
    assert co.is_clean_win is False


def test_project_run_outcome_accepted_dominates_kept_insufficient():
    # e94376a3: trajectory classifier saw only kept_insufficient lanes
    # and returned OPTIMIZER_TRIED_INSUFFICIENT_GAIN; the canonical
    # accepted win must dominate.
    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    projected = CandidateOutcome.project_run_outcome(
        "OPTIMIZER_TRIED_INSUFFICIENT_GAIN", co
    )
    assert projected == "OPTIMIZER_IMPROVED"


def test_project_run_outcome_invariant_violation_is_preserved():
    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    projected = CandidateOutcome.project_run_outcome(
        "OPTIMIZER_INVARIANT_VIOLATION", co
    )
    assert projected == "OPTIMIZER_INVARIANT_VIOLATION"


def test_project_run_outcome_no_candidate_passes_through():
    projected = CandidateOutcome.project_run_outcome(
        "OPTIMIZER_TRIED_INSUFFICIENT_GAIN", None
    )
    assert projected == "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"


def test_project_run_outcome_unaccepted_candidate_passes_through():
    payload = _e943_payload()
    payload["accepted"] = False
    co = CandidateOutcome.from_full_eval_payload(payload)
    projected = CandidateOutcome.project_run_outcome("OPTIMIZER_NO_CANDIDATE", co)
    assert projected == "OPTIMIZER_NO_CANDIDATE"


def test_ledger_overrides_for_accepted_iteration():
    co = CandidateOutcome.from_full_eval_payload(
        _e943_payload(),
        selected_proposal_id="P_H003",
        acceptance_tier="accept_target_fixed",
        patches_applied=1,
    )
    ov = co.ledger_overrides_for_iteration(3, current_tier="reject_loss")
    assert ov["accuracy_delta_pp"] == 12.5
    assert ov["acceptance_tier"] == "accept_target_fixed"
    assert ov["patches_applied"] == 1
    assert ov["selected_proposal_id"] == "P_H003"


def test_ledger_overrides_empty_for_other_iteration():
    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    assert co.ledger_overrides_for_iteration(2) == {}


def test_ledger_overrides_empty_when_not_accepted():
    payload = _e943_payload()
    payload["accepted"] = False
    co = CandidateOutcome.from_full_eval_payload(payload)
    assert co.ledger_overrides_for_iteration(3) == {}


def test_relabel_post_win_unknown_becomes_explicit():
    from genie_space_optimizer.optimization.candidate_outcome import (
        POST_WIN_NO_WORK_TERMINAL_REASON,
        relabel_post_win_terminal_reason,
    )

    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    assert (
        relabel_post_win_terminal_reason(co, "unknown")
        == POST_WIN_NO_WORK_TERMINAL_REASON
    )


def test_relabel_post_win_preserves_real_reason():
    from genie_space_optimizer.optimization.candidate_outcome import (
        relabel_post_win_terminal_reason,
    )

    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    # A genuine terminal reason is never overwritten.
    assert (
        relabel_post_win_terminal_reason(co, "blast_radius_rejected")
        == "blast_radius_rejected"
    )


def test_relabel_post_win_noop_without_win():
    from genie_space_optimizer.optimization.candidate_outcome import (
        relabel_post_win_terminal_reason,
    )

    assert relabel_post_win_terminal_reason(None, "unknown") == "unknown"
    payload = _e943_payload()
    payload["accepted"] = False
    co = CandidateOutcome.from_full_eval_payload(payload)
    assert relabel_post_win_terminal_reason(co, "unknown") == "unknown"


def test_ledger_overrides_default_tier_when_missing():
    co = CandidateOutcome.from_full_eval_payload(_e943_payload())
    ov = co.ledger_overrides_for_iteration(3, current_tier="reject_loss")
    # No tier on the record and the current local is the default — the
    # override promotes it to a truthful "accepted".
    assert ov["acceptance_tier"] == "accepted"
    # No selected proposal known — leave the caller's value untouched.
    assert "selected_proposal_id" not in ov
