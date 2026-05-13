"""Phase 3 follow-up — production-path integration replay.

Asserts the wired reconciliation upgrades NO_STRUCTURAL_CANDIDATE to
FORCE_LLM_DECLINED for L6 when iter_inputs carries the canonical
lever6_force_llm_declined decision record — the exact 2314bb2c AG2 iter 2
failure shape that Phase 3's conservative-zero fallback failed to attribute.

This is the missing assertion the executor's feedback identified: Phase 3's
existing integration test used a hand-constructed ledger. This test starts
from the iter_inputs shape the harness writes and walks the wired path
through to the final outcome.
"""

from __future__ import annotations


def _ag2_lever_directives() -> dict:
    """AG2 from 2314bb2c — L5 + L6 directives both targeting gs_026."""
    return {
        "5": {
            "target_qids": ["7now_delivery_analytics_space_gs_026"],
            "example_sql_seed": "-- placeholder",
        },
        "6": {
            "target_qids": ["7now_delivery_analytics_space_gs_026"],
            "sql_expression": (
                "SUM(f.cy_sales) FILTER (WHERE f.region = 'WEST')"
            ),
        },
    }


def _l6_force_decline_record(*, ag_id: str, iteration: int) -> dict:
    """The exact dict shape _emit_force_l6_outcome writes (after .to_dict())."""
    return {
        "decision_type": "proposal_failure_decided",
        "reason_code": "lever6_force_llm_declined",
        "ag_id": ag_id,
        "iteration": iteration,
        "cluster_id": "c-26",
        "root_cause": "plural_top_n_collapse",
        "evidence_refs": ["signature:c26_zone_vp_routing"],
        "metrics": {"cached": False},
    }


def test_l6_force_decline_upgrades_no_structural_to_force_llm_declined() -> None:
    """The full reconciliation path: classifier sees zero proposals for L6
    (conservative-zero fallback), records contain the L6 decline,
    reconciliation upgrades to FORCE_LLM_DECLINED."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
        reconcile_outcome_from_records,
    )

    # 1. Classifier snapshot — zero proposals for L6, force_llm_declined
    #    conservative-zero False (the actual Phase 3 Task 8 fallback).
    snapshot = LeverProposalSnapshot(
        lever_key=6,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
    )
    classifier_output = classify_lever_proposal_outcome(snapshot)
    assert classifier_output == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE

    # 2. Reconciliation — the harness reads iter_inputs["decision_records"]
    #    and upgrades.
    iter_inputs_decision_records = [
        _l6_force_decline_record(ag_id="AG2", iteration=2),
    ]
    refined = reconcile_outcome_from_records(
        classifier_outcome=classifier_output,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=iter_inputs_decision_records,
    )
    assert refined == DirectiveOutcomeCode.FORCE_LLM_DECLINED


def test_l5_does_not_borrow_l6_decline_attribution() -> None:
    """L5 and L6 are reconciled separately. An L6 decline record in
    iter_inputs must not leak into L5's outcome."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
        reconcile_outcome_from_records,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
    )
    classifier_output = classify_lever_proposal_outcome(snapshot)
    assert classifier_output == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE

    refined = reconcile_outcome_from_records(
        classifier_outcome=classifier_output,
        lever_key=5,
        ag_id="AG2",
        iteration=2,
        decision_records=[_l6_force_decline_record(ag_id="AG2", iteration=2)],
    )
    # L5 stays NO_STRUCTURAL_CANDIDATE — the L6 decline is L6-only attribution.
    assert refined == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_full_ag2_iter_2_shape_produces_correct_per_lever_outcomes() -> None:
    """End-to-end: simulate iter 2 of 2314bb2c AG2 — L5 directive produced
    zero proposals (no structural candidate), L6 directive force-LLM
    declined. After reconciliation:
      - L5 outcome = NO_STRUCTURAL_CANDIDATE
      - L6 outcome = FORCE_LLM_DECLINED
    The full ledger emits with precise attribution, not 2x NO_STRUCTURAL."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
        reconcile_outcome_from_records,
    )

    ag_id = "AG2"
    iteration = 2
    ag_directives = _ag2_lever_directives()
    directive_keys = tuple(sorted(int(k) for k in ag_directives.keys()))

    ledger = AgDirectiveLedger(
        ag_id=ag_id,
        iteration=iteration,
        directives_present=directive_keys,
    )

    # Per-lever snapshots — both conservative-zero from Phase 3 fallback.
    for lever_key in directive_keys:
        snapshot = LeverProposalSnapshot(
            lever_key=lever_key,
            proposals_emitted_count=0,
            structural_gate_drop_count=0,
            applyability_drop_count=0,
            collateral_drop_count=0,
            force_llm_declined=False,
        )
        ledger.outcomes_by_lever[lever_key] = classify_lever_proposal_outcome(
            snapshot
        )

    # Both currently NO_STRUCTURAL_CANDIDATE — the pre-followup state.
    assert ledger.outcomes_by_lever[5] == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )
    assert ledger.outcomes_by_lever[6] == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )

    # Reconciliation pass — harness side.
    decision_records = [
        _l6_force_decline_record(ag_id=ag_id, iteration=iteration),
    ]
    for lever_key in list(ledger.outcomes_by_lever.keys()):
        refined = reconcile_outcome_from_records(
            classifier_outcome=ledger.outcomes_by_lever[lever_key],
            lever_key=lever_key,
            ag_id=ag_id,
            iteration=iteration,
            decision_records=decision_records,
        )
        ledger.outcomes_by_lever[lever_key] = refined

    assert ledger.outcomes_by_lever[5] == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )
    assert ledger.outcomes_by_lever[6] == (
        DirectiveOutcomeCode.FORCE_LLM_DECLINED
    )

    # And the marker payload carries the upgraded attribution.
    payload = ledger.to_marker_payload()
    assert payload["outcomes_by_lever"] == {
        "5": "no_structural_candidate",
        "6": "force_llm_declined",
    }
