"""Unit tests for the lever-loop stdout parser.

The fixture file ``lever_loop_stdout_0ade1a99.txt`` is the recovered
notebook stdout from run 0ade1a99-9406-4a68-a3bc-8c77be78edcb. The
parser must surface every block the postmortem skill needs."""

from pathlib import Path

import pytest

from genie_space_optimizer.tools.lever_loop_stdout_parser import (
    LeverLoopStdoutView,
    parse_lever_loop_stdout,
)


_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "lever_loop_stdout_0ade1a99.txt"


@pytest.fixture
def view() -> LeverLoopStdoutView:
    return parse_lever_loop_stdout(_FIXTURE.read_text())


def test_optimization_run_summary_extracted(view: LeverLoopStdoutView) -> None:
    summary = view.optimization_run_summary
    assert summary is not None
    assert summary.final_accuracy_pct == pytest.approx(91.7, abs=0.1)
    assert summary.iterations_attempted >= 1
    assert summary.terminal_status in {"max_iterations", "all_thresholds_met", "diminishing_returns"}


def test_evaluation_summary_extracted_for_each_iteration(view: LeverLoopStdoutView) -> None:
    """Iter 3's EVALUATION SUMMARY block has target_fixed_qids=none and
    no explicit target_still_hard_qids field (accepted iters never emit
    it). The parser must derive the residual target list from the AG's
    declared target_qids (looked up in the AG Decisions / Proposal
    Inventory blocks) minus target_fixed_qids."""
    assert view.evaluation_summary, "expected at least one EVALUATION SUMMARY block"
    es = view.evaluation_summary[3]
    assert es.target_fixed_qids == ()
    assert "airline_ticketing_and_fare_analysis_gs_009" in es.target_still_hard_qids
    assert es.target_still_hard_qids_source == "derived"


def test_proposal_inventory_carries_rca_id_per_proposal(view: LeverLoopStdoutView) -> None:
    inv = view.proposal_inventory.get(3, {}).get("AG_COVERAGE_H003", ())
    assert inv, "expected proposal inventory for AG_COVERAGE_H003 in iter 3"
    rca_ids = {p.rca_id for p in inv}
    assert None in rca_ids or "" in rca_ids, "0ade1a99 had ungrounded proposals"


def test_patch_survival_separates_selected_and_dropped(view: LeverLoopStdoutView) -> None:
    survival = view.patch_survival.get(3, {}).get("AG_COVERAGE_H003")
    assert survival is not None
    assert survival.selected_count >= 1
    assert survival.dropped_count >= 1
    assert any(d.reason == "lower_causal_rank" for d in survival.dropped)


def test_blast_radius_drops_attributed_to_patch_type(view: LeverLoopStdoutView) -> None:
    drops = view.blast_radius_drops.get(3, ())
    assert any(
        d.reason == "high_collateral_risk_flagged"
        for d in drops
    )


def test_acceptance_decision_records_target_fixed_qids(view: LeverLoopStdoutView) -> None:
    """Iter 3 (AG_COVERAGE_H003, ACCEPTED with target_fixed_qids=∅)
    is the canonical ACCEPTANCE_TARGET_BLIND example. The harness's
    FULL EVAL block on accepted iters does NOT emit
    target_still_hard_qids (the field only appears on
    Regressions: lines of FAIL/REGRESSION blocks). The parser must
    *derive* target_still_hard_qids from AG.target_qids minus
    target_fixed_qids when the field is absent on accepted iters.
    """
    dec = view.acceptance_decision.get(3, {}).get("AG_COVERAGE_H003")
    assert dec is not None
    assert dec.accepted is True
    assert dec.target_fixed_qids == ()
    assert dec.target_still_hard_qids
    assert dec.target_still_hard_qids[0].endswith("gs_009")
    assert dec.target_still_hard_qids_source == "derived"


def test_acceptance_decision_uses_explicit_field_when_present(view: LeverLoopStdoutView) -> None:
    """Iter 4 / 5 (rolled-back FULL EVAL with Regressions: line) emit
    target_still_hard_qids explicitly. The parser must use the
    explicit value verbatim and stamp source='explicit'."""
    explicit = [
        d for it_map in view.acceptance_decision.values() for d in it_map.values()
        if d.target_still_hard_qids and d.target_still_hard_qids_source == "explicit"
    ]
    assert explicit, "expected at least one explicit target_still_hard_qids in fixture (iters 4/5)"
