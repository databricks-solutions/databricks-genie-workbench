"""Phase 0 — cross-sub-phase end-to-end check.

Verifies the four invariants that Phase 0 establishes:

  0.1 — evidence bundle can read replay fixture from any of
        {logs, notebook_output.result, notebook_output.error_trace,
         result+error_trace}
  0.2 — Phase H iteration_summaries totality helper recognises clean
        and mismatched runs (existing helper)
  0.3 — every iteration_summary marker is paired with exactly
        one terminal marker
  0.4 — candidate ledger JSONL is round-trippable
"""
from __future__ import annotations

from pathlib import Path

from genie_space_optimizer.optimization.candidate_ledger import (
    IterationCandidateLedgerEntry,
    read_ledger,
    write_ledger_entry,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_summary_totality,
    check_iteration_terminal_exhaustiveness,
)
from genie_space_optimizer.tools.evidence_bundle import (
    _extract_stdout_with_fallback,
    detect_stale_phase_h_anchor,
)


def test_phase_0_1_evidence_extracts_from_error_trace():
    out = {
        "logs": "",
        "notebook_output": {
            "result": "",
            "error_trace": (
                "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
                '{"iterations":[],"summary":{}}\n'
                "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
            ),
        },
    }
    text, source, _ = _extract_stdout_with_fallback(out)
    assert source == "notebook_output.error_trace"
    assert "PHASE_A_REPLAY_FIXTURE_JSON_BEGIN" in text


def test_phase_0_1_stale_anchor_fires_on_mismatch():
    result = detect_stale_phase_h_anchor(
        chosen_task_run_id="9999",
        phase_h_sibling_task_run_ids=("1111",),
    )
    assert result is not None


def test_phase_0_2_totality_invariant_clean():
    assert check_iteration_summary_totality(
        iteration_counter=3,
        iteration_summary_count=3,
        phase_b_iter_record_counts_length=3,
    ) is None


def test_phase_0_3_exhaustiveness_clean():
    stdout = (
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r","iteration":1,'
        '"accepted_count":1,"rolled_back_count":0,"skipped_count":0,'
        '"gate_drop_count":0,"decision_record_count":5,'
        '"journey_violation_count":0}\n'
        'GSO_FULL_EVAL_V1 {"optimization_run_id":"r","payload":{}}'
    )
    assert check_iteration_terminal_exhaustiveness(stdout=stdout) is None


def test_phase_0_4_ledger_round_trips(tmp_path: Path):
    p = tmp_path / "ledger.jsonl"
    entry = IterationCandidateLedgerEntry(
        iteration=1,
        ag_id="a1",
        cluster_ids=("c1",),
        target_qids=("gs_001",),
        root_cause="r",
        requested_levers=(5,),
        rca_card_id_or_provisional="rca-1",
        proposal_attempts=1,
        selected_proposal_id="p1",
        terminal_reason="no_structural_candidate",
        terminal_outcome="info",
        best_of_n_size=1,
        patches_applied=0,
        subset_isolation_run=False,
        subset_isolation_kept=(),
        subset_isolation_dropped=(),
        protected_dependents=(),
        narrow_replacement_attempted=False,
        narrow_replacement_succeeded=False,
        accuracy_delta_pp=0.0,
        acceptance_tier="reject_loss",
        retire_signature="sig",
    )
    write_ledger_entry(entry, path=str(p))
    parsed = read_ledger(str(p))
    assert len(parsed) == 1
    assert parsed[0].iteration == 1


def test_phase_0_flag_accessors_default_on(monkeypatch):
    """The three new Phase 0 flag accessors return True unless
    explicitly disabled via env var (Task 7 + 9 + 13 flag bodies)."""
    from genie_space_optimizer.common.config import (
        candidate_ledger_enabled,
        iteration_terminal_marker_enabled,
        phase_h_totality_enabled,
    )

    for var in ("GSO_PHASE_H_TOTALITY", "GSO_ITERATION_TERMINAL_MARKER", "GSO_CANDIDATE_LEDGER"):
        monkeypatch.delenv(var, raising=False)
    assert phase_h_totality_enabled() is True
    assert iteration_terminal_marker_enabled() is True
    assert candidate_ledger_enabled() is True

    monkeypatch.setenv("GSO_PHASE_H_TOTALITY", "0")
    monkeypatch.setenv("GSO_ITERATION_TERMINAL_MARKER", "0")
    monkeypatch.setenv("GSO_CANDIDATE_LEDGER", "0")
    assert phase_h_totality_enabled() is False
    assert iteration_terminal_marker_enabled() is False
    assert candidate_ledger_enabled() is False
