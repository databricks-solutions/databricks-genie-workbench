"""Phase 0.2 — Phase H per-iteration totality (B3).

The Phase H ``iteration_summaries.json`` artifact must carry one
entry per logical iteration in the replay fixture. The harness
also emits ``GSO_ITERATION_SUMMARY_TOTALITY_V1`` with the
equality check (handled by Task 6 harness wiring; Task 5 only
adds the pure payload builder).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.run_output_bundle import (
    build_phase_h_aggregate_iteration_summaries_payload,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_summary_totality,
)


def _make_fixture_iterations(n: int) -> list[dict]:
    return [
        {
            "iteration": i,
            "ag_id": f"ag-{i}",
            "decision_record_count": 10 * i,
            "accepted_count": 1 if i % 2 == 0 else 0,
            "rolled_back_count": 0,
            "skipped_count": 0,
            "gate_drop_count": 0,
            "journey_violation_count": 0,
            "eval_rows": 5,
        }
        for i in range(1, n + 1)
    ]


def test_payload_carries_one_entry_per_replay_fixture_iteration():
    """The aggregate writer sourced from the replay fixture
    iterations list must emit exactly len(iterations) summary
    entries."""
    iterations = _make_fixture_iterations(5)
    payload = build_phase_h_aggregate_iteration_summaries_payload(
        optimization_run_id="opt-1",
        iteration_counter=5,
        replay_fixture_iterations=iterations,
    )
    assert isinstance(payload, dict)
    assert "iteration_summaries" in payload
    assert len(payload["iteration_summaries"]) == 5
    assert payload["iteration_counter"] == 5
    for i, summary in enumerate(payload["iteration_summaries"], start=1):
        assert summary["iteration"] == i
        assert summary["ag_id"] == f"ag-{i}"


def test_totality_check_clean_when_lengths_match():
    iterations = _make_fixture_iterations(3)
    payload = build_phase_h_aggregate_iteration_summaries_payload(
        optimization_run_id="opt-1",
        iteration_counter=3,
        replay_fixture_iterations=iterations,
    )
    violation = check_iteration_summary_totality(
        iteration_counter=3,
        iteration_summary_count=len(payload["iteration_summaries"]),
        phase_b_iter_record_counts_length=3,
    )
    assert violation is None


def test_totality_check_alarms_on_mismatch():
    """If the writer somehow records fewer summaries than
    iteration_counter, check_iteration_summary_totality returns
    a violation dict that the marker emitter consumes."""
    violation = check_iteration_summary_totality(
        iteration_counter=5,
        iteration_summary_count=3,
        phase_b_iter_record_counts_length=5,
    )
    assert violation is not None
    assert violation["iteration_summary_count"] == 3
    assert violation["iteration_counter"] == 5


def test_empty_iterations_no_violation_when_counter_zero():
    payload = build_phase_h_aggregate_iteration_summaries_payload(
        optimization_run_id="opt-1",
        iteration_counter=0,
        replay_fixture_iterations=[],
    )
    assert payload["iteration_summaries"] == []
    violation = check_iteration_summary_totality(
        iteration_counter=0,
        iteration_summary_count=0,
        phase_b_iter_record_counts_length=0,
    )
    assert violation is None


def test_optimization_run_id_carried_through():
    payload = build_phase_h_aggregate_iteration_summaries_payload(
        optimization_run_id="opt-42",
        iteration_counter=2,
        replay_fixture_iterations=_make_fixture_iterations(2),
    )
    assert payload["optimization_run_id"] == "opt-42"
    assert payload["schema_version"] == "v1"


def test_phase_h_candidate_ledger_artifact_roundtrips(tmp_path):
    """Phase 0.4 — Task 14: end-to-end. Writing the ledger to a path
    inside the canonical Phase H bundle dir (the same dir the harness
    uploads to and ``download_parent_bundle`` materializes locally)
    must be parseable by ``read_ledger`` so ``build_bundle`` can record
    ``candidate_ledger_entry_count`` on the manifest."""
    from genie_space_optimizer.optimization.candidate_ledger import (
        IterationCandidateLedgerEntry,
        write_ledger_entry,
        read_ledger,
    )

    bundle_dir = tmp_path / "gso_postmortem_bundle"
    bundle_dir.mkdir(parents=True)
    ledger_path = bundle_dir / "iteration_candidate_ledger.jsonl"

    entries = [
        IterationCandidateLedgerEntry(
            iteration=i,
            ag_id=f"ag-{i}",
            cluster_ids=("c1",),
            target_qids=("gs_026",),
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
        for i in range(1, 4)
    ]
    for e in entries:
        write_ledger_entry(e, path=str(ledger_path))

    parsed = read_ledger(str(ledger_path))
    assert len(parsed) == 3
    assert parsed[0].iteration == 1
    assert parsed[2].ag_id == "ag-3"
