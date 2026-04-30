from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    decide_quarantine_continuation,
)


def test_stop_when_patchable_hard_qids_are_quarantined_and_no_hard_clusters_remain() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q001", "q009", "q021"},
        unresolved_patchable_qids={"q001", "q009", "q021"},
        hard_cluster_count_after_prune=0,
        soft_cluster_count_after_prune=2,
    )
    assert decision["action"] == "stop_for_human_review"
    assert decision["reason"] == "quarantined_patchable_hard_failures"
    assert decision["blocking_qids"] == ["q001", "q009", "q021"]


def test_continue_when_only_soft_quarantine_and_hard_clusters_remain() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q004"},
        unresolved_patchable_qids=set(),
        hard_cluster_count_after_prune=1,
        soft_cluster_count_after_prune=3,
    )
    assert decision["action"] == "continue"
    assert decision["reason"] == "hard_clusters_remain"


def test_diagnostic_lane_when_hard_clusters_remain_but_patchable_qids_were_removed() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q021"},
        unresolved_patchable_qids={"q021"},
        hard_cluster_count_after_prune=2,
        soft_cluster_count_after_prune=1,
    )
    assert decision["action"] == "diagnostic_lane"
    assert decision["blocking_qids"] == ["q021"]


def test_harness_logs_target_fixed_disagreement_shape() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "def _log_target_fixed_disagreement(" in source
    assert "CONTROL PLANE TARGET-FIXED DISAGREEMENT" in source
    assert "target_fixed_qids" in source
    assert "_log_target_fixed_disagreement(" in inspect.getsource(harness._run_gate_checks)


def test_harness_tracks_unverified_rollback_before_quarantine_mutation() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    assert "_rollback_state_trusted_for_quarantine = True" in source
    assert "_rollback_state_trusted_for_quarantine = False" in source
    assert "if not _rollback_state_trusted_for_quarantine:" in source
    assert "Skipping convergence quarantine because live state is untrusted" in source


def test_harness_logs_strategist_coverage_gap_diagnostic_shape() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness)

    assert "def _log_strategist_coverage_gap(" in source
    assert "STRATEGIST COVERAGE GAP" in source
    assert "uncovered_cluster_ids" in source
    assert "rca_cards_present" in source
    assert "_log_strategist_coverage_gap(" in source


def test_soft_skip_does_not_persist_into_correction_state_quarantined_qids() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    assert "_iter_local_skip_qids = _quarantine_qids | _soft_skip_qids" in source
    assert "_correction_state[\"quarantined_qids\"] |= _newly_quarantined" in source

    iter_local_idx = source.index("_iter_local_skip_qids = _quarantine_qids | _soft_skip_qids")
    persist_idx = source.index("_correction_state[\"quarantined_qids\"] |= _newly_quarantined")
    assert iter_local_idx < persist_idx

    persist_block = source[persist_idx - 800 : persist_idx]
    # The persist site uses ``_quarantine_qids`` only; the iteration-local
    # skip set is computed elsewhere so soft-skip qids cannot leak in.
    assert "_quarantine_qids - _correction_state[\"quarantined_qids\"]" in persist_block
    assert "_iter_local_skip_qids = " not in persist_block


def test_soft_skip_logs_iteration_local_label() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    source = inspect.getsource(harness._run_lever_loop)

    assert "T4.3 CONVERGENCE QUARANTINE" in source
    assert "iteration-local; not persisted" in source
