"""Static-source + behavioural wiring tests for Phase B observability.

These tests pin the harness wiring that the postmortem demonstrated was
the load-bearing surface — when wiring goes silent, the analyzer can't
see Phase B at all. Static-source tests catch refactor-induced drift;
the dict-shape tests catch manifest-shape regressions.

Plan: ``docs/2026-05-02-unified-trace-and-operator-transcript-plan.md``
postmortem follow-up + ``~/.claude/plans/groovy-gathering-fountain.md``
Task 7.
"""

from __future__ import annotations

from pathlib import Path


def _read_harness_source() -> str:
    root = Path(__file__).resolve().parents[2]
    return (
        root
        / "src"
        / "genie_space_optimizer"
        / "optimization"
        / "harness.py"
    ).read_text()


# ---------------------------------------------------------------------------
# Static-source: 5 producer call signatures
# ---------------------------------------------------------------------------


def test_harness_wires_eval_classification_records_producer() -> None:
    src = _read_harness_source()
    assert "from genie_space_optimizer.optimization.decision_emitters import (" in src
    assert "eval_classification_records as _eval_classification_records" in src
    assert "_eval_classification_records(" in src


def test_harness_wires_cluster_records_producer() -> None:
    src = _read_harness_source()
    assert "cluster_records as _cluster_records" in src
    assert "_cluster_records(" in src


def test_harness_wires_strategist_ag_records_producer() -> None:
    src = _read_harness_source()
    assert "strategist_ag_records as _strategist_ag_records" in src
    assert "_strategist_ag_records(" in src


def test_harness_wires_ag_outcome_record_producer() -> None:
    """The closure ``_phase_b_emit_ag_outcome_record`` must be invoked at
    every ag_outcomes capture site (5 sites: dead_on_arrival,
    pre_ag_snapshot_failed, no_applied_patches, rolled_back, accepted)."""
    src = _read_harness_source()
    assert "_phase_b_emit_ag_outcome_record" in src
    # Pin all 5 outcome strings flow through the closure.
    for outcome in (
        "skipped_dead_on_arrival",
        "skipped_pre_ag_snapshot_failed",
        "skipped_no_applied_patches",
        "rolled_back",
    ):
        assert (
            f'_phase_b_emit_ag_outcome_record(ag, "{outcome}")' in src
        ), f"missing ACCEPTANCE_DECIDED wiring for outcome={outcome}"
    # The "accepted"/"accepted_with_regression_debt" path uses a variable
    # ``_outcome_for_journey`` rather than a literal — pin that anchor.
    assert "_phase_b_emit_ag_outcome_record(ag, _outcome_for_journey)" in src


def test_harness_wires_post_eval_resolution_producer() -> None:
    src = _read_harness_source()
    assert "post_eval_resolution_records as _post_eval_resolution_records" in src
    assert "_post_eval_resolution_records(" in src


# ---------------------------------------------------------------------------
# Static-source: function-scope state + contract version + manifest
# ---------------------------------------------------------------------------


def test_harness_initializes_phase_b_function_scope_state() -> None:
    src = _read_harness_source()
    for name in (
        "_phase_b_iter_record_counts",
        "_phase_b_iter_violation_counts",
        "_phase_b_no_records_iterations",
        "_phase_b_artifact_paths",
        "_phase_b_producer_exceptions",
        "_phase_b_target_qids_missing_count",
        "_phase_b_total_violations",
        "_PHASE_B_CONTRACT_VERSION",
    ):
        assert name in src, f"missing function-scope state: {name}"


def test_harness_sets_phase_b_contract_version_mlflow_tag() -> None:
    src = _read_harness_source()
    assert 'set_tag(\n                "phase_b_contract_version"' in src or (
        "phase_b_contract_version" in src and "set_tag" in src
    )


def test_harness_emits_no_records_marker_via_run_analysis_contract() -> None:
    src = _read_harness_source()
    assert "phase_b_no_records_marker as _phase_b_no_records_marker" in src
    assert "_phase_b_no_records_marker(" in src


def test_harness_emits_end_marker_before_return() -> None:
    src = _read_harness_source()
    assert "phase_b_end_marker as _phase_b_end_marker" in src
    assert "_phase_b_end_marker(" in src


def test_harness_builds_phase_b_manifest_at_return() -> None:
    """The return dict must contain a ``phase_b`` key with the full
    manifest schema. Fields aligned with
    ``run_lever_loop.py:548-563`` allowlist + the postmortem analyzer's
    expected shape."""
    src = _read_harness_source()
    assert '"phase_b": {' in src
    for field in (
        '"contract_version"',
        '"decision_records_total"',
        '"iter_record_counts"',
        '"iter_violation_counts"',
        '"no_records_iterations"',
        '"artifact_paths"',
        '"producer_exceptions"',
        '"target_qids_missing_count"',
        '"total_violations"',
    ):
        assert field in src, f"manifest missing field: {field}"


# ---------------------------------------------------------------------------
# Static-source: skipped_pre_ag_snapshot_failed AG outcome capture
# ---------------------------------------------------------------------------


def test_harness_captures_pre_ag_snapshot_failed_outcome() -> None:
    """Task 4.5: the path that previously discarded the AG without
    writing to ag_outcomes now records ``skipped_pre_ag_snapshot_failed``
    so the ACCEPTANCE_DECIDED producer (Task 6) sees it."""
    src = _read_harness_source()
    assert (
        '_current_iter_inputs["ag_outcomes"][str(ag_id)] = (\n'
        '                    "skipped_pre_ag_snapshot_failed"\n'
        "                )"
    ) in src
