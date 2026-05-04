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
    """Phase F+H A2: the harness routes STRATEGIST_AG_EMITTED emission
    through the F4 stage module (stages.action_groups.select), which
    internally calls strategist_ag_records and emits via
    ctx.decision_emit. The pre-A2 inline alias + call were deleted by
    A2; the producer is still wired, just one indirection deeper."""
    src = _read_harness_source()
    # F4 stage call is present at the post-strategist site.
    assert "from genie_space_optimizer.optimization.stages import" in src
    assert "action_groups as _ags_stage" in src
    assert "_ags_stage.select(" in src


def test_harness_wires_ag_outcome_record_producer() -> None:
    """Phase F+H A5 v2.1 — selective dedup. The closure
    ``_phase_b_emit_ag_outcome_record`` STAYS inline for the 3 pre-gate
    outcome strings (``skipped_*``) because F8.decide() iterates AGs
    that REACHED the gate; pre-gate `continue` paths bypass the gate
    entirely and F8 cannot reproduce them. The 2 post-gate callsites
    (``rolled_back``, ``accepted`` / ``accepted_with_regression_debt``)
    are deleted in v2.1 — F8 emits ACCEPTANCE_DECIDED via
    ``stages.acceptance.decide`` at the post-gate anchor instead.
    """
    src = _read_harness_source()
    assert "_phase_b_emit_ag_outcome_record" in src
    # The 3 pre-gate outcome strings still flow through the closure.
    for outcome in (
        "skipped_dead_on_arrival",
        "skipped_pre_ag_snapshot_failed",
        "skipped_no_applied_patches",
    ):
        assert (
            f'_phase_b_emit_ag_outcome_record(ag, "{outcome}")' in src
        ), f"missing pre-gate ACCEPTANCE_DECIDED wiring for outcome={outcome}"
    # The 2 post-gate sites must be GONE (deleted by A5 v2.1 selective
    # dedup; F8 emits via stages.acceptance.decide instead).
    assert (
        '_phase_b_emit_ag_outcome_record(ag, "rolled_back")' not in src
    ), "post-gate rolled_back closure call must be deleted by A5 v2.1"
    assert (
        "_phase_b_emit_ag_outcome_record(ag, _outcome_for_journey)" not in src
    ), "post-gate accepted closure call must be deleted by A5 v2.1"
    # F8 stage call is wired at the post-gate anchor.
    assert "from genie_space_optimizer.optimization.stages import" in src
    assert "acceptance as _accept_stage" in src
    assert "_accept_stage.decide(" in src


def test_harness_wires_post_eval_resolution_producer() -> None:
    """Phase F+H A5 v2.1 — the harness-inline post_eval_resolution_records
    block is DELETED. F8.decide() emits QID_RESOLUTION via
    ``stages.acceptance.decide`` (which calls
    ``post_eval_resolution_records`` internally). Pin the F8 stage
    call as the new active producer wiring."""
    src = _read_harness_source()
    # Pre-A5: the inline import block was active. Post-A5 v2.1: it is
    # deleted; only doc-comment references remain.
    assert (
        "post_eval_resolution_records as _post_eval_resolution_records"
        not in src
    ), "harness inline post_eval_resolution_records import must be deleted by A5 v2.1"
    # F8 stage call wired at the post-gate anchor; QID_RESOLUTION emits
    # from stages/acceptance.py:230-240 transitively.
    assert "acceptance as _accept_stage" in src
    assert "_accept_stage.decide(" in src


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
