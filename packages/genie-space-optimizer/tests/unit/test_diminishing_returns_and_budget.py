"""Regression tests for Phase C3 gating semantics.

``_diminishing_returns`` now only counts CONTENT_REGRESSION entries,
so two INFRA_FAILURE rollbacks in a row can't artificially terminate
the loop. CONTENT_REGRESSION rollbacks behave as before — two of them
trip the gate.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
    _diminishing_returns,
)


def _rb(rollback_reason: str, **overrides) -> dict:
    return _build_reflection_entry(
        iteration=overrides.pop("iteration", 1),
        ag_id="AG",
        accepted=False,
        levers=[5],
        target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason=rollback_reason,
        patches=[],
        root_cause="missing_filter",
        blame_set=[],
        source_cluster_ids=["C001"],
        **overrides,
    )


def test_zero_grounded_patch_bundle_is_not_evaluable():
    from genie_space_optimizer.optimization.harness import (
        _should_skip_eval_for_patch_bundle,
    )

    decision = _should_skip_eval_for_patch_bundle(
        patches=[],
        apply_log=None,
        stage="post_grounding",
    )

    assert decision.skip is True
    assert decision.reason_code == "no_grounded_patches"
    assert "grounding" in decision.reason_detail


def test_zero_applied_patch_bundle_is_not_evaluable():
    from genie_space_optimizer.optimization.harness import (
        _should_skip_eval_for_patch_bundle,
    )

    decision = _should_skip_eval_for_patch_bundle(
        patches=[{"type": "update_column_description", "column": "avg_txn_day"}],
        apply_log={"applied": [], "patch_deployed": False},
        stage="post_apply",
    )

    assert decision.skip is True
    assert decision.reason_code == "no_applied_patches"


def test_diminishing_returns_ignores_infra_rollbacks() -> None:
    buf = [
        _rb("patch_deploy_failed: 500 Internal Server Error", iteration=1),
        _rb("patch_deploy_failed: Connection timed out", iteration=2),
    ]
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False


def test_diminishing_returns_ignores_schema_rollbacks() -> None:
    buf = [
        _rb(
            "patch_deploy_failed: Invalid serialized_space: "
            "Cannot find field: foo",
            iteration=1,
        ),
        _rb(
            "patch_deploy_failed: Invalid serialized_space: "
            "Cannot find field: bar",
            iteration=2,
        ),
    ]
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False


def test_diminishing_returns_triggers_on_two_content_regressions() -> None:
    buf = [
        _rb("slice_gate: result_correctness", iteration=1),
        _rb("full_eval: schema_accuracy", iteration=2),
    ]
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True


def test_diminishing_returns_resets_on_accepted_content() -> None:
    """An accepted iteration with a positive accuracy delta should stop the
    gate from firing on subsequent rollbacks."""
    accepted = _build_reflection_entry(
        iteration=1,
        ag_id="AG",
        accepted=True,
        levers=[5],
        target_objects=[],
        prev_scores={"result_correctness": 90.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason=None,
        patches=[],
        root_cause="missing_filter",
        blame_set=[],
        source_cluster_ids=["C001"],
    )
    buf = [accepted, _rb("slice_gate: result_correctness", iteration=2)]
    # Only one content signal in the window — not enough to trip the
    # two-iteration lookback.
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False


def test_diminishing_returns_uses_acceptance_delta_not_mean_score_delta() -> None:
    """Plateauing should follow the same post-arbiter acceptance delta
    used by the accept gate, not the average movement across judge scores."""
    accepted = _build_reflection_entry(
        iteration=1,
        ag_id="AG",
        accepted=True,
        levers=[5],
        target_objects=[],
        prev_scores={"result_correctness": 90.0, "schema_accuracy": 70.0},
        new_scores={"result_correctness": 90.5, "schema_accuracy": 99.0},
        rollback_reason=None,
        patches=[],
        root_cause="missing_filter",
        blame_set=[],
        source_cluster_ids=["C001"],
        acceptance_delta_pp=0.5,
    )
    buf = [accepted, _rb("slice_gate: result_correctness", iteration=2)]

    assert accepted["accuracy_delta"] > 2.0
    assert accepted["acceptance_delta_pp"] == 0.5
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True


def test_diminishing_returns_skips_mixed_class_entries() -> None:
    """Mixed buffer: infra, then content. ``_diminishing_returns`` with
    lookback=2 and content-only filter should see only the single
    content entry and return False (not enough evidence yet)."""
    buf = [
        _rb("patch_deploy_failed: Network error", iteration=1),
        _rb("full_eval: schema_accuracy", iteration=2),
    ]
    assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False


def test_harness_patch_cap_uses_causal_selector_not_diversity_first_logic() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    # Task 5 — the harness migrated to the target-aware variant which
    # preserves one patch per AG target QID before global causal rank.
    assert "select_target_aware_causal_patch_cap" in src
    assert "Diversity-aware cap: preserve one patch per distinct lever" not in src
    assert "PATCH CAP APPLIED (causal-first)" in src


def test_harness_suppresses_legacy_plateau_when_rca_terminal_is_patchable() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "legacy_plateau_allows_stop" in src
    assert "LEGACY PLATEAU SUPPRESSED" in src


def test_harness_persists_patch_cap_trace_rows_and_prints_inventory() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "patch_cap_decision_rows" in src
    assert "format_patch_inventory" in src
    assert "PROPOSAL INVENTORY" in src
