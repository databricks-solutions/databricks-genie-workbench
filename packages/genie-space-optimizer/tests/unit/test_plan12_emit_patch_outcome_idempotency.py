"""Plan 12 — emit_patch_outcome must be idempotent per (run_id, iteration,
ag_id, intent_id). Repeated calls within the same scope emit at most
once."""
import json


def test_emit_once_per_scope(capsys):
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()

    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id="ap_001",
    )

    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id="ap_001",
    )

    out = capsys.readouterr().out
    matches = [
        ln for ln in out.splitlines()
        if ln.startswith("GSO_PATCH_OUTCOME_V1 ")
    ]
    assert len(matches) == 1, (
        f"Expected exactly one outcome marker, got {len(matches)}: {matches}"
    )


def test_emit_separately_for_different_intent_ids(capsys):
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()

    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_A",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id="ap_001",
    )
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_B",
        outcome_kind=PatchOutcomeKind.VALIDATOR_REJECTED,
        terminal_reason="patch_body_missing_field",
        validator_errors=("missing example_sql",),
    )

    out = capsys.readouterr().out
    matches = [
        ln for ln in out.splitlines()
        if ln.startswith("GSO_PATCH_OUTCOME_V1 ")
    ]
    assert len(matches) == 2


def test_reset_clears_idempotency_cache(capsys):
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_A",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id="ap_001",
    )
    reset_patch_outcome_emitter()
    emit_patch_outcome(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="H001",
        cluster_id="C001",
        intent_id="intent_A",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        applied_patch_id="ap_001",
    )

    out = capsys.readouterr().out
    matches = [
        ln for ln in out.splitlines()
        if ln.startswith("GSO_PATCH_OUTCOME_V1 ")
    ]
    assert len(matches) == 2
