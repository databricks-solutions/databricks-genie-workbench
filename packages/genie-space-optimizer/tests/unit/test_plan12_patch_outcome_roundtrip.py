"""Plan 12 — PatchOutcome roundtrip and terminal-state vocabulary tests."""
import pytest


def test_patch_outcome_applied_roundtrip():
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcome,
        PatchOutcomeKind,
    )
    p = PatchOutcome(
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.APPLIED,
        terminal_reason="",
        validator_errors=(),
        collateral_qids=(),
        narrow_replacement_attempted=False,
        narrow_outcome="",
        applied_patch_id="ap_001",
    )
    assert PatchOutcome.from_json(p.to_json()) == p


def test_patch_outcome_validator_rejected_carries_errors():
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcome,
        PatchOutcomeKind,
    )
    p = PatchOutcome(
        intent_id="intent_001",
        outcome_kind=PatchOutcomeKind.VALIDATOR_REJECTED,
        terminal_reason="patch_body_missing_field",
        validator_errors=("patch_body.example_sql required",),
        collateral_qids=(),
        narrow_replacement_attempted=False,
        narrow_outcome="",
        applied_patch_id="",
    )
    assert p.outcome_kind == PatchOutcomeKind.VALIDATOR_REJECTED
    assert PatchOutcome.from_json(p.to_json()) == p


def test_patch_outcome_blast_radius_rejected_records_narrow():
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcome,
        PatchOutcomeKind,
    )
    p = PatchOutcome(
        intent_id="intent_002",
        outcome_kind=PatchOutcomeKind.BLAST_RADIUS_REJECTED,
        terminal_reason="blast_radius_rejected",
        validator_errors=(),
        collateral_qids=("gs_003", "gs_005"),
        narrow_replacement_attempted=True,
        narrow_outcome="exhausted",
        applied_patch_id="",
    )
    assert PatchOutcome.from_json(p.to_json()) == p


def test_patch_outcome_contract_failed_carries_reason():
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcome,
        PatchOutcomeKind,
    )
    p = PatchOutcome(
        intent_id="intent_003",
        outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
        terminal_reason="missing_required_field_target_object",
        validator_errors=(),
        collateral_qids=(),
        narrow_replacement_attempted=False,
        narrow_outcome="",
        applied_patch_id="",
    )
    assert PatchOutcome.from_json(p.to_json()) == p


def test_outcome_kind_values_locked():
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    assert set(PatchOutcomeKind) == {
        PatchOutcomeKind.APPLIED,
        PatchOutcomeKind.VALIDATOR_REJECTED,
        PatchOutcomeKind.BLAST_RADIUS_REJECTED,
        PatchOutcomeKind.CONTRACT_FAILED,
    }
