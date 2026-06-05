"""Plan 9 Task 7 — structural_repair_gate rejects ABSENT + 0.0
repairability regardless of intended_patch_shape string.

Closes the 7Now fail-open bug where emitted_patch_shape=absent,
repairability_score=0.0, rca_root_cause=UNKNOWN was admitted
because intended_patch_shape was not literally 'structural'.
"""
from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
)
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


def test_rejects_absent_with_zero_repairability_when_intent_is_empty():
    """The 7Now bug: empty intent + ABSENT + 0.0 was admitted."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "rejected"
    assert verdict.repairability is not None
    # Legacy fail-open score stays 1.0; gate rejects on ABSENT + empty intent.
    assert verdict.repairability.value == 1.0


def test_intent_instruction_with_absent_emits_typed_retry_under_trial19():
    """Trial 19 B4 — when the LLM-first RCA flag is ON (default), a
    non-empty intent + ABSENT emission triggers ``retry_with_typed_feedback``
    instead of a terminal rejection. The retry feedback string names
    the intent verbatim so Stage 3 can re-emit a concrete patch.
    Pre-Trial-19 behavior (reject) is preserved when the flag is OFF.
    """
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="instruction",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "retry_with_typed_feedback"
    assert "instruction" in verdict.retry_feedback


def test_rejects_absent_with_zero_repairability_when_intent_is_structural():
    """Pre-Plan-9 rejection still fires."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "rejected"


def test_admits_non_absent_emitted_with_empty_intent_backward_compat():
    """Legacy RCA cards without Phase-2.3 metadata still fail open
    when emitted shape is non-ABSENT — Plan 9 only tightens the
    ABSENT + 0.0 combo."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",  # legacy
        emitted_patch_shape=EmittedPatchShape.INSTRUCTION,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted"


def test_admits_structural_emitted_with_structural_intent():
    """Happy path unchanged."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        narrow_replacement_available=True,
    )
    assert verdict.outcome == "admitted"
