"""Trial 19 B4 — structural_repair_gate retry_with_typed_feedback verdict.

When the LLM emits a concrete free-text ``intended_patch_shape`` but
the emitted patch is ABSENT (no usable repair), the gate must return
``retry_with_typed_feedback`` with a feedback string that names the
intent. This is the path that closes the Trial 18 postmortem failure
where structural intents fell into the ABSENT + 0.0-repairability
trap and burned an iteration on rejection rather than retrying.

Flag OFF restores pre-Trial-19 reject behavior for byte-stable replay.
"""
import os

from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
)
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


def _set_flag(monkeypatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv("GSO_TRIAL19_LLM_FIRST_RCA", raising=False)
    else:
        monkeypatch.setenv("GSO_TRIAL19_LLM_FIRST_RCA", value)
    monkeypatch.delenv("GSO_TRIAL19_ENFORCE", raising=False)


def test_typed_retry_on_named_intent_and_absent_emission(monkeypatch):
    _set_flag(monkeypatch, "1")
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="enforce_explicit_top_n_cardinality",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "retry_with_typed_feedback"
    assert verdict.terminal_reason == ""
    assert (
        "enforce_explicit_top_n_cardinality" in verdict.retry_feedback
    )


def test_typed_retry_carries_concrete_examples(monkeypatch):
    _set_flag(monkeypatch, "1")
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="remove_unrequested_defensive_filter",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
    )
    assert verdict.outcome == "retry_with_typed_feedback"
    assert "add_sql_snippet_filter" in verdict.retry_feedback


def test_empty_intent_falls_back_to_reject_under_trial19(monkeypatch):
    """Empty intent + ABSENT → reject (Plan 9 rule preserved)."""
    _set_flag(monkeypatch, "1")
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
    )
    assert verdict.outcome == "rejected"


def test_legacy_structural_intent_falls_back_to_reject_under_trial19(
    monkeypatch,
):
    """The pre-Trial-19 sentinel ``"structural"`` keeps legacy reject."""
    _set_flag(monkeypatch, "1")
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
    )
    assert verdict.outcome == "rejected"


def test_flag_off_restores_pre_trial19_reject_behavior(monkeypatch):
    """Trial 19 OFF — named intent + ABSENT goes back to legacy reject."""
    _set_flag(monkeypatch, "0")
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="enforce_explicit_top_n_cardinality",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
    )
    assert verdict.outcome == "rejected"
    assert verdict.retry_feedback == ""


def test_non_absent_emission_admits_regardless_of_flag(monkeypatch):
    """A concrete non-ABSENT emission admits whether the flag is ON or OFF."""
    for flag in ("1", "0"):
        _set_flag(monkeypatch, flag)
        verdict = enforce_structural_repair_shape(
            intended_patch_shape="enforce_explicit_top_n_cardinality",
            emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        )
        assert verdict.outcome == "admitted"
