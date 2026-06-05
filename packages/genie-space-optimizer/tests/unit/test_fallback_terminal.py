"""Phase 3 P3.2 — fallback terminal classification tests.

Pins:

  * The classifier prefers FALLBACK_NO_NEW_STRATEGY over the catch-
    all NO_ACTION_GROUP_EMITTED whenever the regenerator was
    suppressed by a non-empty signature history.
  * The acceptance-gate skip policy explicitly covers
    FALLBACK_NO_NEW_STRATEGY.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.fallback_terminal import (
    SKIPS_ACCEPTANCE_GATE,
    classify_zero_ag_terminal_reason,
    expanded_signature_union_size,
    fallback_marker_payload,
    should_skip_acceptance_gate,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def test_classifier_returns_no_action_group_emitted_on_clean_slate() -> None:
    reason = classify_zero_ag_terminal_reason(
        regenerator_returned_candidates=False,
        expanded_forbidden_count=0,
        insufficient_repair_signatures_count=0,
        prior_terminal_signatures_count=0,
    )
    assert reason is TerminalReason.NO_ACTION_GROUP_EMITTED


def test_classifier_returns_fallback_when_regenerator_suppressed_by_signatures() -> None:
    reason = classify_zero_ag_terminal_reason(
        regenerator_returned_candidates=False,
        expanded_forbidden_count=3,
        insufficient_repair_signatures_count=1,
        prior_terminal_signatures_count=2,
    )
    assert reason is TerminalReason.FALLBACK_NO_NEW_STRATEGY


def test_classifier_returns_fallback_with_only_insufficient_history() -> None:
    reason = classify_zero_ag_terminal_reason(
        regenerator_returned_candidates=False,
        expanded_forbidden_count=1,
        insufficient_repair_signatures_count=1,
        prior_terminal_signatures_count=0,
    )
    assert reason is TerminalReason.FALLBACK_NO_NEW_STRATEGY


def test_classifier_returns_fallback_with_only_prior_terminal_history() -> None:
    reason = classify_zero_ag_terminal_reason(
        regenerator_returned_candidates=False,
        expanded_forbidden_count=1,
        insufficient_repair_signatures_count=0,
        prior_terminal_signatures_count=1,
    )
    assert reason is TerminalReason.FALLBACK_NO_NEW_STRATEGY


def test_classifier_returns_no_action_group_emitted_when_regenerator_produced_candidates() -> None:
    # If the regenerator did emit candidates, the fallback path is
    # NOT the cause — the candidates must have been zeroed by some
    # downstream filter, which the harness classifies differently.
    reason = classify_zero_ag_terminal_reason(
        regenerator_returned_candidates=True,
        expanded_forbidden_count=4,
        insufficient_repair_signatures_count=2,
        prior_terminal_signatures_count=2,
    )
    assert reason is TerminalReason.NO_ACTION_GROUP_EMITTED


def test_should_skip_acceptance_gate_for_fallback() -> None:
    assert (
        should_skip_acceptance_gate(TerminalReason.FALLBACK_NO_NEW_STRATEGY)
        is True
    )


def test_should_skip_acceptance_gate_returns_false_for_no_action_group_emitted() -> None:
    # Pre-existing path is handled by the legacy harness skip;
    # the new predicate only covers the fallback case.
    assert (
        should_skip_acceptance_gate(TerminalReason.NO_ACTION_GROUP_EMITTED)
        is False
    )


def test_skips_acceptance_gate_constant_pins_policy() -> None:
    assert SKIPS_ACCEPTANCE_GATE == (TerminalReason.FALLBACK_NO_NEW_STRATEGY,)


def test_fallback_marker_payload_has_canonical_keys() -> None:
    payload = fallback_marker_payload(
        expanded_forbidden_count=5,
        insufficient_repair_signatures_count=2,
        prior_terminal_signatures_count=3,
    )
    assert payload == {
        "expanded_forbidden_count": 5,
        "insufficient_signatures_count": 2,
        "prior_terminal_signatures_count": 3,
    }


def test_expanded_signature_union_size_handles_overlap() -> None:
    assert (
        expanded_signature_union_size(
            forbidden_signatures=["a", "b"],
            insufficient_repair_signatures=["b", "c"],
        )
        == 3
    )


def test_expanded_signature_union_size_handles_none() -> None:
    assert (
        expanded_signature_union_size(
            forbidden_signatures=None,
            insufficient_repair_signatures=None,
        )
        == 0
    )
