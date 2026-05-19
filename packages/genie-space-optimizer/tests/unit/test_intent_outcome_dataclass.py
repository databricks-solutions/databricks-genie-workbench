"""Plan 1 Task 5 — IntentOutcome contract.

IntentOutcome captures the acceptance verdict for a single
RepairIntent after the acceptance gate runs. It is carried on
AgOutcomeRecord (Task 10) so Plan 4 learning can read per-intent
results without re-traversing the proposal-by-AG dicts.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from genie_space_optimizer.optimization.repair_intent import IntentOutcome
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def _sample() -> IntentOutcome:
    return IntentOutcome(
        intent_id="i1",
        ag_id="AG_X",
        outcome="accepted",
        applied_signature="sig_abc",
        applied_at_iter=2,
        rollback_reason=None,
    )


def test_intent_outcome_mixes_jsonroundtrip() -> None:
    assert issubclass(IntentOutcome, JsonRoundTrip)


def test_intent_outcome_is_frozen() -> None:
    out = _sample()
    with pytest.raises(FrozenInstanceError):
        out.outcome = "rolled_back"  # type: ignore[misc]


def test_round_trip_preserves_fields() -> None:
    out = _sample()
    restored = IntentOutcome.from_json(out.to_json())
    assert restored == out


def test_outcome_string_is_valid_acceptance_vocab() -> None:
    """Outcome strings must match the AgOutcomeRecord.outcome closed
    vocabulary so postmortem readers can join on this field
    directly."""
    valid = {
        "accepted",
        "accepted_with_regression_debt",
        "accepted_with_attribution_drift",
        "accepted_with_partial_harvest_debt",
        "accepted_with_attribution_drift_and_debt",
        "rolled_back",
        "skipped_no_proposal",
        "skipped_no_intent",
    }
    for outcome in valid:
        IntentOutcome(
            intent_id="i1",
            ag_id="AG_X",
            outcome=outcome,
            applied_signature=None,
            applied_at_iter=None,
            rollback_reason=None,
        )


def test_round_trip_handles_optional_none() -> None:
    out = IntentOutcome(
        intent_id="i1",
        ag_id="AG_X",
        outcome="skipped_no_proposal",
        applied_signature=None,
        applied_at_iter=None,
        rollback_reason=None,
    )
    restored = IntentOutcome.from_json(out.to_json())
    assert restored == out
