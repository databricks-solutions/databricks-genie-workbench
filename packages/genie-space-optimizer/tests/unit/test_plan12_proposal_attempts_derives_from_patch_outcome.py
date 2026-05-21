"""Plan 12 — proposal_attempts derives from GSO_PATCH_OUTCOME_V1
markers (unique intent_ids) so it agrees with I22's coverage check."""


def test_proposal_attempts_counts_unique_patch_outcomes():
    from genie_space_optimizer.optimization.harness import (
        derive_proposal_attempts_from_patch_outcomes,
    )
    outcomes = [
        {"intent_id": "intent_001", "outcome_kind": "applied"},
        {"intent_id": "intent_002", "outcome_kind": "validator_rejected"},
        {"intent_id": "intent_003", "outcome_kind": "blast_radius_rejected"},
    ]
    assert derive_proposal_attempts_from_patch_outcomes(outcomes) == 3


def test_proposal_attempts_dedupes_duplicates():
    """If the same intent_id appears twice (a double-emit bug the
    emitter's idempotency guard should prevent, but worth checking
    here too), the deriver collapses to a single count — same as
    what I22 sees."""
    from genie_space_optimizer.optimization.harness import (
        derive_proposal_attempts_from_patch_outcomes,
    )
    outcomes = [
        {"intent_id": "intent_001", "outcome_kind": "applied"},
        {"intent_id": "intent_001", "outcome_kind": "applied"},
    ]
    assert derive_proposal_attempts_from_patch_outcomes(outcomes) == 1


def test_proposal_attempts_skips_empty_intent_ids():
    """Outcome records without an intent_id are skipped (they shouldn't
    happen — the emitter requires intent_id — but we don't want to
    inflate the count if they do)."""
    from genie_space_optimizer.optimization.harness import (
        derive_proposal_attempts_from_patch_outcomes,
    )
    outcomes = [
        {"intent_id": "intent_001", "outcome_kind": "applied"},
        {"intent_id": "", "outcome_kind": "validator_rejected"},
        {"outcome_kind": "applied"},  # missing intent_id entirely
    ]
    assert derive_proposal_attempts_from_patch_outcomes(outcomes) == 1


def test_proposal_attempts_zero_for_empty():
    from genie_space_optimizer.optimization.harness import (
        derive_proposal_attempts_from_patch_outcomes,
    )
    assert derive_proposal_attempts_from_patch_outcomes([]) == 0
    assert derive_proposal_attempts_from_patch_outcomes(None) == 0  # type: ignore[arg-type]
