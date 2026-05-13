"""Tests for Section E Tier 4 trial-outcome bookkeeping."""

from __future__ import annotations

from genie_space_optimizer.optimization.archetype_learning import (
    ProvisionalArchetype,
)
from genie_space_optimizer.optimization.archetype_learning_state import (
    get_state,
    reset_state,
)
from genie_space_optimizer.optimization.rca import RcaKind


def _make_provisional() -> ProvisionalArchetype:
    return ProvisionalArchetype(
        name="x_provisional",
        applicable_rca_kinds=frozenset({RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING}),
        required_grounding_tokens=frozenset({"snack_brand"}),
        evidence_predicates=frozenset(),
        default_priority_step="repair_kit",
        expected_causal_effect_template="x",
        rationale="x",
        provenance="provisional_archetype",
        lifecycle_state="provisional",
        signature_hash="sigX",
        synthesis_iteration=1,
    )


def test_outcome_strict_win_promotes_to_confirmed_in_run() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        record_provisional_archetype_trial_outcome,
    )

    reset_state("run_T4a")
    pa = _make_provisional()
    get_state("run_T4a").provisional_archetypes.append(pa)
    promoted = record_provisional_archetype_trial_outcome(
        run_id="run_T4a",
        signature_hash="sigX",
        iteration=2,
        acceptance_tier="strict_win",
    )
    assert promoted is not None
    assert promoted.lifecycle_state == "confirmed_in_run"
    assert promoted.last_outcome == "strict_win"
    assert 2 in promoted.trial_iterations


def test_outcome_net_win_with_debt_promotes_to_confirmed_in_run() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        record_provisional_archetype_trial_outcome,
    )

    reset_state("run_T4b")
    pa = _make_provisional()
    get_state("run_T4b").provisional_archetypes.append(pa)
    promoted = record_provisional_archetype_trial_outcome(
        run_id="run_T4b",
        signature_hash="sigX",
        iteration=2,
        acceptance_tier="net_win_with_debt",
    )
    assert promoted is not None
    assert promoted.lifecycle_state == "confirmed_in_run"


def test_outcome_loss_marks_failed_in_run() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        record_provisional_archetype_trial_outcome,
    )

    reset_state("run_T4c")
    pa = _make_provisional()
    get_state("run_T4c").provisional_archetypes.append(pa)
    out = record_provisional_archetype_trial_outcome(
        run_id="run_T4c",
        signature_hash="sigX",
        iteration=2,
        acceptance_tier="loss",
    )
    assert out is not None
    assert out.lifecycle_state == "failed_in_run"


def test_outcome_diagnostic_hold_keeps_provisional_for_retrial() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        record_provisional_archetype_trial_outcome,
    )

    reset_state("run_T4d")
    pa = _make_provisional()
    get_state("run_T4d").provisional_archetypes.append(pa)
    out = record_provisional_archetype_trial_outcome(
        run_id="run_T4d",
        signature_hash="sigX",
        iteration=2,
        acceptance_tier="diagnostic_hold",
    )
    assert out is not None
    assert out.lifecycle_state == "provisional"
    assert 2 in out.trial_iterations


def test_outcome_returns_none_when_signature_unknown() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        record_provisional_archetype_trial_outcome,
    )

    reset_state("run_T4e")
    out = record_provisional_archetype_trial_outcome(
        run_id="run_T4e",
        signature_hash="sigUnknown",
        iteration=1,
        acceptance_tier="strict_win",
    )
    assert out is None
