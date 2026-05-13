"""Phase 3 follow-up — reconcile_outcome_from_records helper unit tests."""

from __future__ import annotations

import pytest


def _l6_decline_record(*, ag_id: str = "AG2", iteration: int = 2) -> dict:
    """The exact record shape _emit_force_l6_outcome appends to
    iter_inputs["decision_records"] (post .to_dict())."""
    return {
        "decision_type": "proposal_failure_decided",
        "reason_code": "lever6_force_llm_declined",
        "ag_id": ag_id,
        "iteration": iteration,
        "cluster_id": "c-1",
        "root_cause": "missing_filter",
        "evidence_refs": ["signature:c1_missing_filter"],
        "metrics": {"cached": False},
    }


def test_passes_through_proposal_emitted_unchanged() -> None:
    """If the classifier already saw proposals, no reconciliation needed."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.PROPOSAL_EMITTED,
        lever_key=6,
        ag_id="AG1",
        iteration=1,
        decision_records=[_l6_decline_record(ag_id="AG1", iteration=1)],
    )
    assert result == DirectiveOutcomeCode.PROPOSAL_EMITTED


def test_passes_through_force_llm_declined_unchanged() -> None:
    """If the classifier already returned FORCE_LLM_DECLINED (hypothetical
    direct-attribution path), reconciliation is a no-op."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.FORCE_LLM_DECLINED,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=[_l6_decline_record()],
    )
    assert result == DirectiveOutcomeCode.FORCE_LLM_DECLINED


def test_upgrades_no_structural_candidate_to_force_llm_declined_for_lever6() -> None:
    """The 2314bb2c AG2 iter 2 shape — classifier said NO_STRUCTURAL_CANDIDATE
    because force_llm_declined was conservative-zero False. The records
    contain the real lever6_force_llm_declined signal. Reconciliation
    must upgrade."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=[_l6_decline_record(ag_id="AG2", iteration=2)],
    )
    assert result == DirectiveOutcomeCode.FORCE_LLM_DECLINED


def test_does_not_upgrade_lever5_no_structural_candidate() -> None:
    """L6 force-LLM-declined is L6-only. An L5 NO_STRUCTURAL_CANDIDATE
    must not be reclassified by an L6 decline record."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=5,
        ag_id="AG2",
        iteration=2,
        decision_records=[_l6_decline_record(ag_id="AG2", iteration=2)],
    )
    assert result == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_does_not_match_record_for_different_ag() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        # Record is for AG1, not AG2 — must not match.
        decision_records=[_l6_decline_record(ag_id="AG1", iteration=2)],
    )
    assert result == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_does_not_match_record_for_different_iteration() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        # Record is for iter 1 — must not bleed into iter 2 attribution.
        decision_records=[_l6_decline_record(ag_id="AG2", iteration=1)],
    )
    assert result == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_empty_decision_records_passes_through() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=[],
    )
    assert result == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_none_decision_records_passes_through() -> None:
    """Defensive — None must not crash the helper."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=None,
    )
    assert result == DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE


def test_garbage_records_dropped_silently() -> None:
    """Strings, ints, None entries in the records list must not raise."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=[
            "not a record",
            42,
            None,
            _l6_decline_record(ag_id="AG2", iteration=2),
        ],
    )
    assert result == DirectiveOutcomeCode.FORCE_LLM_DECLINED


def test_record_iteration_is_string_or_int() -> None:
    """The harness sometimes stores iteration as int, sometimes as str
    (e.g. through JSON round-trips). Both must match."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        reconcile_outcome_from_records,
    )

    record_str_iter = {
        "decision_type": "proposal_failure_decided",
        "reason_code": "lever6_force_llm_declined",
        "ag_id": "AG2",
        "iteration": "2",  # string, not int
    }
    result = reconcile_outcome_from_records(
        classifier_outcome=DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        lever_key=6,
        ag_id="AG2",
        iteration=2,
        decision_records=[record_str_iter],
    )
    assert result == DirectiveOutcomeCode.FORCE_LLM_DECLINED
