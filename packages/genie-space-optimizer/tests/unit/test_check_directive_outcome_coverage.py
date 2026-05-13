"""Phase 3 Task 7 — check_directive_outcome_coverage invariant tests."""

from __future__ import annotations


def _make_ledger(ag_id: str, iteration: int, outcomes: dict[int, str]):
    """Build an AgDirectiveLedger with closed-vocabulary outcomes."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )

    return AgDirectiveLedger(
        ag_id=ag_id,
        iteration=iteration,
        directives_present=tuple(sorted(outcomes.keys())),
        outcomes_by_lever={
            int(k): DirectiveOutcomeCode(v) for k, v in outcomes.items()
        },
    )


def test_empty_iteration_passes() -> None:
    """No AGs ⇒ no obligation ⇒ no violation."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage({"action_groups": []})
    assert result.violated is False
    assert result.offending_ag_ids == ()


def test_ag_with_no_directives_passes() -> None:
    """AG present but no lever_directives ⇒ no obligation."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [{"id": "AG1", "lever_directives": {}}],
            "directive_outcomes_by_ag": {},
        }
    )
    assert result.violated is False


def test_full_coverage_passes() -> None:
    """Every directive key present in the ledger."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [
                {
                    "id": "AG1",
                    "lever_directives": {"5": {"target_qids": ["q1"]}},
                },
            ],
            "directive_outcomes_by_ag": {
                "AG1": _make_ledger(
                    "AG1", 1, {5: "proposal_emitted"}
                ),
            },
        }
    )
    assert result.violated is False


def test_missing_ledger_for_ag_fires() -> None:
    """AG has directives but no ledger entry at all — silent budget burn."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [
                {
                    "id": "AG1",
                    "lever_directives": {
                        "5": {"target_qids": ["q1"]},
                        "6": {"sql_expression": "foo"},
                    },
                },
            ],
            "directive_outcomes_by_ag": {},
        }
    )
    assert result.violated is True
    assert result.offending_ag_ids == ("AG1",)
    assert result.offending_lever_keys_by_ag == (("AG1", (5, 6)),)
    assert "silent budget burn" in result.message


def test_partial_coverage_fires_with_missing_keys() -> None:
    """AG has L5 + L6 directives but ledger only covers L5."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [
                {
                    "id": "AG2",
                    "lever_directives": {
                        "5": {"target_qids": ["q1"]},
                        "6": {"sql_expression": "foo"},
                    },
                },
            ],
            "directive_outcomes_by_ag": {
                "AG2": _make_ledger(
                    "AG2", 2, {5: "no_structural_candidate"}
                ),
            },
        }
    )
    assert result.violated is True
    assert result.offending_ag_ids == ("AG2",)
    assert result.offending_lever_keys_by_ag == (("AG2", (6,)),)


def test_multiple_ags_some_offending() -> None:
    """One AG fully covered, one AG missing — only the missing one is reported."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [
                {
                    "id": "AG1",
                    "lever_directives": {"5": {}},
                },
                {
                    "id": "AG2",
                    "lever_directives": {"5": {}, "6": {}},
                },
            ],
            "directive_outcomes_by_ag": {
                "AG1": _make_ledger("AG1", 1, {5: "proposal_emitted"}),
                "AG2": _make_ledger("AG2", 1, {5: "no_structural_candidate"}),
            },
        }
    )
    assert result.violated is True
    assert result.offending_ag_ids == ("AG2",)
    assert result.offending_lever_keys_by_ag == (("AG2", (6,)),)


def test_invariant_handles_missing_ag_id_gracefully() -> None:
    """An AG with no ``id`` key is skipped (defensive)."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [
                {"lever_directives": {"5": {}}},  # no id
                {"id": "AG2", "lever_directives": {"5": {}}},
            ],
            "directive_outcomes_by_ag": {
                "AG2": _make_ledger("AG2", 1, {5: "proposal_emitted"}),
            },
        }
    )
    assert result.violated is False
