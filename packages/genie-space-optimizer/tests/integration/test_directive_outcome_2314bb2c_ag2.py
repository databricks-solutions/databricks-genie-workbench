"""Phase 3 Task 10 — 2314bb2c AG2 replay integration test.

Replays the AG2 iter 2-5 shape against the directive_outcome_coverage
invariant + marker formatter, asserting:

1. With a fully-populated ledger, coverage passes (no marker emitted).
2. With a missing L6 entry, coverage fails and the marker has the
   expected offending payload shape.

The fixture mirrors the postmortem at
docs/runid_analysis/2314bb2c-95a1-4d60-8226-09e5155aee2a/postmortem.md.
"""

from __future__ import annotations

import json
import re


def _ag2_action_group() -> dict:
    """AG2 from 2314bb2c iter 2 — L5 + L6 directives both targeting gs_026."""
    return {
        "id": "AG2",
        "lever_directives": {
            "5": {
                "target_qids": ["7now_delivery_analytics_space_gs_026"],
                "example_sql_seed": (
                    "-- placeholder seed for the deterministic L5 mapper"
                ),
            },
            "6": {
                "target_qids": ["7now_delivery_analytics_space_gs_026"],
                "sql_expression": (
                    "SUM(f.cy_sales) FILTER (WHERE f.region = 'WEST')"
                ),
            },
        },
        "affected_questions": [
            "7now_delivery_analytics_space_gs_026",
        ],
    }


def _full_ledger():
    """The ledger the harness WOULD write on a healthy AG2 — both directives
    classified, both as NO_STRUCTURAL_CANDIDATE / FORCE_LLM_DECLINED."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )

    return AgDirectiveLedger(
        ag_id="AG2",
        iteration=2,
        directives_present=(5, 6),
        outcomes_by_lever={
            5: DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
            6: DirectiveOutcomeCode.FORCE_LLM_DECLINED,
        },
    )


def test_ag2_full_coverage_passes_invariant() -> None:
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [_ag2_action_group()],
            "directive_outcomes_by_ag": {"AG2": _full_ledger()},
        }
    )
    assert result.violated is False, result.message


def test_ag2_full_coverage_marker_round_trip() -> None:
    """When the ledger is populated, the per-AG marker carries both lever
    outcomes and survives a json round-trip — the postmortem reader sees
    'L5 = no_structural_candidate, L6 = force_llm_declined' at a glance."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        directive_outcome_marker,
    )

    line = directive_outcome_marker(
        optimization_run_id="2314bb2c-95a1-4d60-8226-09e5155aee2a",
        ledger=_full_ledger(),
    )
    assert line.startswith("GSO_DIRECTIVE_OUTCOME_V1 ")
    payload = json.loads(re.search(r"\s+(\{.*\})", line).group(1))
    assert payload["ag_id"] == "AG2"
    assert payload["outcomes_by_lever"]["5"] == "no_structural_candidate"
    assert payload["outcomes_by_lever"]["6"] == "force_llm_declined"


def test_ag2_missing_l6_fires_coverage_invariant() -> None:
    """The exact silent-budget-burn shape: L5 was classified but L6 was
    never recorded (e.g. an exception in the per-lever capture block
    swallowed the L6 outcome). The invariant MUST fire."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    partial_ledger = AgDirectiveLedger(
        ag_id="AG2",
        iteration=2,
        directives_present=(5, 6),
        outcomes_by_lever={
            5: DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
        },
    )
    result = check_directive_outcome_coverage(
        {
            "action_groups": [_ag2_action_group()],
            "directive_outcomes_by_ag": {"AG2": partial_ledger},
        }
    )
    assert result.violated is True
    assert result.offending_ag_ids == ("AG2",)
    assert result.offending_lever_keys_by_ag == (("AG2", (6,)),)
    assert "silent budget burn" in result.message


def test_ag2_no_ledger_at_all_fires_coverage_invariant() -> None:
    """The most extreme shape: the per-AG ledger init block raised
    silently and no ledger was ever written. The invariant catches this
    and reports both lever keys as missing."""
    from genie_space_optimizer.optimization.invariants import (
        check_directive_outcome_coverage,
    )

    result = check_directive_outcome_coverage(
        {
            "action_groups": [_ag2_action_group()],
            "directive_outcomes_by_ag": {},
        }
    )
    assert result.violated is True
    assert result.offending_ag_ids == ("AG2",)
    assert result.offending_lever_keys_by_ag == (("AG2", (5, 6)),)


def test_ag2_classifier_accepts_postmortem_shape() -> None:
    """Wire the classifier directly with the snapshot the harness would
    capture inside the per-lever loop on 2314bb2c iter 2 AG2 L6: zero
    proposals, force_llm_declined=True. Outcome must be FORCE_LLM_DECLINED."""
    from genie_space_optimizer.optimization.directive_outcome import (
        DirectiveOutcomeCode,
        LeverProposalSnapshot,
        classify_lever_proposal_outcome,
    )

    snapshot = LeverProposalSnapshot(
        lever_key=6,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=True,
    )
    assert classify_lever_proposal_outcome(snapshot) == (
        DirectiveOutcomeCode.FORCE_LLM_DECLINED
    )
