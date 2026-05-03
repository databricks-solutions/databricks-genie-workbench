"""TDD coverage for proposal direction contradiction detection (Cycle9 T11).

Cycle 9 P002 proposed ``add_sql_snippet_filter PAYMENT_CURRENCY_CD = 'USD'``
while the dominant cluster's ``counterfactual_fix`` said ``Remove the
PAYMENT_CURRENCY_CD = USD filter``. This predicate flags such direction-
inverted proposals so proposal_grounding can demote them.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T11.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.sql_shape_quality import (
    proposal_direction_contradicts_counterfactual,
)


def test_add_filter_when_counterfactual_says_remove():
    patch = {
        "type": "add_sql_snippet_filter",
        "value": "tkt_payment.PAYMENT_CURRENCY_CD = 'USD'",
        "counterfactual_fix": (
            "Remove the PAYMENT_CURRENCY_CD = USD filter (the question "
            "says total payment amount in USD referring to the display "
            "unit)"
        ),
    }
    assert proposal_direction_contradicts_counterfactual(patch) is True


def test_aligned_proposal_does_not_flag():
    patch = {
        "type": "add_sql_snippet_measure",
        "value": "SUM(tkt_payment.PAYMENT_AMT)",
        "counterfactual_fix": "Use SUM(PAYMENT_AMT) instead of COUNT(*)",
    }
    assert proposal_direction_contradicts_counterfactual(patch) is False


def test_no_counterfactual_returns_false():
    patch = {"type": "add_sql_snippet_filter", "value": "X = 1"}
    assert proposal_direction_contradicts_counterfactual(patch) is False


def test_remove_phrasing_variants_detected():
    for cf in (
        "Remove the X filter",
        "remove the X = USD filter",
        "Drop the X filter",
        "Strip the X = USD filter",
    ):
        patch = {
            "type": "add_sql_snippet_filter",
            "value": "T.X = 'USD'",
            "counterfactual_fix": cf,
        }
        assert proposal_direction_contradicts_counterfactual(patch) is True, cf


def test_only_fires_for_add_snippet_types():
    patch = {
        "type": "add_instruction",
        "value": "Always include PAYMENT_CURRENCY_CD = 'USD'",
        "counterfactual_fix": "Remove the PAYMENT_CURRENCY_CD = USD filter",
    }
    assert proposal_direction_contradicts_counterfactual(patch) is False
