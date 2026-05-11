"""RCO-7 Site 1 — strategist-response ingestion is reorder-invariant.

This test exercises the SAME helper the harness wires at the strategist
parse boundary. It does not import the harness (the harness module is
~25k LOC and not unit-testable in isolation), but it does exercise the
exact code path the harness uses at line 17725: ``strategy.get(
"action_groups", [])`` wrapped through ``sort_action_groups_canonically``.

A future harness refactor must continue to route the parsed strategist
response through this helper. The harness-side test
``tests/integration/test_rco7_harness_strategist_sort_call_site.py`` would
be the right home for end-to-end coverage if it becomes necessary; for
now, the helper-level guard plus an inline grep guard (Task 6) is
sufficient.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.llm_boundary_sort import (
    sort_action_groups_canonically,
)


def _simulated_strategist_response(order: list[str]) -> dict:
    """Build a strategist-response dict whose ``action_groups`` list
    follows the requested AG id order. All other fields are identical."""
    ag_dicts = {
        "AG_alpha": {
            "id": "AG_alpha",
            "affected_questions": ["q1", "q2"],
            "lever_directives": {"5": {"target_qids": ["q1"]}},
            "source_cluster_ids": ["C1"],
            "root_cause_summary": "missing_filter",
        },
        "AG_beta": {
            "id": "AG_beta",
            "affected_questions": ["q3"],
            "lever_directives": {"3": {"target_qids": ["q3"]}},
            "source_cluster_ids": ["C2"],
            "root_cause_summary": "wrong_aggregation",
        },
        "AG_gamma": {
            "id": "AG_gamma",
            "affected_questions": ["q4"],
            "lever_directives": {"6": {"target_qids": ["q4"]}},
            "source_cluster_ids": ["C3"],
            "root_cause_summary": "wrong_measure",
        },
    }
    return {
        "_memoized": False,
        "action_groups": [ag_dicts[i] for i in order],
    }


def test_strategist_response_action_groups_are_sort_invariant() -> None:
    """Two strategist responses with the same AG content in different
    order must produce identical canonicalized action_groups."""
    forward = _simulated_strategist_response(["AG_alpha", "AG_beta", "AG_gamma"])
    reversed_order = _simulated_strategist_response(
        ["AG_gamma", "AG_beta", "AG_alpha"]
    )
    shuffled = _simulated_strategist_response(
        ["AG_beta", "AG_gamma", "AG_alpha"]
    )

    forward_sorted = sort_action_groups_canonically(
        forward.get("action_groups", [])
    )
    reversed_sorted = sort_action_groups_canonically(
        reversed_order.get("action_groups", [])
    )
    shuffled_sorted = sort_action_groups_canonically(
        shuffled.get("action_groups", [])
    )

    # Byte-stable comparison via canonical JSON.
    canon = lambda x: json.dumps(x, sort_keys=True)
    assert canon(forward_sorted) == canon(reversed_sorted)
    assert canon(forward_sorted) == canon(shuffled_sorted)
    assert [a["id"] for a in forward_sorted] == [
        "AG_alpha", "AG_beta", "AG_gamma",
    ]


def test_strategist_response_empty_action_groups_yields_empty_list() -> None:
    """Edge case: a strategist that emits zero AGs must produce
    an empty list, not raise."""
    result = sort_action_groups_canonically([])
    assert result == []
    result = sort_action_groups_canonically(None)
    assert result == []
