"""RCO-7 — helper-level unit tests for the LLM-boundary sort module.

Each helper must:
  - return a NEW list/dict (never mutate input)
  - be idempotent (sort(sort(x)) == sort(x))
  - be reorder-invariant (sort(shuffled(x)) == sort(x))
  - tolerate missing canonical-key fields without raising
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.llm_boundary_sort import (
    canonicalize_arbiter_verdict,
    sort_action_groups_canonically,
    sort_patches_canonically,
    sort_proposals_canonically,
)


# ── sort_action_groups_canonically ────────────────────────────────────


def test_sort_action_groups_orders_by_id() -> None:
    ags = [{"id": "AG_b"}, {"id": "AG_a"}, {"id": "AG_c"}]
    result = sort_action_groups_canonically(ags)
    assert [a["id"] for a in result] == ["AG_a", "AG_b", "AG_c"]


def test_sort_action_groups_falls_back_to_ag_id_alias() -> None:
    ags = [{"ag_id": "AG_b"}, {"id": "AG_a"}, {"ag_id": "AG_c"}]
    result = sort_action_groups_canonically(ags)
    keys = [a.get("id") or a.get("ag_id") for a in result]
    assert keys == ["AG_a", "AG_b", "AG_c"]


def test_sort_action_groups_does_not_mutate_input() -> None:
    ags = [{"id": "AG_b"}, {"id": "AG_a"}]
    snapshot = [dict(a) for a in ags]
    sort_action_groups_canonically(ags)
    assert ags == snapshot


def test_sort_action_groups_idempotent() -> None:
    ags = [{"id": "AG_b"}, {"id": "AG_a"}, {"id": "AG_c"}]
    once = sort_action_groups_canonically(ags)
    twice = sort_action_groups_canonically(once)
    assert once == twice


def test_sort_action_groups_handles_missing_id() -> None:
    ags = [{"id": "AG_a"}, {"label": "no_id"}, {"id": "AG_b"}]
    result = sort_action_groups_canonically(ags)
    # The id-less entry sorts to the front (empty string is min).
    assert result[0].get("label") == "no_id"
    assert [a.get("id") for a in result[1:]] == ["AG_a", "AG_b"]


# ── sort_proposals_canonically ────────────────────────────────────────


def test_sort_proposals_orders_by_expanded_patch_id_first() -> None:
    proposals = [
        {"expanded_patch_id": "L5:P002#1", "proposal_id": "P002"},
        {"expanded_patch_id": "L5:P001#1", "proposal_id": "P001"},
    ]
    result = sort_proposals_canonically(proposals)
    assert [p["expanded_patch_id"] for p in result] == [
        "L5:P001#1", "L5:P002#1",
    ]


def test_sort_proposals_falls_back_to_proposal_id() -> None:
    proposals = [
        {"proposal_id": "P002"},
        {"proposal_id": "P001"},
    ]
    result = sort_proposals_canonically(proposals)
    assert [p["proposal_id"] for p in result] == ["P001", "P002"]


def test_sort_proposals_does_not_mutate_input() -> None:
    proposals = [{"proposal_id": "P002"}, {"proposal_id": "P001"}]
    snapshot = [dict(p) for p in proposals]
    sort_proposals_canonically(proposals)
    assert proposals == snapshot


# ── sort_patches_canonically ──────────────────────────────────────────


def test_sort_patches_uses_stable_identity_as_tiebreaker() -> None:
    """Two patches sharing expanded_patch_id but differing in lever
    must sort deterministically by ``_stable_identity`` tiebreaker."""
    patches = [
        {"expanded_patch_id": "P001#2", "lever": 5, "type": "add_snippet"},
        {"expanded_patch_id": "P001#2", "lever": 3, "type": "rewrite_instruction"},
    ]
    once = sort_patches_canonically(patches)
    twice = sort_patches_canonically(list(reversed(patches)))
    assert once == twice


def test_sort_patches_idempotent() -> None:
    patches = [
        {"expanded_patch_id": "L5:P003#1"},
        {"expanded_patch_id": "L5:P001#1"},
        {"expanded_patch_id": "L5:P002#1"},
    ]
    once = sort_patches_canonically(patches)
    twice = sort_patches_canonically(once)
    assert once == twice


def test_sort_patches_does_not_mutate_input() -> None:
    patches = [{"expanded_patch_id": "P002"}, {"expanded_patch_id": "P001"}]
    snapshot = [dict(p) for p in patches]
    sort_patches_canonically(patches)
    assert patches == snapshot


# ── canonicalize_arbiter_verdict ──────────────────────────────────────


def test_arbiter_verdict_sorts_list_fields() -> None:
    verdict = {
        "verdict": "ground_truth_correct",
        "blame_set": ["table_b", "table_a"],
        "expected_objects": ["metric_z", "metric_a"],
        "actual_objects": ["col_y", "col_x"],
        "recommended_levers": [5, 1, 3],
        "rationale": "x",
    }
    result = canonicalize_arbiter_verdict(verdict)
    assert result["blame_set"] == ["table_a", "table_b"]
    assert result["expected_objects"] == ["metric_a", "metric_z"]
    assert result["actual_objects"] == ["col_x", "col_y"]
    assert result["recommended_levers"] == [1, 3, 5]
    # Scalar fields pass through unchanged.
    assert result["verdict"] == "ground_truth_correct"
    assert result["rationale"] == "x"


def test_arbiter_verdict_does_not_mutate_input() -> None:
    verdict = {
        "blame_set": ["b", "a"],
        "recommended_levers": [5, 1],
    }
    snapshot = {
        "blame_set": list(verdict["blame_set"]),
        "recommended_levers": list(verdict["recommended_levers"]),
    }
    canonicalize_arbiter_verdict(verdict)
    assert verdict == snapshot


def test_arbiter_verdict_handles_missing_lists() -> None:
    verdict = {"verdict": "genie_correct", "rationale": "ok"}
    result = canonicalize_arbiter_verdict(verdict)
    assert result["verdict"] == "genie_correct"
    assert result["rationale"] == "ok"
    # Missing fields are not synthesized; absence stays absent.
    assert "blame_set" not in result
    assert "recommended_levers" not in result


def test_arbiter_verdict_idempotent() -> None:
    verdict = {
        "blame_set": ["b", "a"],
        "recommended_levers": [3, 1],
    }
    once = canonicalize_arbiter_verdict(verdict)
    twice = canonicalize_arbiter_verdict(once)
    assert once == twice


@pytest.mark.parametrize("levers", [
    [5, 1, 3],
    ["5", "1", "3"],
    [5, "1", 3],
])
def test_arbiter_verdict_coerces_lever_ints(levers: list) -> None:
    """recommended_levers is documented as an int list; coerce
    string-ints so the sort key is numerically consistent."""
    verdict = {"recommended_levers": levers}
    result = canonicalize_arbiter_verdict(verdict)
    assert result["recommended_levers"] == [1, 3, 5]
