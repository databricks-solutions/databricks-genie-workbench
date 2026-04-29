from __future__ import annotations

from genie_space_optimizer.optimization.applier import (
    _expand_rewrite_splits,
)


def _rewrite_patch() -> dict:
    return {
        "type": "rewrite_instruction",
        "lever": 5,
        "cluster_id": "AG1",
        "proposal_id": "P001#1",
        "parent_proposal_id": "P001",
        "source_proposal_id": "P001",
        "expanded_patch_id": "P001#1",
        "invoked_levers": [3, 5],
        "new_text": (
            "QUERY RULES:\n- Always filter time_window.\n\n"
            "DATA QUALITY NOTES:\n- Coalesce nulls.\n\n"
            "CONSTRAINTS:\n- Never join across schemas.\n"
        ),
        "proposed_value": (
            "QUERY RULES:\n- Always filter time_window.\n\n"
            "DATA QUALITY NOTES:\n- Coalesce nulls.\n\n"
            "CONSTRAINTS:\n- Never join across schemas.\n"
        ),
    }


def test_rewrite_split_assigns_unique_child_ids() -> None:
    expanded = _expand_rewrite_splits([_rewrite_patch()])
    child_ids = [p.get("proposal_id") for p in expanded]
    assert len(child_ids) >= 2
    assert len(child_ids) == len(set(child_ids)), (
        f"expected unique child proposal_ids, got {child_ids}"
    )
    expanded_ids = [p.get("expanded_patch_id") for p in expanded]
    assert len(expanded_ids) == len(set(expanded_ids))


def test_rewrite_split_preserves_parent_link() -> None:
    expanded = _expand_rewrite_splits([_rewrite_patch()])
    for child in expanded:
        assert child.get("parent_proposal_id") == "P001"
        assert child.get("source_proposal_id") == "P001"
        assert child.get("proposal_id", "").startswith("P001#")


def test_rewrite_split_indices_are_sequential_per_parent() -> None:
    parent_a = _rewrite_patch()
    parent_b = dict(parent_a)
    parent_b["proposal_id"] = "P002#1"
    parent_b["parent_proposal_id"] = "P002"
    parent_b["source_proposal_id"] = "P002"
    parent_b["expanded_patch_id"] = "P002#1"

    expanded = _expand_rewrite_splits([parent_a, parent_b])
    a_ids = sorted(
        p["proposal_id"] for p in expanded
        if p.get("parent_proposal_id") == "P001"
    )
    b_ids = sorted(
        p["proposal_id"] for p in expanded
        if p.get("parent_proposal_id") == "P002"
    )
    assert a_ids == [f"P001#{i}" for i in range(1, len(a_ids) + 1)]
    assert b_ids == [f"P002#{i}" for i in range(1, len(b_ids) + 1)]
