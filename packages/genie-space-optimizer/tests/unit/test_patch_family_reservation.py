"""Track C — cap reservation by patch family. Split-children of one
``rewrite_instruction`` parent must collapse to a single family slot so
they do not crowd out direct-fix patches.
"""
from __future__ import annotations


def _split_child(parent_id: str, child_index: int, section: str, score: float) -> dict:
    """Build a section-split-child patch matching what
    ``_split_rewrite_instruction_patch`` produces in the applier
    AFTER the MVP plan's PROPOSAL_METADATA_ALLOWLIST propagation —
    children inherit ``root_cause`` and lineage from the parent.
    """
    return {
        "proposal_id": f"{parent_id}#{child_index}",
        "expanded_patch_id": f"{parent_id}#{child_index}",
        "parent_proposal_id": parent_id,
        "type": "update_instruction_section",
        "section_name": section,
        "lever": 5,
        "_split_from": "rewrite_instruction",
        # MVP plan Track 1/B: split-children inherit parent root_cause
        # via PROPOSAL_METADATA_ALLOWLIST. This is what makes them tie
        # with the direct-fix patch on Pass 1's direct-behavior sort
        # key — and what makes Track C necessary.
        "root_cause": "missing_filter",
        "source_cluster_ids": ["H001"],
        "target_qids": ["q1"],
        "relevance_score": score,
    }


def test_split_children_of_one_parent_consume_one_family_slot_not_n() -> None:
    """Three split-children + one direct-fix L6 patch at cap=3, all
    qualifying as direct-behavior and all attributed to H001. The cap
    must keep at most one split-child (family-collapsed) and preserve
    the direct fix, not consume all three slots with split-children.

    Spec: high-level plan Track C — "treat all
    update_instruction_section patches sharing a _split_from parent
    as one slot, not N".

    Test design: ``per_cluster_slot_floor=0`` to remove the per-cluster
    pre-pass and force Pass 2 (direct-behavior reservation) to do the
    picking. Without family collapse, all three split-children
    out-rank the direct fix on relevance (0.92 / 0.91 / 0.90 vs 0.50)
    and consume all three cap slots. With family collapse, the second
    and third split-children are filtered out by ``selected_families``,
    and Pass 2 / Pass 3 / Pass 4 fill remaining slots from non-family
    candidates — including the direct fix.
    """
    from genie_space_optimizer.optimization.patch_selection import (
        select_target_aware_causal_patch_cap,
    )

    patches = [
        _split_child("P_REWRITE", 1, "QUERY RULES", 0.92),
        _split_child("P_REWRITE", 2, "ASSET ROUTING", 0.91),
        _split_child("P_REWRITE", 3, "QUERY PATTERNS", 0.90),
        {
            "proposal_id": "P_DIRECT",
            "type": "add_sql_snippet_filter",
            "lever": 6,
            "root_cause": "missing_filter",
            "source_cluster_ids": ["H001"],
            "target_qids": ["q1"],
            "relevance_score": 0.50,
        },
    ]

    selected, decisions = select_target_aware_causal_patch_cap(
        patches,
        target_qids=("q1",),
        max_patches=3,
        active_cluster_ids=("H001",),
        per_cluster_slot_floor=0,
    )

    selected_ids = {p["proposal_id"] for p in selected}
    assert "P_DIRECT" in selected_ids, (
        f"direct-fix dropped due to family crowding; selected={selected_ids}"
    )
    split_children_kept = sum(
        1 for p in selected if p.get("_split_from") == "rewrite_instruction"
    )
    assert split_children_kept <= 1, (
        f"family slot collapse failed: {split_children_kept} split-children kept"
    )
