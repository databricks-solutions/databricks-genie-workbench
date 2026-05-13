"""Tests for apply_scoped_variants_to_proposals (Phase 2 Action 2.3)."""

from __future__ import annotations


def test_apply_returns_originals_plus_scoped_for_hub_patches() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        apply_scoped_variants_to_proposals,
    )

    proposals = [
        {
            "proposal_id": "L1:P003#1",
            "target_table": "mv_7now_fact_sales",
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
        {
            "proposal_id": "L1:P002#1",
            "target_table": "small_table",
            "passing_dependents": ["gs_a"],
        },
    ]
    result, summary = apply_scoped_variants_to_proposals(
        proposals,
        target_qids=("gs_026",),
        threshold=5,
    )
    pids = sorted(p["proposal_id"] for p in result)
    assert "L1:P003#1" in pids
    assert "L1:P003#1_scoped" in pids
    assert "L1:P002#1" in pids
    assert summary == {
        "scoped_variants_generated": 1,
        "no_scoped_variant_available": 0,
        "non_hub_proposals_passed_through": 1,
    }


def test_apply_records_no_scoped_variant_available_when_intersection_empty() -> None:
    """Hub-table patch + empty target qids → variant generator returns
    None and the helper records no_scoped_variant_available."""
    from genie_space_optimizer.optimization.scoped_variants import (
        apply_scoped_variants_to_proposals,
    )

    proposals = [
        {
            "proposal_id": "L1:P003#1",
            "target_table": "mv_7now_fact_sales",
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
    ]
    result, summary = apply_scoped_variants_to_proposals(
        proposals,
        target_qids=(),  # empty → variant generator returns None
        threshold=5,
    )
    assert summary == {
        "scoped_variants_generated": 0,
        "no_scoped_variant_available": 1,
        "non_hub_proposals_passed_through": 0,
    }


def test_apply_returns_originals_unchanged_when_no_hub_patches() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        apply_scoped_variants_to_proposals,
    )

    proposals = [
        {"proposal_id": "P1", "target_table": "x", "passing_dependents": ["gs_a"]},
        {"proposal_id": "P2", "target_table": "y", "passing_dependents": []},
    ]
    result, summary = apply_scoped_variants_to_proposals(
        proposals, target_qids=("gs_a",), threshold=5,
    )
    assert sorted(p["proposal_id"] for p in result) == ["P1", "P2"]
    assert summary["scoped_variants_generated"] == 0
