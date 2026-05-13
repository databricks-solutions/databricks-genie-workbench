"""Tests for generate_scoped_variant (Phase 2 Action 2.3)."""

from __future__ import annotations


def test_generate_scoped_variant_returns_sibling_with_scoped_to_qids() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        generate_scoped_variant,
    )

    original = {
        "proposal_id": "L1:P003#1",
        "patch_type": "update_column_description",
        "target_table": "mv_7now_fact_sales",
        "column": "amount",
        "passing_dependents": [f"gs_{i}" for i in range(10)],
    }
    target_qids = ("gs_026",)
    variant = generate_scoped_variant(original, target_qids=target_qids)
    assert variant is not None
    assert variant["proposal_id"] != original["proposal_id"]
    assert variant["_scoped_from_pid"] == "L1:P003#1"
    assert variant["scoped_to_qids"] == ("gs_026",)


def test_generate_scoped_variant_carries_target_table_and_column() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        generate_scoped_variant,
    )

    original = {
        "proposal_id": "L1:P003#1",
        "patch_type": "update_column_description",
        "target_table": "mv_7now_fact_sales",
        "column": "amount",
        "passing_dependents": [f"gs_{i}" for i in range(10)],
    }
    variant = generate_scoped_variant(original, target_qids=("gs_026",))
    assert variant is not None
    assert variant["target_table"] == "mv_7now_fact_sales"
    assert variant["column"] == "amount"


def test_generate_scoped_variant_reduces_passing_dependents_to_target_intersection() -> None:
    """A scoped variant only affects the target qids — the post-apply
    passing_dependents set is the intersection of the original set and
    the target qids."""
    from genie_space_optimizer.optimization.scoped_variants import (
        generate_scoped_variant,
    )

    original = {
        "proposal_id": "L1:P003#1",
        "passing_dependents": ["gs_026", "gs_a", "gs_b"],
        "target_table": "mv_7now_fact_sales",
    }
    variant = generate_scoped_variant(original, target_qids=("gs_026",))
    assert variant is not None
    assert variant["passing_dependents"] == ["gs_026"]


def test_generate_scoped_variant_returns_none_for_non_hub_patch() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        generate_scoped_variant,
    )

    original = {"proposal_id": "P1", "passing_dependents": ["gs_a"]}
    variant = generate_scoped_variant(original, target_qids=("gs_a",))
    assert variant is None  # Not a hub-table patch (no target_table)


def test_generate_scoped_variant_returns_none_for_empty_target_qids() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        generate_scoped_variant,
    )

    original = {
        "proposal_id": "P1",
        "target_table": "x",
        "passing_dependents": [f"gs_{i}" for i in range(10)],
    }
    variant = generate_scoped_variant(original, target_qids=())
    assert variant is None
