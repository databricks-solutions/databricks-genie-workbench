"""Tests for is_hub_table_patch (Phase 2 Action 2.3)."""

from __future__ import annotations


def test_hub_table_detected_when_passing_dependents_exceed_threshold() -> None:
    from genie_space_optimizer.optimization.scoped_variants import is_hub_table_patch

    patch = {
        "target_table": "mv_7now_fact_sales",
        "passing_dependents": [f"gs_{i}" for i in range(10)],
    }
    assert is_hub_table_patch(patch, threshold=5) is True


def test_hub_table_not_detected_at_or_below_threshold() -> None:
    from genie_space_optimizer.optimization.scoped_variants import is_hub_table_patch

    patch = {
        "target_table": "small_table",
        "passing_dependents": ["gs_a", "gs_b", "gs_c", "gs_d", "gs_e"],
    }
    assert is_hub_table_patch(patch, threshold=5) is False


def test_hub_table_not_detected_when_no_passing_dependents_field() -> None:
    from genie_space_optimizer.optimization.scoped_variants import is_hub_table_patch

    patch = {"target_table": "any_table"}
    assert is_hub_table_patch(patch, threshold=5) is False


def test_hub_table_not_detected_when_no_target_table() -> None:
    from genie_space_optimizer.optimization.scoped_variants import is_hub_table_patch

    patch = {
        "passing_dependents": [f"gs_{i}" for i in range(10)],
    }
    # No target_table to scope; treat as not-hub.
    assert is_hub_table_patch(patch, threshold=5) is False


def test_scoped_alternative_available_true_when_scoped_variant_pid_set() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        scoped_alternative_available,
    )

    patches = [
        {"proposal_id": "P1"},
        {"proposal_id": "P1_scoped", "_scoped_from_pid": "P1"},
    ]
    assert scoped_alternative_available(patches, target_pid="P1") is True


def test_scoped_alternative_available_false_when_no_scoped_sibling() -> None:
    from genie_space_optimizer.optimization.scoped_variants import (
        scoped_alternative_available,
    )

    patches = [{"proposal_id": "P1"}, {"proposal_id": "P2"}]
    assert scoped_alternative_available(patches, target_pid="P1") is False
