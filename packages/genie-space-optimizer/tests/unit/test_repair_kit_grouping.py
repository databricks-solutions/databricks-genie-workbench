"""Tests for RepairKit grouping (Phase 2 Action 2.2)."""

from __future__ import annotations


def test_group_patches_into_kits_groups_by_shared_archetype_and_target_qids() -> None:
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY metric"},
        {"proposal_id": "P2", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY metric"},
        {"proposal_id": "P3", "_repair_archetype": "default_time_window_filter",
         "target_qids": ("gs_021",), "_expected_causal_effect": "Add mtd filter"},
    ]
    kits = group_patches_into_kits(patches)
    assert len(kits) == 2
    by_archetype = {k.repair_archetype: k for k in kits}
    assert sorted(p["proposal_id"] for p in by_archetype["plural_top_n_collapse"].patches) == ["P1", "P2"]
    assert by_archetype["default_time_window_filter"].target_qids == ("gs_021",)


def test_group_patches_into_kits_assigns_loose_patches_to_implicit_kit() -> None:
    """Patches without archetype stamps land in a single implicit kit per
    target_qids tuple. Preserves byte-stability for the legacy patch path."""
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    patches = [
        {"proposal_id": "P1", "target_qids": ("gs_x",)},
        {"proposal_id": "P2", "target_qids": ("gs_x",)},
        {"proposal_id": "P3", "target_qids": ("gs_y",)},
    ]
    kits = group_patches_into_kits(patches)
    assert len(kits) == 2
    by_target = {k.target_qids: k for k in kits}
    assert sorted(p["proposal_id"] for p in by_target[("gs_x",)].patches) == ["P1", "P2"]
    assert by_target[("gs_y",)].repair_archetype == "_implicit"


def test_group_patches_into_kits_returns_empty_list_for_empty_input() -> None:
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    assert group_patches_into_kits([]) == []


def test_repair_kit_is_frozen_dataclass() -> None:
    import dataclasses

    from genie_space_optimizer.optimization.repair_kit import RepairKit

    assert dataclasses.is_dataclass(RepairKit)
    fields = {f.name for f in dataclasses.fields(RepairKit)}
    assert fields >= {
        "kit_id",
        "repair_archetype",
        "target_qids",
        "expected_causal_effect",
        "patches",
    }
