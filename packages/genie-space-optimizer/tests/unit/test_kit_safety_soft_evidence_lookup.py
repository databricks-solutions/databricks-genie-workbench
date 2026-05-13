"""Phase 1 Addendum × Phase 2 Section B bridge tests.

Tests for ``build_soft_evidence_lookup_by_kit`` — the helper that joins
each kit (grouped from the proposal slate by Section B's
``group_patches_into_kits``) against the cluster object's
``rca_card_supporting_soft_evidence`` field (populated by Phase 1
Addendum's ``build_rca_card`` when ``GSO_RCA_CARD_SOFT_EVIDENCE=1``).

The resulting dict is fed verbatim to ``select_kit_aware_patch_cap``'s
``soft_evidence_matched_qids_by_kit`` parameter so the kit-safety gate
can apply the co-beneficiary risk downgrade documented in the Phase 2
plan."""

from __future__ import annotations

from genie_space_optimizer.optimization.kit_safety import (
    build_soft_evidence_lookup_by_kit,
)


def test_lookup_joins_kit_to_cluster_via_target_qids() -> None:
    """When a kit's first target_qid matches a cluster's question_ids,
    the lookup returns the soft qids from that cluster's
    rca_card_supporting_soft_evidence list."""
    patches = [
        {
            "proposal_id": "P1",
            "_repair_archetype": "default_time_window_filter",
            "target_qids": ("gs_021",),
            "_expected_causal_effect": "Add mtd filter",
            "patch_type": "update_instruction_section",
        },
    ]
    clusters = [
        {
            "cluster_id": "H002",
            "question_ids": ["gs_021"],
            "rca_card_supporting_soft_evidence": [
                {"soft_qid": f"gs_{i:03d}", "soft_cluster_id": "S001",
                 "match_kind": "matching_counterfactual",
                 "evidence_token": "time_window",
                 "soft_counterfactual": "Add filter on time_window"}
                for i in range(1, 12)
            ],
        },
    ]
    lookup = build_soft_evidence_lookup_by_kit(
        clusters=clusters, patches=patches,
    )
    assert len(lookup) == 1
    soft_qids = next(iter(lookup.values()))
    assert set(soft_qids) == {f"gs_{i:03d}" for i in range(1, 12)}


def test_lookup_returns_empty_when_no_cluster_carries_soft_evidence() -> None:
    """No cluster has rca_card_supporting_soft_evidence → empty dict;
    the wrapper sees no entries and the gate stays in canonical-only
    behaviour."""
    patches = [
        {
            "proposal_id": "P1",
            "_repair_archetype": "default_time_window_filter",
            "target_qids": ("gs_021",),
            "_expected_causal_effect": "x",
            "patch_type": "update_instruction_section",
        },
    ]
    clusters = [{"cluster_id": "H002", "question_ids": ["gs_021"]}]
    assert build_soft_evidence_lookup_by_kit(
        clusters=clusters, patches=patches,
    ) == {}


def test_lookup_skips_kits_with_no_target_qids() -> None:
    """A kit whose target_qids tuple is empty cannot be joined to any
    cluster — no entry in the lookup."""
    patches = [
        {"proposal_id": "P1", "_repair_archetype": "x",
         "_expected_causal_effect": "x"},  # no target_qids
    ]
    clusters = [
        {"cluster_id": "H002", "question_ids": ["gs_021"],
         "rca_card_supporting_soft_evidence": [
            {"soft_qid": "gs_001"},
         ]},
    ]
    assert build_soft_evidence_lookup_by_kit(
        clusters=clusters, patches=patches,
    ) == {}


def test_lookup_skips_kits_whose_target_qids_match_no_cluster() -> None:
    patches = [
        {
            "proposal_id": "P1",
            "_repair_archetype": "x",
            "target_qids": ("gs_unrelated",),
            "_expected_causal_effect": "x",
        },
    ]
    clusters = [
        {"cluster_id": "H002", "question_ids": ["gs_021"],
         "rca_card_supporting_soft_evidence": [
            {"soft_qid": "gs_001"},
         ]},
    ]
    assert build_soft_evidence_lookup_by_kit(
        clusters=clusters, patches=patches,
    ) == {}


def test_lookup_key_matches_group_patches_into_kits_kit_id() -> None:
    """Required invariant: the dict's key for a kit must match the
    ``kit_id`` that ``group_patches_into_kits`` computes for the same
    patches. Otherwise ``select_kit_aware_patch_cap``'s
    ``soft_lookup.get(kit.kit_id, ())`` lookup would miss every entry."""
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    patches = [
        {
            "proposal_id": "P1",
            "_repair_archetype": "default_time_window_filter",
            "target_qids": ("gs_021",),
            "_expected_causal_effect": "Add mtd filter",
            "patch_type": "update_instruction_section",
        },
    ]
    clusters = [
        {"cluster_id": "H002", "question_ids": ["gs_021"],
         "rca_card_supporting_soft_evidence": [{"soft_qid": "gs_001"}]},
    ]
    lookup = build_soft_evidence_lookup_by_kit(
        clusters=clusters, patches=patches,
    )
    kits = group_patches_into_kits(patches)
    assert set(lookup.keys()) == {kits[0].kit_id}
