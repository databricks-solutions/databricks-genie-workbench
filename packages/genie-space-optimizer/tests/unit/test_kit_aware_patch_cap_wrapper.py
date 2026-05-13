"""Tests for select_kit_aware_patch_cap wrapper (Phase 2 Action 2.2)."""

from __future__ import annotations


def test_wrapper_returns_legacy_result_when_no_kit_atomicity_violation() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "patch_type": "update_column_description"},
        {"proposal_id": "P2", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "patch_type": "add_column_synonym"},
    ]
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    selected_pids = {p["proposal_id"] for p in selected}
    assert selected_pids == {"P1", "P2"}
    # Both patches in the same kit — kit atomicity preserved.
    assert all(o["accepted"] for o in kit_outcomes)


def test_wrapper_rejects_fragmented_kit_when_only_subset_survives_legacy_cap() -> None:
    """The legacy cap keeps top-K by relevance. If the top-K splits a
    kit (some members kept, others dropped), the kit-aware wrapper
    rejects that selection and emits KIT_ATOMICITY_VIOLATION."""
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "relevance_score": 0.9, "patch_type": "update_column_description"},
        {"proposal_id": "P2", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "relevance_score": 0.85, "patch_type": "add_column_synonym"},
        {"proposal_id": "P3", "_repair_archetype": "dimension_disambiguation",
         "target_qids": ("gs_026",), "_expected_causal_effect": "Disambiguate",
         "relevance_score": 0.95, "patch_type": "update_column_description"},
    ]
    # max_patches=2 forces the legacy cap to keep top-2 by relevance:
    # P3 (0.95) and P1 (0.9). That fragments the plural_top_n_collapse
    # kit (drops P2).
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=2,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    fragmented_outcomes = [o for o in kit_outcomes if not o["accepted"]]
    assert any(
        o["reason"] == "kit_atomicity_violation" for o in fragmented_outcomes
    )


def test_wrapper_rejects_high_risk_kit_with_no_scoped_alternative() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "patch_type": "add_sql_snippet_filter",
         "passing_dependents": [f"gs_{i}" for i in range(10)]},
    ]
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    # High-risk + no scoped alt → kit rejected; selected list is empty.
    assert selected == []
    rejected = [o for o in kit_outcomes if not o["accepted"]]
    assert any(
        o["reason"] == "high_risk_no_scoped_alternative" for o in rejected
    )


def test_wrapper_rejects_kit_when_union_dependents_exceeds_threshold() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "patch_type": "update_column_description",
         "passing_dependents": [f"gs_{i}" for i in range(20)]},
    ]
    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    rejected = [o for o in kit_outcomes if not o["accepted"]]
    assert any(
        o["reason"] == "union_passing_dependents_exceeds_threshold"
        for o in rejected
    )


def test_wrapper_returns_empty_for_empty_patches() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        [],
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    assert selected == []
    assert decisions == []
    assert kit_outcomes == []


def test_wrapper_threads_soft_evidence_into_summary_and_gate() -> None:
    """Phase 2 Action 2.2 — when the harness (Phase 3 Action 3.3
    populates this) passes ``soft_evidence_matched_qids_by_kit``, the
    wrapper looks up each kit's matched soft qids by ``kit.kit_id``,
    threads them into ``build_kit_safety_summary`` as
    ``soft_evidence_matched_qids``, and the gate's
    ``effective_risk_class`` reflects the co-beneficiary downgrade.

    Mirrors ccf1d60d H002: a high-risk SQL-snippet kit with 11 soft
    co-beneficiaries clears the gate (effective_risk_class='medium')
    instead of being rejected for high_risk_no_scoped_alternative."""
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )
    from genie_space_optimizer.optimization.repair_kit import (
        group_patches_into_kits,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "default_time_window_filter",
         "target_qids": ("gs_021",), "_expected_causal_effect": "Add mtd filter",
         "patch_type": "add_sql_snippet_filter",
         "passing_dependents": [f"gs_dep_{i}" for i in range(8)]},
    ]
    # Compute the kit_id the same way the wrapper will.
    kit = group_patches_into_kits(patches)[0]
    soft_lookup = {kit.kit_id: tuple(f"gs_co_{i}" for i in range(11))}

    selected, _decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_021",),
        max_patches=8,
        cluster_target_qids=("gs_021",),
        policy=KitSafetyPolicy(
            passing_dependents_threshold=15,
            co_beneficiary_downgrade_threshold=5,
        ),
        soft_evidence_matched_qids_by_kit=soft_lookup,
    )
    assert len(selected) == 1
    assert kit_outcomes[0]["accepted"] is True
    assert kit_outcomes[0]["co_beneficiary_count"] == 11
    assert kit_outcomes[0]["effective_risk_class"] == "medium"


def test_wrapper_default_soft_evidence_is_no_op() -> None:
    """When the harness does not pass ``soft_evidence_matched_qids_by_kit``
    (Phase 2 default — Phase 3 has not yet wired the matcher), the
    wrapper treats every kit as having ``()`` co-beneficiaries and the
    co-beneficiary downgrade is a no-op. Replay byte-stability holds."""
    from genie_space_optimizer.optimization.kit_safety import (
        KitSafetyPolicy,
        select_kit_aware_patch_cap,
    )

    patches = [
        {"proposal_id": "P1", "_repair_archetype": "plural_top_n_collapse",
         "target_qids": ("gs_026",), "_expected_causal_effect": "ORDER BY",
         "patch_type": "update_column_description"},
    ]
    selected, _decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=8,
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )
    assert kit_outcomes[0]["co_beneficiary_count"] == 0
