"""Tests for build_kit_safety_summary (Phase 2 Action 2.2)."""

from __future__ import annotations

from genie_space_optimizer.optimization.repair_kit import RepairKit


_NON_SEMANTIC_TYPES = {
    "update_column_description",
    "add_column_synonym",
    "update_table_description",
    "add_metric_view_instruction",
    "add_table_instruction",
}

_INSTRUCTION_TYPES = {
    "add_instruction",
    "update_instruction_section",
    "rewrite_instruction",
}


def _kit(*patches: dict, archetype: str = "plural_top_n_collapse") -> RepairKit:
    return RepairKit(
        kit_id="kit_test",
        repair_archetype=archetype,
        target_qids=("gs_026",),
        expected_causal_effect="ORDER BY metric DESC",
        patches=tuple(patches),
    )


def test_summary_collects_union_target_objects_across_patches() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"target_table": "mv_7now_fact_sales", "column": "amount"},
        {"target_table": "mv_7now_store_sales"},
        {"target_object": "main.public.t1"},
    )
    summary = build_kit_safety_summary(kit)
    assert set(summary.union_target_objects) == {
        "main.public.t1",
        "mv_7now_fact_sales",
        "mv_7now_fact_sales.amount",
        "mv_7now_store_sales",
    }


def test_summary_collects_union_passing_dependents() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"target_table": "x", "passing_dependents": ["gs_a", "gs_b"]},
        {"target_table": "y", "passing_dependents": ["gs_b", "gs_c"]},
    )
    summary = build_kit_safety_summary(kit)
    assert set(summary.union_passing_dependents) == {"gs_a", "gs_b", "gs_c"}


def test_summary_risk_class_low_for_metadata_only_patches() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"patch_type": "update_column_description", "target_table": "t"},
        {"patch_type": "add_column_synonym", "target_table": "t"},
    )
    summary = build_kit_safety_summary(kit)
    assert summary.risk_class == "low"


def test_summary_risk_class_medium_for_instruction_edits() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"patch_type": "update_instruction_section", "section_name": "QUERY RULES"},
    )
    summary = build_kit_safety_summary(kit)
    assert summary.risk_class == "medium"


def test_summary_risk_class_high_for_hub_table_or_many_dependents() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {
            "patch_type": "add_sql_snippet_filter",
            "target_table": "mv_7now_fact_sales",
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
    )
    summary = build_kit_safety_summary(kit)
    assert summary.risk_class == "high"


def test_summary_risk_class_high_for_high_collateral_risk_flag() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {
            "patch_type": "update_instruction_section",
            "high_collateral_risk": True,
            "target_table": "any",
            "passing_dependents": ["gs_a", "gs_b"],
        },
    )
    summary = build_kit_safety_summary(kit)
    assert summary.risk_class == "high"


def test_summary_records_required_companions_from_kit_archetype() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"patch_type": "update_instruction_section"},
        archetype="plural_top_n_collapse",
    )
    summary = build_kit_safety_summary(kit)
    # Implicit / heuristic: patches in a kit MUST share the kit's
    # target_qids; required_companions reflects the patch types present.
    assert isinstance(summary.required_companions, tuple)


def test_summary_carries_expected_causal_effect_from_kit() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"patch_type": "update_instruction_section"},
    )
    summary = build_kit_safety_summary(kit)
    assert summary.expected_causal_effect == "ORDER BY metric DESC"


def test_summary_scoped_alternative_available_defaults_false() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit({"patch_type": "update_column_description"})
    summary = build_kit_safety_summary(kit)
    assert summary.scoped_alternative_available is False


def test_summary_scoped_alternative_available_when_any_patch_has_scoped_variant() -> None:
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {"patch_type": "update_column_description"},
        {
            "patch_type": "add_sql_snippet_filter",
            "_scoped_variant_pid": "P3_scoped",
        },
    )
    summary = build_kit_safety_summary(kit)
    assert summary.scoped_alternative_available is True


def test_summary_target_qids_echoed_from_kit() -> None:
    """Phase 2 Action 2.2 — KitSafetySummary echoes kit.target_qids
    so the gate's correctness predicate can read it without
    re-traversing the kit object."""
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = RepairKit(
        kit_id="kit_test",
        repair_archetype="default_time_window_filter",
        target_qids=("gs_021",),
        expected_causal_effect="Add WHERE f.time_window = mtd",
        patches=({"patch_type": "update_instruction_section"},),
    )
    summary = build_kit_safety_summary(kit)
    assert summary.target_qids == ("gs_021",)


def test_summary_co_beneficiary_qids_default_empty_when_phase_3_matcher_absent() -> None:
    """Phase 2 ships with the soft-evidence matcher (Phase 3 Action 3.3)
    not yet wired. Default behaviour: empty co_beneficiary_qids — the
    gate's co-beneficiary downgrade is therefore a no-op until Phase 3."""
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit({"patch_type": "update_column_description"})
    summary = build_kit_safety_summary(kit)
    assert summary.co_beneficiary_qids == ()


def test_summary_co_beneficiary_qids_populated_from_soft_evidence_input() -> None:
    """When the wrapper passes ``soft_evidence_matched_qids`` (Phase 3
    Action 3.3 wires this), the summary stores them as
    co_beneficiary_qids so the gate can apply the co-beneficiary risk
    downgrade. The example mirrors ccf1d60d H002 — 1 hard target qid
    (gs_021) with 11 soft co-beneficiaries from S001 sharing a
    time_window counterfactual."""
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = RepairKit(
        kit_id="kit_h002",
        repair_archetype="default_time_window_filter",
        target_qids=("gs_021",),
        expected_causal_effect="Add WHERE f.time_window = mtd default filter",
        patches=({"patch_type": "update_instruction_section"},),
    )
    soft_qids = tuple(f"gs_{i:03d}" for i in range(1, 12))  # gs_001 .. gs_011
    summary = build_kit_safety_summary(
        kit,
        soft_evidence_matched_qids=soft_qids,
    )
    assert summary.target_qids == ("gs_021",)
    assert set(summary.co_beneficiary_qids) == set(soft_qids)
    # Co-beneficiary scope is target_qids ∪ co_beneficiary_qids (12 qids).
    assert (
        len(summary.target_qids) + len(summary.co_beneficiary_qids) == 12
    )


def test_summary_co_beneficiary_qids_disjoint_from_target_qids() -> None:
    """A soft qid that overlaps the hard target list is a hard target,
    not a co-beneficiary. Dedup keeps the two sets disjoint so the
    gate's downgrade math is unambiguous."""
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = RepairKit(
        kit_id="kit_test",
        repair_archetype="default_time_window_filter",
        target_qids=("gs_021", "gs_022"),
        expected_causal_effect="Add mtd filter",
        patches=({"patch_type": "update_instruction_section"},),
    )
    summary = build_kit_safety_summary(
        kit,
        soft_evidence_matched_qids=("gs_021", "gs_001", "gs_002"),
    )
    # gs_021 already in target_qids — must NOT appear in co_beneficiary_qids.
    assert "gs_021" not in summary.co_beneficiary_qids
    assert set(summary.co_beneficiary_qids) == {"gs_001", "gs_002"}


def test_summary_risk_class_pure_from_patches_unaffected_by_co_beneficiaries() -> None:
    """The summary's ``risk_class`` is computed pure-from-patches —
    co-beneficiaries do NOT downgrade it here. The downgrade lives in
    the gate's ``effective_risk_class`` so the summary stays a pure
    data artifact and policy is concentrated in one place."""
    from genie_space_optimizer.optimization.kit_safety import (
        build_kit_safety_summary,
    )

    kit = _kit(
        {
            "patch_type": "add_sql_snippet_filter",
            "target_table": "mv_7now_fact_sales",
            "passing_dependents": [f"gs_{i}" for i in range(10)],
        },
    )
    summary_no_soft = build_kit_safety_summary(kit)
    summary_with_soft = build_kit_safety_summary(
        kit,
        soft_evidence_matched_qids=tuple(f"gs_co_{i}" for i in range(20)),
    )
    # Both summaries report the same raw risk_class — co-beneficiaries
    # only affect effective_risk_class in the gate.
    assert summary_no_soft.risk_class == "high"
    assert summary_with_soft.risk_class == "high"
    # ...but the soft-aware summary carries the data the gate will use.
    assert len(summary_with_soft.co_beneficiary_qids) == 20
