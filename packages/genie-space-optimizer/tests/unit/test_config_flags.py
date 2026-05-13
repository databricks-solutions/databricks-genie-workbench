"""Cycle 10 — config-flag accessors for the seven workstreams."""
from __future__ import annotations


def test_rca_ungrounded_records_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RCA_UNGROUNDED_RECORDS_ENABLED", raising=False)
    from genie_space_optimizer.common.config import (
        rca_ungrounded_records_enabled,
    )
    assert rca_ungrounded_records_enabled() is True


def test_rca_ungrounded_records_enabled_off(monkeypatch):
    monkeypatch.setenv("GSO_RCA_UNGROUNDED_RECORDS_ENABLED", "0")
    from genie_space_optimizer.common.config import (
        rca_ungrounded_records_enabled,
    )
    assert rca_ungrounded_records_enabled() is False


def test_ag_levers_union_recommended_default_on(monkeypatch):
    monkeypatch.delenv("GSO_AG_LEVERS_UNION_RECOMMENDED", raising=False)
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
    )
    assert ag_levers_union_recommended_enabled() is True


def test_ag_levers_union_recommended_off(monkeypatch):
    monkeypatch.setenv("GSO_AG_LEVERS_UNION_RECOMMENDED", "0")
    from genie_space_optimizer.common.config import (
        ag_levers_union_recommended_enabled,
    )
    assert ag_levers_union_recommended_enabled() is False


def test_lever6_force_typed_outcomes_default_on(monkeypatch):
    monkeypatch.delenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", raising=False)
    from genie_space_optimizer.common.config import (
        lever6_force_typed_outcomes_enabled,
    )
    assert lever6_force_typed_outcomes_enabled() is True


def test_lever6_force_typed_outcomes_off(monkeypatch):
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "0")
    from genie_space_optimizer.common.config import (
        lever6_force_typed_outcomes_enabled,
    )
    assert lever6_force_typed_outcomes_enabled() is False


def test_l6_narrow_replacement_patch_aware_default_on(monkeypatch):
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", raising=False)
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_patch_aware_enabled,
    )
    assert l6_narrow_replacement_patch_aware_enabled() is True


def test_l6_narrow_replacement_patch_aware_off(monkeypatch):
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", "0")
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_patch_aware_enabled,
    )
    assert l6_narrow_replacement_patch_aware_enabled() is False


def test_doa_fingerprint_patch_body_match_default_on(monkeypatch):
    monkeypatch.delenv("GSO_DOA_FINGERPRINT_PATCH_BODY_MATCH", raising=False)
    from genie_space_optimizer.common.config import (
        doa_fingerprint_patch_body_match_enabled,
    )
    assert doa_fingerprint_patch_body_match_enabled() is True


def test_doa_fingerprint_patch_body_match_off(monkeypatch):
    monkeypatch.setenv("GSO_DOA_FINGERPRINT_PATCH_BODY_MATCH", "0")
    from genie_space_optimizer.common.config import (
        doa_fingerprint_patch_body_match_enabled,
    )
    assert doa_fingerprint_patch_body_match_enabled() is False


def test_plateau_counts_quarantined_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PLATEAU_COUNTS_QUARANTINED", raising=False)
    from genie_space_optimizer.common.config import (
        plateau_counts_quarantined_enabled,
    )
    assert plateau_counts_quarantined_enabled() is True


def test_plateau_counts_quarantined_off(monkeypatch):
    monkeypatch.setenv("GSO_PLATEAU_COUNTS_QUARANTINED", "0")
    from genie_space_optimizer.common.config import (
        plateau_counts_quarantined_enabled,
    )
    assert plateau_counts_quarantined_enabled() is False


def test_proposal_trace_one_source_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PROPOSAL_TRACE_ONE_SOURCE", raising=False)
    from genie_space_optimizer.common.config import (
        proposal_trace_one_source_enabled,
    )
    assert proposal_trace_one_source_enabled() is True


def test_proposal_trace_one_source_off(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_TRACE_ONE_SOURCE", "0")
    from genie_space_optimizer.common.config import (
        proposal_trace_one_source_enabled,
    )
    assert proposal_trace_one_source_enabled() is False


def test_gso_run_manifest_v2_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_RUN_MANIFEST_V2_ENABLED", raising=False)
    from genie_space_optimizer.common.config import (
        gso_run_manifest_v2_enabled,
    )
    assert gso_run_manifest_v2_enabled() is True


def test_gso_run_manifest_v2_enabled_off(monkeypatch):
    monkeypatch.setenv("GSO_RUN_MANIFEST_V2_ENABLED", "0")
    from genie_space_optimizer.common.config import (
        gso_run_manifest_v2_enabled,
    )
    assert gso_run_manifest_v2_enabled() is False


# ── Cycle 14-T1 + T2 — default-on flags ──────────────────────────────


def test_phase_b_aggregator_in_finalize_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PHASE_B_AGGREGATOR_IN_FINALIZE", raising=False)
    from genie_space_optimizer.common.config import (
        phase_b_aggregator_in_finalize_enabled,
    )
    assert phase_b_aggregator_in_finalize_enabled() is True


def test_phase_b_aggregator_in_finalize_disabled_via_env(monkeypatch):
    monkeypatch.setenv("GSO_PHASE_B_AGGREGATOR_IN_FINALIZE", "0")
    from genie_space_optimizer.common.config import (
        phase_b_aggregator_in_finalize_enabled,
    )
    assert phase_b_aggregator_in_finalize_enabled() is False


def test_canonical_acceptance_render_enabled_default_on(monkeypatch):
    monkeypatch.delenv("GSO_CANONICAL_ACCEPTANCE_RENDER", raising=False)
    from genie_space_optimizer.common.config import (
        canonical_acceptance_render_enabled,
    )
    assert canonical_acceptance_render_enabled() is True


def test_canonical_acceptance_render_disabled_via_env(monkeypatch):
    monkeypatch.setenv("GSO_CANONICAL_ACCEPTANCE_RENDER", "0")
    from genie_space_optimizer.common.config import (
        canonical_acceptance_render_enabled,
    )
    assert canonical_acceptance_render_enabled() is False


# Phase 1 Action 1.1 — RCA card builder flags.


def test_rca_card_builder_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_RCA_CARD_BUILDER", raising=False)
    from genie_space_optimizer.common.config import rca_card_builder_enabled
    assert rca_card_builder_enabled() is False


def test_rca_card_builder_enabled_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_RCA_CARD_BUILDER", "1")
    from genie_space_optimizer.common.config import rca_card_builder_enabled
    assert rca_card_builder_enabled() is True


def test_rca_card_llm_normalization_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_RCA_CARD_LLM_NORMALIZATION", raising=False)
    from genie_space_optimizer.common.config import (
        rca_card_llm_normalization_enabled,
    )
    assert rca_card_llm_normalization_enabled() is False


def test_rca_card_llm_normalization_enabled_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_RCA_CARD_LLM_NORMALIZATION", "yes")
    from genie_space_optimizer.common.config import (
        rca_card_llm_normalization_enabled,
    )
    assert rca_card_llm_normalization_enabled() is True


def test_acceptance_four_tier_gate_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_ACCEPTANCE_FOUR_TIER_GATE", raising=False)
    from genie_space_optimizer.common.config import (
        acceptance_four_tier_gate_enabled,
    )
    assert acceptance_four_tier_gate_enabled() is False


def test_acceptance_four_tier_gate_enabled_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ACCEPTANCE_FOUR_TIER_GATE", "1")
    from genie_space_optimizer.common.config import (
        acceptance_four_tier_gate_enabled,
    )
    assert acceptance_four_tier_gate_enabled() is True


# Phase 2 Section A — Repair Planner
def test_repair_planner_enabled_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_REPAIR_PLANNER", raising=False)
    from genie_space_optimizer.common.config import repair_planner_enabled
    assert repair_planner_enabled() is False


def test_repair_planner_enabled_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_REPAIR_PLANNER", "1")
    from genie_space_optimizer.common.config import repair_planner_enabled
    assert repair_planner_enabled() is True


def test_propagation_root_cause_default_unknown(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PROPAGATION_ROOT_CAUSE", raising=False)
    from genie_space_optimizer.common.config import propagation_root_cause
    assert propagation_root_cause() == "unknown"


def test_propagation_root_cause_reads_env_value(monkeypatch) -> None:
    from genie_space_optimizer.common.config import propagation_root_cause
    for value in (
        "propagation_lag",
        "instruction_not_scoped_to_qid",
        "instruction_insufficient_force",
        "eval_cache_stale",
    ):
        monkeypatch.setenv("GSO_PROPAGATION_ROOT_CAUSE", value)
        assert propagation_root_cause() == value


def test_propagation_root_cause_unknown_for_invalid_value(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PROPAGATION_ROOT_CAUSE", "garbage_value")
    from genie_space_optimizer.common.config import propagation_root_cause
    assert propagation_root_cause() == "unknown"


# Phase 2 Section B — Kit-aware patch cap
def test_kit_aware_patch_cap_enabled_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_KIT_AWARE_PATCH_CAP", raising=False)
    from genie_space_optimizer.common.config import kit_aware_patch_cap_enabled
    assert kit_aware_patch_cap_enabled() is False


def test_kit_passing_dependents_threshold_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_KIT_PASSING_DEPENDENTS_THRESHOLD", raising=False)
    from genie_space_optimizer.common.config import kit_passing_dependents_threshold
    assert kit_passing_dependents_threshold() == 15


def test_co_beneficiary_downgrade_threshold_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_CO_BENEFICIARY_DOWNGRADE_THRESHOLD", raising=False)
    from genie_space_optimizer.common.config import co_beneficiary_downgrade_threshold
    assert co_beneficiary_downgrade_threshold() == 5


# Phase 2 Section C — Hub-table scoped variants
def test_hub_table_scoped_variants_enabled_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_HUB_TABLE_SCOPED_VARIANTS", raising=False)
    from genie_space_optimizer.common.config import hub_table_scoped_variants_enabled
    assert hub_table_scoped_variants_enabled() is False


def test_hub_table_dependents_threshold_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_HUB_TABLE_DEPENDENTS_THRESHOLD", raising=False)
    from genie_space_optimizer.common.config import hub_table_dependents_threshold
    assert hub_table_dependents_threshold() == 5


# Phase 2 Section D — Strategist coverage re-call
def test_strategist_coverage_recall_enabled_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_STRATEGIST_COVERAGE_RECALL", raising=False)
    from genie_space_optimizer.common.config import strategist_coverage_recall_enabled
    assert strategist_coverage_recall_enabled() is False


# Phase 2 Section E — Archetype learning
def test_archetype_learning_enabled_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_ARCHETYPE_LEARNING", raising=False)
    from genie_space_optimizer.common.config import archetype_learning_enabled
    assert archetype_learning_enabled() is False


def test_pattern_candidate_member_threshold_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PATTERN_CANDIDATE_MEMBER_THRESHOLD", raising=False)
    from genie_space_optimizer.common.config import pattern_candidate_member_threshold
    assert pattern_candidate_member_threshold() == 3


def test_provisional_synthesis_max_per_iteration_default(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PROVISIONAL_SYNTHESIS_MAX_PER_ITERATION", raising=False)
    from genie_space_optimizer.common.config import provisional_synthesis_max_per_iteration
    assert provisional_synthesis_max_per_iteration() == 3
