"""Pin enum values so renames are CI-detected."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def test_decision_type_includes_ag_retired():
    assert DecisionType.AG_RETIRED.value == "ag_retired"


def test_decision_outcome_includes_retired():
    assert DecisionOutcome.RETIRED.value == "retired"


def test_reason_code_includes_ag_target_no_longer_hard():
    assert ReasonCode.AG_TARGET_NO_LONGER_HARD.value == "ag_target_no_longer_hard"


def test_rca_ungrounded_reason_enum_values():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    assert RcaUngroundedReason.NO_PARENT_RCA.value == "no_parent_rca"
    assert RcaUngroundedReason.NO_FINDINGS.value == "no_findings"
    assert RcaUngroundedReason.NO_TERM_OVERLAP.value == "no_term_overlap"
    assert RcaUngroundedReason.NO_CAUSAL_TARGET.value == "no_causal_target"
    assert RcaUngroundedReason.MISSING_TARGET_QIDS.value == "missing_target_qids"
    assert RcaUngroundedReason.NO_EVIDENCE_AVAILABLE.value == "no_evidence_available"
    assert RcaUngroundedReason.UNKNOWN.value == "unknown"


def test_rca_ungrounded_reason_membership_count():
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    # Pin the seven-member surface so an inadvertent addition fails
    # the test until the policy table (Task 4) is updated alongside.
    assert len(list(RcaUngroundedReason)) == 7


def test_rca_regeneration_succeeded_reason_code_exists():
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode

    assert ReasonCode.RCA_REGENERATION_SUCCEEDED.value == "rca_regeneration_succeeded"


def test_rca_classified_ungrounded_reason_code_exists():
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode

    assert ReasonCode.RCA_CLASSIFIED_UNGROUNDED.value == "rca_classified_ungrounded"


def test_rca_card_self_check_failed_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.RCA_CARD_SELF_CHECK_FAILED.value == "rca_card_self_check_failed"


def test_rca_card_llm_skipped_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.RCA_CARD_LLM_SKIPPED.value == "rca_card_llm_skipped"


def test_tier_strict_win_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.TIER_STRICT_WIN.value == "tier_strict_win"


def test_tier_net_win_with_debt_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.TIER_NET_WIN_WITH_DEBT.value == "tier_net_win_with_debt"


def test_tier_diagnostic_hold_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.TIER_DIAGNOSTIC_HOLD.value == "tier_diagnostic_hold"


def test_tier_loss_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.TIER_LOSS.value == "tier_loss"


# Phase 2 Section A — Repair Planner reason codes.
def test_cluster_archetype_classified_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.CLUSTER_ARCHETYPE_CLASSIFIED.value == "cluster_archetype_classified"


def test_repair_planner_no_archetype_match_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.REPAIR_PLANNER_NO_ARCHETYPE_MATCH.value == "repair_planner_no_archetype_match"


def test_repair_plan_propagation_guarded_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.REPAIR_PLAN_PROPAGATION_GUARDED.value == "repair_plan_propagation_guarded"


# Phase 2 Section B — Kit-aware patch cap.
def test_kit_safety_summary_built_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.KIT_SAFETY_SUMMARY_BUILT.value == "kit_safety_summary_built"


def test_kit_level_gate_rejected_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.KIT_LEVEL_GATE_REJECTED.value == "kit_level_gate_rejected"


def test_repair_kit_no_safe_variant_available_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.REPAIR_KIT_NO_SAFE_VARIANT_AVAILABLE.value == "repair_kit_no_safe_variant_available"


def test_kit_atomicity_violation_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.KIT_ATOMICITY_VIOLATION.value == "kit_atomicity_violation"


# Phase 2 Section C — Hub-table scoped variants.
def test_hub_table_scoped_variant_generated_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.HUB_TABLE_SCOPED_VARIANT_GENERATED.value == "hub_table_scoped_variant_generated"


def test_hub_table_no_scoped_variant_available_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.HUB_TABLE_NO_SCOPED_VARIANT_AVAILABLE.value == "hub_table_no_scoped_variant_available"


def test_kit_risk_downgraded_by_scoped_variant_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.KIT_RISK_DOWNGRADED_BY_SCOPED_VARIANT.value == "kit_risk_downgraded_by_scoped_variant"


# Phase 2 Section D — Strategist coverage re-call.
def test_strategist_coverage_recall_invoked_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.STRATEGIST_COVERAGE_RECALL_INVOKED.value == "strategist_coverage_recall_invoked"


def test_strategist_coverage_recall_result_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.STRATEGIST_COVERAGE_RECALL_RESULT.value == "strategist_coverage_recall_result"


# Phase 2 Section E — In-loop archetype learning.
def test_unmatched_pattern_record_emitted_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.UNMATCHED_PATTERN_RECORD_EMITTED.value == "unmatched_pattern_record_emitted"


def test_pattern_candidate_detected_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.PATTERN_CANDIDATE_DETECTED.value == "pattern_candidate_detected"


def test_provisional_archetype_synthesized_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.PROVISIONAL_ARCHETYPE_SYNTHESIZED.value == "provisional_archetype_synthesized"


def test_provisional_archetype_synthesis_declined_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.PROVISIONAL_ARCHETYPE_SYNTHESIS_DECLINED.value == "provisional_archetype_synthesis_declined"


def test_provisional_archetype_trial_outcome_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.PROVISIONAL_ARCHETYPE_TRIAL_OUTCOME.value == "provisional_archetype_trial_outcome"


def test_confirmed_in_run_archetype_promoted_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.CONFIRMED_IN_RUN_ARCHETYPE_PROMOTED.value == "confirmed_in_run_archetype_promoted"


def test_cross_run_promotion_candidate_recorded_reason_code_present() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode
    assert ReasonCode.CROSS_RUN_PROMOTION_CANDIDATE_RECORDED.value == "cross_run_promotion_candidate_recorded"
