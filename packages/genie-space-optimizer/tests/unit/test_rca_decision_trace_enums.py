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
