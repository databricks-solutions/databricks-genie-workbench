"""Phase 3 — verify 6 new ReasonCodes are present and unique."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode


def test_iteration_feedback_built_reason_code_exists() -> None:
    assert ReasonCode.ITERATION_FEEDBACK_BUILT.value == "iteration_feedback_built"


def test_near_miss_reflection_emitted_reason_code_exists() -> None:
    assert ReasonCode.NEAR_MISS_REFLECTION_EMITTED.value == "near_miss_reflection_emitted"


def test_near_miss_ag_shape_repeated_reason_code_exists() -> None:
    assert ReasonCode.NEAR_MISS_AG_SHAPE_REPEATED.value == "near_miss_ag_shape_repeated"


def test_near_miss_ag_shape_differs_reason_code_exists() -> None:
    assert ReasonCode.NEAR_MISS_AG_SHAPE_DIFFERS.value == "near_miss_ag_shape_differs"


def test_soft_evidence_lifted_to_kit_reason_code_exists() -> None:
    assert ReasonCode.SOFT_EVIDENCE_LIFTED_TO_KIT.value == "soft_evidence_lifted_to_kit"


def test_soft_signal_trend_report_reason_code_exists() -> None:
    assert ReasonCode.SOFT_SIGNAL_TREND_REPORT.value == "soft_signal_trend_report"


def test_phase_3_reason_codes_unique_values() -> None:
    """Phase 3 codes must not collide with the existing namespace."""
    phase_3 = {
        "iteration_feedback_built",
        "near_miss_reflection_emitted",
        "near_miss_ag_shape_repeated",
        "near_miss_ag_shape_differs",
        "soft_evidence_lifted_to_kit",
        "soft_signal_trend_report",
    }
    other = {rc.value for rc in ReasonCode if rc.value not in phase_3}
    assert phase_3.isdisjoint(other)
