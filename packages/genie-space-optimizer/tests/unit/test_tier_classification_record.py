from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_policy import (
    AcceptedClass,
    TierVerdict,
)
from genie_space_optimizer.optimization.decision_emitters import (
    tier_classification_record,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def test_strict_win_record_outcome_accepted() -> None:
    verdict = TierVerdict(
        accepted_class=AcceptedClass.STRICT_WIN,
        accept=True,
        debt_classification={},
        reflection_payload={},
    )
    rec = tier_classification_record(
        run_id="r1", iteration=1, ag_id="AG1",
        target_qids=("gs_026",), verdict=verdict,
    )
    assert rec.decision_type == DecisionType.ACCEPTANCE_DECIDED
    assert rec.outcome == DecisionOutcome.ACCEPTED
    assert rec.reason_code == ReasonCode.TIER_STRICT_WIN
    assert rec.metrics["accepted_class"] == "strict_win"


def test_diagnostic_hold_record_outcome_rolled_back_with_reflection() -> None:
    verdict = TierVerdict(
        accepted_class=AcceptedClass.DIAGNOSTIC_HOLD,
        accept=False,
        debt_classification={"unknown_to_hard": ["gs_012"]},
        reflection_payload={
            "fixes_vs_regressions": "fixes=0, regressions=1, floor=2",
            "tripped_net_win_bounds": ["fixes_margin_ok"],
        },
    )
    rec = tier_classification_record(
        run_id="r1", iteration=1, ag_id="AG1",
        target_qids=("gs_026",), verdict=verdict,
    )
    assert rec.outcome == DecisionOutcome.ROLLED_BACK
    assert rec.reason_code == ReasonCode.TIER_DIAGNOSTIC_HOLD
    assert rec.metrics["accepted_class"] == "diagnostic_hold"
    assert rec.metrics["debt_classification"] == {"unknown_to_hard": ["gs_012"]}
    assert "fixes_vs_regressions" in rec.metrics["reflection"]


def test_net_win_with_debt_record_outcome_accepted_with_debt() -> None:
    verdict = TierVerdict(
        accepted_class=AcceptedClass.NET_WIN_WITH_DEBT,
        accept=True,
        debt_classification={"unknown_to_hard": ["gs_x"]},
        reflection_payload={"global_improvement_target_not_fixed": True},
    )
    rec = tier_classification_record(
        run_id="r1", iteration=2, ag_id="AG2",
        target_qids=("gs_y",), verdict=verdict,
    )
    assert rec.outcome == DecisionOutcome.ACCEPTED
    assert rec.reason_code == ReasonCode.TIER_NET_WIN_WITH_DEBT
    assert rec.metrics["debt_classification"] == {"unknown_to_hard": ["gs_x"]}


def test_loss_record_outcome_rolled_back() -> None:
    verdict = TierVerdict(
        accepted_class=AcceptedClass.LOSS,
        accept=False,
        debt_classification={},
        reflection_payload={"delta_pp": -1.2},
    )
    rec = tier_classification_record(
        run_id="r1", iteration=3, ag_id="AG1",
        target_qids=(), verdict=verdict,
    )
    assert rec.outcome == DecisionOutcome.ROLLED_BACK
    assert rec.reason_code == ReasonCode.TIER_LOSS
