"""Cover the wire-format of AG_RETIRED records from a plateau resolver result."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionRecord,
    DecisionType,
    ReasonCode,
)
from genie_space_optimizer.optimization.rca_terminal import (
    resolve_terminal_on_plateau,
)


def test_ag_retired_record_shape_matches_resolver_output():
    decision = resolve_terminal_on_plateau(
        quarantined_qids=set(),
        current_hard_qids={"q9", "q24"},
        regression_debt_qids=set(),
        sql_delta_qids=set(),
        pending_diagnostic_ags=[
            {
                "id": "AG_DECOMPOSED_H003",
                "_stable_signature": ("AG_DECOMPOSED_H003", ("q16",)),
                "affected_questions": ["q16"],
            }
        ],
    )

    records = [
        DecisionRecord(
            run_id="opt-run-id",
            iteration=4,
            decision_type=DecisionType.AG_RETIRED,
            outcome=DecisionOutcome.RETIRED,
            reason_code=ReasonCode.AG_TARGET_NO_LONGER_HARD,
            ag_id=ag_id,
            target_qids=qids,
            affected_qids=qids,
            reason_detail=(
                f"AG {ag_id} retired at plateau because target qids "
                f"{list(qids)} are no longer in the live hard-failure set."
            ),
        )
        for ag_id, qids in decision.retired_ags
    ]

    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.AG_RETIRED
    assert rec.outcome == DecisionOutcome.RETIRED
    assert rec.reason_code == ReasonCode.AG_TARGET_NO_LONGER_HARD
    assert rec.ag_id == "AG_DECOMPOSED_H003"
    assert rec.target_qids == ("q16",)
    assert "q16" in rec.reason_detail
