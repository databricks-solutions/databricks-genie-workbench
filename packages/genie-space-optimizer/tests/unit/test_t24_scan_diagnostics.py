"""Phase 3c Task C — per-proposal high_collateral_risk diagnostic.

The T2.4 scan summary today shows only the count of high-risk proposals
and a 5-qid sample. To diagnose whether the threshold (>= 2 * affected)
is correctly tuned, operators need the full dependents list and the
threshold value per proposal.
"""
from __future__ import annotations

import logging

from genie_space_optimizer.optimization.harness import (
    _t24_counterfactual_scan,
)


def _ag(affected: list[str]) -> dict:
    return {"id": "AG_TEST_DIAG", "affected_questions": list(affected)}


def _benchmark(qid: str, tables: list[str]) -> dict:
    return {"id": qid, "required_tables": list(tables)}


def test_emits_per_proposal_threshold_log_for_each_high_risk_proposal(caplog) -> None:
    snippet = {
        "type": "add_sql_snippet_measure",
        "proposal_id": "P_HIGH",
        "target": "catalog.schema.tkt_payment",
    }
    benchmarks = [
        _benchmark("gs_003", ["catalog.schema.tkt_payment"]),
        _benchmark("gs_005", ["catalog.schema.tkt_payment"]),
    ]
    with caplog.at_level(logging.INFO, logger="genie_space_optimizer.optimization.harness"):
        _t24_counterfactual_scan(
            all_proposals=[snippet],
            benchmarks=benchmarks,
            ag=_ag(["gs_024"]),
            prev_failure_qids={"gs_024"},
        )
    matching = [r for r in caplog.records if "high_collateral_risk" in r.getMessage()]
    assert matching, "expected at least one high_collateral_risk log line"
    msg = matching[0].getMessage()
    assert "P_HIGH" in msg
    assert "tkt_payment" in msg
    assert "gs_003" in msg
    assert "gs_005" in msg
    assert "threshold=2" in msg


def test_does_not_emit_log_when_no_proposals_are_high_risk(caplog) -> None:
    snippet = {
        "type": "add_sql_snippet_measure",
        "proposal_id": "P_LOW",
        "target": "catalog.schema.tkt_payment",
    }
    benchmarks = [
        _benchmark("gs_003", ["catalog.schema.tkt_payment"]),
    ]
    with caplog.at_level(logging.INFO, logger="genie_space_optimizer.optimization.harness"):
        _t24_counterfactual_scan(
            all_proposals=[snippet],
            benchmarks=benchmarks,
            ag=_ag(["gs_024"]),
            prev_failure_qids={"gs_024"},
        )
    matching = [r for r in caplog.records if "high_collateral_risk" in r.getMessage()]
    assert matching == []
