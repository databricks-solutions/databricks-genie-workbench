"""Phase 3c Task A — T2.4 must stamp passing_dependents on every proposal
it visits, including instruction-rewrite proposals that have no target
table. Without the stamp, _split_rewrite_instruction_patch propagates None
to children and the instruction-scope gate fails them all loud.

See docs/2026-05-05-cycle8-bug1-phase3c-blast-radius-instruction-scope-deadlock-plan.md
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _t24_counterfactual_scan,
)


def _ag(affected: list[str]) -> dict:
    return {"id": "AG_TEST", "affected_questions": list(affected)}


def _benchmark(qid: str, tables: list[str]) -> dict:
    return {"id": qid, "required_tables": list(tables)}


def test_no_target_proposal_gets_empty_passing_dependents() -> None:
    rewrite = {
        "type": "rewrite_instruction",
        "proposal_id": "P001",
        "proposed_value": "QUERY RULES:\n- be careful\n",
    }
    benchmarks = [
        _benchmark("gs_001", ["catalog.schema.tkt_payment"]),
        _benchmark("gs_002", ["catalog.schema.tkt_coupon"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[rewrite],
        benchmarks=benchmarks,
        ag=_ag(["gs_024"]),
        prev_failure_qids={"gs_024"},
    )
    assert rewrite["passing_dependents"] == []
    assert "high_collateral_risk" not in rewrite


def test_target_proposal_still_gets_real_dependents() -> None:
    snippet = {
        "type": "add_sql_snippet_measure",
        "proposal_id": "P002",
        "target": "catalog.schema.tkt_payment",
    }
    benchmarks = [
        _benchmark("gs_003", ["catalog.schema.tkt_payment"]),
        _benchmark("gs_004", ["catalog.schema.tkt_coupon"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_024"]),
        prev_failure_qids={"gs_024"},
    )
    assert snippet["passing_dependents"] == ["gs_003"]


def test_target_proposal_with_no_dependents_gets_empty_list() -> None:
    snippet = {
        "type": "add_sql_snippet_measure",
        "proposal_id": "P003",
        "target": "catalog.schema.unknown_table",
    }
    benchmarks = [
        _benchmark("gs_003", ["catalog.schema.tkt_payment"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_024"]),
        prev_failure_qids={"gs_024"},
    )
    assert snippet["passing_dependents"] == []


def test_high_collateral_risk_threshold_unchanged() -> None:
    snippet = {
        "type": "add_sql_snippet_measure",
        "proposal_id": "P004",
        "target": "catalog.schema.tkt_payment",
    }
    benchmarks = [
        _benchmark("gs_003", ["catalog.schema.tkt_payment"]),
        _benchmark("gs_005", ["catalog.schema.tkt_payment"]),
        _benchmark("gs_007", ["catalog.schema.tkt_payment"]),
    ]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=benchmarks,
        ag=_ag(["gs_024"]),
        prev_failure_qids={"gs_024"},
    )
    assert snippet["passing_dependents"] == ["gs_003", "gs_005", "gs_007"]
    assert snippet["high_collateral_risk"] is True
