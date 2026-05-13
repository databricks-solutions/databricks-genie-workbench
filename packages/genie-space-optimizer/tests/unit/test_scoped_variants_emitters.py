"""Tests for Section C scoped-variant decision-record emitters."""

from __future__ import annotations


def test_hub_table_scoped_variant_generated_record_carries_parent_and_child_pids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        hub_table_scoped_variant_generated_record,
    )

    rec = hub_table_scoped_variant_generated_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        parent_pid="L1:P003#1",
        scoped_pid="L1:P003#1_scoped",
        target_qids=("gs_026",),
        target_table="mv_7now_fact_sales",
    )
    assert rec.reason_code == "hub_table_scoped_variant_generated"
    assert "L1:P003#1" in rec.evidence_refs[0]
    assert "L1:P003#1_scoped" in rec.next_action


def test_hub_table_no_scoped_variant_available_record_carries_parent_pid() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        hub_table_no_scoped_variant_available_record,
    )

    rec = hub_table_no_scoped_variant_available_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        parent_pid="L1:P003#1",
        target_qids=("gs_026",),
        target_table="mv_7now_fact_sales",
    )
    assert rec.reason_code == "hub_table_no_scoped_variant_available"
    assert "L1:P003#1" in rec.evidence_refs[0]


def test_kit_risk_downgraded_by_scoped_variant_record_carries_kit_id() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        kit_risk_downgraded_by_scoped_variant_record,
    )

    rec = kit_risk_downgraded_by_scoped_variant_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        kit_id="kit_xyz",
        original_risk_class="high",
        downgraded_to="medium",
        target_qids=("gs_026",),
    )
    assert rec.reason_code == "kit_risk_downgraded_by_scoped_variant"
    assert "high" in rec.expected_effect
    assert "medium" in rec.expected_effect
