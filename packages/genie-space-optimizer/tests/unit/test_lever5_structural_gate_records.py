"""Cycle 8 Bug 1 Phase 3b Task B — lever5_structural_gate_records producer.

The Lever 5 structural gate at ``optimizer.py:13961-13971`` silently
zeroes ``instruction_sections`` and ``instruction_guidance`` when the
dominant cluster root cause is SQL-shape but no ``example_sql`` is
attached. Phase B's operator transcript renders nothing for the drop
because no ``DecisionRecord`` is emitted. This producer mirrors
``blast_radius_decision_records`` and surfaces every Lever 5 drop as a
``GATE_DECISION`` / ``DROPPED`` / ``RCA_UNGROUNDED`` row in the
``Proposal Survival And Gate Drops`` section of the operator transcript.

Plan: ``docs/2026-05-04-cycle8-bug1-phase3b-lever5-structural-gate-rerouting-plan.md``
Task B.
"""
from __future__ import annotations


def _drop(
    ag_id: str = "AG_DECOMPOSED_H001",
    root_causes: tuple[str, ...] = ("wrong_aggregation",),
    *,
    had_example_sqls: bool = False,
) -> dict:
    return {
        "ag_id": ag_id,
        "source_clusters": ("H001",),
        "root_causes": tuple(sorted(root_causes)),
        "target_lever": 5,
        "had_example_sqls": had_example_sqls,
        "instruction_sections_dropped": True,
        "instruction_guidance_dropped": False,
    }


def test_one_record_per_drop_with_correct_decision_shape() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = lever5_structural_gate_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="wrong_aggregation",
        target_qids=("gs_024",),
        drops=[_drop()],
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.decision_type == DecisionType.GATE_DECISION
    assert rec.outcome == DecisionOutcome.DROPPED
    assert rec.reason_code == ReasonCode.RCA_UNGROUNDED
    assert rec.gate == "lever5_structural_gate"
    assert rec.ag_id == "AG_DECOMPOSED_H001"
    assert rec.rca_id == "rca_h001"
    assert rec.root_cause == "wrong_aggregation"


def test_metrics_carry_lever5_signals() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )

    records = lever5_structural_gate_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_X",
        rca_id="rca_x",
        root_cause="wrong_aggregation",
        target_qids=("q1",),
        drops=[_drop(root_causes=("wrong_aggregation",), had_example_sqls=False)],
    )
    assert records[0].metrics["root_causes"] == ["wrong_aggregation"]
    assert records[0].metrics["target_lever"] == 5
    assert records[0].metrics["had_example_sqls"] is False


def test_target_qids_mirror_ags_affected_questions() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )

    target_qids = ("gs_024", "gs_025")
    records = lever5_structural_gate_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        rca_id="rca_1",
        root_cause="wrong_aggregation",
        target_qids=target_qids,
        drops=[_drop()],
    )
    assert records[0].target_qids == target_qids
    assert records[0].affected_qids == target_qids


def test_next_action_rerouting_string() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )

    records = lever5_structural_gate_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        rca_id="rca_1",
        root_cause="wrong_aggregation",
        target_qids=("q1",),
        drops=[_drop()],
    )
    assert records[0].next_action == (
        "Re-route via Lever 6 (sql_snippet) or attach example_sql via "
        "cluster-driven synthesis"
    )


def test_empty_drops_yields_empty_list() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )

    assert lever5_structural_gate_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        rca_id="",
        root_cause="",
        target_qids=(),
        drops=(),
    ) == []
