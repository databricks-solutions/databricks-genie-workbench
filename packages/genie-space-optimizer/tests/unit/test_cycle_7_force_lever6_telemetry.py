"""Cycle 7 T2 — typed decision record + stdout marker for forced L6.

The postmortem skill must see this event on BOTH the typed channel
(decision_records[*].reason_code) AND the stdout channel
(GSO_LEVER6_FORCED_V1) per the run-output contract spine §52-61.
"""
from __future__ import annotations

import json


def test_reason_code_value_is_stable() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert (
        ReasonCode.LEVER6_FORCED_FOR_SQL_SHAPE_RCA.value
        == "lever6_forced_for_sql_shape_rca"
    )


def test_record_emitter_shape() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_forced_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    rec = lever6_forced_record(
        run_id="run-abc",
        iteration=2,
        ag_id="AG2",
        cluster_id="H001",
        rca_id="RCA-1",
        root_cause="missing_filter",
        target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
        recommended_levers=(3, 5, 6),
        existing_patch_types=("rewrite_instruction",),
    )
    assert rec.run_id == "run-abc"
    assert rec.iteration == 2
    assert rec.decision_type is DecisionType.PROPOSAL_GENERATED
    assert rec.outcome is DecisionOutcome.INFO
    assert rec.reason_code is ReasonCode.LEVER6_FORCED_FOR_SQL_SHAPE_RCA
    assert rec.ag_id == "AG2"
    assert rec.cluster_id == "H001"
    assert rec.rca_id == "RCA-1"
    assert rec.root_cause == "missing_filter"
    assert rec.target_qids == (
        "airline_ticketing_and_fare_analysis_gs_009",
    )
    assert "recommended_levers=3,5,6" in rec.reason_detail
    assert "existing_patch_types=rewrite_instruction" in rec.reason_detail


def test_marker_emitter_shape() -> None:
    from genie_space_optimizer.common.mlflow_markers import (
        lever6_forced_marker,
    )

    line = lever6_forced_marker(
        optimization_run_id="run-abc",
        iteration=2,
        ag_id="AG2",
        cluster_id="H001",
        root_cause="missing_filter",
        target_qids=("gs_009",),
        recommended_levers=(3, 5, 6),
        existing_patch_types=("rewrite_instruction",),
    )
    assert line.startswith("GSO_LEVER6_FORCED_V1 ")
    payload = json.loads(line[len("GSO_LEVER6_FORCED_V1 "):])
    assert payload == {
        "optimization_run_id": "run-abc",
        "iteration": 2,
        "ag_id": "AG2",
        "cluster_id": "H001",
        "root_cause": "missing_filter",
        "target_qids": ["gs_009"],
        "recommended_levers": [3, 5, 6],
        "existing_patch_types": ["rewrite_instruction"],
    }


def test_marker_parser_round_trip() -> None:
    from genie_space_optimizer.common.mlflow_markers import (
        lever6_forced_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_lever6_forced_marker,
    )

    line = lever6_forced_marker(
        optimization_run_id="run-abc",
        iteration=2,
        ag_id="AG2",
        cluster_id="H001",
        root_cause="missing_filter",
        target_qids=("gs_009",),
        recommended_levers=(3, 5, 6),
        existing_patch_types=("rewrite_instruction",),
    )
    parsed = parse_lever6_forced_marker(line)
    assert parsed["optimization_run_id"] == "run-abc"
    assert parsed["iteration"] == 2
    assert parsed["ag_id"] == "AG2"
    assert parsed["cluster_id"] == "H001"
    assert parsed["root_cause"] == "missing_filter"
    assert parsed["target_qids"] == ["gs_009"]
    assert parsed["recommended_levers"] == [3, 5, 6]
    assert parsed["existing_patch_types"] == ["rewrite_instruction"]


def test_parser_tolerates_missing_optional_fields() -> None:
    """Older runs (pre-Cycle 7) won't emit this marker; the parser
    must not crash on synthetic ``GSO_LEVER6_FORCED_V1 {}``."""
    from genie_space_optimizer.tools.marker_parser import (
        parse_lever6_forced_marker,
    )
    parsed = parse_lever6_forced_marker("GSO_LEVER6_FORCED_V1 {}")
    assert parsed["optimization_run_id"] == ""
    assert parsed["iteration"] == 0
    assert parsed["ag_id"] == ""
    assert parsed["cluster_id"] == ""
    assert parsed["root_cause"] == ""
    assert parsed["target_qids"] == []
    assert parsed["recommended_levers"] == []
    assert parsed["existing_patch_types"] == []
