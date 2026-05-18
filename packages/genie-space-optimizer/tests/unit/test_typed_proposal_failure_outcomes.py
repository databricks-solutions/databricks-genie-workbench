"""Typed proposal-failure outcomes.

Today the optimizer collapses three distinct failure modes into the
same "no candidate state" path:
  * proposer returned zero proposals
  * proposer returned a proposal but the lever-5 structural gate
    dropped it (SQL-shape RCA + no example_sql)
  * synthesis attempted but no fallback existed

This suite pins the new ReasonCode values and the emitter helpers
that distinguish them.
"""
from __future__ import annotations


def test_reason_code_proposal_generation_empty_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.PROPOSAL_GENERATION_EMPTY.value == "proposal_generation_empty"


def test_reason_code_structural_gate_dropped_instruction_only_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert (
        ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
        == "structural_gate_dropped_instruction_only"
    )


def test_reason_code_no_structural_candidate_exists() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    assert ReasonCode.NO_STRUCTURAL_CANDIDATE.value == "no_structural_candidate"


def test_proposal_generation_empty_record_shape() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generation_empty_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    rec = proposal_generation_empty_record(
        run_id="r1",
        iteration=3,
        ag_id="AG_COVERAGE_H001",
        cluster_id="H001",
        rca_id="rca_h001",
        root_cause="wrong_aggregation",
        target_qids=("gs_026",),
    )
    assert rec.decision_type == DecisionType.PROPOSAL_GENERATED
    assert rec.outcome == DecisionOutcome.DROPPED
    assert rec.reason_code == ReasonCode.PROPOSAL_GENERATION_EMPTY
    assert rec.ag_id == "AG_COVERAGE_H001"
    assert rec.cluster_id == "H001"
    assert rec.target_qids == ("gs_026",)


def test_lever5_structural_gate_record_uses_specific_reason() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    drops = [
        {
            "ag_id": "AG_COVERAGE_H002",
            "cluster_id": "H002",
            "root_causes": ["missing_filter"],
            "target_qids": ["gs_021"],
            "patch_type": "rewrite_instruction",
            "proposal_id": "P001",
        }
    ]
    records = lever5_structural_gate_records(
        run_id="r1",
        iteration=2,
        ag_id="AG_COVERAGE_H002",
        rca_id="rca_h002",
        root_cause="missing_filter",
        target_qids=["gs_021"],
        drops=drops,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec.reason_code == ReasonCode.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY


def test_no_structural_candidate_record_shape() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        no_structural_candidate_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
        DecisionOutcome,
        ReasonCode,
    )

    rec = no_structural_candidate_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_COVERAGE_H002",
        cluster_id="H002",
        rca_id="rca_h002",
        root_cause="missing_filter",
        target_qids=("gs_021",),
        attempted_archetypes=("ordered_list_by_metric",),
    )
    assert rec.decision_type == DecisionType.PROPOSAL_GENERATED
    assert rec.outcome == DecisionOutcome.DROPPED
    assert rec.reason_code == ReasonCode.NO_STRUCTURAL_CANDIDATE
    assert "ordered_list_by_metric" in rec.reason_detail


def test_marker_round_trip_proposal_generation_empty() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        proposal_generation_empty_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_proposal_generation_empty_marker,
    )

    line = proposal_generation_empty_marker(
        ag_id="AG_COVERAGE_H001",
        iteration=3,
        target_qids=["gs_026"],
    )
    parsed = parse_proposal_generation_empty_marker(line)
    assert parsed["ag_id"] == "AG_COVERAGE_H001"
    assert parsed["iteration"] == 3
    assert parsed["target_qids"] == ["gs_026"]


def test_marker_round_trip_structural_gate_dropped() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        structural_gate_dropped_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_structural_gate_dropped_marker,
    )

    line = structural_gate_dropped_marker(
        ag_id="AG_COVERAGE_H002",
        iteration=2,
        root_causes=["missing_filter"],
        target_qids=["gs_021"],
    )
    parsed = parse_structural_gate_dropped_marker(line)
    assert parsed["root_causes"] == ["missing_filter"]


def test_marker_round_trip_no_structural_candidate() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        no_structural_candidate_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_no_structural_candidate_marker,
    )

    line = no_structural_candidate_marker(
        ag_id="AG_COVERAGE_H002",
        iteration=2,
        attempted_archetypes=["ordered_list_by_metric"],
    )
    parsed = parse_no_structural_candidate_marker(line)
    assert parsed["attempted_archetypes"] == ["ordered_list_by_metric"]


def test_no_structural_candidate_record_metrics_contains_attempted_archetypes() -> None:
    """Phase 0.5 (Bug 3) — the metrics dict must surface attempted_archetypes
    so the marker emit site at harness.py:23035-23043 can read it via
    record.metrics["attempted_archetypes"] instead of falling back to ()."""
    from genie_space_optimizer.optimization.decision_emitters import (
        no_structural_candidate_record,
    )

    rec = no_structural_candidate_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_COVERAGE_H002",
        cluster_id="H002",
        rca_id="rca_h002",
        root_cause="missing_filter",
        target_qids=("gs_021",),
        attempted_archetypes=("ordered_list_by_metric", "single_row_top_n"),
    )

    assert rec.metrics.get("attempted_archetypes") == [
        "ordered_list_by_metric", "single_row_top_n",
    ], (
        "Bug 3 fix: producer must surface attempted_archetypes in metrics "
        "so the harness marker emit can route it to the stdout marker payload."
    )
    # Defense: existing keys must remain.
    assert rec.metrics.get("proposals_total") == 0
    assert rec.metrics.get("synthesis_attempted") is True


def test_no_structural_candidate_record_metrics_empty_archetypes_list_not_missing_key() -> None:
    """Defense: when no archetypes were attempted, the key must still be
    present with an empty list — not absent.

    Phase 1.5 (2026-05-17) — the refuse-on-empty invariant requires
    either a non-empty ``attempted_archetypes`` OR a non-empty
    ``skipped_reason``. This test now passes a typed
    ``skipped_reason`` (the synthesizer always knows one) and still
    asserts the empty-archetypes-list shape.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        no_structural_candidate_record,
    )

    rec = no_structural_candidate_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_COVERAGE_H002",
        attempted_archetypes=(),
        skipped_reason="missing_rca_card",
    )

    assert "attempted_archetypes" in rec.metrics, (
        "Key must be present even when the tuple is empty — absence vs. "
        "empty-list is the difference between 'producer never tried "
        "anything' and 'producer tried zero archetypes'."
    )
    assert rec.metrics["attempted_archetypes"] == []
