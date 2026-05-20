"""Plan 11 — marker builder output and JSON-parseability tests."""
import json
import pytest


def _parse_marker(line: str) -> tuple[str, dict]:
    name, _, payload_str = line.partition(" ")
    return name, json.loads(payload_str)


def test_stage1_diagnosis_marker_happy_path():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_diagnosis_marker,
    )
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=3,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="top-N collapsed",
        confidence="high",
        blame_set_size=2,
        evidence_summary_chars=312,
        duration_ms=1840,
        tokens_input=1200,
        tokens_output=340,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_STAGE1_DIAGNOSIS_V1"
    assert payload["qid"] == "gs_009"
    assert payload["outcome"] == "diagnosed"
    assert payload["rca_kind_label"] == "top-N collapsed"


def test_stage2_clustering_marker():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage2_clustering_marker,
    )
    line = plan11_stage2_clustering_marker(
        optimization_run_id="run_x",
        iteration=1,
        namespace="hard",
        outcome="clustered",
        input_qids_count=2,
        clusters_count=2,
        cluster_ids=["H001", "H002"],
        duration_ms=900,
        tokens_input=800,
        tokens_output=200,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_STAGE2_CLUSTERING_V1"
    assert payload["clusters_count"] == 2
    assert payload["cluster_ids"] == ["H001", "H002"]


def test_stage3_synthesis_marker():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )
    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001_lever5b",
        cluster_id="H001",
        outcome="synthesized",
        proposals_count=2,
        proposal_ids=["intent_001", "intent_002"],
        patch_types=["add_example_sql", "update_instruction_section"],
        target_qids_union=["gs_009"],
        duration_ms=2100,
        tokens_input=1500,
        tokens_output=600,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_STAGE3_SYNTHESIS_V1"
    assert payload["proposals_count"] == 2


def test_repair_loop_marker():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_repair_loop_marker,
    )
    line = plan11_repair_loop_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001_lever5b",
        cluster_id="H001",
        patch_id="intent_001",
        attempt=1,
        max_attempts=2,
        outcome="repaired",
        error_kinds=["genie_schema"],
        error_count=1,
        duration_ms=900,
        tokens_input=700,
        tokens_output=250,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_REPAIR_LOOP_V1"
    assert payload["outcome"] == "repaired"
    assert payload["error_kinds"] == ["genie_schema"]


def test_narrow_replacement_marker():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_narrow_replacement_marker,
    )
    line = plan11_narrow_replacement_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001_lever5b",
        cluster_id="H001",
        patch_id="intent_002",
        attempt=1,
        max_attempts=2,
        outcome="narrowed",
        collateral_qids_count=3,
        target_qids=["gs_009"],
        duration_ms=1100,
        tokens_input=900,
        tokens_output=300,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_NARROW_REPLACEMENT_V1"
    assert payload["collateral_qids_count"] == 3
