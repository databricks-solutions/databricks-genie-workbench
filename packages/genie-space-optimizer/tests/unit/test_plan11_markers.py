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
    # Trial 17.1 — the four new lever-selection fields are present on
    # every emit (default empty when caller omits them).
    for key in (
        "selected_levers",
        "expected_behavioral_changes",
        "fallback_levers",
        "bundle_ids",
    ):
        assert key in payload, (
            f"Trial 17.1 telemetry field {key!r} missing from "
            "GSO_PLAN11_STAGE3_SYNTHESIS_V1 payload"
        )
        assert payload[key] == []


def test_stage3_synthesis_marker_carries_trial17_lever_selection():
    """Trial 17.1 — when the LLM emits ``selected_lever`` etc on each
    proposal, the marker must surface those values index-parallel to
    ``proposal_ids`` so operators can see *which lever was picked and
    why* from structured logs alone.
    """
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
        patch_types=["add_sql_snippet_filter", "add_example_sql"],
        target_qids_union=["gs_009"],
        duration_ms=2100,
        tokens_input=1500,
        tokens_output=600,
        selected_levers=["sql_expressions", "instructions_example_sql"],
        expected_behavioral_changes=[
            "Use snippet filter to project top-N rows instead of all rows.",
            "Anchor the LLM to an example SQL with LIMIT 10 / ORDER BY.",
        ],
        fallback_levers=["instructions_prose", ""],
        bundle_ids=["bundle_topN", "bundle_topN"],
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_STAGE3_SYNTHESIS_V1"
    assert payload["selected_levers"] == [
        "sql_expressions",
        "instructions_example_sql",
    ]
    assert payload["fallback_levers"] == ["instructions_prose", ""]
    assert payload["bundle_ids"] == ["bundle_topN", "bundle_topN"]
    assert payload["expected_behavioral_changes"][0].startswith(
        "Use snippet filter"
    )


def test_stage3_synthesis_marker_rejects_non_parallel_lever_arrays():
    """Index-parallel contract: when supplied, each lever array must
    have the same length as ``proposal_ids`` — silent misalignment
    would make the marker unusable for postmortems."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )
    import pytest
    with pytest.raises(ValueError, match="selected_levers"):
        plan11_stage3_synthesis_marker(
            optimization_run_id="run_x",
            iteration=1,
            ag_id="AG_H001",
            cluster_id="H001",
            outcome="synthesized",
            proposals_count=2,
            proposal_ids=["a", "b"],
            patch_types=["add_example_sql", "add_instruction"],
            target_qids_union=["gs_009"],
            selected_levers=["only_one_lever"],
        )


def test_stage3_synthesis_marker_truncates_long_behavioral_change():
    """Behavioral-change strings can run long when the LLM justifies
    its lever pick verbosely. The marker caps each entry so the line
    stays parseable; the untruncated text lives on ``RepairProposal``.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        _TRIAL17_BEHAVIORAL_CHANGE_MAX_CHARS,
        plan11_stage3_synthesis_marker,
    )
    long_text = "x" * (_TRIAL17_BEHAVIORAL_CHANGE_MAX_CHARS + 50)
    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG_H001",
        cluster_id="H001",
        outcome="synthesized",
        proposals_count=1,
        proposal_ids=["a"],
        patch_types=["add_instruction"],
        target_qids_union=["gs_009"],
        selected_levers=["instructions_prose"],
        expected_behavioral_changes=[long_text],
    )
    _, payload = _parse_marker(line)
    assert (
        len(payload["expected_behavioral_changes"][0])
        == _TRIAL17_BEHAVIORAL_CHANGE_MAX_CHARS
    )


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
