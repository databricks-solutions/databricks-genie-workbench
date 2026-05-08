_STDOUT_SAMPLE = """
Some preamble line that is not a marker.
GSO_RUN_MANIFEST_V1 {"databricks_job_id":"j-1","databricks_parent_run_id":"r-1","event":"start","lever_loop_task_run_id":"tr-1","mlflow_experiment_id":"exp-1","optimization_run_id":"opt-abc","space_id":"sp-1"}
[INFO] iteration starting
GSO_ITERATION_SUMMARY_V1 {"accepted_count":2,"decision_record_count":7,"gate_drop_count":1,"iteration":1,"journey_violation_count":0,"optimization_run_id":"opt-abc","rolled_back_count":0,"skipped_count":1}
GSO_PHASE_B_V1 {"decision_record_count":7,"decision_trace_artifact":"phase_b/decision_trace/iter_01.json","decision_validation_count":7,"iteration":1,"operator_transcript_artifact":"phase_b/operator_transcript/iter_01.txt","optimization_run_id":"opt-abc","persist_ok":true,"transcript_chars":4096}
===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===
{"version":1,"iterations":[{"iteration":1,"qids":["q1"]}]}
===PHASE_A_REPLAY_FIXTURE_JSON_END===
GSO_CONVERGENCE_V1 {"best_accuracy":0.84,"iteration_counter":1,"optimization_run_id":"opt-abc","reason":"baseline_met","thresholds_met":true}
GSO_FUTURE_FANCY_V1 {"new_field":42}
"""


def test_parse_markers_returns_typed_log() -> None:
    from genie_space_optimizer.tools.marker_parser import MarkerLog, parse_markers

    log = parse_markers(_STDOUT_SAMPLE)
    assert isinstance(log, MarkerLog)
    assert log.run_manifest is not None
    assert log.run_manifest["optimization_run_id"] == "opt-abc"
    assert len(log.iteration_summaries) == 1
    assert log.iteration_summaries[0]["iteration"] == 1
    assert log.iteration_summaries[0]["accepted_count"] == 2
    assert len(log.phase_b) == 1
    assert log.phase_b[0]["persist_ok"] is True
    assert log.convergence is not None
    assert log.convergence["reason"] == "baseline_met"
    assert "GSO_FUTURE_FANCY_V1" in log.unknown
    assert log.unknown["GSO_FUTURE_FANCY_V1"][0] == {"new_field": 42}


def test_parse_markers_optimization_run_id_resolution() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    log = parse_markers(_STDOUT_SAMPLE)
    assert log.optimization_run_id() == "opt-abc"


def test_parse_markers_unresolved_optimization_run_id() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    log = parse_markers("nothing relevant\nrandom text\n")
    assert log.run_manifest is None
    assert log.optimization_run_id() is None


def test_parse_markers_skips_malformed_payload() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    bad = "GSO_ITERATION_SUMMARY_V1 not-json-at-all"
    log = parse_markers(bad)
    assert log.iteration_summaries == ()
    assert log.parse_errors == ("GSO_ITERATION_SUMMARY_V1: invalid json",)


def test_extract_replay_fixture_returns_dict() -> None:
    from genie_space_optimizer.tools.marker_parser import extract_replay_fixture

    fixture = extract_replay_fixture(_STDOUT_SAMPLE)
    assert fixture == {"version": 1, "iterations": [{"iteration": 1, "qids": ["q1"]}]}


def test_extract_replay_fixture_returns_none_when_absent() -> None:
    from genie_space_optimizer.tools.marker_parser import extract_replay_fixture

    assert extract_replay_fixture("no markers here") is None


def test_parse_markers_extracts_artifact_index_v1() -> None:
    """Phase H Task 8: GSO_ARTIFACT_INDEX_V1 parses into MarkerLog.artifact_index."""
    from genie_space_optimizer.tools.marker_parser import parse_markers
    stdout = (
        'GSO_ARTIFACT_INDEX_V1 {"artifact_index_path": "gso_postmortem_bundle/artifact_index.json", '
        '"iterations": [1, 2], "optimization_run_id": "r1", "parent_bundle_run_id": "br1"}\n'
    )
    log = parse_markers(stdout)
    assert log.artifact_index is not None
    assert log.artifact_index["parent_bundle_run_id"] == "br1"
    assert log.artifact_index["iterations"] == [1, 2]
    assert log.artifact_index["artifact_index_path"] == "gso_postmortem_bundle/artifact_index.json"


def test_parse_markers_artifact_index_absent_when_no_marker() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers
    log = parse_markers("(no markers)")
    assert log.artifact_index is None


def test_artifact_index_marker_emits_valid_marker_line() -> None:
    """Phase H Task 8: artifact_index_marker round-trips through parse_markers."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        artifact_index_marker,
    )
    from genie_space_optimizer.tools.marker_parser import parse_markers

    line = artifact_index_marker(
        optimization_run_id="r1",
        parent_bundle_run_id="br1",
        artifact_index_path="gso_postmortem_bundle/artifact_index.json",
        iterations=[1, 2, 3],
    )
    assert line.startswith("GSO_ARTIFACT_INDEX_V1 ")
    log = parse_markers(line + "\n")
    assert log.artifact_index is not None
    assert log.artifact_index["parent_bundle_run_id"] == "br1"
    assert log.artifact_index["iterations"] == [1, 2, 3]


def test_bundle_assembly_failed_marker_extracted() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    text = (
        "some unrelated stuff\n"
        'GSO_BUNDLE_ASSEMBLY_FAILED_V1 {"error_message": "boom", '
        '"error_type": "RuntimeError", "optimization_run_id": "r1", '
        '"parent_bundle_run_id": "a1"}\n'
        "more unrelated stuff\n"
    )
    markers = parse_markers(text)
    assert len(markers.bundle_assembly_failed) == 1
    failure = markers.bundle_assembly_failed[0]
    assert failure["optimization_run_id"] == "r1"
    assert failure["parent_bundle_run_id"] == "a1"
    assert failure["error_type"] == "RuntimeError"


def test_bundle_assembly_failed_absent_when_no_marker() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    markers = parse_markers("hello world")
    assert markers.bundle_assembly_failed == ()


# Cycle 10 — parser entries
def test_marker_parser_recognizes_lever6_force_llm_declined():
    from genie_space_optimizer.tools.marker_parser import (
        parse_lever6_force_llm_declined_marker,
    )
    line = (
        'GSO_LEVER6_FORCE_LLM_DECLINED_V1 {"ag_id": "AG_X", '
        '"cluster_id": "H004", "iteration": 2, "root_cause": '
        '"missing_filter", "run_id": "r1"}'
    )
    out = parse_lever6_force_llm_declined_marker(line)
    assert out["ag_id"] == "AG_X"
    assert out["cluster_id"] == "H004"
    assert out["iteration"] == 2
    assert out["root_cause"] == "missing_filter"
    assert out["run_id"] == "r1"


def test_marker_parser_recognizes_lever6_force_raised():
    from genie_space_optimizer.tools.marker_parser import (
        parse_lever6_force_raised_marker,
    )
    line = (
        'GSO_LEVER6_FORCE_RAISED_V1 {"ag_id": "AG_X", "cluster_id": '
        '"H004", "exception_repr": "ValueError(\'boom\')", '
        '"iteration": 2, "root_cause": "missing_filter", "run_id": "r1"}'
    )
    out = parse_lever6_force_raised_marker(line)
    assert out["ag_id"] == "AG_X"
    assert out["exception_repr"].startswith("ValueError")


def test_marker_parser_recognizes_narrow_not_applicable():
    from genie_space_optimizer.tools.marker_parser import (
        parse_narrow_not_applicable_marker,
    )
    line = (
        'GSO_NARROW_NOT_APPLICABLE_V1 {"ag_id": "AG_X", "cluster_id": '
        '"H001", "iteration": 3, "original_patch_type": '
        '"add_sql_snippet_measure", "reason": '
        '"patch_type_lacks_where_predicate", "root_cause": '
        '"missing_filter", "run_id": "r1"}'
    )
    out = parse_narrow_not_applicable_marker(line)
    assert out["ag_id"] == "AG_X"
    assert out["original_patch_type"] == "add_sql_snippet_measure"
    assert out["reason"] == "patch_type_lacks_where_predicate"


def test_marker_parser_recognizes_ag_levers_unioned():
    from genie_space_optimizer.tools.marker_parser import (
        parse_ag_levers_unioned_marker,
    )
    line = (
        'GSO_AG_LEVERS_UNIONED_V1 {"ag_id": "AG_X", "cluster_id": '
        '"H001", "iteration": 2, "levers_after": ["3", "5", "6"], '
        '"levers_before": ["5"], "run_id": "r1"}'
    )
    out = parse_ag_levers_unioned_marker(line)
    assert out["ag_id"] == "AG_X"
    assert out["levers_before"] == ["5"]
    assert out["levers_after"] == ["3", "5", "6"]


# Cycle 12-T1 — GSO_RUN_MANIFEST_V2 parsing
import pathlib


def test_parse_markers_captures_run_manifest_v2() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    fixture = pathlib.Path(__file__).parent / "fixtures" / "lever_loop_stdout_v2_minimal.txt"
    log = parse_markers(fixture.read_text())

    assert log.run_manifest is not None
    assert log.run_manifest_v2 is not None
    # V1 still authoritative for legacy keys.
    assert log.run_manifest["optimization_run_id"] == "opt1"
    # V2 carries the new metadata.
    assert log.run_manifest_v2["wheel_sha"] == "1.2.3"
    assert log.run_manifest_v2["git_sha"] == "abc"
    assert log.run_manifest_v2["python_version"] == "3.11.6"
    assert log.run_manifest_v2["domain"] == "airline"
    assert log.run_manifest_v2["effective_flags"] == {"target_aware_acceptance": True}
    # V2 is not in `unknown` — the parser routes it to its own field.
    assert "GSO_RUN_MANIFEST_V2" not in log.unknown


def test_parse_markers_v1_only_back_compat() -> None:
    """Existing V1-only stdout (e.g. the 0ade1a99 fixture) keeps parsing
    cleanly with run_manifest_v2 == None."""
    from genie_space_optimizer.tools.marker_parser import parse_markers

    fixture = (
        pathlib.Path(__file__).parent
        / "fixtures"
        / "lever_loop_stdout_0ade1a99.txt"
    )
    log = parse_markers(fixture.read_text())

    assert log.run_manifest is not None
    assert log.run_manifest_v2 is None
    assert log.parse_errors == ()


# Cycle 12-T2 — GSO_PHASE_H_STRICT_VALIDATION_V1 routing
def test_parse_markers_captures_phase_h_strict_validation() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers

    stdout = (
        'GSO_RUN_MANIFEST_V1 {"databricks_job_id":"","databricks_parent_run_id":"",'
        '"event":"start","lever_loop_task_run_id":"","mlflow_experiment_id":"e1",'
        '"optimization_run_id":"opt1","space_id":"s1"}\n'
        'GSO_PHASE_H_STRICT_VALIDATION_V1 {"declared_count":163,"exception_class":"",'
        '"flag_enabled":true,"listing_status":"ok","materialized_count":36,'
        '"missing_count":123,"optimization_run_id":"opt1","self_write_count":4,'
        '"validator_status":"ok"}\n'
    )

    log = parse_markers(stdout)

    assert log.phase_h_strict_validation is not None
    assert log.phase_h_strict_validation["declared_count"] == 163
    assert log.phase_h_strict_validation["missing_count"] == 123
    assert log.phase_h_strict_validation["validator_status"] == "ok"
    assert "GSO_PHASE_H_STRICT_VALIDATION_V1" not in log.unknown


def test_parse_markers_phase_h_strict_validation_absent() -> None:
    """Pre-Cycle-12-T2 stdout has no such marker; field is None."""
    from genie_space_optimizer.tools.marker_parser import parse_markers

    stdout = (
        'GSO_RUN_MANIFEST_V1 {"databricks_job_id":"","databricks_parent_run_id":"",'
        '"event":"start","lever_loop_task_run_id":"","mlflow_experiment_id":"e1",'
        '"optimization_run_id":"opt1","space_id":"s1"}\n'
    )

    log = parse_markers(stdout)

    assert log.phase_h_strict_validation is None
