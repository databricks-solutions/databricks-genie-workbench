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
