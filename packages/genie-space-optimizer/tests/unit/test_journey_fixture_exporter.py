"""Unit tests for the replay-fixture exporter."""

from __future__ import annotations

import json
from pathlib import Path


def _sample_iteration_input() -> dict:
    """Return one iteration's worth of input data in the exporter's input shape."""
    return {
        "iteration": 1,
        "eval_rows": [
            {"question_id": "q_001", "result_correctness": "yes",
             "arbiter": "both_correct"},
            {"question_id": "q_002", "result_correctness": "no",
             "arbiter": "ground_truth_correct"},
        ],
        "clusters": [
            {"cluster_id": "c1", "root_cause": "missing_filter",
             "question_ids": ["q_002"]},
        ],
        "soft_clusters": [],
        "strategist_response": {
            "action_groups": [
                {
                    "id": "AG_1",
                    "affected_questions": ["q_002"],
                    "patches": [
                        {"proposal_id": "p1", "patch_type": "instruction",
                         "target_qids": ["q_002"], "cluster_id": "c1"},
                    ],
                },
            ],
        },
        "ag_outcomes": {"AG_1": "accepted"},
        "post_eval_passing_qids": ["q_001", "q_002"],
    }


def test_serialize_returns_compact_single_line_json() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    out = serialize_replay_fixture(
        fixture_id="test_v1",
        iterations_data=[_sample_iteration_input()],
    )
    assert isinstance(out, str)
    assert "\n" not in out, "Compact JSON must be single-line for log extraction"
    parsed = json.loads(out)
    assert parsed["fixture_id"] == "test_v1"
    assert len(parsed["iterations"]) == 1


def test_dump_writes_well_formed_fixture(tmp_path: Path) -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        dump_replay_fixture,
    )

    out = tmp_path / "fixture.json"
    dump_replay_fixture(
        path=str(out),
        fixture_id="test_v1",
        iterations_data=[_sample_iteration_input()],
    )
    assert out.exists()
    parsed = json.loads(out.read_text())
    assert parsed["fixture_id"] == "test_v1"
    assert isinstance(parsed["iterations"], list)
    assert len(parsed["iterations"]) == 1
    it0 = parsed["iterations"][0]
    assert it0["iteration"] == 1
    assert {r["question_id"] for r in it0["eval_rows"]} == {"q_001", "q_002"}
    assert it0["clusters"][0]["cluster_id"] == "c1"
    assert it0["strategist_response"]["action_groups"][0]["id"] == "AG_1"
    assert it0["ag_outcomes"]["AG_1"] == "accepted"
    assert "q_001" in it0["post_eval_passing_qids"]


def test_serialize_and_dump_produce_equivalent_data(tmp_path: Path) -> None:
    """The disk-dump and the string-serialize must encode identical content."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        dump_replay_fixture,
        serialize_replay_fixture,
    )

    iterations = [_sample_iteration_input()]
    out = tmp_path / "fixture.json"
    dump_replay_fixture(
        path=str(out),
        fixture_id="equiv_v1",
        iterations_data=iterations,
    )
    on_disk = json.loads(out.read_text())
    in_string = json.loads(serialize_replay_fixture(
        fixture_id="equiv_v1",
        iterations_data=iterations,
    ))
    assert on_disk == in_string


def test_round_trip_via_replay_engine() -> None:
    """Exporter output must be loadable by run_replay and produce zero violations."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )
    from genie_space_optimizer.optimization.lever_loop_replay import (
        run_replay,
    )

    raw = serialize_replay_fixture(
        fixture_id="test_round_trip_v1",
        iterations_data=[_sample_iteration_input()],
    )
    fixture = json.loads(raw)
    result = run_replay(fixture)
    assert result.validation.is_valid, (
        f"Replay should produce zero violations on a clean exporter output; "
        f"got {len(result.validation.violations)} violations: "
        + "; ".join(
            f"qid={v.question_id} kind={v.kind}"
            for v in result.validation.violations
        )
    )


def test_handles_empty_iterations_list() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    parsed = json.loads(serialize_replay_fixture(
        fixture_id="empty_v1",
        iterations_data=[],
    ))
    assert parsed == {"fixture_id": "empty_v1", "iterations": []}


def test_strips_volatile_fields() -> None:
    """Timestamps, durations, MLflow run IDs must not enter the fixture."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    inp = _sample_iteration_input()
    inp["_timestamp"] = "2026-05-01T12:00:00Z"
    inp["_duration_ms"] = 1234
    inp["_mlflow_run_id"] = "abc123"
    inp["eval_rows"][0]["_response_time_ms"] = 500
    out = serialize_replay_fixture(
        fixture_id="strip_v1",
        iterations_data=[inp],
    )
    assert "_timestamp" not in out
    assert "_duration_ms" not in out
    assert "_mlflow_run_id" not in out
    assert "_response_time_ms" not in out


def test_begin_iteration_capture_appends_immediately_and_mutates_in_place() -> None:
    """Append-on-begin: snapshot enters the run-level list before any
    early-exit (continue/break) can drop it; subsequent in-place
    mutation via the returned ref is reflected in the list entry.
    """
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
    )

    iters: list[dict] = []

    snap1 = begin_iteration_capture(iterations_data=iters, iteration=1)
    assert len(iters) == 1, "snapshot must be registered immediately"
    assert iters[0] is snap1, "list entry must be the same dict reference"
    assert snap1["iteration"] == 1
    assert snap1["eval_rows"] == []
    assert snap1["clusters"] == []
    assert snap1["soft_clusters"] == []
    assert snap1["strategist_response"] == {"action_groups": []}
    assert snap1["ag_outcomes"] == {}
    assert snap1["post_eval_passing_qids"] == []

    snap1["eval_rows"].append({"question_id": "q1", "result_correctness": "yes"})
    snap1["clusters"].append(
        {"cluster_id": "c1", "root_cause": "x", "question_ids": ["q1"]}
    )
    snap1["strategist_response"]["action_groups"].append(
        {"id": "AG1", "affected_questions": ["q1"], "patches": []}
    )
    snap1["ag_outcomes"]["AG1"] = "accepted"
    snap1["post_eval_passing_qids"].append("q1")

    assert iters[0]["eval_rows"][0]["question_id"] == "q1"
    assert iters[0]["clusters"][0]["cluster_id"] == "c1"
    assert iters[0]["strategist_response"]["action_groups"][0]["id"] == "AG1"
    assert iters[0]["ag_outcomes"]["AG1"] == "accepted"
    assert iters[0]["post_eval_passing_qids"] == ["q1"]

    snap2 = begin_iteration_capture(iterations_data=iters, iteration=2)
    assert len(iters) == 2
    assert iters[1] is snap2
    assert snap2["iteration"] == 2
    assert snap1 is not snap2, "each iteration must be a fresh dict"
    assert iters[0] is snap1, "earlier snapshot must remain unchanged"


def test_early_exit_path_preserves_partial_iteration() -> None:
    """Even if a code path bails before populating every field, the
    partial snapshot still reaches the fixture because it was appended
    at iteration begin (this models the airline iter_02 rollback
    scenario where post_eval / cap-drop paths bypassed the late append).
    """
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
        serialize_replay_fixture,
    )

    iters: list[dict] = []

    snap = begin_iteration_capture(iterations_data=iters, iteration=1)
    snap["clusters"].append(
        {"cluster_id": "h1", "root_cause": "x", "question_ids": ["q1"]}
    )
    snap["strategist_response"]["action_groups"].append(
        {"id": "AG1", "affected_questions": ["q1"], "patches": []}
    )
    snap["ag_outcomes"]["AG1"] = "rolled_back"

    raw = serialize_replay_fixture(
        fixture_id="early_exit_v1",
        iterations_data=iters,
    )
    parsed = json.loads(raw)
    assert len(parsed["iterations"]) == 1
    it0 = parsed["iterations"][0]
    assert it0["iteration"] == 1
    assert it0["clusters"][0]["cluster_id"] == "h1"
    assert it0["strategist_response"]["action_groups"][0]["id"] == "AG1"
    assert it0["ag_outcomes"]["AG1"] == "rolled_back"
    assert it0["eval_rows"] == []
    assert it0["post_eval_passing_qids"] == []


def test_summarize_replay_fixture_counts_iterations_and_key_fields() -> None:
    """Operator-facing summary log must count iterations and per-iter
    key fields so a missing-iteration or empty-eval_rows run is
    triagable without parsing the fixture body.
    """
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
        summarize_replay_fixture,
    )

    iters: list[dict] = []

    s1 = begin_iteration_capture(iterations_data=iters, iteration=1)
    s1["eval_rows"] = [
        {"question_id": "q1", "result_correctness": "yes"},
        {"question_id": "q2", "result_correctness": "no"},
        {"question_id": "q3", "result_correctness": "yes"},
    ]
    s1["clusters"] = [
        {"cluster_id": "h1", "root_cause": "x", "question_ids": ["q2"]},
    ]
    s1["soft_clusters"] = [
        {"cluster_id": "s1", "root_cause": "y", "question_ids": ["q1"]},
        {"cluster_id": "s2", "root_cause": "y", "question_ids": ["q3"]},
    ]
    s1["strategist_response"]["action_groups"].append(
        {"id": "AG1", "affected_questions": ["q2"], "patches": []}
    )
    s1["ag_outcomes"]["AG1"] = "accepted"
    s1["post_eval_passing_qids"] = ["q1", "q2", "q3"]

    s2 = begin_iteration_capture(iterations_data=iters, iteration=2)
    s2["ag_outcomes"]["AG2"] = "rolled_back"

    summary = summarize_replay_fixture(iterations_data=iters)

    assert summary["iterations"] == 2
    assert len(summary["per_iter"]) == 2

    p1 = summary["per_iter"][0]
    assert p1["iteration"] == 1
    assert p1["eval_rows"] == 3
    assert p1["clusters"] == 1
    assert p1["soft_clusters"] == 2
    assert p1["action_groups"] == 1
    assert p1["ag_outcomes"] == 1
    assert p1["post_eval_passing_qids"] == 3

    p2 = summary["per_iter"][1]
    assert p2["iteration"] == 2
    assert p2["eval_rows"] == 0
    assert p2["clusters"] == 0
    assert p2["action_groups"] == 0
    assert p2["ag_outcomes"] == 1
    assert p2["post_eval_passing_qids"] == 0


def test_summarize_replay_fixture_handles_empty_list() -> None:
    """Empty iteration list must produce iterations=0 and empty per_iter."""
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        summarize_replay_fixture,
    )

    summary = summarize_replay_fixture(iterations_data=[])
    assert summary == {"iterations": 0, "per_iter": []}


def test_exporter_passes_journey_validation_field_through() -> None:
    """If an iteration snapshot has a journey_validation dict, the exporter
    must preserve it byte-for-byte through serialize_replay_fixture."""
    import json
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [
        {
            "iteration": 1,
            "eval_rows": [
                {"question_id": "q1", "result_correctness": "yes",
                 "arbiter": "both_correct"}
            ],
            "clusters": [],
            "soft_clusters": [],
            "strategist_response": {"action_groups": []},
            "ag_outcomes": {},
            "post_eval_passing_qids": ["q1"],
            "journey_validation": {
                "is_valid": True,
                "missing_qids": [],
                "violations": [],
                "terminal_state_by_qid": {"q1": "already_passing"},
            },
        },
    ]

    s = serialize_replay_fixture(
        fixture_id="t_jv_v1", iterations_data=iterations_data,
    )
    fx = json.loads(s)
    assert fx["iterations"][0]["journey_validation"] == {
        "is_valid": True,
        "missing_qids": [],
        "violations": [],
        "terminal_state_by_qid": {"q1": "already_passing"},
    }


def test_exporter_handles_missing_journey_validation_field() -> None:
    """Legacy fixtures (pre-L4a) have no journey_validation field. The
    exporter must not invent one — output omits the key entirely."""
    import json
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [
        {
            "iteration": 1,
            "eval_rows": [],
            "clusters": [],
            "soft_clusters": [],
            "strategist_response": {"action_groups": []},
            "ag_outcomes": {},
            "post_eval_passing_qids": [],
        },
    ]

    fx = json.loads(serialize_replay_fixture(
        fixture_id="t_legacy_v1", iterations_data=iterations_data,
    ))
    assert "journey_validation" not in fx["iterations"][0]


def test_exporter_passes_decision_records_through() -> None:
    import json
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        serialize_replay_fixture,
    )

    iterations_data = [_sample_iteration_input()]
    iterations_data[0]["decision_records"] = [
        {
            "run_id": "run_1",
            "iteration": 1,
            "decision_type": "gate_decision",
            "outcome": "dropped",
            "reason_code": "patch_cap_dropped",
            "question_id": "q_002",
            "ag_id": "AG_1",
            "proposal_id": "p1",
            "gate": "patch_cap",
            "affected_qids": ["q_002"],
            "evidence_refs": ["eval:q_002"],
            "root_cause": "missing_filter",
            "target_qids": ["q_002"],
            "expected_effect": "Patch should resolve q_002.",
            "observed_effect": "Patch was dropped before apply.",
            "regression_qids": [],
            "next_action": "Inspect cap ranking.",
            "metrics": {"rank": 2},
            "_volatile": "strip-me",
        },
    ]

    parsed = json.loads(serialize_replay_fixture(
        fixture_id="decision_records_v1",
        iterations_data=iterations_data,
    ))

    assert parsed["iterations"][0]["decision_records"] == [
        {
            "run_id": "run_1",
            "iteration": 1,
            "decision_type": "gate_decision",
            "outcome": "dropped",
            "reason_code": "patch_cap_dropped",
            "question_id": "q_002",
            "ag_id": "AG_1",
            "proposal_id": "p1",
            "gate": "patch_cap",
            "affected_qids": ["q_002"],
            "evidence_refs": ["eval:q_002"],
            "root_cause": "missing_filter",
            "target_qids": ["q_002"],
            "expected_effect": "Patch should resolve q_002.",
            "observed_effect": "Patch was dropped before apply.",
            "regression_qids": [],
            "next_action": "Inspect cap ranking.",
            "metrics": {"rank": 2},
        },
    ]


def test_begin_iteration_capture_initializes_decision_records() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
    )

    iters: list[dict] = []
    snap = begin_iteration_capture(iterations_data=iters, iteration=1)

    assert snap["decision_records"] == []
    assert iters[0]["decision_records"] == []


def test_summarize_replay_fixture_counts_decision_records() -> None:
    from genie_space_optimizer.optimization.journey_fixture_exporter import (
        begin_iteration_capture,
        summarize_replay_fixture,
    )

    iters: list[dict] = []
    snap = begin_iteration_capture(iterations_data=iters, iteration=1)
    snap["decision_records"].append({
        "run_id": "run_1",
        "iteration": 1,
        "decision_type": "gate_decision",
        "outcome": "accepted",
        "reason_code": "patch_cap_selected",
    })

    summary = summarize_replay_fixture(iterations_data=iters)
    assert summary["per_iter"][0]["decision_records"] == 1
