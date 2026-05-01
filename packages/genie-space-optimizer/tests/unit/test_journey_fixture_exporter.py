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
