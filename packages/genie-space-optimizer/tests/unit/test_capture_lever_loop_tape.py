"""Unit tests for the lever-loop tape capture script."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.capture_lever_loop_tape_from_export import (
    capture_tape_from_export,
)
from genie_space_optimizer.optimization.tape import LeverLoopTape


def test_capture_emits_strategist_entry_per_iteration(tmp_path: Path):
    export = {
        "fixture_id": "fx-1",
        "source_run_id": "run-abc",
        "iterations": [
            {
                "iteration_idx": 0,
                "eval_rows": [{"question_id": "q1"}],
                "clusters": [],
                "soft_clusters": [],
                "strategist_prompt": "PROMPT 0",
                "strategist_response": "RESPONSE 0",
                "decision_records": [],
            },
            {
                "iteration_idx": 1,
                "eval_rows": [{"question_id": "q1"}],
                "clusters": [],
                "soft_clusters": [],
                "strategist_prompt": "PROMPT 1",
                "strategist_response": "RESPONSE 1",
                "decision_records": [],
            },
        ],
    }
    src = tmp_path / "export.json"
    src.write_text(json.dumps(export))
    out = tmp_path / "tape.json"

    capture_tape_from_export(src, out, tape_id="tape-test")

    tape = LeverLoopTape.from_json_file(out)
    assert tape.tape_id == "tape-test"
    assert tape.source_run_id == "run-abc"
    assert len(tape.entries) == 2
    stages = {e.key.stage for e in tape.entries}
    assert stages == {"adaptive_strategy"}
    iters = sorted(e.key.iteration for e in tape.entries)
    assert iters == [0, 1]
    responses = {e.key.iteration: e.response_text for e in tape.entries}
    assert responses[0] == "RESPONSE 0"
    assert responses[1] == "RESPONSE 1"
    assert tape.evals_by_iteration == {
        0: [{"question_id": "q1"}],
        1: [{"question_id": "q1"}],
    }


def test_capture_records_synthesis_entries_when_present(tmp_path: Path):
    export = {
        "fixture_id": "fx-2",
        "source_run_id": "run-xyz",
        "iterations": [
            {
                "iteration_idx": 2,
                "eval_rows": [],
                "clusters": [],
                "soft_clusters": [],
                "strategist_prompt": "P",
                "strategist_response": "R",
                "decision_records": [],
                "synthesis_calls": [
                    {
                        "ag_id": "AG_001",
                        "cluster_id": "H001",
                        "prompt": "SYNTH-P",
                        "response": "SYNTH-R",
                    }
                ],
            }
        ],
    }
    src = tmp_path / "export.json"
    src.write_text(json.dumps(export))
    out = tmp_path / "tape.json"

    capture_tape_from_export(src, out, tape_id="tape-2")

    tape = LeverLoopTape.from_json_file(out)
    synth = [e for e in tape.entries if e.key.stage == "cluster_driven_synthesis"]
    assert len(synth) == 1
    assert synth[0].key.ag_id == "AG_001"
    assert synth[0].key.cluster_id == "H001"
    assert synth[0].response_text == "SYNTH-R"


def test_capture_raises_on_missing_strategist_fields(tmp_path: Path):
    export = {
        "fixture_id": "fx-3",
        "iterations": [
            {"iteration_idx": 0, "eval_rows": [], "clusters": []}
        ],
    }
    src = tmp_path / "export.json"
    src.write_text(json.dumps(export))
    out = tmp_path / "tape.json"

    with pytest.raises(
        ValueError, match="iteration 0 is missing strategist_prompt"
    ):
        capture_tape_from_export(src, out, tape_id="tape-3")
