"""Phase 3.6 Task 4 — prompt_sha_only miss policy on LeverLoopTape."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.tape import (
    LeverLoopTape,
    TapeEntry,
    TapeKey,
    TapeMissError,
    prompt_sha256,
)


def _tape_with_entry(prompt: str, *, ag_id: str, iteration: int):
    return LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="0",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage="stage_1_discovery",
                    iteration=iteration,
                    ag_id=ag_id,
                    cluster_id="",
                    prompt_sha256=prompt_sha256(prompt),
                ),
                prompt=prompt,
                response_text="captured-response",
                response_metadata={},
            ),
        ],
        miss_policy="prompt_sha_only",
    )


def test_prompt_sha_only_matches_ignoring_iteration_ag():
    tape = _tape_with_entry("P", iteration=2, ag_id="AG_77")
    entry = tape.lookup(
        stage="stage_1_discovery",
        iteration=99,
        ag_id="WHATEVER",
        cluster_id="",
        prompt="P",
    )
    assert entry.response_text == "captured-response"


def test_prompt_sha_only_still_requires_stage_match():
    tape = _tape_with_entry("P", iteration=0, ag_id="")
    with pytest.raises(TapeMissError):
        tape.lookup(
            stage="lever_4_join_discovery",
            iteration=0,
            ag_id="",
            cluster_id="",
            prompt="P",
        )


def test_prompt_sha_only_raises_on_prompt_content_change():
    tape = _tape_with_entry("P", iteration=0, ag_id="")
    with pytest.raises(TapeMissError):
        tape.lookup(
            stage="stage_1_discovery",
            iteration=0,
            ag_id="",
            cluster_id="",
            prompt="P-modified",
        )


def test_strict_raise_policy_unchanged_by_phase_3_6():
    """Regression: pre-existing behaviour must be preserved."""
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="0",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage="stage_1_discovery",
                    iteration=0, ag_id="AG_77", cluster_id="",
                    prompt_sha256=prompt_sha256("P"),
                ),
                prompt="P", response_text="r", response_metadata={},
            ),
        ],
        miss_policy="raise",
    )
    with pytest.raises(TapeMissError):
        tape.lookup(
            stage="stage_1_discovery",
            iteration=1,
            ag_id="AG_77",
            cluster_id="",
            prompt="P",
        )


def test_tape_json_round_trip_with_prompt_sha_only(tmp_path: Path):
    """Tape file preserves miss_policy across save/load."""
    payload = {
        "tape_id": "t", "source_run_id": "r", "captured_at": "0",
        "entries": [{
            "key": {
                "stage": "stage_1_discovery",
                "iteration": 0, "ag_id": "", "cluster_id": "",
                "prompt_sha256": prompt_sha256("P"),
            },
            "prompt": "P", "response_text": "r", "response_metadata": {},
        }],
        "miss_policy": "prompt_sha_only",
    }
    p = tmp_path / "t.json"
    p.write_text(json.dumps(payload))
    tape = LeverLoopTape.from_json_file(p)
    assert tape.miss_policy == "prompt_sha_only"
    entry = tape.lookup(
        stage="stage_1_discovery",
        iteration=42, ag_id="AG_OTHER", cluster_id="X",
        prompt="P",
    )
    assert entry.response_text == "r"
