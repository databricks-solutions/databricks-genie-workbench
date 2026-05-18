"""Unit tests for the tape data model."""
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


def test_prompt_sha256_is_deterministic_and_hex64():
    p = "system: be helpful\nuser: hello"
    h1 = prompt_sha256(p)
    h2 = prompt_sha256(p)
    assert h1 == h2
    assert len(h1) == 64
    assert all(c in "0123456789abcdef" for c in h1)


def test_prompt_sha256_differs_for_different_text():
    assert prompt_sha256("a") != prompt_sha256("b")


def test_tape_from_json_file_round_trips(tmp_path: Path):
    payload = {
        "tape_id": "test-tape",
        "source_run_id": "abc-123",
        "captured_at": "2026-05-17T20:00:00Z",
        "entries": [
            {
                "key": {
                    "stage": "adaptive_strategy",
                    "iteration": 1,
                    "ag_id": "",
                    "cluster_id": "",
                    "prompt_sha256": prompt_sha256("hello"),
                },
                "prompt": "hello",
                "response_text": "world",
                "response_metadata": {"model": "claude-sonnet-4-6"},
            }
        ],
        "evals_by_iteration": {"0": [{"question_id": "q1"}]},
        "clusters_by_iteration": {"0": []},
        "rca_cards_by_cluster": {"H001": {"summary": "x"}},
        "miss_policy": "raise",
    }
    path = tmp_path / "tape.json"
    path.write_text(json.dumps(payload))

    tape = LeverLoopTape.from_json_file(path)

    assert tape.tape_id == "test-tape"
    assert tape.source_run_id == "abc-123"
    assert len(tape.entries) == 1
    assert tape.entries[0].response_text == "world"
    assert tape.evals_by_iteration[0] == [{"question_id": "q1"}]
    assert tape.rca_cards_by_cluster["H001"] == {"summary": "x"}


def test_lookup_hit_returns_entry():
    tape = LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage="s",
                    iteration=1,
                    ag_id="",
                    cluster_id="",
                    prompt_sha256=prompt_sha256("p"),
                ),
                prompt="p",
                response_text="r-text",
                response_metadata={},
            )
        ],
    )
    got = tape.lookup(
        stage="s", iteration=1, ag_id="", cluster_id="", prompt="p",
    )
    assert got.response_text == "r-text"


def test_lookup_miss_raises_under_default_policy():
    tape = LeverLoopTape(
        tape_id="t", source_run_id="r", captured_at="", entries=[],
    )
    with pytest.raises(TapeMissError):
        tape.lookup(
            stage="s", iteration=1, ag_id="", cluster_id="",
            prompt="anything",
        )


def test_loader_refuses_one_indexed_side_tables(tmp_path: Path):
    """Phase 3.6.1 (2026-05-18) — tape side-tables must be 0-indexed
    so the replay harness's ``_iter_num - 1`` lookup matches. A
    1-indexed tape would silently miss every lookup; refuse to load
    so the capture-side bug surfaces at construction time, not at
    test-assertion time."""
    payload = {
        "tape_id": "t",
        "source_run_id": "r",
        "captured_at": "0",
        "entries": [],
        "evals_by_iteration": {
            "1": [{"qid": "q1"}],
            "2": [{"qid": "q1"}],
            "3": [{"qid": "q1"}],
            "4": [{"qid": "q1"}],
        },
        "miss_policy": "raise",
    }
    p = tmp_path / "broken.json"
    p.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="0-indexed"):
        LeverLoopTape.from_json_file(p)


def test_loader_accepts_zero_indexed_side_tables(tmp_path: Path):
    """Mirror of the above: the canonical 0-indexed shape loads cleanly."""
    payload = {
        "tape_id": "t",
        "source_run_id": "r",
        "captured_at": "0",
        "entries": [],
        "evals_by_iteration": {
            "0": [{"qid": "q1"}],
            "1": [{"qid": "q1"}],
            "2": [{"qid": "q1"}],
            "3": [{"qid": "q1"}],
        },
        "miss_policy": "raise",
    }
    p = tmp_path / "good.json"
    p.write_text(json.dumps(payload))
    tape = LeverLoopTape.from_json_file(p)
    assert sorted(tape.evals_by_iteration) == [0, 1, 2, 3]


def test_loader_accepts_empty_side_tables(tmp_path: Path):
    """Empty side-tables are fine — the 0-indexed invariant only
    applies when there are entries to check."""
    payload = {
        "tape_id": "t",
        "source_run_id": "r",
        "captured_at": "0",
        "entries": [],
        "miss_policy": "raise",
    }
    p = tmp_path / "empty.json"
    p.write_text(json.dumps(payload))
    LeverLoopTape.from_json_file(p)  # no raise


def test_lookup_miss_warns_and_returns_sibling_under_warn_policy(caplog):
    tape = LeverLoopTape(
        tape_id="t",
        source_run_id="r",
        captured_at="",
        entries=[
            TapeEntry(
                key=TapeKey(
                    stage="s", iteration=1, ag_id="", cluster_id="",
                    prompt_sha256=prompt_sha256("original"),
                ),
                prompt="original",
                response_text="sibling-response",
                response_metadata={},
            )
        ],
        miss_policy="warn",
    )
    with caplog.at_level("WARNING"):
        got = tape.lookup(
            stage="s", iteration=1, ag_id="", cluster_id="",
            prompt="drifted",
        )
    assert got.response_text == "sibling-response"
    assert "tape drift" in caplog.text
