"""Phase 3.7 Task 3 — tape format v4 (replay_mode_by_stage)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.tape import (
    TAPE_FORMAT_VERSION,
    LeverLoopTape,
    _SUPPORTED_FORMAT_VERSIONS,
    _VALID_REPLAY_MODES,
)


def _v4_payload(replay_mode_by_stage: dict | None = None) -> dict:
    """Build a minimum-valid v4 tape payload."""
    payload = {
        "tape_id": "v4-test",
        "source_run_id": "run-xyz",
        "captured_at": "2026-05-18T00:00:00Z",
        "format_version": 4,
        "entries": [],
        "iteration_payloads": {
            "0": {"rows_json": "[]", "scores_json": "{}"},
        },
        "miss_policy": "raise",
    }
    if replay_mode_by_stage is not None:
        payload["replay_mode_by_stage"] = replay_mode_by_stage
    return payload


def test_tape_format_version_is_4():
    assert TAPE_FORMAT_VERSION == 4
    assert 4 in _SUPPORTED_FORMAT_VERSIONS
    # v1/v2/v3 still load (backward compat)
    assert {1, 2, 3, 4}.issubset(_SUPPORTED_FORMAT_VERSIONS)


def test_v4_tape_loads_replay_mode_by_stage(tmp_path: Path):
    path = tmp_path / "tape.json"
    path.write_text(json.dumps(_v4_payload(
        replay_mode_by_stage={"lever6_llm": "historic_inject"},
    )))
    tape = LeverLoopTape.from_json_file(path)
    assert tape.format_version == 4
    assert tape.replay_mode_by_stage == {"lever6_llm": "historic_inject"}


def test_v3_tape_loads_with_empty_replay_mode(tmp_path: Path):
    """Back-compat: v3 tape with no replay_mode_by_stage field still loads."""
    payload = _v4_payload(replay_mode_by_stage=None)
    payload["format_version"] = 3
    path = tmp_path / "tape.json"
    path.write_text(json.dumps(payload))
    tape = LeverLoopTape.from_json_file(path)
    assert tape.format_version == 3
    assert tape.replay_mode_by_stage == {}


def test_invalid_replay_mode_raises(tmp_path: Path):
    """Typo in mode name is loud at load time."""
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(_v4_payload(
        replay_mode_by_stage={"lever6_llm": "magical_unicorn"},
    )))
    with pytest.raises(ValueError, match="invalid replay_mode"):
        LeverLoopTape.from_json_file(path)


def test_valid_replay_modes_vocab_is_closed():
    """Lock in the typed vocabulary today."""
    assert _VALID_REPLAY_MODES == {"rebuild_and_match", "historic_inject"}
