"""Unit tests for the log-extractor script."""

from __future__ import annotations

import json
from pathlib import Path


def _build_log_with_markers(fixture_json: str, noise_lines: int = 5) -> str:
    """Synthesize a log file with surrounding noise + markers + the JSON."""
    lines: list[str] = []
    for i in range(noise_lines):
        lines.append(f"2026-05-01 12:00:0{i} INFO some other log line {i}")
    lines.append("===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===")
    lines.append(fixture_json)
    lines.append("===PHASE_A_REPLAY_FIXTURE_JSON_END===")
    for i in range(noise_lines):
        lines.append(f"2026-05-01 12:00:1{i} INFO post-fixture line {i}")
    return "\n".join(lines) + "\n"


def test_extractor_finds_fixture_between_markers(tmp_path: Path) -> None:
    from genie_space_optimizer.scripts.extract_replay_fixture_from_log import (
        extract_fixture_from_log_text,
    )

    fixture = {"fixture_id": "test_v1", "iterations": [{"iteration": 1}]}
    log_text = _build_log_with_markers(json.dumps(fixture, separators=(",", ":")))
    out = extract_fixture_from_log_text(log_text)
    assert out == fixture


def test_extractor_raises_on_missing_markers() -> None:
    from genie_space_optimizer.scripts.extract_replay_fixture_from_log import (
        extract_fixture_from_log_text,
    )
    import pytest

    with pytest.raises(ValueError, match="markers"):
        extract_fixture_from_log_text("no markers here\nat all\n")


def test_extractor_handles_extra_whitespace_around_json() -> None:
    from genie_space_optimizer.scripts.extract_replay_fixture_from_log import (
        extract_fixture_from_log_text,
    )

    log_text = (
        "noise\n"
        "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
        "  \n"
        '{"fixture_id":"x","iterations":[]}\n'
        "  \n"
        "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
    )
    out = extract_fixture_from_log_text(log_text)
    assert out == {"fixture_id": "x", "iterations": []}
