"""WU-5 — replay fixture dual-emit + extractor tests."""
from __future__ import annotations

import base64
import json

from genie_space_optimizer.optimization.replay_fixture_marker import (
    emit_dual_fixture,
    extract_replay_fixture_from_stream,
)


def test_emit_dual_fixture_writes_plain_and_base64_marker_pairs(capsys):
    payload = {"fixture_id": "abc", "iterations": []}
    emit_dual_fixture(payload=payload, stream_name="stderr")
    captured = capsys.readouterr()
    assert "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===" in captured.err
    assert "===PHASE_A_REPLAY_FIXTURE_JSON_END===" in captured.err
    assert (
        "===PHASE_A_REPLAY_FIXTURE_BASE64_BEGIN===" in captured.err
    )
    assert "===PHASE_A_REPLAY_FIXTURE_BASE64_END===" in captured.err


def test_extractor_returns_plain_json_when_unpolluted():
    payload = {"fixture_id": "abc", "iterations": []}
    raw_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    raw_b64 = base64.b64encode(raw_json.encode("utf-8")).decode("ascii")
    stream = (
        "log line A\n"
        "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
        f"{raw_json}\n"
        "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
        "===PHASE_A_REPLAY_FIXTURE_BASE64_BEGIN===\n"
        f"{raw_b64}\n"
        "===PHASE_A_REPLAY_FIXTURE_BASE64_END===\n"
        "log line B\n"
    )
    result = extract_replay_fixture_from_stream(stream)
    assert result.payload == payload
    assert result.source == "plain_json"


def test_extractor_falls_back_to_base64_when_plain_json_polluted():
    """Plain-JSON window is polluted with prompt source text;
    base64 window untouched."""
    payload = {"fixture_id": "abc", "iterations": [{"iteration": 1}]}
    raw_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    raw_b64 = base64.b64encode(raw_json.encode("utf-8")).decode("ascii")
    polluted = (
        "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
        + raw_json[:30]
        + "\n--- prompt source pasted in ---\n"
        + "Hello, you are a helpful assistant ...\n"
        + raw_json[30:]
        + "\n"
        + "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
        + "===PHASE_A_REPLAY_FIXTURE_BASE64_BEGIN===\n"
        + raw_b64
        + "\n"
        + "===PHASE_A_REPLAY_FIXTURE_BASE64_END===\n"
    )
    result = extract_replay_fixture_from_stream(polluted)
    assert result.payload == payload
    assert result.source == "base64_fallback"


def test_extractor_returns_none_when_both_markers_missing():
    result = extract_replay_fixture_from_stream("nothing here\n")
    assert result.payload is None
    assert result.source == "missing"


def test_emit_then_extract_roundtrip_recovers_payload(capsys):
    payload = {"fixture_id": "abc", "iterations": [{"iteration": 1}]}
    emit_dual_fixture(payload=payload, stream_name="stdout")
    captured = capsys.readouterr()
    # Simulate in-band pollution between plain-JSON markers
    polluted = captured.out.replace(
        "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n",
        "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\nBROKEN ",
        1,
    )
    result = extract_replay_fixture_from_stream(polluted)
    assert result.payload == payload
    assert result.source == "base64_fallback"
