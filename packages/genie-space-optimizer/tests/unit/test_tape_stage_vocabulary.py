"""Phase 3.5 Task 5 — closed stage vocabulary in tape.py."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from genie_space_optimizer.optimization.tape import (
    _KNOWN_STAGES,
    LeverLoopTape,
)


def test_known_stages_includes_three_stage_pipeline():
    assert "stage_1_discovery" in _KNOWN_STAGES
    assert "lever_4_join_discovery" in _KNOWN_STAGES
    assert "lever_5b_example_sql" in _KNOWN_STAGES
    assert "adaptive_strategy" in _KNOWN_STAGES
    assert "cluster_driven_example_synthesis" in _KNOWN_STAGES


def test_unknown_stage_logs_warning_but_loads(tmp_path: Path, caplog):
    payload = {
        "tape_id": "t",
        "source_run_id": "r",
        "captured_at": "0",
        "entries": [
            {
                "key": {
                    "stage": "this_is_a_typo_stage",
                    "iteration": 0,
                    "ag_id": "",
                    "cluster_id": "",
                    "prompt_sha256": "a" * 64,
                },
                "prompt": "p",
                "response_text": "r",
                "response_metadata": {},
            },
        ],
    }
    p = tmp_path / "t.json"
    p.write_text(json.dumps(payload))

    with caplog.at_level(logging.WARNING):
        tape = LeverLoopTape.from_json_file(p)

    assert len(tape.entries) == 1
    assert tape.entries[0].key.stage == "this_is_a_typo_stage"
    assert any(
        "unknown stage" in rec.message.lower()
        and "this_is_a_typo_stage" in rec.message
        for rec in caplog.records
    )


def test_known_stage_does_not_log_warning(tmp_path: Path, caplog):
    payload = {
        "tape_id": "t",
        "source_run_id": "r",
        "captured_at": "0",
        "entries": [
            {
                "key": {
                    "stage": "stage_1_discovery",
                    "iteration": 0,
                    "ag_id": "",
                    "cluster_id": "",
                    "prompt_sha256": "a" * 64,
                },
                "prompt": "p",
                "response_text": "r",
                "response_metadata": {},
            },
        ],
    }
    p = tmp_path / "t.json"
    p.write_text(json.dumps(payload))

    with caplog.at_level(logging.WARNING):
        LeverLoopTape.from_json_file(p)
    assert not any(
        "unknown stage" in rec.message.lower() for rec in caplog.records
    )
