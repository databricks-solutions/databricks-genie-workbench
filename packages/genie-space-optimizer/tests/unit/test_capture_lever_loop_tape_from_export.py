"""Phase 3.5 Task 6 — capture script reads llm_call_log primary."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from genie_space_optimizer.optimization.tape import LeverLoopTape

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "capture_lever_loop_tape_from_export.py"


def _run_capture(in_path: Path, out_path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable, str(SCRIPT),
            "--export", str(in_path),
            "--out", str(out_path),
        ],
        capture_output=True, text=True, check=False,
    )


def test_capture_emits_one_entry_per_llm_call_log_entry(tmp_path: Path):
    export = {
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 0,
                "llm_call_log": [
                    {
                        "span_name": "stage_1_discovery",
                        "iteration": 0,
                        "ag_id": "",
                        "cluster_id": "",
                        "prompt_sha256": "a" * 64,
                        "system_msg": "sys",
                        "prompt": "p1",
                        "response_text": "r1",
                        "response_metadata": {},
                    },
                    {
                        "span_name": "lever_4_join_discovery",
                        "iteration": 0,
                        "ag_id": "AG_77",
                        "cluster_id": "H001",
                        "prompt_sha256": "b" * 64,
                        "system_msg": "",
                        "prompt": "p2",
                        "response_text": "r2",
                        "response_metadata": {"model": "stub"},
                    },
                ],
            },
        ],
    }
    in_path = tmp_path / "export.json"
    out_path = tmp_path / "tape.json"
    in_path.write_text(json.dumps(export))

    result = _run_capture(in_path, out_path)
    assert result.returncode == 0, (result.stdout, result.stderr)

    tape = LeverLoopTape.from_json_file(out_path)
    assert len(tape.entries) == 2
    stages = [e.key.stage for e in tape.entries]
    assert stages == ["stage_1_discovery", "lever_4_join_discovery"]
    # Stage-2 call retains AG binding.
    l4 = tape.entries[1]
    assert l4.key.iteration == 0
    assert l4.key.ag_id == "AG_77"
    assert l4.key.cluster_id == "H001"


def test_capture_legacy_export_falls_back_with_warning(tmp_path: Path):
    """Old exports without llm_call_log MUST still load (warn-only)."""
    legacy_export = {
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 0,
                "strategist_response": {
                    "action_groups": [
                        {"id": "AG_X", "affected_questions": ["q1"], "patches": []},
                    ],
                },
            },
        ],
    }
    in_path = tmp_path / "legacy.json"
    out_path = tmp_path / "tape.json"
    in_path.write_text(json.dumps(legacy_export))

    result = _run_capture(in_path, out_path)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert (
        "legacy" in result.stderr.lower()
        or "fallback" in result.stderr.lower()
    )

    tape = LeverLoopTape.from_json_file(out_path)
    assert isinstance(tape.entries, list)


def test_capture_unknown_span_name_warns_but_does_not_crash(tmp_path: Path):
    export = {
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 0,
                "llm_call_log": [
                    {
                        "span_name": "bogus_typo_span",
                        "iteration": 0,
                        "ag_id": "",
                        "cluster_id": "",
                        "prompt_sha256": "a" * 64,
                        "system_msg": "",
                        "prompt": "x",
                        "response_text": "y",
                        "response_metadata": {},
                    },
                ],
            },
        ],
    }
    in_path = tmp_path / "x.json"
    out_path = tmp_path / "tape.json"
    in_path.write_text(json.dumps(export))

    result = _run_capture(in_path, out_path)
    assert result.returncode == 0, (result.stdout, result.stderr)
