"""Phase 3.6 Task 5 — CLI: capture_tape_from_mlflow.py."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "capture_tape_from_mlflow.py"


def test_script_exists_and_help_runs():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "experiment-id" in result.stdout
    assert "run-id" in result.stdout
    assert "export-json" in result.stdout
    assert "out" in result.stdout


def test_end_to_end_assembly(tmp_path: Path, monkeypatch):
    """In-process invocation: stub MlflowClient + extractor, assert tape JSON shape."""
    export_path = tmp_path / "export.json"
    export_path.write_text(json.dumps({
        "fixture_id": "f1",
        "iterations": [
            {
                "iteration": 0,
                "eval_rows": [
                    {"qid": "gs_009", "passed": False, "got_sql": "..."},
                ],
                "clusters": [
                    {"cluster_id": "H001", "qids": ["gs_009"], "root_cause": "x"},
                ],
            },
        ],
    }))

    fake_calls = [{
        "span_name": "stage_1_discovery",
        "iteration": -1,
        "ag_id": "",
        "cluster_id": "",
        "prompt_sha256": "a" * 64,
        "system_msg": "sys",
        "prompt": "p",
        "response_text": "r",
        "response_metadata": {"model": "stub"},
    }]

    from genie_space_optimizer.optimization import mlflow_trace_extractor
    monkeypatch.setattr(
        mlflow_trace_extractor,
        "extract_llm_calls_from_traces",
        lambda traces: iter(fake_calls),
    )

    fake_trace = MagicMock(name="FakeTrace")
    fake_client = MagicMock(name="FakeMlflowClient")
    fake_client.search_traces.return_value = [fake_trace]

    from scripts import capture_tape_from_mlflow as cli
    monkeypatch.setattr(cli, "_build_mlflow_client", lambda: fake_client)

    out_path = tmp_path / "tape.json"
    rc = cli.main(argv=[
        "--experiment-id", "EXP_X",
        "--run-id",        "RUN_X",
        "--export-json",   str(export_path),
        "--out",           str(out_path),
        "--miss-policy",   "prompt_sha_only",
    ])
    assert rc == 0

    from genie_space_optimizer.optimization.tape import LeverLoopTape
    tape = LeverLoopTape.from_json_file(out_path)
    assert tape.miss_policy == "prompt_sha_only"
    assert len(tape.entries) == 1
    assert tape.entries[0].key.stage == "stage_1_discovery"
    assert tape.entries[0].prompt == "p"
    # Side-tables from the export carry through.
    assert tape.evals_by_iteration.get(0)
    assert tape.evals_by_iteration[0][0]["qid"] == "gs_009"
    assert tape.clusters_by_iteration.get(0)
