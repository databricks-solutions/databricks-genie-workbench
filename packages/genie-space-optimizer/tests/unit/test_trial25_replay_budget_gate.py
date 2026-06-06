"""Trial 25 W25.5 — pre-trigger task-value budget guardrail.

`scripts/check_parent_task_value_budget.py` is a small CLI that the
`gso-lever-loop-replay` skill MUST invoke BEFORE scheduling a replay on
an existing parent run. It counts the accumulated `taskValues` for the
target parent_run_id and refuses to proceed when the count is at or
above the threshold (default 200; Databricks platform hard-cap is 250).

This converts a silent end-of-replay platform crash
(`PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250`) into an explicit
pre-replay rejection that the operator (and /goal) can route around by
rotating the parent run via `gso-lever-loop-trigger`.

The CLI design (covered by these tests):
  - argv: --job-id <int> --parent-run-id <int> [--threshold <int>]
          [--profile <str>] [--databricks-bin <path>]
  - exit 0    : count < threshold (PASS — safe to replay)
  - exit 10   : count >= threshold (BUDGET_NEAR_CEILING — refuse)
  - exit 11   : Databricks CLI returned a non-zero exit (UNKNOWN — refuse)
  - exit 2    : bad usage (argparse rejects)
  - stdout    : single line `GSO_TRIAL25_BUDGET_GATE_<verdict>_V1{...}`
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "check_parent_task_value_budget.py"
)


@pytest.fixture(scope="module")
def gate_module():
    """Import check_parent_task_value_budget.py as a module so unit
    tests can call its helpers directly without subprocess overhead."""
    spec = importlib.util.spec_from_file_location(
        "check_parent_task_value_budget", SCRIPT_PATH,
    )
    if spec is None or spec.loader is None:
        pytest.skip(f"cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- Pure-function unit tests (count extraction) -------------------------


def test_count_task_values_from_get_run_output_empty_payload(gate_module):
    payload = {"tasks": []}
    assert gate_module._count_task_values_in_run_payload(payload) == 0


def test_count_task_values_from_get_run_output_single_task(gate_module):
    payload = {
        "tasks": [
            {
                "task_key": "lever_loop",
                "resolved_values": {
                    "scores": "{}", "accuracy": "0.92",
                    "model_id": "m-1", "iteration_counter": "3",
                },
            },
        ],
    }
    assert gate_module._count_task_values_in_run_payload(payload) == 4


def test_count_task_values_from_get_run_output_many_tasks(gate_module):
    """Real shape from `databricks jobs get-run --include-resolved-values`
    on a 4-task DAG: preflight=15 + baseline=6 + lever_loop=11 +
    finalize=10 = 42 (the per-replay budget the tracker calls out)."""
    payload = {
        "tasks": [
            {"task_key": "preflight",     "resolved_values": {f"k{i}": "" for i in range(15)}},
            {"task_key": "baseline_eval", "resolved_values": {f"k{i}": "" for i in range(6)}},
            {"task_key": "lever_loop",    "resolved_values": {f"k{i}": "" for i in range(11)}},
            {"task_key": "finalize",      "resolved_values": {f"k{i}": "" for i in range(10)}},
        ],
    }
    assert gate_module._count_task_values_in_run_payload(payload) == 42


def test_count_task_values_handles_missing_resolved_values_key(gate_module):
    payload = {"tasks": [{"task_key": "lever_loop"}]}
    assert gate_module._count_task_values_in_run_payload(payload) == 0


def test_count_task_values_handles_None_payload_returns_zero(gate_module):
    assert gate_module._count_task_values_in_run_payload(None) == 0
    assert gate_module._count_task_values_in_run_payload({}) == 0


# --- Verdict helper ------------------------------------------------------


def test_verdict_pass_when_count_strictly_less_than_threshold(gate_module):
    assert gate_module._verdict(199, threshold=200) == "PASS"


def test_verdict_block_at_threshold(gate_module):
    assert gate_module._verdict(200, threshold=200) == "NEAR_CEILING"


def test_verdict_block_above_threshold(gate_module):
    assert gate_module._verdict(250, threshold=200) == "NEAR_CEILING"


# --- CLI integration (subprocess, with a mocked databricks bin) ---------


def _make_fake_databricks_bin(tmp_path: Path, payload: dict, exit_code: int = 0) -> Path:
    """Write a tiny shell script that pretends to be the `databricks`
    CLI and prints `payload` as JSON regardless of arguments. Used to
    avoid hitting a real Databricks workspace from the test suite."""
    fake = tmp_path / "databricks"
    body = json.dumps(payload).replace("'", "'\\''")
    fake.write_text(
        "#!/usr/bin/env bash\n"
        f"cat <<'EOF_PAYLOAD'\n{body}\nEOF_PAYLOAD\n"
        f"exit {exit_code}\n"
    )
    fake.chmod(0o755)
    return fake


def test_cli_exits_zero_when_well_below_threshold(tmp_path):
    fake_bin = _make_fake_databricks_bin(
        tmp_path,
        {"tasks": [{"task_key": "lever_loop",
                    "resolved_values": {f"k{i}": "" for i in range(42)}}]},
    )
    result = subprocess.run(
        [
            "python", str(SCRIPT_PATH),
            "--job-id", "488860692117207",
            "--parent-run-id", "501649560474489",
            "--threshold", "200",
            "--profile", "fevm-prashanth",
            "--databricks-bin", str(fake_bin),
        ],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "GSO_TRIAL25_BUDGET_GATE_PASSED_V1" in result.stdout


def test_cli_exits_10_when_at_or_above_threshold(tmp_path):
    fake_bin = _make_fake_databricks_bin(
        tmp_path,
        {"tasks": [{"task_key": "lever_loop",
                    "resolved_values": {f"k{i}": "" for i in range(201)}}]},
    )
    result = subprocess.run(
        [
            "python", str(SCRIPT_PATH),
            "--job-id", "488860692117207",
            "--parent-run-id", "501649560474489",
            "--threshold", "200",
            "--profile", "fevm-prashanth",
            "--databricks-bin", str(fake_bin),
        ],
        capture_output=True, text=True,
    )
    assert result.returncode == 10, (
        f"expected exit 10 (NEAR_CEILING), got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "GSO_TRIAL25_BUDGET_GATE_BLOCKED_V1" in result.stdout
    assert "gso-lever-loop-trigger" in result.stdout, (
        "blocked verdict must name the rotation skill the operator should run"
    )


def test_cli_exits_11_when_databricks_cli_fails(tmp_path):
    fake_bin = _make_fake_databricks_bin(
        tmp_path, {"error_code": "PERMISSION_DENIED"}, exit_code=1,
    )
    result = subprocess.run(
        [
            "python", str(SCRIPT_PATH),
            "--job-id", "488860692117207",
            "--parent-run-id", "501649560474489",
            "--threshold", "200",
            "--profile", "fevm-prashanth",
            "--databricks-bin", str(fake_bin),
        ],
        capture_output=True, text=True,
    )
    assert result.returncode == 11, (
        f"expected exit 11 (UNKNOWN — refuse), got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "GSO_TRIAL25_BUDGET_GATE_UNKNOWN_V1" in result.stdout
