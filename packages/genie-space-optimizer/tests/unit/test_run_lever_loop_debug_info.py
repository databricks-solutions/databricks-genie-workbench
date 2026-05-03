"""Pin the ``debug_info`` allowlist in run_lever_loop.py.

Cycle-9 postmortem (run 894992655057610) revealed that
``databricks jobs get-run-output`` for the lever_loop task exposes only
the ``dbutils.notebook.exit(...)`` JSON — not stdout. So the Phase B
``loop_out["phase_b"]`` manifest is the CLI-visible truth surface for
the postmortem analyzer.

run_lever_loop.py builds ``debug_info`` from ``loop_out`` with a key
filter. If a future contributor removes ``"phase_b"`` from that
allowlist, the manifest disappears from the notebook exit JSON without
any test catching it. This test pins the filter behavior at the source
level so a regression breaks the test loud, not the analyzer silent.

Plan: `docs/2026-05-02-unified-trace-and-operator-transcript-plan.md`
postmortem follow-up Task 8.
"""

from __future__ import annotations

import re
from pathlib import Path


def _read_run_lever_loop_source() -> str:
    root = Path(__file__).resolve().parents[2]
    return (
        root
        / "src"
        / "genie_space_optimizer"
        / "jobs"
        / "run_lever_loop.py"
    ).read_text()


def test_run_lever_loop_debug_info_allowlist_includes_phase_b() -> None:
    """Hard-pin: the debug_info filter allowlist contains ``phase_b``.

    Without this, ``loop_out["phase_b"]`` is silently dropped from
    ``dbutils.notebook.exit(...)`` and the postmortem analyzer can't see
    the Phase B manifest at all (regression mode of the run-894 case).
    """
    src = _read_run_lever_loop_source()
    # The exact tuple membership expression. If someone refactors this
    # block, update the regex but NEVER remove ``"phase_b"`` from the
    # allowlist.
    assert '"phase_b"' in src, (
        "run_lever_loop.py debug_info allowlist must include 'phase_b' "
        "or the Phase B manifest will be silently dropped from notebook.exit"
    )


def test_run_lever_loop_debug_info_filter_simulation_preserves_phase_b() -> None:
    """Simulate the filter against a synthetic ``loop_out`` shape.

    Reproduces the filter logic and confirms a ``phase_b`` key survives.
    Even though we can't import ``run_lever_loop.py`` (it has Databricks
    notebook globals), we can verify the contract by re-applying the
    same filter to a known input.
    """
    loop_out = {
        "iteration_counter": 5,
        "levers_attempted": [5, 1, 2, 6],
        "levers_accepted": [],
        "levers_rolled_back": [],
        "_debug_ref_sqls_count": 24,
        "_debug_failure_rows_loaded": 24,
        "phase_b": {
            "contract_version": "v1",
            "decision_records_total": 120,
            "iter_record_counts": [24, 24, 24, 24, 24],
        },
        # Keys that must NOT survive the filter.
        "scores": {"result_correctness": 0.85},
        "model_id": "abc",
        "all_eval_mlflow_run_ids": [],
    }

    # Mirror the filter at run_lever_loop.py:548-562 exactly.
    filtered = {
        k: v for k, v in loop_out.items()
        if k.startswith("_debug_") or k in (
            "levers_attempted",
            "levers_accepted",
            "levers_rolled_back",
            "iteration_counter",
            "phase_b",
        )
    }

    assert "phase_b" in filtered
    assert filtered["phase_b"]["contract_version"] == "v1"
    assert filtered["phase_b"]["decision_records_total"] == 120
    # Confirm non-allowlisted keys are dropped.
    assert "scores" not in filtered
    assert "model_id" not in filtered
    assert "all_eval_mlflow_run_ids" not in filtered


def test_run_lever_loop_debug_info_does_not_swallow_other_phase_b_subkeys() -> None:
    """Subtle: the allowlist matches ``phase_b`` exactly; nested keys ride
    along inside the value. Confirm a complex manifest survives intact."""
    loop_out = {
        "iteration_counter": 5,
        "phase_b": {
            "contract_version": "v1",
            "decision_records_total": 0,
            "iter_record_counts": [0, 0, 0, 0, 0],
            "iter_violation_counts": [0, 0, 0, 0, 0],
            "no_records_iterations": [1, 2, 3, 4, 5],
            "artifact_paths": [],
            "producer_exceptions": {"eval_classification": 0},
            "target_qids_missing_count": 5,
            "total_violations": 0,
        },
    }

    filtered = {
        k: v for k, v in loop_out.items()
        if k.startswith("_debug_") or k in (
            "levers_attempted",
            "levers_accepted",
            "levers_rolled_back",
            "iteration_counter",
            "phase_b",
        )
    }

    pb = filtered["phase_b"]
    assert pb["no_records_iterations"] == [1, 2, 3, 4, 5]
    assert pb["target_qids_missing_count"] == 5
    assert pb["producer_exceptions"] == {"eval_classification": 0}


def test_run_lever_loop_debug_info_filter_block_is_present_at_known_anchor() -> None:
    """Source-level guard: the filter expression must contain the four
    historical keys plus ``phase_b``. If a future contributor refactors
    this in a way that breaks the JSON exit contract, this test breaks."""
    src = _read_run_lever_loop_source()

    expected_keys = (
        '"levers_attempted"',
        '"levers_accepted"',
        '"levers_rolled_back"',
        '"iteration_counter"',
        '"phase_b"',
    )
    for key in expected_keys:
        assert key in src, f"missing required allowlist key: {key}"
    # Confirm the filter is shaped as ``k in (...)`` rather than something
    # exotic — keeps the regex/grep pattern stable.
    assert re.search(r"k\s+in\s+\(", src), (
        "run_lever_loop.py debug_info filter expression has been refactored "
        "in a way the allowlist test cannot follow; update this test."
    )
