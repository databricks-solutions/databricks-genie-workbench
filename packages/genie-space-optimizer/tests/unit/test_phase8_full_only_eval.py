"""GSO v2 Phase 8 — full-benchmark-only eval inside the 03_optimize loop.

Arch §7.3: every attempt (coverage + surgical) is scored on the full 30–40-question
benchmark so the Attempt Ladder plots one consistent per-attempt accuracy. This
supersedes the Phase-1 subset-first 3-gate (slice → P0 → full) FOR THE LOOP and
makes the EvalBudget cap the primary stop. The now-dead
``genie_eval_question_regressions`` write (a subset-gate vestige) is retired.
"""

from __future__ import annotations

import inspect

from genie_space_optimizer.common import config as cfg
from genie_space_optimizer.optimization import eval_budget
from genie_space_optimizer.optimization import harness


# ── config flag: default ON, env opt-out ───────────────────────────────────
def test_full_benchmark_only_eval_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_FULL_BENCHMARK_ONLY_EVAL", raising=False)
    assert cfg.full_benchmark_only_eval_enabled() is True


def test_full_benchmark_only_eval_env_opt_out(monkeypatch) -> None:
    monkeypatch.setenv("GSO_FULL_BENCHMARK_ONLY_EVAL", "false")
    assert cfg.full_benchmark_only_eval_enabled() is False


# ── budget estimate: a single full run, not the 3-gate cycle ────────────────
def test_full_benchmark_estimate_is_one_full_run() -> None:
    one_full = eval_budget.estimate_full_benchmark_seconds(30)
    assert one_full == eval_budget.estimate_eval_run_seconds(30)
    # The 3-gate cycle adds the slice + P0 prelude, so it is strictly larger;
    # Phase 8 drops that prelude inside the loop.
    three_gate = eval_budget.estimate_three_gate_seconds(working_set_size=30)
    assert one_full < three_gate


def test_loop_uses_full_benchmark_estimate_not_three_gate() -> None:
    # The loop's budget guard swaps _estimate_three_gate_seconds for the
    # full-benchmark estimate (the per-attempt cost is now one full run).
    src = inspect.getsource(harness._run_lever_loop)
    assert "estimate_full_benchmark_seconds" in src
    assert "_estimate_three_gate_seconds(" not in src


# ── _run_gate_checks: subset gates skipped under full-only ──────────────────
def test_gate_checks_skips_subset_gates_under_full_only() -> None:
    src = inspect.getsource(harness._run_gate_checks)
    assert "full_benchmark_only_eval_enabled" in src
    assert "_full_only" in src
    # Both subset gates short-circuit when full-only is on.
    assert src.count("Phase 8 full-eval-only") >= 2


# ── regressions write retired (subset-gate vestige) ─────────────────────────
def test_question_regressions_write_retired_from_loop() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "write_question_regressions(" not in src
    assert "build_question_regression_rows(" not in src
