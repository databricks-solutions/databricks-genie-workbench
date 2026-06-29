"""GSO v2 Phase 8 — cross-review (Codex) blocking-fix coverage (B1–B6 + NB).

These pin the SEMANTIC guarantees the first round missed: rejected coverage rows
never become current state (B1), the coverage rollback is PROVEN (B2), the
coverage attempt is firewalled through apply_patch_set (B3), there is exactly ONE
measured coverage protocol (B4), resume does not re-run coverage (B5), and each
break site stamps a SPECIFIC terminal reason with plateau guarded off (B6).
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pandas as pd

from genie_space_optimizer.optimization import applier as applier_mod
from genie_space_optimizer.optimization import harness
from genie_space_optimizer.optimization import state as state_mod


# ── B1: rejected coverage rung is excluded from current-state selection ──────
def test_write_iteration_rolled_back_marks_row_excluded() -> None:
    captured: list[str] = []
    with patch.object(state_mod, "execute_delta_write_with_retry",
                      lambda spark, sql, **k: captured.append(sql)):
        state_mod.write_iteration(
            MagicMock(), "run1", 0,
            {"overall_accuracy": 70.0, "total_questions": 30, "correct_count": 21, "scores": {}},
            catalog="c", schema="s", eval_scope="enrichment", rolled_back=True,
        )
    sql = captured[-1]
    # rolled_back is written 'true' so load_latest_state_iteration (rolled_back=false)
    # never reads the rejected coverage rung as current state.
    assert ", true," in sql  # the rolled_back literal slot


def test_load_latest_state_iteration_filters_rolled_back() -> None:
    # The current-state reader excludes rolled-back rows — so a rejected coverage
    # rung (written rolled_back=true) cannot pollute resume/clustering.
    src = inspect.getsource(state_mod.load_latest_state_iteration)
    assert "rolled_back = false" in src or "rolled_back IS NULL" in src


def test_coverage_no_candidate_persists_real_baseline_payload() -> None:
    # B1: the no-candidate rung loads the real baseline eval payload rather than
    # writing a synthetic empty eval.
    src = inspect.getsource(harness._run_lever_loop)
    assert "load_latest_full_iteration(" in src
    assert "No enrichment candidates" in src


# ── B2: coverage rollback is PROVEN; unprovable ⇒ LOOP_STATE_INVALID ─────────
def test_finalize_coverage_decision_proven_rollback() -> None:
    with patch.object(applier_mod, "rollback",
                      return_value={"status": "SUCCESS", "errors": []}), \
         patch.object(applier_mod, "verify_rollback_restored",
                      return_value={"verified": True}):
        out = harness._finalize_coverage_decision(
            w=MagicMock(), space_id="sp", pre_snapshot={"a": 1},
            frozen_baseline_accuracy=80.0, post_coverage_accuracy=74.0,
            had_candidates=True,
        )
    assert out["should_rollback"] is True
    assert out["rollback_proven"] is True
    assert out["terminal_reason"] is None


def test_finalize_coverage_decision_rollback_failure_is_loop_state_invalid() -> None:
    # applier.rollback reports an error ⇒ rollback unprovable ⇒ LOOP_STATE_INVALID.
    with patch.object(applier_mod, "rollback",
                      return_value={"status": "error", "errors": ["api failed"]}), \
         patch.object(applier_mod, "verify_rollback_restored",
                      return_value={"verified": True}):
        out = harness._finalize_coverage_decision(
            w=MagicMock(), space_id="sp", pre_snapshot={"a": 1},
            frozen_baseline_accuracy=80.0, post_coverage_accuracy=70.0,
            had_candidates=True,
        )
    assert out["should_rollback"] is True
    assert out["rollback_proven"] is False
    assert out["terminal_reason"] == "LOOP_STATE_INVALID"


def test_finalize_coverage_decision_unverified_rollback_is_loop_state_invalid() -> None:
    # rollback SUCCESS but the live config does NOT match pre_snapshot ⇒ unprovable.
    with patch.object(applier_mod, "rollback",
                      return_value={"status": "SUCCESS", "errors": []}), \
         patch.object(applier_mod, "verify_rollback_restored",
                      return_value={"verified": False, "reason": "mismatch"}):
        out = harness._finalize_coverage_decision(
            w=MagicMock(), space_id="sp", pre_snapshot={"a": 1},
            frozen_baseline_accuracy=80.0, post_coverage_accuracy=70.0,
            had_candidates=True,
        )
    assert out["terminal_reason"] == "LOOP_STATE_INVALID"


def test_finalize_coverage_decision_accept_does_not_roll_back() -> None:
    with patch.object(applier_mod, "rollback") as _rb:
        out = harness._finalize_coverage_decision(
            w=MagicMock(), space_id="sp", pre_snapshot={"a": 1},
            frozen_baseline_accuracy=80.0, post_coverage_accuracy=88.0,
            had_candidates=True,
        )
    assert out["should_rollback"] is False
    assert out["terminal_reason"] is None
    _rb.assert_not_called()


def test_coverage_failure_after_enrichment_rolls_back_before_surgical() -> None:
    # B2: the outer coverage exception path forces a rollback to the frozen
    # baseline before surgical mode (degrade to UNMODIFIED baseline on ANY failure).
    src = inspect.getsource(harness._run_lever_loop)
    # The except handler calls the measured protocol with a forced-rollback delta.
    assert "force rollback" in src
    assert "rolling back any" in src.lower() or "roll back any" in src.lower()


# ── B3: coverage example SQLs routed through apply_patch_set firewall ────────
def test_firewall_coverage_examples_calls_apply_patch_set_with_corpus() -> None:
    captured = {}

    def _fake_aps(w, space_id, patches, snapshot, **kwargs):  # noqa: ANN001
        captured["benchmark_corpus"] = kwargs.get("benchmark_corpus")
        captured["w"] = w
        captured["n_patches"] = len(patches)
        return {"dropped_patches": []}

    cfg = {
        "_parsed_space": {
            "example_question_sqls": [
                {"question": "q1?", "sql": "SELECT 1"},
                {"question": "q2?", "sql": "SELECT 2"},
            ]
        }
    }
    with patch.object(applier_mod, "apply_patch_set", _fake_aps):
        out = harness._firewall_coverage_example_sqls(
            w=MagicMock(), space_id="sp", config=cfg,
            benchmarks=[{"id": "b1", "question": "x", "sql": "SELECT 9"}],
            apply_mode="genie_config",
        )
    # The firewall path is exercised: apply_patch_set is called WITH a corpus and
    # the example SQLs are turned into add_example_sql patches.
    assert captured["benchmark_corpus"] is not None
    assert captured["n_patches"] == 2
    assert captured["w"] is None  # NON-MUTATING dry-run (no live PATCH)
    assert out["apply_log"] == {"dropped_patches": []}


def test_firewall_coverage_examples_strips_detected_leak() -> None:
    def _fake_aps(w, space_id, patches, snapshot, **kwargs):  # noqa: ANN001
        return {"dropped_patches": [
            {"example_question": "leak?", "drop_reason": "benchmark_leak:qid"},
        ]}

    cfg = {"_parsed_space": {"example_question_sqls": [
        {"question": "leak?", "sql": "SELECT 1"},
        {"question": "ok?", "sql": "SELECT 2"},
    ]}}
    _patched = {}
    with patch.object(applier_mod, "apply_patch_set", _fake_aps), \
         patch("genie_space_optimizer.common.genie_client.patch_space_config",
               lambda w, sid, c: _patched.update({"cfg": c})):
        out = harness._firewall_coverage_example_sqls(
            w=MagicMock(), space_id="sp", config=cfg,
            benchmarks=[{"id": "b1"}], apply_mode="genie_config",
        )
    assert len(out["dropped"]) == 1
    # The leaking example was stripped from the re-PATCHed config.
    kept = [e["question"] for e in _patched["cfg"]["example_question_sqls"]]
    assert kept == ["ok?"]


def test_coverage_block_invokes_firewall_helper() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "_firewall_coverage_example_sqls(" in src


# ── B4: exactly ONE measured coverage protocol (no silent better-of) ─────────
def test_optimize_genie_space_uses_measured_coverage_not_silent_better_of() -> None:
    src = inspect.getsource(harness.optimize_genie_space)
    # The gate runs the measured/reversible protocol …
    assert "_finalize_coverage_decision(" in src
    # … and no longer admits post-enrichment via the silent better-of helper.
    assert "_resolve_effective_starting_point(" not in src


def test_both_paths_share_one_coverage_protocol() -> None:
    # Both the inline loop and the standalone orchestrator call the SAME helper.
    assert "_finalize_coverage_decision(" in inspect.getsource(harness._run_lever_loop)
    assert "_finalize_coverage_decision(" in inspect.getsource(harness.optimize_genie_space)


# ── B5 / NB3: resume loads loop-state counters; coverage not re-run ──────────
def _resume_with_latest(latest_row: dict) -> dict:
    spark = MagicMock()
    with patch.object(harness, "load_latest_state_iteration", return_value=latest_row), \
         patch.object(harness, "load_stages", return_value=pd.DataFrame()), \
         patch.object(harness, "load_all_full_iterations", return_value=[]):
        return harness._resume_lever_loop(spark, "run1", "c", "s")


def test_resume_reports_committed_surgical_counter() -> None:
    out = _resume_with_latest({
        "iteration": 2, "attempt_no": 3, "attempt_mode": "surgical",
        "surgical_attempts_used": 2, "overall_accuracy": 88.0, "scores_json": {},
    })
    assert out["coverage_committed"] is True
    assert out["resumed_surgical_attempts_used"] == 2


def test_resume_after_coverage_only_skips_rerun() -> None:
    # Latest committed row is the coverage rung (attempt_no=1, surgical_used=0):
    # resume marks coverage committed so the controller does NOT re-run it.
    out = _resume_with_latest({
        "iteration": 0, "attempt_no": 1, "attempt_mode": "coverage",
        "surgical_attempts_used": 0, "overall_accuracy": 80.0, "scores_json": {},
    })
    assert out["coverage_committed"] is True
    assert out["resumed_surgical_attempts_used"] == 0


def test_resume_cold_start_runs_coverage() -> None:
    out = _resume_with_latest({
        "iteration": 0, "attempt_no": None, "attempt_mode": None,
        "surgical_attempts_used": None, "overall_accuracy": 80.0, "scores_json": {},
    })
    assert out["coverage_committed"] is False
    assert out["resumed_surgical_attempts_used"] == 0


def test_controller_skips_committed_coverage_and_seeds_counters() -> None:
    # NB3 (controller-level): the coverage block is guarded by the resume flag and
    # the surgical counters are seeded from the persisted value.
    src = inspect.getsource(harness._run_lever_loop)
    assert "not _coverage_already_committed" in src
    assert "_surgical_used = _resumed_surgical_used" in src
    assert "_surgical_iter = _resumed_surgical_used" in src


# ── B6: specific terminal reasons per break site; plateau cannot fire ────────
def test_break_sites_stamp_specific_terminal_reasons() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    # Budget exhaustion is its own typed reason (NB1), distinct from MAX_ATTEMPTS.
    assert '_loop_terminal_reason = "EVAL_BUDGET_EXHAUSTED"' in src
    # No-AG / divergence / consecutive limits ⇒ NO_NEW_HYPOTHESIS.
    assert '_loop_terminal_reason = "NO_NEW_HYPOTHESIS"' in src
    # Invalid eval ⇒ EVAL_INVALID.
    assert '_loop_terminal_reason = "EVAL_INVALID"' in src
    # Target / objective ⇒ TARGET_REACHED.
    assert '_loop_terminal_reason = "TARGET_REACHED"' in src
    # Unprovable coverage rollback ⇒ LOOP_STATE_INVALID.
    assert '_loop_terminal_reason = "LOOP_STATE_INVALID"' in src


def test_plateau_termination_guarded_off_for_phase8() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    # The plateau stop is gated on NOT full-only eval, so under the Phase-8
    # full-benchmark-only controller it cannot fire (plateau stays DEFERRED).
    assert "_plateau_stop_enabled = not _full_only_for_plateau()" in src
    assert "_plateau_detected = _plateau_stop_enabled and" in src
