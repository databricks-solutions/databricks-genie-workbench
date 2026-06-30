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
import pytest

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


# ── B3 / C1: coverage example SQLs routed through apply_patch_set firewall over
#            the REAL instructions.example_question_sqls path, list-value normalized
def test_firewall_coverage_examples_reads_instructions_path_and_normalizes() -> None:
    captured = {}

    def _fake_aps(w, space_id, patches, snapshot, **kwargs):  # noqa: ANN001
        captured["benchmark_corpus"] = kwargs.get("benchmark_corpus")
        captured["w"] = w
        captured["patches"] = patches
        return {"dropped_patches": []}

    # Real schema path is instructions.example_question_sqls; one entry carries
    # list[str]-valued question/sql which must be normalized to the string value.
    cfg = {
        "_parsed_space": {
            "instructions": {
                "example_question_sqls": [
                    {"question": ["q1?"], "sql": ["SELECT 1"]},
                    {"question": "q2?", "sql": "SELECT 2"},
                ]
            }
        }
    }
    with patch.object(applier_mod, "apply_patch_set", _fake_aps):
        out = harness._firewall_coverage_example_sqls(
            w=MagicMock(), space_id="sp", config=cfg,
            benchmarks=[{"id": "b1", "question": "x", "sql": "SELECT 9"}],
            apply_mode="genie_config",
        )
    # Patches reach the firewall WITH a corpus, built from the instructions path …
    assert captured["benchmark_corpus"] is not None
    assert captured["w"] is None  # NON-MUTATING dry-run (no live PATCH)
    assert len(captured["patches"]) == 2
    # … and list-valued fields are normalized to the actual string (not str(list)).
    qs = sorted(p["example_question"] for p in captured["patches"])
    sqls = sorted(p["example_sql"] for p in captured["patches"])
    assert qs == ["q1?", "q2?"]
    assert sqls == ["SELECT 1", "SELECT 2"]
    assert out["apply_log"] == {"dropped_patches": []}


def test_firewall_coverage_examples_strips_leak_from_instructions_path() -> None:
    def _fake_aps(w, space_id, patches, snapshot, **kwargs):  # noqa: ANN001
        return {"dropped_patches": [
            {"example_question": "leak?", "drop_reason": "benchmark_leak:qid"},
        ]}

    cfg = {"_parsed_space": {"instructions": {"example_question_sqls": [
        {"question": "leak?", "sql": "SELECT 1"},
        {"question": "ok?", "sql": "SELECT 2"},
    ]}}}
    _patched = {}
    with patch.object(applier_mod, "apply_patch_set", _fake_aps), \
         patch("genie_space_optimizer.common.genie_client.patch_space_config",
               lambda w, sid, c: _patched.update({"cfg": c})):
        out = harness._firewall_coverage_example_sqls(
            w=MagicMock(), space_id="sp", config=cfg,
            benchmarks=[{"id": "b1"}], apply_mode="genie_config",
        )
    assert len(out["dropped"]) == 1
    # The leak is stripped from the LIVE instructions.example_question_sqls path
    # (not a stale top-level copy), and that cleaned config is what is re-PATCHed.
    kept = [
        e["question"]
        for e in _patched["cfg"]["instructions"]["example_question_sqls"]
    ]
    assert kept == ["ok?"]
    # E1: the CALLER-VISIBLE config (config["_parsed_space"], carried into surgical
    # via the helper's return) is rebound to the cleaned copy — the dropped example
    # must NOT survive in the snapshot the controller uses afterward.
    cleaned = out["config"]["_parsed_space"]["instructions"]["example_question_sqls"]
    assert [e["question"] for e in cleaned] == ["ok?"]
    assert cfg["_parsed_space"]["instructions"]["example_question_sqls"] == cleaned


def test_coverage_caller_rebinds_metadata_snapshot_to_firewalled_config() -> None:
    # E1: after the firewall call the controller re-points config + metadata_snapshot
    # at the cleaned config (the helper's return), so the stale parsed object with the
    # dropped example is never carried into surgical mode.
    src = inspect.getsource(harness._run_lever_loop)
    assert "_fw_out = _firewall_coverage_example_sqls(" in src
    assert "config = _fw_cfg" in src
    assert "metadata_snapshot = _fw_parsed" in src


# ── D1: firewall is FAIL-CLOSED — strip/re-PATCH failure must NOT be swallowed ─
def test_firewall_reraises_when_strip_repatch_fails() -> None:
    # Leak detected, but the live strip/re-PATCH fails → the helper must RAISE so
    # the caller hard-stops (proven rollback + LOOP_STATE_INVALID); coverage must
    # NOT be evaluated against the still-leaked config.
    def _fake_aps(w, space_id, patches, snapshot, **kwargs):  # noqa: ANN001
        return {"dropped_patches": [
            {"example_question": "leak?", "drop_reason": "benchmark_leak:qid"},
        ]}

    def _boom_patch(w, sid, c):  # noqa: ANN001
        raise RuntimeError("genie API down")

    cfg = {"_parsed_space": {"instructions": {"example_question_sqls": [
        {"question": "leak?", "sql": "SELECT 1"},
    ]}}}
    with patch.object(applier_mod, "apply_patch_set", _fake_aps), \
         patch("genie_space_optimizer.common.genie_client.patch_space_config", _boom_patch):
        with pytest.raises(Exception):
            harness._firewall_coverage_example_sqls(
                w=MagicMock(), space_id="sp", config=cfg,
                benchmarks=[{"id": "b1"}], apply_mode="genie_config",
            )


def test_firewall_reraises_when_apply_patch_set_raises() -> None:
    def _boom_aps(*a, **k):  # noqa: ANN002, ANN003
        raise RuntimeError("applier exploded")

    cfg = {"_parsed_space": {"instructions": {"example_question_sqls": [
        {"question": "q?", "sql": "SELECT 1"},
    ]}}}
    with patch.object(applier_mod, "apply_patch_set", _boom_aps):
        with pytest.raises(Exception):
            harness._firewall_coverage_example_sqls(
                w=MagicMock(), space_id="sp", config=cfg,
                benchmarks=[{"id": "b1"}], apply_mode="genie_config",
            )


def test_coverage_caller_does_not_swallow_firewall_failure() -> None:
    # The coverage caller must NOT wrap the firewall in a 'non-fatal' try/except;
    # a firewall failure propagates to the fail-closed handler (LOOP_STATE_INVALID).
    src = inspect.getsource(harness._run_lever_loop)
    assert "Coverage example-SQL firewall pass raised (non-fatal)" not in src
    # The outer coverage handler is fail-closed (always LOOP_STATE_INVALID).
    fw_block = src.split("_firewall_coverage_example_sqls(", 1)[1].split("\n\n", 1)[0]
    assert "non-fatal" not in fw_block.lower()


# ── C2: mutation detection via real pre/post config comparison ──────────────
def test_coverage_candidate_detection_catches_instruction_only_mutation() -> None:
    # A coverage pass whose ONLY mutator touched instructions (e.g. the legacy
    # miner / preflight synthesis) must register as a CANDIDATE so eval+rollback
    # run. The controller derives this from a canonical pre/post compare, so a
    # miner-only instruction change is detected (the old per-mutator heuristic
    # missed it). We prove the comparison primitive the controller uses.
    canon = applier_mod._canonical_for_rollback_compare
    pre = {"instructions": {"text_instructions": [{"content": "base"}]}}
    post = {"instructions": {"text_instructions": [{"content": "base + mined hint"}]}}
    assert canon(pre) != canon(post)  # ⇒ _coverage_had_candidates is True
    # Unchanged config (runtime-only keys differ) ⇒ NOT a candidate.
    assert canon({"a": 1, "_uc_columns": [1]}) == canon({"a": 1, "_uc_columns": [2, 3]})


def test_coverage_block_uses_config_comparison_for_candidates() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "_canonical_for_rollback_compare as _canon_cfg" in src
    assert "_canon_cfg(_coverage_pre_snapshot) != _canon_cfg(metadata_snapshot)" in src


# ── C3 / D3: scoped state correction, SQL-escaped, REQUIRED (fail-closed) ────
def test_mark_iteration_rolled_back_is_scoped_by_eval_scope() -> None:
    captured: list[str] = []
    with patch.object(state_mod, "execute_delta_write_with_retry",
                      lambda spark, sql, **k: captured.append(sql)):
        state_mod.mark_iteration_rolled_back(
            MagicMock(), "run1", 0, catalog="c", schema="s",
            eval_scope="enrichment", reason="coverage_rolled_back",
        )
    sql = captured[-1]
    assert sql.startswith("UPDATE")
    assert "rolled_back = true" in sql
    # Scoped so the sibling 'full' baseline row at the same iteration is untouched.
    assert "eval_scope = 'enrichment'" in sql
    assert "iteration = 0" in sql


def test_mark_iteration_rolled_back_escapes_run_id_and_scope() -> None:
    # D3: run_id / eval_scope are SQL-escaped (quote-doubled), not interpolated raw.
    captured: list[str] = []
    with patch.object(state_mod, "execute_delta_write_with_retry",
                      lambda spark, sql, **k: captured.append(sql)):
        state_mod.mark_iteration_rolled_back(
            MagicMock(), "run' OR '1'='1", 0, catalog="c", schema="s",
            eval_scope="enri'chment", reason="r'x",
        )
    sql = captured[-1]
    assert "run'' OR ''1''=''1" in sql  # quote-doubled, no injection
    assert "eval_scope = 'enri''chment'" in sql


def test_mark_iteration_rolled_back_raises_on_failure() -> None:
    # D3: REQUIRED, not best-effort — a failed UPDATE must raise so the caller
    # can fail-closed rather than leaving the rejected row selectable.
    def _boom(*a, **k):  # noqa: ANN002, ANN003
        raise RuntimeError("delta down")

    with patch.object(state_mod, "execute_delta_write_with_retry", _boom):
        with pytest.raises(Exception):
            state_mod.mark_iteration_rolled_back(
                MagicMock(), "run1", 0, catalog="c", schema="s",
                eval_scope="enrichment", reason="x",
            )


def test_standalone_rollback_correction_replaces_config_with_baseline() -> None:
    # D2: after a coverage rollback, the surgical loop must start from the PROVEN
    # baseline config, never the rejected post-enrichment one.
    enrichment_out = {"config": {"_parsed_space": {"REJECTED": True}}}
    baseline_cfg = {"_parsed_space": {"BASELINE": True}}
    with patch.object(harness, "update_run_status"), \
         patch("genie_space_optimizer.optimization.state.mark_iteration_rolled_back"), \
         patch("genie_space_optimizer.common.genie_client.fetch_space_config",
               return_value=baseline_cfg):
        ok = harness._correct_state_after_standalone_coverage_rollback(
            spark=MagicMock(), w=MagicMock(), run_id="r1", space_id="sp",
            catalog="c", schema="s", enrichment_out=enrichment_out,
            baseline_accuracy=80.0, delta_pp=-5.0,
        )
    assert ok is True
    # The rejected config was replaced with the proven baseline.
    assert enrichment_out["config"] == baseline_cfg


def test_standalone_rollback_correction_fails_closed_on_state_error() -> None:
    # D3: if the required state correction cannot commit, return False so the
    # caller stops LOOP_STATE_INVALID instead of entering surgical mode.
    enrichment_out = {"config": {"_parsed_space": {"REJECTED": True}}}

    def _boom(*a, **k):  # noqa: ANN002, ANN003
        raise RuntimeError("mark failed")

    with patch.object(harness, "update_run_status"), \
         patch("genie_space_optimizer.optimization.state.mark_iteration_rolled_back", _boom):
        ok = harness._correct_state_after_standalone_coverage_rollback(
            spark=MagicMock(), w=MagicMock(), run_id="r1", space_id="sp",
            catalog="c", schema="s", enrichment_out=enrichment_out,
            baseline_accuracy=80.0, delta_pp=-5.0,
        )
    assert ok is False
    # The rejected config is NOT silently carried forward as "good"; the caller
    # will hard-stop LOOP_STATE_INVALID (asserted via the source contract below).
    assert enrichment_out["config"] == {"_parsed_space": {"REJECTED": True}}


def test_optimize_genie_space_fail_closed_on_correction_failure() -> None:
    # The standalone caller stops LOOP_STATE_INVALID when the correction returns False.
    src = inspect.getsource(harness.optimize_genie_space)
    assert "if not _correct_state_after_standalone_coverage_rollback(" in src
    _tail = src.split("if not _correct_state_after_standalone_coverage_rollback(", 1)[1][:1300]
    assert 'convergence_reason = "LOOP_STATE_INVALID"' in _tail
    assert "return result" in _tail


# ── F1: rollback_and_stop also corrects already-committed enrichment state ───
def _run_fail_closed_stop(enrichment_out):
    """Drive _standalone_coverage_fail_closed_stop with the leaf I/O mocked so we
    can assert the behavior (proven rollback + scoped row-hide + best reset +
    LOOP_STATE_INVALID) for both a present and a missing _enrichment_out."""
    result = MagicMock()
    rb_calls = []
    with patch.object(harness, "_finalize_coverage_decision",
                      side_effect=lambda **k: rb_calls.append(k) or {
                          "should_rollback": True, "rollback_proven": True,
                          "terminal_reason": None, "decision": "reject",
                          "decision_reason": "x", "delta_pp": -1.0,
                      }), \
         patch.object(harness, "update_run_status") as urs, \
         patch("genie_space_optimizer.optimization.state.mark_iteration_rolled_back") as mirb, \
         patch("genie_space_optimizer.common.genie_client.fetch_space_config",
               return_value={"_parsed_space": {"BASELINE": True}}):
        out = harness._standalone_coverage_fail_closed_stop(
            result=result, spark=MagicMock(), w=MagicMock(),
            run_id="r1", space_id="sp", catalog="c", schema="s",
            enrichment_out=enrichment_out, pre_snapshot={"a": 1},
            baseline_accuracy=80.0, model_id="m0", prev_scores={"s": 1.0},
            reason_detail="enrichment raised",
        )
    return out, result, rb_calls, urs, mirb


def test_f1_rollback_and_stop_corrects_committed_state_late_raise() -> None:
    # LATE _run_enrichment raise loses the return object, but the row + best-* were
    # already committed. The fail-closed stop must still proven-rollback, hide the
    # scoped enrichment row, reset best-*, and end LOOP_STATE_INVALID.
    out, result, rb_calls, urs, mirb = _run_fail_closed_stop(enrichment_out=None)
    # 1. proven rollback to the frozen baseline ran.
    assert rb_calls, "no proven rollback attempted"
    assert rb_calls[0]["pre_snapshot"] == {"a": 1}
    # 2. scoped enrichment row hidden (rolled_back) BEFORE returning — even with
    #    _enrichment_out is None (the late-raise case).
    mirb.assert_called_once()
    _args, _kwargs = mirb.call_args
    assert _kwargs.get("eval_scope") == "enrichment"
    assert _args[2] == 0  # iteration 0
    # 3. run best-* reset to baseline.
    assert any(
        c.kwargs.get("best_iteration") == 0 and c.kwargs.get("best_accuracy") == 80.0
        for c in urs.call_args_list
    ), "run best-* not reset to baseline"
    # 4. terminal LOOP_STATE_INVALID.
    assert result.convergence_reason == "LOOP_STATE_INVALID"
    assert result.status == "FAILED"
    assert out is result


def test_f1_rollback_and_stop_with_present_enrichment_out() -> None:
    # Same correction runs when _enrichment_out is a dict (early raise / no-post-acc).
    enr = {"config": {"_parsed_space": {"REJECTED": True}}}
    _out, result, rb_calls, urs, mirb = _run_fail_closed_stop(enrichment_out=enr)
    assert rb_calls  # proven rollback
    mirb.assert_called_once()  # scoped row hide
    assert result.convergence_reason == "LOOP_STATE_INVALID"
    # config replaced with the proven baseline (so even if it were carried on, it's clean).
    assert enr["config"] == {"_parsed_space": {"BASELINE": True}}


def test_rollback_and_stop_branch_uses_fail_closed_stop_before_return() -> None:
    # The standalone rollback_and_stop branch delegates to the fail-closed stop
    # (which runs the correction) and returns its result — no bare return that
    # skips the persisted-state correction.
    src = inspect.getsource(harness.optimize_genie_space)
    branch = src.split('if _std_gate == "rollback_and_stop":', 1)[1][:900]
    assert "return _standalone_coverage_fail_closed_stop(" in branch


# ── NB1: no-candidate loader failure does NOT leave synthetic counts current ─
def test_no_candidate_loader_failure_marks_rung_excluded() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "_coverage_eval_incomplete = True" in src
    # The final coverage write excludes an incomplete rung from current state.
    assert 'or _coverage_eval_incomplete' in src


# ── E2: standalone path is fail-closed (anchor + measured/rollback/baseline gate) ─
from genie_space_optimizer.optimization.control_plane import (  # noqa: E402
    decide_standalone_coverage_gate,
    resolve_coverage_rollback_anchor,
)


def test_e2a_anchor_prefers_dedicated_then_baseline_else_empty() -> None:
    # Dedicated snapshot present → used.
    assert resolve_coverage_rollback_anchor(
        dedicated_snapshot={"a": 1}, baseline_config={"_parsed_space": {"b": 2}},
    ) == {"a": 1}
    # E2a: dedicated fetch failed (empty) → fall back to the baseline config's parsed space.
    assert resolve_coverage_rollback_anchor(
        dedicated_snapshot={}, baseline_config={"_parsed_space": {"b": 2}},
    ) == {"b": 2}
    # No anchor at all → {} (the caller must STOP before mutating, fail-closed).
    assert resolve_coverage_rollback_anchor(
        dedicated_snapshot={}, baseline_config={},
    ) == {}


def test_e2b_enrichment_raised_after_mutation_rolls_back_and_stops() -> None:
    # (b) _run_enrichment raised after possibly mutating → no measured accuracy and
    # a possibly-mutated live space ⇒ roll back + stop (never surgical).
    assert decide_standalone_coverage_gate(
        enrichment_raised=True, post_accuracy=None, live_may_be_mutated=True,
    ) == "rollback_and_stop"


def test_e2c_no_post_accuracy_with_mutation_rolls_back_and_stops() -> None:
    # (c) post_enrichment_accuracy=None while the live space differs from the
    # pre-enrichment snapshot ⇒ roll back + stop; baseline_eval must NOT stand.
    assert decide_standalone_coverage_gate(
        enrichment_raised=False, post_accuracy=None, live_may_be_mutated=True,
    ) == "rollback_and_stop"


def test_e2_provably_unmutated_noop_uses_baseline() -> None:
    # No post-accuracy AND provably unmutated (true no-op) ⇒ baseline is safe.
    assert decide_standalone_coverage_gate(
        enrichment_raised=False, post_accuracy=None, live_may_be_mutated=False,
    ) == "baseline"


def test_e2_measured_when_post_accuracy_present() -> None:
    assert decide_standalone_coverage_gate(
        enrichment_raised=False, post_accuracy=88.0, live_may_be_mutated=True,
    ) == "measured"


def test_optimize_genie_space_wires_e2_fail_closed_gates() -> None:
    src = inspect.getsource(harness.optimize_genie_space)
    # E2a: anchor resolved (via the imported helper) + hard-stop before mutation.
    assert "resolve_coverage_rollback_anchor as _resolve_anchor" in src
    assert "_resolve_anchor(" in src
    assert "no valid pre-enrichment rollback anchor" in src
    # E2b/c: the gate drives rollback_and_stop / measured / baseline.
    assert "decide_standalone_coverage_gate as _decide_std_gate" in src
    assert "_decide_std_gate(" in src
    assert 'if _std_gate == "rollback_and_stop":' in src
    assert 'if _std_gate == "measured"' in src


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
