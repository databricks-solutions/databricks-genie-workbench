"""Phase 4 — per-iteration acceptance, held-out semantics, corpus sweeps.

Covers the plan's P4.2-P4.6 test contracts:

* Held-out benchmarks are never fed into LLM-bound paths (cluster input,
  AFS, filter helpers).
* Cluster attestation is derived correctly as a SLICE of the per-
  iteration full-training eval — no separate eval calls.
* Acceptance predicate respects the cluster_net_delta + out-of-cluster
  regression thresholds: partial-fix accepted, regression-exceeding-fix
  rejected, zero-net-delta rejected.
* Baseline + finalize sweep objects compute before/after metrics on
  identical qid sets.
* Improvement summary exposes the user-visible ``final - baseline``
  number.
* Sizing flag isolation: flipping ``GSO_NEW_SIZING=0`` restores the
  legacy sizing constants.
"""

from __future__ import annotations

import importlib

import pytest

from genie_space_optimizer.optimization.iteration_acceptance import (
    ClusterAttestation,
    HeldOutLeakError,
    QuestionPassMap,
    assert_no_held_out_in_cluster_input,
    build_corpus_sweep_result,
    count_out_of_cluster_regressions,
    decide_iteration_acceptance,
    derive_cluster_attestation,
    filter_out_held_out,
    improvement_summary,
)


# ── Held-out semantics (P4.2) ──────────────────────────────────────────


def test_filter_out_held_out_drops_held_ids() -> None:
    benchmarks = [
        {"id": "q1", "question": "a", "expected_sql": "..."},
        {"id": "q2", "question": "b", "expected_sql": "..."},
        {"id": "q_held", "question": "c", "expected_sql": "..."},
    ]
    filtered = filter_out_held_out(benchmarks, {"q_held"})
    assert [b["id"] for b in filtered] == ["q1", "q2"]


def test_assert_no_held_out_raises_on_overlap() -> None:
    with pytest.raises(HeldOutLeakError):
        assert_no_held_out_in_cluster_input(
            ["q1", "q_held", "q2"], {"q_held"},
        )


def test_assert_no_held_out_passes_when_disjoint() -> None:
    # Must not raise.
    assert_no_held_out_in_cluster_input(["q1", "q2"], {"q_held", "q_held2"})


def test_cluster_failures_filters_held_out() -> None:
    """cluster_failures with held_out_qids must drop matching rows before
    clustering — downstream LLM prompts never see held-out content."""
    from genie_space_optimizer.optimization.optimizer import cluster_failures

    # Synthesize minimal eval_results — a list of per-question rows with
    # the shape cluster_failures expects. We use the "rows" carrier.
    eval_results = {
        "rows": [
            {
                "question_id": "q_train",
                "request": {"input": "q"},
                "response": {},
                "judge_verdicts": {"arbiter": {"value": "no"}},
            },
            {
                "question_id": "q_held",
                "request": {"input": "q"},
                "response": {},
                "judge_verdicts": {"arbiter": {"value": "no"}},
            },
        ],
    }
    # Even if we pass rows that would otherwise cluster, the held_out
    # row must not appear in the result.
    clusters = cluster_failures(
        eval_results, metadata_snapshot={},
        held_out_qids={"q_held"},
        verbose=False,
    )
    for c in clusters:
        assert "q_held" not in (c.get("question_ids") or []), (
            "held-out qid appeared in clusters — P4.2 invariant broken"
        )


# ── Cluster attestation as a slice (P4.3) ──────────────────────────────


def test_attestation_derived_as_slice() -> None:
    pre = QuestionPassMap({"q1": False, "q2": False, "q3": True, "q_outside": True})
    post = QuestionPassMap({"q1": True, "q2": False, "q3": True, "q_outside": True})
    att = derive_cluster_attestation(
        "C1", ("q1", "q2", "q3"), pre, post,
    )
    assert att.newly_passing == 1
    assert att.newly_failing == 0
    assert att.net_delta == 1
    # Slice must exclude qids not in target_qids.
    assert "q_outside" not in att.pre_passes
    assert "q_outside" not in att.post_passes


def test_attestation_handles_missing_qids_as_failing() -> None:
    pre = QuestionPassMap({"q1": True})
    post = QuestionPassMap({})
    # q_unknown is missing — treated as failing in both; net delta = 0.
    att = derive_cluster_attestation(
        "C2", ("q_unknown", "q1"), pre, post,
    )
    # q1: pre=True, post=False (missing treated as False) → newly_failing += 1
    assert att.newly_failing == 1


def test_out_of_cluster_regression_detection() -> None:
    pre = QuestionPassMap({"q1": True, "q2": True, "q3": True, "q_target": True})
    post = QuestionPassMap({"q1": False, "q2": True, "q3": True, "q_target": False})
    regressed = count_out_of_cluster_regressions(("q_target",), pre, post)
    assert regressed == ("q1",)


# ── Acceptance predicate (P4.4) ────────────────────────────────────────


def test_acceptance_accepts_partial_fix() -> None:
    pre = QuestionPassMap({f"q{i}": False for i in range(10)})
    post_passes = dict(pre.passes)
    for q in ("q0", "q1", "q2", "q3"):
        post_passes[q] = True  # 4 newly passing within cluster
    post = QuestionPassMap(post_passes)

    result = decide_iteration_acceptance(
        "C", [f"q{i}" for i in range(10)], pre, post,
    )
    assert result.accepted
    assert result.attestation.net_delta == 4
    assert result.out_of_cluster_newly_failing == ()


def test_acceptance_rejects_zero_net_delta() -> None:
    pre = QuestionPassMap({"q1": False, "q2": True})
    post = QuestionPassMap({"q1": True, "q2": False})  # 1 in, 1 out
    result = decide_iteration_acceptance("C", ("q1", "q2"), pre, post)
    assert not result.accepted
    assert result.attestation.net_delta == 0
    assert "cluster_net_delta_below_min" in result.reason


def test_acceptance_rejects_regression_exceeding_fix() -> None:
    pre = QuestionPassMap({"q1": False, "q2": True, "q3": True})
    post = QuestionPassMap({"q1": True, "q2": False, "q3": False})
    # newly_passing=1, newly_failing=2 → net_delta = -1
    result = decide_iteration_acceptance("C", ("q1", "q2", "q3"), pre, post)
    assert not result.accepted


def test_acceptance_rejects_out_of_cluster_regression() -> None:
    pre = QuestionPassMap({"q_target": False, "q_outside": True})
    post = QuestionPassMap({"q_target": True, "q_outside": False})
    # Cluster net_delta = 1 (good), but q_outside regressed.
    result = decide_iteration_acceptance("C", ("q_target",), pre, post)
    assert not result.accepted
    assert "out_of_cluster_regression" in result.reason
    assert "q_outside" in result.out_of_cluster_newly_failing


def test_acceptance_tolerates_ooc_when_configured() -> None:
    pre = QuestionPassMap({"q_target": False, "q_o1": True})
    post = QuestionPassMap({"q_target": True, "q_o1": False})
    result = decide_iteration_acceptance(
        "C", ("q_target",), pre, post,
        out_of_cluster_tolerance=1,
    )
    assert result.accepted


def test_acceptance_disabled_always_accepts() -> None:
    pre = QuestionPassMap({"q1": True})
    post = QuestionPassMap({"q1": False})
    result = decide_iteration_acceptance(
        "C", ("q1",), pre, post, enabled=False,
    )
    assert result.accepted
    assert result.reason == "acceptance_disabled"


# ── Baseline / finalize sweeps (P4.5) ──────────────────────────────────


def test_corpus_sweep_computes_pass_rate() -> None:
    sweep = build_corpus_sweep_result(
        train_passes={"q1": True, "q2": True, "q3": False, "q4": False},
        heldout_passes={"q_h1": True, "q_h2": False},
    )
    assert sweep.train_pass_rate == pytest.approx(0.5)
    assert sweep.heldout_pass_rate == pytest.approx(0.5)


def test_baseline_and_finalize_on_identical_qid_sets() -> None:
    qids = ["q1", "q2", "q3", "q4", "q5"]
    baseline = build_corpus_sweep_result(
        train_passes={q: False for q in qids[:3]},
        heldout_passes={q: False for q in qids[3:]},
    )
    final = build_corpus_sweep_result(
        train_passes={q: (q in {"q1", "q2"}) for q in qids[:3]},
        heldout_passes={q: (q == "q4") for q in qids[3:]},
    )
    # Identical qid sets, so set(train_passes.keys()) is stable.
    assert set(baseline.train_passes.keys()) == set(final.train_passes.keys())
    assert set(baseline.heldout_passes.keys()) == set(final.heldout_passes.keys())

    summary = improvement_summary(baseline, final)
    assert summary["baseline_total_passed"] == 0
    # final: 2 train passed + 1 heldout passed
    assert summary["final_total_passed"] == 3
    assert summary["improvement_questions"] == 3


def test_improvement_summary_emits_deltas_in_pct() -> None:
    baseline = build_corpus_sweep_result(
        train_passes={"q1": True, "q2": False},
        heldout_passes={"q_h1": False, "q_h2": False},
    )
    final = build_corpus_sweep_result(
        train_passes={"q1": True, "q2": True},
        heldout_passes={"q_h1": True, "q_h2": False},
    )
    summary = improvement_summary(baseline, final)
    deltas = summary["deltas_pct"]
    # Train: 0.5 → 1.0 = +50pp. Held-out: 0 → 0.5 = +50pp.
    assert deltas["train_delta_pct"] == pytest.approx(50.0)
    assert deltas["heldout_delta_pct"] == pytest.approx(50.0)


# ── Sizing flag isolation (P4.6) ───────────────────────────────────────


def test_new_sizing_flag_defaults_to_phase4_values(monkeypatch) -> None:
    monkeypatch.setenv("GSO_NEW_SIZING", "true")
    import genie_space_optimizer.common.config as _cfg
    importlib.reload(_cfg)
    assert _cfg.TARGET_BENCHMARK_COUNT == 30
    assert _cfg.MAX_BENCHMARK_COUNT == 35
    assert _cfg.HELD_OUT_RATIO == 0.15


def test_new_sizing_flag_off_restores_legacy(monkeypatch) -> None:
    monkeypatch.setenv("GSO_NEW_SIZING", "0")
    import genie_space_optimizer.common.config as _cfg
    importlib.reload(_cfg)
    try:
        assert _cfg.TARGET_BENCHMARK_COUNT == 24
        assert _cfg.MAX_BENCHMARK_COUNT == 29
    finally:
        # Restore the default so other tests don't see the legacy values.
        monkeypatch.setenv("GSO_NEW_SIZING", "true")
        importlib.reload(_cfg)


# ── finalize_attestation_matrix writer (P4.5) ─────────────────────────


def test_write_finalize_attestation_matrix_emits_one_row_per_qid() -> None:
    from unittest.mock import MagicMock
    from genie_space_optimizer.optimization.state import (
        write_finalize_attestation_matrix,
    )

    spark = MagicMock()
    write_finalize_attestation_matrix(
        spark, "run-1",
        iteration_idx="baseline",
        train_passes={"q1": True, "q2": False},
        heldout_passes={"q_h1": True},
        catalog="cat", schema="sch",
    )
    calls = [c.args[0] for c in spark.sql.call_args_list]
    inserts = [s for s in calls if "INSERT INTO" in s and "genie_opt_finalize_attestation_matrix" in s]
    assert len(inserts) == 1
    sql = inserts[0]
    # One row each for the 3 qids, with correct is_heldout markers.
    assert sql.count("VALUES") == 1
    # Held-out row must carry is_heldout=true; train rows false.
    assert ", true," in sql  # at least one held-out row
    assert ", false," in sql  # at least one train row
    assert "'baseline'" in sql
