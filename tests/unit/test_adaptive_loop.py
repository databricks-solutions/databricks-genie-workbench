"""Unit tests for the adaptive lever loop helpers.

Tests cover pure-Python functions: cluster_impact, rank_clusters,
format_reflection_buffer, _build_reflection_entry, _diminishing_returns,
and _filter_tried_clusters.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    cluster_impact,
    detect_regressions,
    format_reflection_buffer,
    rank_clusters,
)
from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
    _diminishing_returns,
    _filter_tried_clusters,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_cluster(
    *,
    root_cause: str = "wrong_column",
    question_ids: list[str] | None = None,
    affected_judge: str = "schema_accuracy",
    asi_failure_type: str | None = None,
    asi_counterfactual_fixes: list[str] | None = None,
    asi_blame_set: str | None = None,
    cluster_id: str = "C001",
) -> dict:
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "question_ids": question_ids or ["q1", "q2"],
        "affected_judge": affected_judge,
        "affected_judges": [affected_judge],
        "asi_failure_type": asi_failure_type or root_cause,
        "asi_blame_set": asi_blame_set,
        "asi_counterfactual_fixes": asi_counterfactual_fixes or [],
    }


def _make_reflection(
    *,
    iteration: int = 1,
    accepted: bool = True,
    accuracy_delta: float = 5.0,
    score_deltas: dict | None = None,
    do_not_retry: list[str] | None = None,
) -> dict:
    return {
        "iteration": iteration,
        "ag_id": f"AG{iteration}",
        "accepted": accepted,
        "action": f"update_description on table_{iteration}",
        "levers": [1],
        "target_objects": [f"table_{iteration}"],
        "score_deltas": score_deltas or {"schema_accuracy": accuracy_delta},
        "accuracy_delta": accuracy_delta,
        "new_failures": None,
        "rollback_reason": None if accepted else "regression",
        "do_not_retry": do_not_retry or [],
    }


# ═══════════════════════════════════════════════════════════════════════
# cluster_impact
# ═══════════════════════════════════════════════════════════════════════


class TestClusterImpact:
    def test_basic_scoring(self):
        c = _make_cluster(question_ids=["q1", "q2", "q3"])
        score = cluster_impact(c)
        assert score > 0

    def test_more_questions_increases_score(self):
        c_small = _make_cluster(question_ids=["q1"])
        c_large = _make_cluster(question_ids=["q1", "q2", "q3"])
        assert cluster_impact(c_large) > cluster_impact(c_small)

    def test_upstream_judge_has_higher_weight(self):
        c_syntax = _make_cluster(affected_judge="syntax_validity")
        c_quality = _make_cluster(affected_judge="response_quality")
        assert cluster_impact(c_syntax) > cluster_impact(c_quality)

    def test_counterfactual_increases_fixability(self):
        c_no_cf = _make_cluster(asi_counterfactual_fixes=[])
        c_with_cf = _make_cluster(asi_counterfactual_fixes=["ADD COLUMN x"])
        assert cluster_impact(c_with_cf) > cluster_impact(c_no_cf)

    def test_severe_failure_type_scores_higher(self):
        c_wrong_table = _make_cluster(root_cause="wrong_table", asi_failure_type="wrong_table")
        c_perf = _make_cluster(root_cause="performance_issue", asi_failure_type="performance_issue")
        assert cluster_impact(c_wrong_table) > cluster_impact(c_perf)

    def test_empty_cluster(self):
        c = _make_cluster(question_ids=[])
        score = cluster_impact(c)
        assert score > 0  # min 1 question

    def test_unknown_judge_defaults(self):
        c = _make_cluster(affected_judge="some_future_judge")
        score = cluster_impact(c)
        assert score > 0


# ═══════════════════════════════════════════════════════════════════════
# rank_clusters
# ═══════════════════════════════════════════════════════════════════════


class TestRankClusters:
    def test_returns_sorted_by_impact(self):
        c1 = _make_cluster(cluster_id="C001", question_ids=["q1"])
        c2 = _make_cluster(cluster_id="C002", question_ids=["q1", "q2", "q3"])
        ranked = rank_clusters([c1, c2])
        assert ranked[0]["cluster_id"] == "C002"
        assert ranked[1]["cluster_id"] == "C001"

    def test_adds_rank_and_impact_score(self):
        c1 = _make_cluster()
        ranked = rank_clusters([c1])
        assert ranked[0]["rank"] == 1
        assert "impact_score" in ranked[0]
        assert isinstance(ranked[0]["impact_score"], float)

    def test_does_not_mutate_original(self):
        c1 = _make_cluster()
        original_keys = set(c1.keys())
        rank_clusters([c1])
        assert set(c1.keys()) == original_keys

    def test_empty_input(self):
        assert rank_clusters([]) == []


# ═══════════════════════════════════════════════════════════════════════
# format_reflection_buffer
# ═══════════════════════════════════════════════════════════════════════


class TestFormatReflectionBuffer:
    def test_empty_buffer(self):
        result = format_reflection_buffer([])
        assert "first attempt" in result.lower()

    def test_single_entry_full_detail(self):
        r = _make_reflection(iteration=1, accepted=True, accuracy_delta=5.0)
        result = format_reflection_buffer([r], full_window=3)
        assert "ITERATION 1" in result
        assert "ACCEPTED" in result

    def test_old_entries_compressed(self):
        entries = [_make_reflection(iteration=i) for i in range(1, 6)]
        result = format_reflection_buffer(entries, full_window=2)
        assert "ITERATION 4" in result
        assert "ITERATION 5" in result
        for line in result.split("\n"):
            if line.startswith("Iter "):
                assert "ACCEPTED" in line or "ROLLED_BACK" in line

    def test_do_not_retry_section(self):
        r1 = _make_reflection(
            iteration=1,
            accepted=False,
            do_not_retry=["update_description on orders"],
        )
        result = format_reflection_buffer([r1], full_window=3)
        assert "DO NOT RETRY" in result
        assert "update_description on orders" in result

    def test_score_deltas_shown(self):
        r = _make_reflection(
            iteration=1,
            accepted=True,
            score_deltas={"schema_accuracy": 5.0, "syntax_validity": -2.0},
        )
        result = format_reflection_buffer([r], full_window=3)
        assert "schema_accuracy" in result

    def test_reflection_text_and_refinement_mode_shown(self):
        r = _make_reflection(iteration=1, accepted=False)
        r["reflection_text"] = "Rollback: instructions caused regressions."
        r["refinement_mode"] = "in_plan"
        result = format_reflection_buffer([r], full_window=3)
        assert "Reflection: Rollback: instructions caused regressions." in result
        assert "Refinement guidance: in_plan" in result


# ═══════════════════════════════════════════════════════════════════════
# _build_reflection_entry
# ═══════════════════════════════════════════════════════════════════════


class TestBuildReflectionEntry:
    def test_accepted_entry(self):
        entry = _build_reflection_entry(
            iteration=1,
            ag_id="AG1",
            accepted=True,
            levers=[1, 5],
            target_objects=["catalog.schema.orders"],
            prev_scores={"schema_accuracy": 60.0, "syntax_validity": 80.0},
            new_scores={"schema_accuracy": 70.0, "syntax_validity": 80.0},
            rollback_reason=None,
            patches=[
                {"patch_type": "update_description", "target_object": "catalog.schema.orders"},
            ],
        )
        assert entry["accepted"] is True
        assert entry["iteration"] == 1
        assert entry["accuracy_delta"] == 5.0  # (70+80)/2 - (60+80)/2 = 5.0
        assert entry["do_not_retry"] == []

    def test_rolled_back_entry_has_do_not_retry(self):
        entry = _build_reflection_entry(
            iteration=2,
            ag_id="AG2",
            accepted=False,
            levers=[4],
            target_objects=["catalog.schema.orders"],
            prev_scores={"schema_accuracy": 70.0},
            new_scores={"schema_accuracy": 60.0},
            rollback_reason="regression",
            patches=[
                {"patch_type": "add_join_spec", "target_object": "orders_items"},
            ],
        )
        assert entry["accepted"] is False
        assert len(entry["do_not_retry"]) == 1
        assert "add_join_spec on orders_items" in entry["do_not_retry"]

    def test_score_deltas_computed(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=True, levers=[1],
            target_objects=[],
            prev_scores={"a": 50.0, "b": 60.0},
            new_scores={"a": 55.0, "b": 65.0},
            rollback_reason=None, patches=[],
        )
        assert entry["score_deltas"]["a"] == 5.0
        assert entry["score_deltas"]["b"] == 5.0


# ═══════════════════════════════════════════════════════════════════════
# _diminishing_returns
# ═══════════════════════════════════════════════════════════════════════


class TestDiminishingReturns:
    def test_not_enough_entries(self):
        buf = [_make_reflection(iteration=1, accepted=True, accuracy_delta=0.5)]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_detects_diminishing_returns_accepted(self):
        buf = [
            _make_reflection(
                iteration=i,
                accepted=True,
                accuracy_delta=0.5,
                score_deltas={"a": 0.5},
            )
            for i in range(1, 4)
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True

    def test_no_diminishing_returns_when_large_delta(self):
        buf = [
            _make_reflection(
                iteration=1, accepted=True,
                accuracy_delta=0.5, score_deltas={"a": 0.5},
            ),
            _make_reflection(
                iteration=2, accepted=True,
                accuracy_delta=10.0, score_deltas={"a": 10.0},
            ),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_rollbacks_count_as_no_improvement(self):
        buf = [
            _make_reflection(iteration=1, accepted=True, accuracy_delta=5.0, score_deltas={"a": 5.0}),
            _make_reflection(iteration=2, accepted=False, score_deltas={"a": -5.0}),
            _make_reflection(iteration=3, accepted=False, score_deltas={"a": -3.0}),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True

    def test_recent_accepted_with_gain_prevents_trigger(self):
        buf = [
            _make_reflection(iteration=1, accepted=False, score_deltas={"a": -5.0}),
            _make_reflection(iteration=2, accepted=True, accuracy_delta=5.0, score_deltas={"a": 5.0}),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_empty_buffer(self):
        assert _diminishing_returns([], epsilon=2.0, lookback=2) is False


# ═══════════════════════════════════════════════════════════════════════
# _filter_tried_clusters
# ═══════════════════════════════════════════════════════════════════════


class TestFilterTriedClusters:
    def test_no_filter_when_empty(self):
        clusters = [_make_cluster(), _make_cluster(cluster_id="C002")]
        result = _filter_tried_clusters(clusters, set())
        assert len(result) == 2

    def test_removes_tried_cluster(self):
        c1 = _make_cluster(
            cluster_id="C001",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col1",
        )
        c2 = _make_cluster(
            cluster_id="C002",
            root_cause="wrong_join",
            asi_failure_type="wrong_join",
            asi_blame_set="orders|items",
        )
        tried = {("wrong_column", "orders.col1")}
        result = _filter_tried_clusters([c1, c2], tried)
        assert len(result) == 1
        assert result[0]["cluster_id"] == "C002"

    def test_keeps_clusters_with_different_blame(self):
        c1 = _make_cluster(
            cluster_id="C001",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col1",
        )
        c2 = _make_cluster(
            cluster_id="C002",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col2",
        )
        tried = {("wrong_column", "orders.col1")}
        result = _filter_tried_clusters([c1, c2], tried)
        assert len(result) == 1
        assert result[0]["cluster_id"] == "C002"

    def test_all_filtered_returns_empty(self):
        c = _make_cluster(
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="x",
        )
        tried = {("wrong_column", "x")}
        result = _filter_tried_clusters([c], tried)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════
# escalation_handled — new field and logic
# ═══════════════════════════════════════════════════════════════════════


class TestEscalationHandled:
    """Tests for the ``escalation_handled`` field on reflection entries
    and its effect on consecutive-rollback and diminishing-returns checks."""

    def _make_escalation_reflection(self, iteration: int = 1) -> dict:
        r = _make_reflection(iteration=iteration, accepted=False)
        r["escalation_handled"] = True
        return r

    def test_build_reflection_entry_includes_escalation_handled(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=False, levers=[],
            target_objects=["q1"], prev_scores={"a": 50.0},
            new_scores={"a": 50.0}, rollback_reason="escalation:gt_repair",
            patches=[], escalation_handled=True,
        )
        assert entry["escalation_handled"] is True

    def test_build_reflection_entry_default_false(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=False, levers=[1],
            target_objects=["t1"], prev_scores={"a": 50.0},
            new_scores={"a": 40.0}, rollback_reason="regression",
            patches=[],
        )
        assert entry["escalation_handled"] is False

    def test_diminishing_returns_skips_escalation_entries(self):
        buf = [
            self._make_escalation_reflection(iteration=1),
            self._make_escalation_reflection(iteration=2),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_diminishing_returns_uses_non_escalation_only(self):
        buf = [
            _make_reflection(iteration=1, accepted=True, accuracy_delta=0.5, score_deltas={"a": 0.5}),
            self._make_escalation_reflection(iteration=2),
            _make_reflection(iteration=3, accepted=True, accuracy_delta=0.3, score_deltas={"a": 0.3}),
            self._make_escalation_reflection(iteration=4),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True

    def test_consecutive_rollback_logic_skips_escalations(self):
        buf = [
            _make_reflection(iteration=1, accepted=True, accuracy_delta=5.0, score_deltas={"a": 5.0}),
            self._make_escalation_reflection(iteration=2),
            self._make_escalation_reflection(iteration=3),
            _make_reflection(iteration=4, accepted=False, score_deltas={"a": -5.0}),
        ]
        _consecutive_rb = 0
        for _rb_entry in reversed(buf):
            if _rb_entry.get("escalation_handled"):
                continue
            if not _rb_entry.get("accepted"):
                _consecutive_rb += 1
            else:
                break
        assert _consecutive_rb == 1


# ═══════════════════════════════════════════════════════════════════════
# Asset fingerprint
# ═══════════════════════════════════════════════════════════════════════


class TestAssetFingerprint:
    """Tests for compute_asset_fingerprint from preflight.py."""

    def test_stable_for_same_config(self):
        from genie_space_optimizer.optimization.preflight import compute_asset_fingerprint
        config = {"_tables": ["a.b.c", "a.b.d"], "_metric_views": [], "_functions": []}
        fp1 = compute_asset_fingerprint(config)
        fp2 = compute_asset_fingerprint(config)
        assert fp1 == fp2
        assert len(fp1) == 16

    def test_different_for_changed_tables(self):
        from genie_space_optimizer.optimization.preflight import compute_asset_fingerprint
        config1 = {"_tables": ["a.b.c"], "_metric_views": [], "_functions": []}
        config2 = {"_tables": ["a.b.c", "a.b.d"], "_metric_views": [], "_functions": []}
        fp1 = compute_asset_fingerprint(config1)
        fp2 = compute_asset_fingerprint(config2)
        assert fp1 != fp2

    def test_order_independent(self):
        from genie_space_optimizer.optimization.preflight import compute_asset_fingerprint
        config1 = {"_tables": ["z", "a"], "_metric_views": [], "_functions": []}
        config2 = {"_tables": ["a", "z"], "_metric_views": [], "_functions": []}
        assert compute_asset_fingerprint(config1) == compute_asset_fingerprint(config2)

    def test_empty_config(self):
        from genie_space_optimizer.optimization.preflight import compute_asset_fingerprint
        config = {"_tables": [], "_metric_views": [], "_functions": []}
        fp = compute_asset_fingerprint(config)
        assert len(fp) == 16

    def test_dict_entries(self):
        from genie_space_optimizer.optimization.preflight import compute_asset_fingerprint
        config = {
            "_tables": [{"identifier": "a.b.c"}, {"identifier": "a.b.d"}],
            "_metric_views": [],
            "_functions": [{"identifier": "fn1"}],
        }
        fp = compute_asset_fingerprint(config)
        assert len(fp) == 16


# ═══════════════════════════════════════════════════════════════════════
# Arbiter-adjusted judge scores
# ═══════════════════════════════════════════════════════════════════════


class TestArbiterAdjustedJudges:
    """Test that arbiter-adjustable judges are overridden when arbiter says both_correct."""

    def test_failing_judge_overridden_by_arbiter(self):
        from genie_space_optimizer.optimization.evaluation import _ARBITER_CORRECT_VERDICTS
        rows = [
            {
                "logical_accuracy/value": "no",
                "schema_accuracy/value": "no",
                "semantic_equivalence/value": "yes",
                "completeness/value": "yes",
                "arbiter/value": "both_correct",
            },
            {
                "logical_accuracy/value": "yes",
                "schema_accuracy/value": "yes",
                "semantic_equivalence/value": "yes",
                "completeness/value": "yes",
                "arbiter/value": "both_correct",
            },
        ]
        _ARBITER_ADJUSTABLE_JUDGES = [
            "logical_accuracy", "semantic_equivalence", "completeness", "schema_accuracy",
        ]
        for _judge_name in _ARBITER_ADJUSTABLE_JUDGES:
            _j_total = _j_correct = 0
            for _row in rows:
                _j_val = str(_row.get(f"{_judge_name}/value", "")).lower()
                if _j_val == "excluded":
                    continue
                _j_total += 1
                if _j_val in ("yes", "true", "1", "1.0", "pass"):
                    _j_correct += 1
                elif str(_row.get("arbiter/value", "")).lower() in _ARBITER_CORRECT_VERDICTS:
                    _j_correct += 1
            if _j_total > 0:
                score = _j_correct / _j_total
                assert score == 1.0, f"{_judge_name} should be 1.0 after arbiter adjustment"

    def test_non_correct_arbiter_does_not_override(self):
        from genie_space_optimizer.optimization.evaluation import _ARBITER_CORRECT_VERDICTS
        rows = [
            {
                "logical_accuracy/value": "no",
                "arbiter/value": "ground_truth_correct",
            },
        ]
        _j_total = _j_correct = 0
        for _row in rows:
            _j_val = str(_row.get("logical_accuracy/value", "")).lower()
            _j_total += 1
            if _j_val in ("yes", "true", "1", "1.0", "pass"):
                _j_correct += 1
            elif str(_row.get("arbiter/value", "")).lower() in _ARBITER_CORRECT_VERDICTS:
                _j_correct += 1
        assert _j_total == 1
        assert _j_correct == 0


# ═══════════════════════════════════════════════════════════════════════
# Acceptance gate guard — overall accuracy hard guard
# ═══════════════════════════════════════════════════════════════════════


def _apply_noise_filter_and_guard(
    best_scores: dict[str, float],
    new_scores: dict[str, float],
    best_accuracy: float,
    full_accuracy: float,
    num_benchmarks: int,
    regression_threshold: float = 2.0,
    has_patches: bool = True,
) -> list[dict]:
    """Reproduce the noise-filter + hard-guard logic from _run_gate_checks.

    Returns the final ``regressions`` list after both the noise filter
    and the overall_accuracy_guard have been applied.
    """
    question_weight = 100.0 / max(num_benchmarks, 1)
    regressions = detect_regressions(
        new_scores, best_scores, threshold=regression_threshold,
    )

    if regressions and has_patches:
        _noise_limit = question_weight * 1.5
        _noise_regs = [r for r in regressions if r["drop"] <= _noise_limit]
        if len(_noise_regs) == len(regressions):
            regressions = []

    if not regressions and full_accuracy < best_accuracy:
        regressions.append({
            "judge": "overall_accuracy_guard",
            "previous": best_accuracy,
            "current": full_accuracy,
            "drop": best_accuracy - full_accuracy,
        })

    return regressions


class TestAcceptanceGateGuard:
    """Tests for the overall_accuracy_guard that prevents accepting
    iterations where noise filter clears per-judge regressions but
    overall accuracy actually dropped."""

    def test_guard_fires_when_noise_filter_clears_regressions_but_accuracy_dropped(self):
        """Scenario from testrun6: 24 benchmarks, accuracy dropped 91.67% -> 87.5%.
        Per-judge drops are within question_weight * 1.5 = 6.25%, so noise filter
        clears them. The guard must re-inject a regression."""
        best_scores = {"schema_accuracy": 95.0, "logical_accuracy": 92.0}
        new_scores = {"schema_accuracy": 91.0, "logical_accuracy": 88.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=91.67,
            full_accuracy=87.5,
            num_benchmarks=24,
        )
        assert len(regressions) == 1
        assert regressions[0]["judge"] == "overall_accuracy_guard"
        assert regressions[0]["drop"] == pytest.approx(4.17, abs=0.01)

    def test_guard_does_not_fire_when_accuracy_unchanged(self):
        """If accuracy stayed the same, guard should not trigger."""
        best_scores = {"schema_accuracy": 90.0}
        new_scores = {"schema_accuracy": 87.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=90.0,
            full_accuracy=90.0,
            num_benchmarks=24,
        )
        assert len(regressions) == 0

    def test_guard_does_not_fire_when_accuracy_improved(self):
        """If accuracy improved, guard should not trigger."""
        best_scores = {"schema_accuracy": 90.0}
        new_scores = {"schema_accuracy": 87.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=88.0,
            full_accuracy=90.0,
            num_benchmarks=24,
        )
        assert len(regressions) == 0

    def test_guard_not_needed_when_noise_filter_does_not_clear(self):
        """If the noise filter does NOT clear regressions (large drops),
        the guard is irrelevant — regressions already present."""
        best_scores = {"schema_accuracy": 95.0}
        new_scores = {"schema_accuracy": 80.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=95.0,
            full_accuracy=80.0,
            num_benchmarks=24,
        )
        assert len(regressions) >= 1
        assert regressions[0]["judge"] == "schema_accuracy"

    def test_guard_not_needed_when_no_regressions_and_no_accuracy_drop(self):
        """If there are no regressions and accuracy held, everything is clean."""
        best_scores = {"schema_accuracy": 90.0}
        new_scores = {"schema_accuracy": 92.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=90.0,
            full_accuracy=92.0,
            num_benchmarks=24,
        )
        assert len(regressions) == 0

    def test_guard_with_no_patches_skips_noise_filter(self):
        """If no patches were applied, noise filter is not invoked,
        so regressions remain and guard is not needed."""
        best_scores = {"schema_accuracy": 95.0}
        new_scores = {"schema_accuracy": 91.0}
        regressions = _apply_noise_filter_and_guard(
            best_scores=best_scores,
            new_scores=new_scores,
            best_accuracy=95.0,
            full_accuracy=91.0,
            num_benchmarks=24,
            has_patches=False,
        )
        assert len(regressions) >= 1
        assert regressions[0]["judge"] == "schema_accuracy"


# ═══════════════════════════════════════════════════════════════════════
# TVF removal — force_apply bypass and config constants
# ═══════════════════════════════════════════════════════════════════════


class TestTvfRemovalWiring:
    """Tests for the force_apply bypass that allows escalation-approved
    TVF removal patches to skip the high-risk queue in apply_patch_set."""

    def _make_tvf_config(self) -> dict:
        """Minimal Genie Space config with one TVF."""
        return {
            "instructions": {
                "sql_functions": [
                    {"id": "fn1", "identifier": "catalog.schema.get_payment_analysis"},
                ],
            },
        }

    def _make_remove_tvf_patch(self) -> dict:
        return {
            "type": "remove_tvf",
            "target": "catalog.schema.get_payment_analysis",
            "new_text": "",
            "old_text": "",
            "previous_tvf_asset": {
                "id": "fn1",
                "identifier": "catalog.schema.get_payment_analysis",
            },
            "lever": 3,
            "risk_level": "high",
        }

    def test_force_apply_bypasses_high_risk_queue(self):
        """With force_apply=True, a remove_tvf patch should be applied
        directly instead of being queued."""
        from genie_space_optimizer.optimization.applier import apply_patch_set

        config = self._make_tvf_config()
        patch = self._make_remove_tvf_patch()

        result = apply_patch_set(
            None, "space123", [patch], config,
            apply_mode="genie_config",
            force_apply=True,
        )
        assert len(result["queued_high"]) == 0
        assert len(result["applied"]) == 1
        funcs = result["post_snapshot"].get("instructions", {}).get("sql_functions", [])
        assert len(funcs) == 0, "TVF should be removed from config"

    def test_without_force_apply_queues_high_risk(self):
        """Without force_apply, a remove_tvf patch should be queued
        (not applied)."""
        from genie_space_optimizer.optimization.applier import apply_patch_set

        config = self._make_tvf_config()
        patch = self._make_remove_tvf_patch()

        result = apply_patch_set(
            None, "space123", [patch], config,
            apply_mode="genie_config",
            force_apply=False,
        )
        assert len(result["queued_high"]) == 1
        assert len(result["applied"]) == 0
        funcs = result["post_snapshot"].get("instructions", {}).get("sql_functions", [])
        assert len(funcs) == 1, "TVF should still be in config"

    def test_tvf_removal_min_iterations_is_conservative(self):
        """TVF_REMOVAL_MIN_ITERATIONS must stay at 2 — TVF removal is
        destructive and requires evidence from multiple iterations."""
        from genie_space_optimizer.common.config import TVF_REMOVAL_MIN_ITERATIONS
        assert TVF_REMOVAL_MIN_ITERATIONS == 2
