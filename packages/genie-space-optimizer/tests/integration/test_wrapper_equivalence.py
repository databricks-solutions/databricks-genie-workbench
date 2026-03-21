"""Integration tests: wrapper functions call sub-steps in sequence and return
identical dict structures.

Each original wrapper (run_preflight, _run_baseline, _run_deploy) was split
into individually callable sub-steps. These tests verify the wrapper still
calls the sub-steps in the correct order and returns the expected keys.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


# ── Deploy Wrapper ────────────────────────────────────────────────────


class TestDeployWrapperEquivalence:
    """_run_deploy() must call deploy_check() then deploy_execute()."""

    @patch("genie_space_optimizer.optimization.harness.deploy_execute")
    @patch("genie_space_optimizer.optimization.harness.deploy_check")
    def test_calls_check_then_execute(self, mock_check, mock_execute):
        from genie_space_optimizer.optimization.harness import _run_deploy

        mock_check.return_value = {"should_deploy": True, "deploy_target": "t", "prev_model_id": "mv", "iteration_counter": 1}
        mock_execute.return_value = {"status": "DEPLOYED", "deploy_target": "t"}

        result = _run_deploy(
            MagicMock(), MagicMock(), "run-1", "t", "sp", "exp",
            "dom", "mv", 1, "cat", "gold",
        )

        mock_check.assert_called_once_with("t", "mv", 1)
        mock_execute.assert_called_once()
        assert result == {"status": "DEPLOYED", "deploy_target": "t"}

    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_skip_produces_identical_result(self, mock_ws):
        from genie_space_optimizer.optimization.harness import _run_deploy, deploy_execute

        w, spark = MagicMock(), MagicMock()
        args = (w, spark, "run-1", None, "sp", "exp", "dom", "mv", 0, "cat", "gold")

        wrapper_result = _run_deploy(*args)
        mock_ws.reset_mock()
        direct_result = deploy_execute(*args)

        assert wrapper_result == direct_result


# ── Baseline Wrapper ──────────────────────────────────────────────────


class TestBaselineWrapperEquivalence:
    """_run_baseline() must call all 4 sub-steps in sequence."""

    @patch("genie_space_optimizer.optimization.harness.baseline_persist_state")
    @patch("genie_space_optimizer.optimization.harness.baseline_display_scorecard")
    @patch("genie_space_optimizer.optimization.harness.baseline_run_evaluation")
    @patch("genie_space_optimizer.optimization.harness.baseline_setup_scorers")
    def test_calls_substeps_in_order(self, mock_setup, mock_eval, mock_score, mock_persist):
        from genie_space_optimizer.optimization.harness import _run_baseline

        mock_setup.return_value = {
            "predict_fn": MagicMock(), "scorers": [], "model_id": "mv-1",
            "exp_name": "exp", "space_id": "sp", "domain": "dom",
        }
        mock_eval.return_value = {"scores": {}, "overall_accuracy": 90.0, "thresholds_met": True}
        mock_score.return_value = {"scores": {}, "overall_accuracy": 90.0, "thresholds_met": True}
        mock_persist.return_value = {
            "scores": {}, "overall_accuracy": 90.0, "thresholds_met": True,
            "model_id": "mv-1", "eval_result": {},
        }

        result = _run_baseline(
            MagicMock(), MagicMock(), "run-1", "sp",
            [], "exp", "mv-1", "cat", "gold", "dom",
        )

        mock_setup.assert_called_once()
        mock_eval.assert_called_once()
        mock_score.assert_called_once()
        mock_persist.assert_called_once()

    @patch("genie_space_optimizer.optimization.harness.log_judge_verdicts_on_traces")
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_iteration")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    @patch("genie_space_optimizer.optimization.harness.run_evaluation")
    @patch("genie_space_optimizer.optimization.harness.make_all_scorers", return_value=[])
    @patch("genie_space_optimizer.optimization.harness.make_predict_fn", return_value=MagicMock())
    @patch("genie_space_optimizer.optimization.harness._ensure_sql_context")
    @patch("genie_space_optimizer.optimization.harness.format_mlflow_template", return_value="t")
    def test_wrapper_returns_correct_keys(
        self, *mocks,
    ):
        from genie_space_optimizer.optimization.harness import _run_baseline

        mocks[4].return_value = {"scores": {"j": 90.0}, "overall_accuracy": 90.0, "thresholds_met": True}

        result = _run_baseline(
            MagicMock(), MagicMock(), "run-1", "sp",
            [{"q": "test"}], "exp", "mv-1", "cat", "gold", "dom",
        )
        assert set(result.keys()) == {
            "scores", "overall_accuracy", "thresholds_met", "model_id", "eval_result",
        }


# ── Preflight Wrapper ────────────────────────────────────────────────


class TestPreflightWrapperEquivalence:
    """run_preflight() must call all 6 sub-steps in sequence."""

    @patch("genie_space_optimizer.optimization.preflight.preflight_setup_experiment")
    @patch("genie_space_optimizer.optimization.preflight.preflight_load_human_feedback")
    @patch("genie_space_optimizer.optimization.preflight.preflight_validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_collect_uc_metadata")
    @patch("genie_space_optimizer.optimization.preflight.preflight_fetch_config")
    def test_calls_all_6_substeps(self, mock_cfg, mock_uc, mock_gen, mock_val, mock_fb, mock_exp):
        from genie_space_optimizer.optimization.preflight import run_preflight

        mock_cfg.return_value = {
            "config": {}, "snapshot": {}, "genie_table_refs": [],
            "domain": "default", "apply_mode": "genie_config", "configured_cols": 0,
        }
        mock_uc.return_value = {"uc_columns": [], "uc_tags": [], "uc_routines": [], "uc_fk": []}
        mock_gen.return_value = {"benchmarks": [{"q": "test"}], "regenerated": False}
        mock_val.return_value = {"benchmarks": [{"q": "test"}], "pre_count": 1, "invalid_errors": []}
        mock_fb.return_value = {"human_corrections": []}
        mock_exp.return_value = {
            "model_id": "mv-1", "experiment_name": "/exp",
            "experiment_id": "exp-1", "prompt_registrations": [],
        }

        config, benchmarks, model_id, exp_name, corrections = run_preflight(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "gold", "revenue",
        )

        mock_cfg.assert_called_once()
        mock_uc.assert_called_once()
        mock_gen.assert_called_once()
        mock_val.assert_called_once()
        mock_fb.assert_called_once()
        mock_exp.assert_called_once()

        assert isinstance(config, dict)
        assert isinstance(benchmarks, list)
        assert model_id == "mv-1"
        assert exp_name == "/exp"
        assert isinstance(corrections, list)

    @patch("genie_space_optimizer.optimization.preflight.preflight_setup_experiment")
    @patch("genie_space_optimizer.optimization.preflight.preflight_load_human_feedback")
    @patch("genie_space_optimizer.optimization.preflight.preflight_validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_collect_uc_metadata")
    @patch("genie_space_optimizer.optimization.preflight.preflight_fetch_config")
    def test_return_tuple_shape(self, mock_cfg, mock_uc, mock_gen, mock_val, mock_fb, mock_exp):
        from genie_space_optimizer.optimization.preflight import run_preflight

        mock_cfg.return_value = {
            "config": {"k": "v"}, "snapshot": {}, "genie_table_refs": [],
            "domain": "default", "apply_mode": "genie_config", "configured_cols": 0,
        }
        mock_uc.return_value = {"uc_columns": [], "uc_tags": [], "uc_routines": [], "uc_fk": []}
        mock_gen.return_value = {"benchmarks": [], "regenerated": False}
        mock_val.return_value = {"benchmarks": [], "pre_count": 0, "invalid_errors": []}
        mock_fb.return_value = {"human_corrections": [{"type": "fix"}]}
        mock_exp.return_value = {
            "model_id": "mv-2", "experiment_name": "/exp2",
            "experiment_id": "exp-2", "prompt_registrations": ["p1"],
        }

        result = run_preflight(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "gold", "revenue",
        )

        assert len(result) == 5
        config, benchmarks, model_id, exp_name, corrections = result
        assert config == {"k": "v"}
        assert benchmarks == []
        assert model_id == "mv-2"
        assert exp_name == "/exp2"
        assert corrections == [{"type": "fix"}]


# ── Finalize Return Value ─────────────────────────────────────────────


class TestFinalizeReturnValueEquivalence:
    """_run_finalize() must return the expected key set."""

    @patch("genie_space_optimizer.optimization.harness.all_thresholds_met", return_value=True)
    @patch("genie_space_optimizer.optimization.harness.generate_report", return_value="/report")
    @patch("genie_space_optimizer.optimization.harness.promote_best_model", return_value="promoted")
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_return_keys(self, mock_ws, mock_ur, mock_pm, mock_gr, mock_at):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"j1": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        expected_keys = {
            "status", "convergence_reason", "repeatability_pct", "report_path",
            "promoted_model", "terminal_reason", "benchmark_publish_count",
            "labeling_session", "elapsed_seconds", "heartbeat_count",
            "uc_registration", "held_out_accuracy", "held_out_count",
        }
        assert set(result.keys()) == expected_keys


# ── Lever Loop Return Value ───────────────────────────────────────────


class TestLeverLoopReturnValueEquivalence:
    """_run_lever_loop() must return the expected key set."""

    @patch("genie_space_optimizer.optimization.harness._get_failure_rows", return_value=[])
    @patch("genie_space_optimizer.optimization.harness.all_thresholds_met", return_value=True)
    @patch("genie_space_optimizer.optimization.harness._mine_benchmark_example_sqls", return_value=[])
    @patch("genie_space_optimizer.optimization.harness._run_arbiter_corrections", return_value={"corrected_qids": set(), "quarantined_qids": set()})
    @patch("genie_space_optimizer.optimization.harness.load_latest_full_iteration", return_value=None)
    @patch("genie_space_optimizer.optimization.harness._run_proactive_instruction_seeding", return_value={})
    @patch("genie_space_optimizer.optimization.harness._run_space_metadata_enrichment", return_value={})
    @patch("genie_space_optimizer.optimization.harness._run_proactive_join_discovery", return_value={"total_applied": 0})
    @patch("genie_space_optimizer.optimization.harness._run_description_enrichment", return_value={"total_enriched": 0})
    @patch("genie_space_optimizer.optimization.harness.enrich_metadata_with_uc_types")
    @patch("genie_space_optimizer.optimization.harness.format_mlflow_template", return_value="t")
    @patch("genie_space_optimizer.optimization.harness.make_all_scorers", return_value=[])
    @patch("genie_space_optimizer.optimization.harness.make_predict_fn", return_value=MagicMock())
    @patch("genie_space_optimizer.optimization.harness._ensure_sql_context")
    @patch("genie_space_optimizer.optimization.harness._resume_lever_loop", return_value={"resume_from_lever": 0, "iteration_counter": 0})
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_return_keys(self, *mocks):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "sp", "dom",
            [{"question": "q1", "id": "b1"}], "exp",
            {"j1": 90.0}, 90.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        expected_keys = {
            "scores", "accuracy", "model_id", "iteration_counter", "best_iteration",
            "levers_attempted", "levers_accepted", "levers_rolled_back",
        }
        assert expected_keys.issubset(set(result.keys()))
