"""Unit tests for baseline sub-step functions extracted from _run_baseline().

Guards against regressions when the monolithic _run_baseline() was split into
baseline_setup_scorers(), baseline_run_evaluation(), baseline_display_scorecard(),
and baseline_persist_state().
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def _mock_evaluation_deps():
    """Patch heavy evaluation dependencies so sub-steps can be imported."""
    with patch("genie_space_optimizer.optimization.harness.make_predict_fn") as mock_pred, \
         patch("genie_space_optimizer.optimization.harness.make_all_scorers") as mock_scorers, \
         patch("genie_space_optimizer.optimization.harness.write_stage") as mock_ws, \
         patch("genie_space_optimizer.optimization.harness._ensure_sql_context"), \
         patch("genie_space_optimizer.optimization.harness.format_mlflow_template", return_value="tmpl"):
        mock_pred.return_value = MagicMock(name="predict_fn")
        mock_scorers.return_value = [MagicMock(name=f"scorer_{i}") for i in range(9)]
        yield {
            "make_predict_fn": mock_pred,
            "make_all_scorers": mock_scorers,
            "write_stage": mock_ws,
        }


class TestBaselineSetupScorers:
    def test_returns_expected_keys(self, _mock_evaluation_deps):
        from genie_space_optimizer.optimization.harness import baseline_setup_scorers

        result = baseline_setup_scorers(
            MagicMock(), MagicMock(), "space-1", "run-1",
            "cat", "gold", "exp", "mv-1", "revenue",
        )
        assert set(result.keys()) == {
            "predict_fn", "scorers", "model_id", "exp_name", "space_id", "domain",
        }

    def test_writes_started_stage(self, _mock_evaluation_deps):
        from genie_space_optimizer.optimization.harness import baseline_setup_scorers

        baseline_setup_scorers(
            MagicMock(), MagicMock(), "space-1", "run-1",
            "cat", "gold", "exp", "mv-1", "revenue",
        )
        ws = _mock_evaluation_deps["write_stage"]
        ws.assert_called_once()
        assert ws.call_args[0][2] == "BASELINE_EVAL_STARTED"

    def test_prints_setup_block(self, _mock_evaluation_deps, capsys):
        from genie_space_optimizer.optimization.harness import baseline_setup_scorers

        baseline_setup_scorers(
            MagicMock(), MagicMock(), "space-1", "run-1",
            "cat", "gold", "exp", "mv-1", "revenue",
        )
        captured = capsys.readouterr()
        assert "EVALUATION SETUP" in captured.out
        assert "space-1" in captured.out


class TestBaselineDisplayScorecard:
    def test_returns_scores_and_thresholds(self):
        from genie_space_optimizer.optimization.harness import baseline_display_scorecard

        eval_result = {
            "scores": {"syntax_validity": 95.0, "schema_accuracy": 80.0},
            "overall_accuracy": 72.5,
            "thresholds_met": False,
        }
        result = baseline_display_scorecard(eval_result)
        assert result["scores"] == {"syntax_validity": 95.0, "schema_accuracy": 80.0}
        assert result["overall_accuracy"] == 72.5
        assert result["thresholds_met"] is False

    def test_return_keys_are_stable(self):
        from genie_space_optimizer.optimization.harness import baseline_display_scorecard

        result = baseline_display_scorecard({"scores": {}, "overall_accuracy": 0.0, "thresholds_met": True})
        assert set(result.keys()) == {"scores", "overall_accuracy", "thresholds_met"}

    def test_prints_scorecard_block(self, capsys):
        from genie_space_optimizer.optimization.harness import baseline_display_scorecard

        baseline_display_scorecard({
            "scores": {"syntax_validity": 95.0},
            "overall_accuracy": 95.0,
            "thresholds_met": True,
        })
        captured = capsys.readouterr()
        assert "SCORECARD" in captured.out
        assert "95.0%" in captured.out

    def test_scorecard_does_not_alter_eval_result(self):
        from genie_space_optimizer.optimization.harness import baseline_display_scorecard

        eval_result = {
            "scores": {"syntax_validity": 95.0},
            "overall_accuracy": 95.0,
            "thresholds_met": True,
            "extra_key": "should_be_ignored",
        }
        original = dict(eval_result)
        baseline_display_scorecard(eval_result)
        assert eval_result == original


class TestBaselinePersistState:
    @patch("genie_space_optimizer.optimization.harness.log_judge_verdicts_on_traces")
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_iteration")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_returns_expected_keys(self, mock_ws, mock_wi, mock_ur, mock_verdicts):
        from genie_space_optimizer.optimization.harness import baseline_persist_state

        eval_result = {"scores": {"j1": 90.0}, "overall_accuracy": 90.0, "thresholds_met": True}
        scorecard = {"scores": {"j1": 90.0}, "overall_accuracy": 90.0, "thresholds_met": True}
        result = baseline_persist_state(
            MagicMock(), MagicMock(), "run-1", "mv-1", "cat", "gold",
            eval_result, scorecard,
        )
        assert set(result.keys()) == {
            "scores", "overall_accuracy", "thresholds_met", "model_id", "eval_result",
        }

    @patch("genie_space_optimizer.optimization.harness.log_judge_verdicts_on_traces")
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_iteration")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    def test_writes_iteration_and_stage(self, mock_ws, mock_wi, mock_ur, mock_verdicts):
        from genie_space_optimizer.optimization.harness import baseline_persist_state

        eval_result = {"scores": {}, "overall_accuracy": 0.0, "thresholds_met": False}
        scorecard = {"scores": {}, "overall_accuracy": 0.0, "thresholds_met": False}
        baseline_persist_state(
            MagicMock(), MagicMock(), "run-1", "mv-1", "cat", "gold",
            eval_result, scorecard,
        )
        mock_wi.assert_called_once()
        assert mock_ws.call_count >= 1


class TestBaselineWrapperEquivalence:
    @patch("genie_space_optimizer.optimization.harness.log_judge_verdicts_on_traces")
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.write_iteration")
    @patch("genie_space_optimizer.optimization.harness.write_stage")
    @patch("genie_space_optimizer.optimization.harness.run_evaluation")
    @patch("genie_space_optimizer.optimization.harness.make_all_scorers")
    @patch("genie_space_optimizer.optimization.harness.make_predict_fn")
    @patch("genie_space_optimizer.optimization.harness._ensure_sql_context")
    @patch("genie_space_optimizer.optimization.harness.format_mlflow_template", return_value="tmpl")
    def test_wrapper_returns_same_keys_as_substeps(
        self, mock_fmt, mock_ctx, mock_pred, mock_scorers, mock_eval,
        mock_ws, mock_wi, mock_ur, mock_verdicts,
    ):
        from genie_space_optimizer.optimization.harness import _run_baseline

        mock_pred.return_value = MagicMock()
        mock_scorers.return_value = [MagicMock()]
        mock_eval.return_value = {
            "scores": {"j1": 90.0},
            "overall_accuracy": 90.0,
            "thresholds_met": True,
        }

        result = _run_baseline(
            MagicMock(), MagicMock(), "run-1", "space-1",
            [{"question": "test"}], "exp", "mv-1", "cat", "gold", "revenue",
        )
        assert set(result.keys()) == {
            "scores", "overall_accuracy", "thresholds_met", "model_id", "eval_result",
        }
        assert result["model_id"] == "mv-1"
        assert result["thresholds_met"] is True
