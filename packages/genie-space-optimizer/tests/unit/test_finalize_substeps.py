"""Unit tests for finalize print-block phases within _run_finalize().

Since the finalize function uses tightly coupled heartbeat/timeout closures
that prevent clean sub-step extraction, these tests verify the phase print
blocks and the overall return value contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def _mock_finalize_deps():
    """Patch heavy finalize dependencies."""
    with patch("genie_space_optimizer.optimization.harness.write_stage") as mock_ws, \
         patch("genie_space_optimizer.optimization.harness.update_run_status") as mock_ur, \
         patch("genie_space_optimizer.optimization.harness.promote_best_model", return_value="promoted-mv") as mock_pm, \
         patch("genie_space_optimizer.optimization.harness.generate_report", return_value="/report/path") as mock_gr, \
         patch("genie_space_optimizer.optimization.harness.all_thresholds_met", return_value=True) as mock_at:
        yield {
            "write_stage": mock_ws,
            "update_run_status": mock_ur,
            "promote_best_model": mock_pm,
            "generate_report": mock_gr,
            "all_thresholds_met": mock_at,
        }


class TestFinalizePhaseBlocks:
    """Verify that _run_finalize prints each phase header."""

    def test_prints_repeatability_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "REPEATABILITY TESTING" in captured.out

    def test_prints_review_session_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "HUMAN REVIEW SESSION" in captured.out

    def test_prints_promotion_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "MODEL PROMOTION" in captured.out

    def test_prints_terminal_status_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "TERMINAL STATUS" in captured.out

    def test_prints_final_status_block(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "FINAL STATUS" in captured.out
        assert "CONVERGED" in captured.out


class TestFinalizeReturnValue:
    """Verify _run_finalize return value contract."""

    def test_return_keys_converged(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        expected_keys = {
            "status", "convergence_reason", "repeatability_pct", "report_path",
            "promoted_model", "terminal_reason", "benchmark_publish_count",
            "labeling_session", "elapsed_seconds", "heartbeat_count",
            "uc_registration", "held_out_accuracy", "held_out_count",
        }
        assert set(result.keys()) == expected_keys
        assert result["status"] == "CONVERGED"

    def test_return_keys_max_iterations(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        _mock_finalize_deps["all_thresholds_met"].return_value = False
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 50.0}, "mv-1", 3, "cat", "gold",
            run_repeatability=False, benchmarks=None, max_iterations=3,
        )
        assert result["status"] == "MAX_ITERATIONS"

    def test_return_keys_stalled(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        _mock_finalize_deps["all_thresholds_met"].return_value = False
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 50.0}, "mv-1", 1, "cat", "gold",
            run_repeatability=False, benchmarks=None, max_iterations=5,
        )
        assert result["status"] == "STALLED"

    def test_held_out_none_when_no_held_out_benchmarks(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        benchmarks = [
            {"id": f"q{i}", "question": f"Q{i}", "split": "train"}
            for i in range(5)
        ]
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=benchmarks,
        )
        assert result["held_out_accuracy"] is None
        assert result["held_out_count"] == 0

    @patch("genie_space_optimizer.optimization.harness.run_evaluation", return_value={"overall_accuracy": 66.7, "rows_json": []})
    @patch("genie_space_optimizer.optimization.harness.write_iteration")
    @patch("genie_space_optimizer.optimization.harness.make_predict_fn", return_value=MagicMock())
    @patch("genie_space_optimizer.optimization.harness.make_all_scorers", return_value=[])
    @patch("genie_space_optimizer.optimization.harness._ensure_sql_context")
    def test_held_out_eval_runs_with_held_out_benchmarks(
        self, _esc, _mas, _mpf, mock_write_iter, mock_run_eval, _mock_finalize_deps, capsys,
    ):
        from genie_space_optimizer.optimization.harness import _run_finalize

        benchmarks = [
            {"id": f"q{i}", "question": f"Q{i}", "split": "train"}
            for i in range(5)
        ] + [
            {"id": f"ho{i}", "question": f"HO{i}", "split": "held_out"}
            for i in range(2)
        ]
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"genie_correct": 85.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=benchmarks,
        )
        mock_run_eval.assert_called_once()
        call_args = mock_run_eval.call_args
        assert call_args[0][6] == "held_out"
        assert len(call_args[0][3]) == 2  # held_out benchmarks only
        assert result["held_out_accuracy"] == 66.7
        assert result["held_out_count"] == 2

        captured = capsys.readouterr()
        assert "HELD-OUT GENERALIZATION CHECK" in captured.out
