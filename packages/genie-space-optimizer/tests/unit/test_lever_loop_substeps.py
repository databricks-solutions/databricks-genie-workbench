"""Unit tests for lever loop phase print blocks within _run_lever_loop().

Since the lever loop has deeply coupled state (proactive enrichment, pre-loop
setup, iteration loop, summary), these tests verify the phase print blocks
and the overall return value contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock

import pytest


@pytest.fixture
def _mock_lever_loop_deps():
    """Patch heavy lever-loop dependencies for fast unit testing."""
    with patch("genie_space_optimizer.optimization.harness.write_stage") as mock_ws, \
         patch("genie_space_optimizer.optimization.harness.update_run_status") as mock_ur, \
         patch("genie_space_optimizer.optimization.harness._ensure_sql_context"), \
         patch("genie_space_optimizer.optimization.harness._resume_lever_loop", return_value={"resume_from_lever": 0, "iteration_counter": 0}), \
         patch("genie_space_optimizer.optimization.harness.make_predict_fn", return_value=MagicMock()), \
         patch("genie_space_optimizer.optimization.harness.make_all_scorers", return_value=[]), \
         patch("genie_space_optimizer.optimization.harness.format_mlflow_template", return_value="tmpl"), \
         patch("genie_space_optimizer.optimization.harness.enrich_metadata_with_uc_types"), \
         patch("genie_space_optimizer.optimization.harness._run_description_enrichment", return_value={"total_enriched": 0}), \
         patch("genie_space_optimizer.optimization.harness._run_proactive_join_discovery", return_value={"total_applied": 0}), \
         patch("genie_space_optimizer.optimization.harness._run_space_metadata_enrichment", return_value={}), \
         patch("genie_space_optimizer.optimization.harness._run_proactive_instruction_seeding", return_value={}), \
         patch("genie_space_optimizer.optimization.harness.load_latest_full_iteration", return_value=None), \
         patch("genie_space_optimizer.optimization.harness._run_arbiter_corrections", return_value={"corrected_qids": set(), "quarantined_qids": set()}), \
         patch("genie_space_optimizer.optimization.harness._mine_benchmark_example_sqls", return_value=[]), \
         patch("genie_space_optimizer.optimization.harness.all_thresholds_met", return_value=True), \
         patch("genie_space_optimizer.optimization.harness._get_failure_rows", return_value=[]):
        yield {
            "write_stage": mock_ws,
            "update_run_status": mock_ur,
        }


class TestLeverLoopPhaseBlocks:
    """Verify that _run_lever_loop prints each phase header."""

    def test_prints_proactive_enrichment_phase(self, _mock_lever_loop_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain",
            [{"question": "q1", "id": "b1"}], "exp",
            {"syntax_validity": 95.0}, 95.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        captured = capsys.readouterr()
        assert "PROACTIVE ENRICHMENT" in captured.out

    def test_prints_pre_loop_setup_phase(self, _mock_lever_loop_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain",
            [{"question": "q1", "id": "b1"}], "exp",
            {"syntax_validity": 95.0}, 95.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        captured = capsys.readouterr()
        assert "PRE-LOOP SETUP" in captured.out

    def test_prints_adaptive_iteration_phase(self, _mock_lever_loop_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain",
            [{"question": "q1", "id": "b1"}], "exp",
            {"syntax_validity": 95.0}, 95.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        captured = capsys.readouterr()
        assert "ADAPTIVE ITERATION" in captured.out

    def test_prints_optimization_summary(self, _mock_lever_loop_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain",
            [{"question": "q1", "id": "b1"}], "exp",
            {"syntax_validity": 95.0}, 95.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        captured = capsys.readouterr()
        assert "OPTIMIZATION RUN SUMMARY" in captured.out


class TestLeverLoopReturnValue:
    """Verify _run_lever_loop return value contract."""

    def test_return_keys_converged(self, _mock_lever_loop_deps):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        result = _run_lever_loop(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain",
            [{"question": "q1", "id": "b1"}], "exp",
            {"syntax_validity": 95.0}, 95.0, "mv-1",
            {"_parsed_space": {}, "_uc_columns": []},
            "cat", "gold", levers=[], max_iterations=0,
        )
        expected_keys = {
            "scores", "accuracy", "model_id", "iteration_counter", "best_iteration",
            "levers_attempted", "levers_accepted", "levers_rolled_back",
            "question_trace_map", "reflection_buffer",
            "all_eval_mlflow_run_ids", "all_failure_trace_ids",
            "all_regression_trace_ids", "all_failure_question_ids",
            "_debug_ref_sqls_count", "_debug_failure_rows_loaded",
        }
        assert expected_keys.issubset(set(result.keys()))
        assert result["model_id"] == "mv-1"
        assert result["accuracy"] == 95.0
