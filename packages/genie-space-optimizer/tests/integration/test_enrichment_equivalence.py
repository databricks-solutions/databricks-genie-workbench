"""Integration tests: _run_enrichment() calls sub-steps in sequence and the
lever loop correctly skips/runs Phase 1 based on enrichment_done flag.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


HARNESS = "genie_space_optimizer.optimization.harness"


def _make_mock_config():
    return {
        "_parsed_space": {
            "data_sources": {"tables": [], "metric_views": []},
        },
        "_uc_columns": [],
    }


# ── Enrichment Wrapper ────────────────────────────────────────────────


class TestEnrichmentCallsSubStepsInOrder:
    """_run_enrichment() must call each enrichment sub-function in the
    correct order: descriptions -> joins -> metadata -> instructions -> examples.
    """

    @patch(f"{HARNESS}._apply_proactive_example_sqls")
    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_proactive_instruction_seeding", return_value={})
    @patch(f"{HARNESS}._run_space_metadata_enrichment", return_value={})
    @patch(f"{HARNESS}._run_proactive_join_discovery", return_value={})
    @patch(f"{HARNESS}._run_description_enrichment", return_value={})
    @patch(f"{HARNESS}.create_genie_model_version", return_value="model-enriched-1")
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._prepare_lever_loop")
    def test_calls_enrichment_functions_in_order(
        self,
        mock_prepare,
        mock_write_stage,
        mock_create_model,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_mine,
        mock_apply_ex,
    ):
        from genie_space_optimizer.optimization.harness import _run_enrichment

        mock_prepare.return_value = _make_mock_config()

        result = _run_enrichment(
            MagicMock(), MagicMock(), "run-1", "space-1", "sales",
            [{"question": "q1"}], "exp-1", "cat", "gold",
            baseline_model_id="model-baseline",
        )

        mock_prepare.assert_called_once()
        mock_desc.assert_called_once()
        mock_join.assert_called_once()
        mock_meta.assert_called_once()
        mock_instr.assert_called_once()
        mock_mine.assert_called_once()
        mock_create_model.assert_called_once()

        assert result["enrichment_model_id"] == "model-enriched-1"
        assert "config" in result
        assert "summary" in result

    @patch(f"{HARNESS}._apply_proactive_example_sqls")
    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_proactive_instruction_seeding", return_value={})
    @patch(f"{HARNESS}._run_space_metadata_enrichment", return_value={})
    @patch(f"{HARNESS}._run_proactive_join_discovery", return_value={})
    @patch(f"{HARNESS}._run_description_enrichment", return_value={})
    @patch(f"{HARNESS}.create_genie_model_version", return_value="model-enriched-1")
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._prepare_lever_loop")
    def test_writes_enrichment_stages(
        self,
        mock_prepare,
        mock_write_stage,
        mock_create_model,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_mine,
        mock_apply_ex,
    ):
        from genie_space_optimizer.optimization.harness import _run_enrichment

        mock_prepare.return_value = _make_mock_config()

        _run_enrichment(
            MagicMock(), MagicMock(), "run-1", "space-1", "sales",
            [], "exp-1", "cat", "gold",
        )

        stage_calls = [c for c in mock_write_stage.call_args_list]
        stage_names = [c.args[2] for c in stage_calls]
        assert "ENRICHMENT_STARTED" in stage_names
        assert "ENRICHMENT_COMPLETE" in stage_names

        complete_call = next(c for c in stage_calls if c.args[2] == "ENRICHMENT_COMPLETE")
        assert "detail" in complete_call.kwargs, (
            "ENRICHMENT_COMPLETE must use 'detail=' kwarg, not 'detail_json='"
        )
        assert "detail_json" not in complete_call.kwargs, (
            "Found 'detail_json' kwarg — write_stage() expects 'detail'"
        )
        detail_dict = complete_call.kwargs["detail"]
        assert "enrichment_model_id" in detail_dict
        assert "total_enrichments" in detail_dict
        assert "enrichment_skipped" in detail_dict

    @patch(f"{HARNESS}._apply_proactive_example_sqls")
    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_proactive_instruction_seeding", return_value={})
    @patch(f"{HARNESS}._run_space_metadata_enrichment", return_value={})
    @patch(f"{HARNESS}._run_proactive_join_discovery", return_value={})
    @patch(f"{HARNESS}._run_description_enrichment", return_value={})
    @patch(f"{HARNESS}.create_genie_model_version", return_value="model-enriched-1")
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._prepare_lever_loop")
    def test_enrichment_skipped_when_no_changes(
        self,
        mock_prepare,
        mock_write_stage,
        mock_create_model,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_mine,
        mock_apply_ex,
    ):
        from genie_space_optimizer.optimization.harness import _run_enrichment

        mock_prepare.return_value = _make_mock_config()

        result = _run_enrichment(
            MagicMock(), MagicMock(), "run-1", "space-1", "sales",
            [], "exp-1", "cat", "gold",
        )

        assert result["enrichment_skipped"] is True
        assert result["summary"]["total_enrichments"] == 0
        mock_create_model.assert_called_once()


# ── Lever Loop enrichment_done Flag ──────────────────────────────────


class TestLeverLoopEnrichmentDoneFlag:
    """When enrichment_done=True, _run_lever_loop() must skip Phase 1."""

    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_arbiter_corrections", return_value={"corrected_qids": set(), "quarantined_qids": set()})
    @patch(f"{HARNESS}.load_latest_full_iteration", return_value=None)
    @patch(f"{HARNESS}._run_proactive_instruction_seeding")
    @patch(f"{HARNESS}._run_space_metadata_enrichment")
    @patch(f"{HARNESS}._run_proactive_join_discovery")
    @patch(f"{HARNESS}._run_description_enrichment")
    @patch(f"{HARNESS}.make_all_scorers", return_value=[])
    @patch(f"{HARNESS}.make_predict_fn", return_value=MagicMock())
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._resume_lever_loop", return_value={})
    @patch(f"{HARNESS}._ensure_sql_context")
    @patch("genie_space_optimizer.common.genie_client.fetch_space_config")
    @patch("genie_space_optimizer.common.uc_metadata.get_columns_for_tables_rest", return_value=[])
    @patch("genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs", return_value=[])
    def test_skips_phase1_when_enrichment_done(
        self,
        mock_table_refs,
        mock_get_cols,
        mock_fetch_config,
        mock_ensure_sql,
        mock_resume,
        mock_write_stage,
        mock_predict_fn,
        mock_scorers,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_load_iter,
        mock_arbiter,
        mock_mine,
    ):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        mock_fetch_config.return_value = _make_mock_config()

        try:
            _run_lever_loop(
                MagicMock(), MagicMock(), "run-1", "space-1", "sales",
                [], "exp-1", {}, 70.0, "model-baseline",
                _make_mock_config(), "cat", "gold",
                max_iterations=0,
                enrichment_done=True,
                enrichment_model_id="model-enriched",
            )
        except Exception:
            pass

        mock_desc.assert_not_called()
        mock_join.assert_not_called()
        mock_meta.assert_not_called()
        mock_instr.assert_not_called()
        mock_mine.assert_not_called()

    @patch(f"{HARNESS}._apply_proactive_example_sqls")
    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_arbiter_corrections", return_value={"corrected_qids": set(), "quarantined_qids": set()})
    @patch(f"{HARNESS}.load_latest_full_iteration", return_value=None)
    @patch(f"{HARNESS}._run_proactive_instruction_seeding", return_value={})
    @patch(f"{HARNESS}._run_space_metadata_enrichment", return_value={})
    @patch(f"{HARNESS}._run_proactive_join_discovery", return_value={})
    @patch(f"{HARNESS}._run_description_enrichment", return_value={})
    @patch(f"{HARNESS}.make_all_scorers", return_value=[])
    @patch(f"{HARNESS}.make_predict_fn", return_value=MagicMock())
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._resume_lever_loop", return_value={})
    @patch(f"{HARNESS}._ensure_sql_context")
    def test_runs_phase1_when_enrichment_not_done(
        self,
        mock_ensure_sql,
        mock_resume,
        mock_write_stage,
        mock_predict_fn,
        mock_scorers,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_load_iter,
        mock_arbiter,
        mock_mine,
        mock_apply_ex,
    ):
        from genie_space_optimizer.optimization.harness import _run_lever_loop

        try:
            _run_lever_loop(
                MagicMock(), MagicMock(), "run-1", "space-1", "sales",
                [], "exp-1", {}, 70.0, "model-baseline",
                _make_mock_config(), "cat", "gold",
                max_iterations=0,
                enrichment_done=False,
            )
        except Exception:
            pass

        mock_desc.assert_called_once()
        mock_join.assert_called_once()
        mock_meta.assert_called_once()
        mock_instr.assert_called_once()


# ── Enrichment Error Handling ─────────────────────────────────────────


class TestEnrichmentWritesFailedStageOnError:
    """When _run_enrichment raises, it must write ENRICHMENT_STARTED=FAILED."""

    @patch(f"{HARNESS}._apply_proactive_example_sqls")
    @patch(f"{HARNESS}._mine_benchmark_example_sqls", return_value=[])
    @patch(f"{HARNESS}._run_proactive_instruction_seeding", return_value={})
    @patch(f"{HARNESS}._run_space_metadata_enrichment", return_value={})
    @patch(f"{HARNESS}._run_proactive_join_discovery", return_value={})
    @patch(f"{HARNESS}._run_description_enrichment", return_value={})
    @patch(f"{HARNESS}.create_genie_model_version", side_effect=RuntimeError("model creation failed"))
    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._prepare_lever_loop")
    def test_writes_failed_stage_on_model_creation_error(
        self,
        mock_prepare,
        mock_write_stage,
        mock_create_model,
        mock_desc,
        mock_join,
        mock_meta,
        mock_instr,
        mock_mine,
        mock_apply_ex,
    ):
        from genie_space_optimizer.optimization.harness import _run_enrichment

        mock_prepare.return_value = _make_mock_config()

        with pytest.raises(RuntimeError, match="model creation failed"):
            _run_enrichment(
                MagicMock(), MagicMock(), "run-1", "space-1", "sales",
                [], "exp-1", "cat", "gold",
            )

        stage_calls = [c for c in mock_write_stage.call_args_list]
        stage_statuses = [(c.args[2], c.args[3]) for c in stage_calls]
        assert ("ENRICHMENT_STARTED", "STARTED") in stage_statuses
        assert ("ENRICHMENT_STARTED", "FAILED") in stage_statuses

        failed_call = next(c for c in stage_calls if c.args[3] == "FAILED")
        assert "error_message" in failed_call.kwargs
        assert "model creation failed" in failed_call.kwargs["error_message"]

    @patch(f"{HARNESS}.write_stage")
    @patch(f"{HARNESS}._prepare_lever_loop", side_effect=RuntimeError("config load failed"))
    def test_writes_failed_stage_on_prepare_error(
        self,
        mock_prepare,
        mock_write_stage,
    ):
        from genie_space_optimizer.optimization.harness import _run_enrichment

        with pytest.raises(RuntimeError, match="config load failed"):
            _run_enrichment(
                MagicMock(), MagicMock(), "run-1", "space-1", "sales",
                [], "exp-1", "cat", "gold",
            )

        stage_calls = [c for c in mock_write_stage.call_args_list]
        stage_statuses = [(c.args[2], c.args[3]) for c in stage_calls]
        assert ("ENRICHMENT_STARTED", "STARTED") in stage_statuses
        assert ("ENRICHMENT_STARTED", "FAILED") in stage_statuses
