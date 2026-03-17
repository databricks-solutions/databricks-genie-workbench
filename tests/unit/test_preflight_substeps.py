"""Unit tests for preflight sub-step functions extracted from run_preflight().

Guards against regressions when the monolithic run_preflight() was split into
6 individually callable sub-steps.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    spark = MagicMock(name="spark")
    spark.sql.return_value.collect.return_value = [{"user": "sp@test"}]
    return spark


@pytest.fixture
def mock_ws():
    ws = MagicMock(name="workspace_client")
    ws.tables.get.return_value = MagicMock(columns=[MagicMock()])
    return ws


# ---------------------------------------------------------------------------
# Step 1: preflight_fetch_config
# ---------------------------------------------------------------------------

class TestPreflightFetchConfig:
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space", return_value=(True, []))
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_returns_expected_keys(self, mock_ws_stage, mock_load, mock_val, mock_refs, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        mock_load.return_value = {"config_snapshot": {"_parsed_space": {"data_sources": {"tables": []}}}}
        result = preflight_fetch_config(mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "revenue")
        assert set(result.keys()) == {
            "config", "snapshot", "genie_table_refs", "domain", "apply_mode", "configured_cols",
        }

    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space", return_value=(True, []))
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_uses_snapshot_when_available(self, mock_ws_stage, mock_load, mock_val, mock_refs, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        snap = {"tables": ["t1"], "_parsed_space": {"data_sources": {"tables": []}}}
        mock_load.return_value = {"config_snapshot": snap}
        result = preflight_fetch_config(mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "revenue")
        assert result["config"] is snap

    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space", return_value=(True, []))
    @patch("genie_space_optimizer.optimization.preflight.fetch_space_config")
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_falls_back_to_api(self, mock_ws_stage, mock_load, mock_fetch, mock_val, mock_refs, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        mock_load.return_value = {}
        mock_fetch.return_value = {"_parsed_space": {"data_sources": {"tables": []}}}
        result = preflight_fetch_config(mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "revenue")
        mock_fetch.assert_called_once()
        assert "config" in result

    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space", return_value=(True, []))
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_prints_config_block(self, mock_ws_stage, mock_load, mock_val, mock_refs, mock_spark, mock_ws, capsys):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        mock_load.return_value = {"config_snapshot": {"_parsed_space": {"data_sources": {"tables": []}}}}
        preflight_fetch_config(mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "revenue")
        captured = capsys.readouterr()
        assert "GENIE SPACE CONFIGURATION" in captured.out

    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space", return_value=(True, []))
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_normalizes_domain(self, mock_ws_stage, mock_load, mock_val, mock_refs, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        mock_load.return_value = {"config_snapshot": {"_parsed_space": {"data_sources": {"tables": []}}}}
        result = preflight_fetch_config(mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "My Domain!!")
        assert result["domain"] == "my_domain"


# ---------------------------------------------------------------------------
# Step 2: preflight_collect_uc_metadata
# ---------------------------------------------------------------------------

class TestPreflightCollectUcMetadata:
    @patch("genie_space_optimizer.optimization.preflight._compute_join_overlaps", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight._validate_core_access")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_returns_expected_keys_no_refs(self, mock_ws, mock_val, mock_join, mock_spark):
        from genie_space_optimizer.optimization.preflight import preflight_collect_uc_metadata

        result = preflight_collect_uc_metadata(
            MagicMock(), mock_spark, "run-1", "cat", "gold",
            config={}, snapshot={}, genie_table_refs=[],
        )
        assert set(result.keys()) == {"uc_columns", "uc_tags", "uc_routines", "uc_fk"}

    @patch("genie_space_optimizer.optimization.preflight._compute_join_overlaps", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight._validate_core_access")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_without_warehouse_id_uses_spark(self, mock_ws_stage, mock_val, mock_join, mock_spark):
        """Calling without warehouse_id preserves Spark-only behavior (R7)."""
        from genie_space_optimizer.optimization.preflight import preflight_collect_uc_metadata

        result = preflight_collect_uc_metadata(
            MagicMock(), mock_spark, "run-1", "cat", "gold",
            config={}, snapshot={}, genie_table_refs=[],
        )
        assert "uc_columns" in result

    @patch("genie_space_optimizer.optimization.preflight._collect_data_profile")
    @patch("genie_space_optimizer.optimization.preflight._compute_join_overlaps", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight._validate_core_access")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_with_warehouse_id_threads_to_profile(
        self, mock_ws_stage, mock_val, mock_join, mock_profile, mock_spark
    ):
        """warehouse_id is forwarded to _collect_data_profile."""
        from genie_space_optimizer.optimization.preflight import preflight_collect_uc_metadata

        mock_profile.return_value = {}
        preflight_collect_uc_metadata(
            MagicMock(), mock_spark, "run-1", "cat", "gold",
            config={}, snapshot={}, genie_table_refs=[],
            warehouse_id="wh-123",
        )
        if mock_profile.called:
            _, kwargs = mock_profile.call_args
            assert kwargs.get("warehouse_id") == "wh-123"


# ---------------------------------------------------------------------------
# Step 3: preflight_generate_benchmarks
# ---------------------------------------------------------------------------

class TestPreflightGenerateBenchmarks:
    @patch("genie_space_optimizer.optimization.preflight._load_or_generate_benchmarks")
    def test_returns_benchmarks_and_flag(self, mock_gen):
        from genie_space_optimizer.optimization.preflight import preflight_generate_benchmarks

        mock_gen.return_value = ([{"question": "q1"}], False)
        result = preflight_generate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold",
            {}, [], [], [], "default",
        )
        assert "benchmarks" in result
        assert "regenerated" in result
        assert len(result["benchmarks"]) == 1

    @patch("genie_space_optimizer.optimization.preflight._load_or_generate_benchmarks")
    def test_prints_generation_block(self, mock_gen, capsys):
        from genie_space_optimizer.optimization.preflight import preflight_generate_benchmarks

        mock_gen.return_value = ([{"question": "q1", "id": "b1"}], True)
        preflight_generate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold",
            {}, [], [], [], "default",
        )
        captured = capsys.readouterr()
        assert "BENCHMARK GENERATION" in captured.out

    @patch("genie_space_optimizer.optimization.preflight._load_or_generate_benchmarks")
    def test_without_warehouse_id_backward_compat(self, mock_gen):
        """Calling without warehouse_id preserves existing behavior (R7)."""
        from genie_space_optimizer.optimization.preflight import preflight_generate_benchmarks

        mock_gen.return_value = ([{"question": "q1"}], False)
        result = preflight_generate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold",
            {}, [], [], [], "default",
        )
        assert "benchmarks" in result
        _, kwargs = mock_gen.call_args
        assert kwargs.get("warehouse_id", "") == ""

    @patch("genie_space_optimizer.optimization.preflight._load_or_generate_benchmarks")
    def test_with_warehouse_id_threads_through(self, mock_gen):
        """warehouse_id is forwarded to _load_or_generate_benchmarks."""
        from genie_space_optimizer.optimization.preflight import preflight_generate_benchmarks

        mock_gen.return_value = ([{"question": "q1"}], False)
        preflight_generate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold",
            {}, [], [], [], "default",
            warehouse_id="wh-456",
        )
        _, kwargs = mock_gen.call_args
        assert kwargs.get("warehouse_id") == "wh-456"


# ---------------------------------------------------------------------------
# Step 4: preflight_validate_benchmarks
# ---------------------------------------------------------------------------

class TestPreflightValidateBenchmarks:
    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    def test_filters_invalid_benchmarks(self, mock_validate):
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        benchmarks = [{"question": f"q{i}", "id": f"b{i}"} for i in range(7)]
        validations = [{"valid": True}] * 6 + [{"valid": False, "error": "missing column"}]
        mock_validate.return_value = validations
        result = preflight_validate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
            benchmarks, [], [], [], "default",
        )
        assert len(result["benchmarks"]) == 6
        assert result["pre_count"] == 7

    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    def test_without_warehouse_id_backward_compat(self, mock_validate):
        """Calling without warehouse_id preserves existing behavior (R7)."""
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        benchmarks = [{"question": f"q{i}", "id": f"b{i}"} for i in range(6)]
        mock_validate.return_value = [{"valid": True}] * 6
        result = preflight_validate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
            benchmarks, [], [], [], "default",
        )
        assert "benchmarks" in result
        _, kwargs = mock_validate.call_args
        assert kwargs.get("warehouse_id", "") == ""

    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    def test_with_warehouse_id_threads_through(self, mock_validate):
        """warehouse_id is forwarded to validate_benchmarks."""
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        benchmarks = [{"question": f"q{i}", "id": f"b{i}"} for i in range(6)]
        mock_validate.return_value = [{"valid": True}] * 6
        preflight_validate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
            benchmarks, [], [], [], "default",
            warehouse_id="wh-789",
        )
        _, kwargs = mock_validate.call_args
        assert kwargs.get("warehouse_id") == "wh-789"


# ---------------------------------------------------------------------------
# Step 5: preflight_load_human_feedback
# ---------------------------------------------------------------------------

class TestPreflightLoadHumanFeedback:
    def test_returns_empty_corrections_on_failure(self):
        from genie_space_optimizer.optimization.preflight import preflight_load_human_feedback

        result = preflight_load_human_feedback(
            MagicMock(), "run-1", "space-1", "cat", "gold", "default",
        )
        assert "human_corrections" in result
        assert isinstance(result["human_corrections"], list)

    def test_prints_feedback_block(self, capsys):
        from genie_space_optimizer.optimization.preflight import preflight_load_human_feedback

        preflight_load_human_feedback(
            MagicMock(), "run-1", "space-1", "cat", "gold", "default",
        )
        captured = capsys.readouterr()
        assert "HUMAN FEEDBACK" in captured.out


# ---------------------------------------------------------------------------
# Step 6: preflight_setup_experiment
# ---------------------------------------------------------------------------

class TestPreflightSetupExperiment:
    @patch("genie_space_optimizer.optimization.preflight.create_evaluation_dataset")
    @patch("genie_space_optimizer.optimization.preflight._drop_benchmark_table")
    @patch("genie_space_optimizer.optimization.preflight.compute_asset_fingerprint", return_value="fp123")
    @patch("genie_space_optimizer.optimization.preflight._flag_stale_temporal_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.register_instruction_version")
    @patch("genie_space_optimizer.optimization.preflight._get_general_instructions", return_value="instructions")
    @patch("genie_space_optimizer.optimization.preflight.mlflow")
    @patch("genie_space_optimizer.optimization.preflight._ensure_experiment_parent_dir")
    @patch("genie_space_optimizer.optimization.preflight._resolve_experiment_path", return_value="/exp/path")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_returns_expected_keys(self, mock_ws, mock_resolve, mock_dir, mock_mlflow,
                                    mock_instr_fn, mock_reg_instr, mock_flag, mock_fp,
                                    mock_drop, mock_create_ds):
        from genie_space_optimizer.optimization.preflight import preflight_setup_experiment

        mock_exp = MagicMock()
        mock_exp.experiment_id = "exp-123"
        mock_mlflow.get_experiment_by_name.return_value = mock_exp

        result = preflight_setup_experiment(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "gold", "default",
            {"_parsed_space": {}}, [{"question": "q1"}],
            [], [], [], [],
        )
        assert set(result.keys()) == {
            "model_id", "experiment_name", "experiment_id",
        }
        assert result["model_id"] is None
        assert result["experiment_name"] == "/exp/path"


# ---------------------------------------------------------------------------
# Wrapper equivalence
# ---------------------------------------------------------------------------

class TestPreflightWrapperEquivalence:
    @patch("genie_space_optimizer.optimization.preflight.preflight_setup_experiment")
    @patch("genie_space_optimizer.optimization.preflight.preflight_load_human_feedback")
    @patch("genie_space_optimizer.optimization.preflight.preflight_validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_collect_uc_metadata")
    @patch("genie_space_optimizer.optimization.preflight.preflight_fetch_config")
    def test_wrapper_calls_all_6_substeps(self, mock_cfg, mock_uc, mock_gen, mock_val, mock_fb, mock_exp):
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

        assert model_id == "mv-1"
        assert exp_name == "/exp"
        assert isinstance(config, dict)
        assert isinstance(benchmarks, list)
        assert isinstance(corrections, list)
