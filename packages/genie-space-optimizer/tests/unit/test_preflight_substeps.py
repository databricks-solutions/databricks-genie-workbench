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

        mock_profile.return_value = ({}, [])
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
    def _enough_benchmarks(self, n=25):
        """Return enough benchmarks to avoid post-validation top-up."""
        return [{"question": f"q{i}", "id": f"b{i}"} for i in range(n)]

    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    def test_filters_invalid_benchmarks(self, mock_validate):
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        benchmarks = self._enough_benchmarks(25) + [{"question": "qbad", "id": "bbad"}]
        validations = [{"valid": True}] * 25 + [{"valid": False, "error": "missing column"}]
        mock_validate.return_value = validations
        result = preflight_validate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
            benchmarks, [], [], [], "default",
        )
        assert len(result["benchmarks"]) == 25
        assert result["pre_count"] == 26

    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    def test_without_warehouse_id_backward_compat(self, mock_validate):
        """Calling without warehouse_id preserves existing behavior (R7)."""
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        benchmarks = self._enough_benchmarks(25)
        mock_validate.return_value = [{"valid": True}] * 25
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

        benchmarks = self._enough_benchmarks(25)
        mock_validate.return_value = [{"valid": True}] * 25
        preflight_validate_benchmarks(
            MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
            benchmarks, [], [], [], "default",
            warehouse_id="wh-789",
        )
        _, kwargs = mock_validate.call_args
        assert kwargs.get("warehouse_id") == "wh-789"

    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks")
    def test_post_validation_topup_does_not_reextract_curated_benchmarks(
        self,
        mock_extract,
        mock_generate,
        mock_validate,
        mock_write_stage,
    ):
        """Top-up must fill only the synthetic gap after validation.

        Regression for the 30 -> 19 handoff mismatch: re-extracting curated
        Genie benchmark rows during top-up reintroduced gs_001.. IDs that were
        already present in the validated corpus.
        """
        from genie_space_optimizer.optimization.preflight import preflight_validate_benchmarks

        initial = [
            {
                "id": f"sales_gs_{i + 1:03d}",
                "question": f"validated question {i + 1}",
                "expected_sql": "SELECT 1",
            }
            for i in range(18)
        ]
        topped_up = initial + [
            {
                "id": f"sales_{i + 19:03d}",
                "question": f"synthetic top-up question {i + 1}",
                "expected_sql": "SELECT 1",
            }
            for i in range(12)
        ]

        mock_validate.side_effect = [
            [{"valid": True}] * 18,
            [{"valid": True}] * 30,
        ]
        mock_generate.return_value = topped_up

        result = preflight_validate_benchmarks(
            MagicMock(),
            MagicMock(),
            "run-1",
            "cat",
            "gold",
            {"_parsed_space": {}},
            initial,
            [],
            [],
            [],
            "sales",
            target_benchmark_count=30,
            max_benchmark_count=30,
        )

        assert len(result["benchmarks"]) == 30
        assert result["benchmarks"] == topped_up
        mock_extract.assert_not_called()
        _, kwargs = mock_generate.call_args
        assert kwargs["genie_space_benchmarks"] == []
        assert kwargs["existing_benchmarks"] == initial


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
    """Tests for preflight_setup_experiment (step 6).

    All tests share the same decorator stack to mock out external
    dependencies (MLflow, state writes, benchmark operations).
    """

    _COMMON_PATCHES = [
        "genie_space_optimizer.optimization.preflight.write_stage",
        "genie_space_optimizer.optimization.preflight._resolve_experiment_path",
        "genie_space_optimizer.optimization.preflight._ensure_experiment_parent_dir",
        "genie_space_optimizer.optimization.preflight.mlflow",
        "genie_space_optimizer.optimization.preflight._get_general_instructions",
        "genie_space_optimizer.optimization.preflight.register_instruction_version",
        "genie_space_optimizer.optimization.preflight._flag_stale_temporal_benchmarks",
        "genie_space_optimizer.optimization.preflight.compute_asset_fingerprint",
        "genie_space_optimizer.optimization.preflight._drop_benchmark_table",
        "genie_space_optimizer.optimization.preflight.create_evaluation_dataset",
    ]

    def _call_setup(self, mock_spark=None, catalog="cat", schema="gold", **extra_mocks):
        """Helper: invoke preflight_setup_experiment with all deps mocked."""
        from genie_space_optimizer.optimization.preflight import preflight_setup_experiment

        if mock_spark is None:
            mock_spark = MagicMock(name="spark")

        with (
            patch(self._COMMON_PATCHES[0]) as mock_ws,
            patch(self._COMMON_PATCHES[1], return_value="/exp/path"),
            patch(self._COMMON_PATCHES[2]),
            patch(self._COMMON_PATCHES[3]) as mock_mlflow,
            patch(self._COMMON_PATCHES[4], return_value="instructions"),
            patch(self._COMMON_PATCHES[5]),
            patch(self._COMMON_PATCHES[6]),
            patch(self._COMMON_PATCHES[7], return_value="fp123"),
            patch(self._COMMON_PATCHES[8]) as mock_drop,
            patch(self._COMMON_PATCHES[9]) as mock_create_ds,
        ):
            mock_exp = MagicMock()
            mock_exp.experiment_id = "exp-123"
            mock_mlflow.get_experiment_by_name.return_value = mock_exp

            for k, v in extra_mocks.items():
                if k == "drop_side_effect":
                    mock_drop.side_effect = v
                elif k == "create_ds_side_effect":
                    mock_create_ds.side_effect = v

            result = preflight_setup_experiment(
                MagicMock(), mock_spark, "run-1", "space-1", catalog, schema, "default",
                {"_parsed_space": {}}, [{"question": "q1"}],
                [], [], [], [],
            )
        return result

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
            "benchmark_count", "evaluation_dataset",
        }
        assert result["model_id"] is None
        assert result["experiment_name"] == "/exp/path"

    def test_returns_writer_benchmark_count_from_create_dataset(self):
        """preflight_setup_experiment exposes writer count for downstream task values."""
        writer_result = {
            "dataset": object(),
            "table_name": "cat.gold.genie_benchmarks_default",
            "input_count": 30,
            "record_count": 24,
            "unique_question_id_count": 24,
        }

        result = self._call_setup(
            create_ds_side_effect=lambda *a, **kw: writer_result,
        )

        assert result["benchmark_count"] == 24
        assert result["evaluation_dataset"] == writer_result

    def test_sets_sql_context_with_use_catalog_and_schema(self):
        """USE CATALOG / USE SCHEMA must be issued with the correct values."""
        mock_spark = MagicMock(name="spark")
        self._call_setup(mock_spark=mock_spark, catalog="psk", schema="genie_space_optimizer")

        sql_args = [str(call) for call in mock_spark.sql.call_args_list]
        use_catalog = [s for s in sql_args if "USE CATALOG" in s]
        use_schema = [s for s in sql_args if "USE SCHEMA" in s]
        assert use_catalog, "Expected at least one USE CATALOG call"
        assert use_schema, "Expected at least one USE SCHEMA call"
        assert "psk" in use_catalog[0]
        assert "genie_space_optimizer" in use_schema[0]

    def test_sql_context_set_before_drop_and_create(self):
        """_set_sql_context must run before _drop_benchmark_table and create_evaluation_dataset."""
        call_order = []

        with patch(
            "genie_space_optimizer.optimization.preflight._set_sql_context",
            side_effect=lambda *a, **kw: call_order.append("set_sql_context"),
        ):
            self._call_setup(
                drop_side_effect=lambda *a, **kw: call_order.append("drop_benchmark_table"),
                create_ds_side_effect=lambda *a, **kw: call_order.append("create_evaluation_dataset"),
            )

        assert "set_sql_context" in call_order, \
            f"_set_sql_context was not called; order={call_order}"
        assert call_order.index("set_sql_context") < call_order.index("drop_benchmark_table"), \
            f"_set_sql_context must precede _drop_benchmark_table; order={call_order}"
        assert call_order.index("set_sql_context") < call_order.index("create_evaluation_dataset"), \
            f"_set_sql_context must precede create_evaluation_dataset; order={call_order}"

    def test_sql_context_receives_correct_catalog_and_schema(self):
        """_set_sql_context must receive the catalog and schema arguments unchanged."""
        with patch(
            "genie_space_optimizer.optimization.preflight._set_sql_context"
        ) as mock_ctx:
            self._call_setup(catalog="my_catalog", schema="my_schema")

        mock_ctx.assert_called_once()
        _, spark_arg, cat_arg, sch_arg = mock_ctx.call_args[0][0], mock_ctx.call_args[0][0], mock_ctx.call_args[0][1], mock_ctx.call_args[0][2]
        assert cat_arg == "my_catalog"
        assert sch_arg == "my_schema"


# ---------------------------------------------------------------------------
# _set_sql_context helper (evaluation.py)
# ---------------------------------------------------------------------------

class TestSetSqlContext:
    """Direct unit tests for the _set_sql_context helper."""

    def test_sets_catalog_and_schema(self):
        from genie_space_optimizer.optimization.evaluation import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "my_catalog", "my_schema")
        assert spark.sql.call_count == 2
        calls = [str(c) for c in spark.sql.call_args_list]
        assert "USE CATALOG" in calls[0] and "my_catalog" in calls[0]
        assert "USE SCHEMA" in calls[1] and "my_schema" in calls[1]

    def test_skips_when_catalog_empty(self):
        from genie_space_optimizer.optimization.evaluation import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "", "my_schema")
        assert spark.sql.call_count == 1
        assert "USE SCHEMA" in str(spark.sql.call_args)

    def test_skips_when_schema_empty(self):
        from genie_space_optimizer.optimization.evaluation import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "my_catalog", "")
        assert spark.sql.call_count == 1
        assert "USE CATALOG" in str(spark.sql.call_args)

    def test_skips_when_both_empty(self):
        from genie_space_optimizer.optimization.evaluation import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "", "")
        spark.sql.assert_not_called()

    def test_escapes_backticks_in_identifiers(self):
        from genie_space_optimizer.optimization.evaluation import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "cat`alog", "sch`ema")
        calls = [str(c) for c in spark.sql.call_args_list]
        assert "cat``alog" in calls[0]
        assert "sch``ema" in calls[1]


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

    @patch("genie_space_optimizer.optimization.preflight.preflight_setup_experiment")
    @patch("genie_space_optimizer.optimization.preflight.preflight_load_human_feedback")
    @patch("genie_space_optimizer.optimization.preflight.preflight_validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.preflight_collect_uc_metadata")
    @patch("genie_space_optimizer.optimization.preflight.preflight_fetch_config")
    def test_wrapper_forwards_warehouse_id_to_warehouse_aware_substeps(
        self, mock_cfg, mock_uc, mock_gen, mock_val, mock_fb, mock_exp
    ):
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

        run_preflight(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "gold", "revenue",
            warehouse_id="wh-test",
        )

        assert mock_uc.call_args.kwargs["warehouse_id"] == "wh-test"
        assert mock_gen.call_args.kwargs["warehouse_id"] == "wh-test"
        assert mock_val.call_args.kwargs["warehouse_id"] == "wh-test"


class TestHarnessPreflightWarehouseID:
    @patch("genie_space_optimizer.optimization.harness.update_run_status")
    @patch("genie_space_optimizer.optimization.harness.resolve_warehouse_id", return_value="wh-env")
    @patch("genie_space_optimizer.optimization.harness._safe_stage")
    def test_run_preflight_resolves_and_forwards_warehouse_id(
        self, mock_safe_stage, mock_resolve, mock_update, mock_spark
    ):
        from genie_space_optimizer.optimization import harness

        mock_safe_stage.return_value = (
            {"_gso_iq_scan_recommended_levers": [], "_gso_iq_scan_summary": None},
            [],
            "model-1",
            "/exp",
            [],
        )

        with patch.object(harness, "mlflow", create=True) as mock_mlflow:
            mock_mlflow.get_experiment_by_name.return_value = MagicMock(
                experiment_id="exp-1",
            )
            out = harness._run_preflight(
                MagicMock(), mock_spark, "run-1", "space-1",
                "cat", "gold", "revenue",
            )

        mock_resolve.assert_called_once_with("")
        assert mock_safe_stage.call_args.args[-1] == "wh-env"
        assert out["model_id"] == "model-1"


def test_update_run_status_retries_delta_concurrent_append(monkeypatch) -> None:
    from genie_space_optimizer.optimization import state

    calls: list[dict] = []

    class ConcurrentAppendLike(Exception):
        pass

    def fake_update_row(_spark, _catalog, _schema, _table, _keys, updates):
        calls.append(updates)
        if len(calls) == 1:
            raise ConcurrentAppendLike(
                "[DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT] Transaction conflict detected"
            )

    monkeypatch.setattr(state, "update_row", fake_update_row)
    monkeypatch.setattr(state.time, "sleep", lambda _seconds: None)

    state.update_run_status(
        spark=object(),
        run_id="run_1",
        catalog="cat",
        schema="sch",
        config_snapshot={"serialized_space": {"name": "snapshot"}},
    )

    assert len(calls) == 2
    assert "config_snapshot" in calls[1]
