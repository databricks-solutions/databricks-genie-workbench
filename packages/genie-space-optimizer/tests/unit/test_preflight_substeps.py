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

    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_table_refs", return_value=[])
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_normalizes_id_only_instruction_in_stored_snapshot(
        self,
        mock_write_stage,
        mock_load,
        mock_refs,
        mock_spark,
        mock_ws,
        caplog,
    ):
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        snapshot = {
            "_parsed_space": {
                "version": 2,
                "data_sources": {
                    "tables": [{"identifier": "cat.gold.orders"}],
                    "metric_views": [],
                },
                "instructions": {
                    "text_instructions": [{"id": "a" * 32}],
                },
            }
        }
        mock_load.return_value = {"config_snapshot": snapshot}

        with caplog.at_level("WARNING"):
            result = preflight_fetch_config(
                mock_ws,
                mock_spark,
                "run-1",
                "space-1",
                "cat",
                "gold",
                "revenue",
            )

        assert result["config"]["_parsed_space"]["instructions"][
            "text_instructions"
        ] == []
        assert "empty text-instruction placeholder" in caplog.text

    @patch("genie_space_optimizer.optimization.preflight.validate_serialized_space")
    @patch("genie_space_optimizer.optimization.preflight.load_run")
    @patch("genie_space_optimizer.optimization.preflight.write_failure_stage_safely")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_structural_validation_failure_is_persisted_and_raised(
        self,
        mock_write_stage,
        mock_write_failure,
        mock_load,
        mock_validate,
        mock_spark,
        mock_ws,
    ):
        from genie_space_optimizer.common.genie_schema import SerializedSpaceValidationError
        from genie_space_optimizer.optimization.preflight import preflight_fetch_config

        mock_load.return_value = {
            "config_snapshot": {"_parsed_space": {"instructions": {"sql_snippets": {}}}}
        }
        mock_validate.return_value = (
            False,
            ["instructions.sql_snippets.measures.0.id: Field required"],
        )

        with pytest.raises(SerializedSpaceValidationError, match="CONFIG_VALIDATION_FAILED"):
            preflight_fetch_config(
                mock_ws, mock_spark, "run-1", "space-1", "cat", "gold", "revenue"
            )

        mock_write_stage.assert_called_once()
        failure_call = mock_write_failure.call_args
        assert failure_call.args[2] == "PREFLIGHT_CONFIG_VALIDATION"
        assert failure_call.kwargs["error_message"] == "CONFIG_VALIDATION_FAILED"
        assert failure_call.kwargs["detail"]["errors"] == [
            "instructions.sql_snippets.measures.0.id: Field required"
        ]


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


class TestLoadOrGenerateBenchmarks:
    def test_reuse_topup_does_not_double_count_curated_questions(self):
        from genie_space_optimizer.optimization.preflight import (
            _load_or_generate_benchmarks,
        )

        existing = [
            {
                "id": f"native-{index}",
                "question": f"question {index}",
                "expected_sql": "SELECT 1",
                "source": "genie_benchmark",
                "space_question_id": f"native-{index}",
            }
            for index in range(15)
        ]
        generated = existing + [
            {
                "id": f"synthetic-{index}",
                "question": f"synthetic question {index}",
                "expected_sql": "SELECT 1",
            }
            for index in range(15)
        ]

        with (
            patch(
                "genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks",
                return_value=[dict(row) for row in existing],
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.load_benchmark_corpus",
                return_value=[dict(row) for row in existing],
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.validate_benchmarks",
                return_value=[{"valid": True}] * len(existing),
            ),
            patch(
                "genie_space_optimizer.optimization.benchmarking._filter_example_sql_mirrored_benchmarks",
                side_effect=lambda rows, _config: rows,
            ),
            patch(
                "genie_space_optimizer.optimization.benchmarks.validate_question_sql_alignment",
                side_effect=lambda rows: [
                    {"question": row["question"], "aligned": True, "issues": []}
                    for row in rows
                ],
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.generate_benchmarks",
                return_value=generated,
            ) as mock_generate,
            patch("genie_space_optimizer.optimization.preflight.write_stage"),
        ):
            result, regenerated = _load_or_generate_benchmarks(
                MagicMock(),
                MagicMock(),
                {},
                [],
                [],
                [],
                "default",
                "cat",
                "gold",
                "cat.gold",
                "run-1",
            )

        assert regenerated is False
        assert len(result) == 30
        assert mock_generate.call_args.kwargs["genie_space_benchmarks"] == []
        passed_existing = mock_generate.call_args.kwargs["existing_benchmarks"]
        assert [row["id"] for row in passed_existing] == [row["id"] for row in existing]
        assert [row["question"] for row in passed_existing] == [
            row["question"] for row in existing
        ]


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

    def test_fourteen_rows_still_fail_after_regeneration_attempt(self):
        from genie_space_optimizer.optimization.benchmark_repair import (
            BenchmarkCorpusTooSmallError,
        )
        from genie_space_optimizer.optimization.preflight import (
            preflight_validate_benchmarks,
        )

        benchmarks = self._enough_benchmarks(14)
        with (
            patch(
                "genie_space_optimizer.optimization.preflight.validate_benchmarks",
                return_value=[{"valid": True}] * 14,
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks",
                return_value=[],
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.generate_benchmarks",
                return_value=benchmarks,
            ) as mock_generate,
            patch("genie_space_optimizer.optimization.preflight.write_stage"),
        ):
            with pytest.raises(BenchmarkCorpusTooSmallError):
                preflight_validate_benchmarks(
                    MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
                    benchmarks, [], [], [], "default",
                )

        mock_generate.assert_called_once()

    @pytest.mark.parametrize("count", [15, 17])
    def test_fifteen_or_seventeen_rows_pass_if_topup_cannot_reach_30(self, count):
        from genie_space_optimizer.optimization.preflight import (
            preflight_validate_benchmarks,
        )

        benchmarks = self._enough_benchmarks(count)

        def all_valid(rows, *args, **kwargs):
            return [{"valid": True}] * len(rows)

        with (
            patch(
                "genie_space_optimizer.optimization.preflight.validate_benchmarks",
                side_effect=all_valid,
            ),
            patch(
                "genie_space_optimizer.optimization.preflight.generate_benchmarks",
                return_value=benchmarks,
            ) as mock_generate,
            patch("genie_space_optimizer.optimization.preflight.write_stage"),
        ):
            result = preflight_validate_benchmarks(
                MagicMock(), MagicMock(), "run-1", "cat", "gold", {},
                benchmarks, [], [], [], "default",
            )

        assert len(result["benchmarks"]) == count
        assert mock_generate.call_args.kwargs["target_count"] == 30

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

        with patch(
            "genie_space_optimizer.optimization.benchmarks.validate_question_sql_alignment",
            side_effect=lambda rows: [{"aligned": True} for _ in rows],
        ):
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
# preflight_persist_benchmark_corpus
# ---------------------------------------------------------------------------

class TestPreflightPersistBenchmarkCorpus:
    """Tests for preflight_persist_benchmark_corpus (step 6).

    All tests share the same decorator stack to mock out external
    dependencies (MLflow tracing, state writes, benchmark operations).
    """

    _COMMON_PATCHES = [
        "genie_space_optimizer.optimization.preflight.write_stage",
        "genie_space_optimizer.optimization.preflight._resolve_experiment_path",
        "genie_space_optimizer.optimization.preflight._ensure_experiment_parent_dir",
        "genie_space_optimizer.optimization.preflight.mlflow",
        "genie_space_optimizer.optimization.preflight._flag_stale_temporal_benchmarks",
        "genie_space_optimizer.optimization.preflight.compute_asset_fingerprint",
        "genie_space_optimizer.optimization.preflight.persist_benchmark_corpus",
    ]

    def _call_setup(self, mock_spark=None, catalog="cat", schema="gold", **extra_mocks):
        """Invoke preflight_persist_benchmark_corpus with external deps mocked."""
        from genie_space_optimizer.optimization.preflight import (
            preflight_persist_benchmark_corpus,
        )

        if mock_spark is None:
            mock_spark = MagicMock(name="spark")

        with (
            patch(self._COMMON_PATCHES[0]) as mock_ws,
            patch(self._COMMON_PATCHES[1], return_value="/exp/path"),
            patch(self._COMMON_PATCHES[2]),
            patch(self._COMMON_PATCHES[3]) as mock_mlflow,
            patch(self._COMMON_PATCHES[4]),
            patch(self._COMMON_PATCHES[5], return_value="fp123"),
            patch(self._COMMON_PATCHES[6]) as mock_persist,
        ):
            for k, v in extra_mocks.items():
                if k == "persist_side_effect":
                    mock_persist.side_effect = v
                elif k == "trace_side_effect":
                    mock_mlflow.set_experiment.side_effect = v

            result = preflight_persist_benchmark_corpus(
                MagicMock(), mock_spark, "run-1", "space-1", catalog, schema, "default",
                {"_parsed_space": {}}, [{"question": "q1"}],
                [],
            )
        return result

    @patch("genie_space_optimizer.optimization.preflight.persist_benchmark_corpus")
    @patch("genie_space_optimizer.optimization.preflight.compute_asset_fingerprint", return_value="fp123")
    @patch("genie_space_optimizer.optimization.preflight._flag_stale_temporal_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.mlflow")
    @patch("genie_space_optimizer.optimization.preflight._ensure_experiment_parent_dir")
    @patch("genie_space_optimizer.optimization.preflight._resolve_experiment_path", return_value="/exp/path")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_returns_expected_keys(self, mock_ws, mock_resolve, mock_dir, mock_mlflow,
                                    mock_flag, mock_fp, mock_persist):
        from genie_space_optimizer.optimization.preflight import (
            preflight_persist_benchmark_corpus,
        )

        result = preflight_persist_benchmark_corpus(
            MagicMock(), MagicMock(), "run-1", "space-1", "cat", "gold", "default",
            {"_parsed_space": {}}, [{"question": "q1"}],
            [],
        )
        assert set(result.keys()) == {
            "experiment_name", "benchmark_count", "benchmark_corpus",
        }
        assert result["experiment_name"] == "/exp/path"

    def test_returns_writer_benchmark_count_from_delta_persist(self):
        """The handoff step exposes the writer count to downstream tasks."""
        writer_result = {
            "table_name": "cat.gold.genie_benchmarks_default",
            "input_count": 30,
            "record_count": 24,
            "unique_question_id_count": 24,
        }

        result = self._call_setup(
            persist_side_effect=lambda *a, **kw: writer_result,
        )

        assert result["benchmark_count"] == 24
        assert result["benchmark_corpus"] == writer_result

    def test_trace_setup_failure_does_not_block_delta_handoff(self):
        writer_result = {
            "table_name": "cat.gold.genie_benchmarks_default",
            "record_count": 1,
        }

        result = self._call_setup(
            trace_side_effect=RuntimeError("experiment unavailable"),
            persist_side_effect=lambda *a, **kw: writer_result,
        )

        assert result["benchmark_count"] == 1
        assert result["benchmark_corpus"] == writer_result

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

    def test_sql_context_set_before_delta_persist(self):
        """_set_sql_context must run before the direct Delta overwrite."""
        call_order = []

        with patch(
            "genie_space_optimizer.optimization.preflight._set_sql_context",
            side_effect=lambda *a, **kw: call_order.append("set_sql_context"),
        ):
            self._call_setup(
                persist_side_effect=lambda *a, **kw: call_order.append("persist_benchmark_corpus"),
            )

        assert "set_sql_context" in call_order, \
            f"_set_sql_context was not called; order={call_order}"
        assert call_order.index("set_sql_context") < call_order.index("persist_benchmark_corpus"), \
            f"_set_sql_context must precede persist_benchmark_corpus; order={call_order}"

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
        from genie_space_optimizer.optimization.benchmarking import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "my_catalog", "my_schema")
        assert spark.sql.call_count == 2
        calls = [str(c) for c in spark.sql.call_args_list]
        assert "USE CATALOG" in calls[0] and "my_catalog" in calls[0]
        assert "USE SCHEMA" in calls[1] and "my_schema" in calls[1]

    def test_skips_when_catalog_empty(self):
        from genie_space_optimizer.optimization.benchmarking import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "", "my_schema")
        assert spark.sql.call_count == 1
        assert "USE SCHEMA" in str(spark.sql.call_args)

    def test_skips_when_schema_empty(self):
        from genie_space_optimizer.optimization.benchmarking import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "my_catalog", "")
        assert spark.sql.call_count == 1
        assert "USE CATALOG" in str(spark.sql.call_args)

    def test_skips_when_both_empty(self):
        from genie_space_optimizer.optimization.benchmarking import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "", "")
        spark.sql.assert_not_called()

    def test_escapes_backticks_in_identifiers(self):
        from genie_space_optimizer.optimization.benchmarking import _set_sql_context

        spark = MagicMock()
        _set_sql_context(spark, "cat`alog", "sch`ema")
        calls = [str(c) for c in spark.sql.call_args_list]
        assert "cat``alog" in calls[0]
        assert "sch``ema" in calls[1]


def test_update_run_status_retries_delta_concurrent_append(monkeypatch) -> None:
    from genie_space_optimizer.optimization import state

    calls: list[dict] = []

    class ConcurrentAppendLike(Exception):
        pass

    def fake_update_row(_spark, _catalog, _schema, _table, _keys, updates, **_kwargs):
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
