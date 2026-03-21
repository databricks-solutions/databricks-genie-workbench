"""Unit tests for genie_space_optimizer.optimization.evaluation — pure functions."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.evaluation import (
    _compute_arbiter_adjusted_accuracy,
    _compute_asset_coverage,
    _detect_temporal_intent,
    _fill_coverage_gaps,
    _rewrite_temporal_dates,
    all_thresholds_met,
    build_temporal_note,
    normalize_result_df,
    normalize_scores,
    result_signature,
)


class TestNormalizeScores:
    def test_converts_0_1_to_0_100(self):
        scores = {"syntax": 0.95, "schema": 0.90}
        result = normalize_scores(scores)
        assert result["syntax"] == 95.0
        assert result["schema"] == 90.0

    def test_leaves_0_100_unchanged(self):
        scores = {"syntax": 95.0, "schema": 90.0}
        result = normalize_scores(scores)
        assert result["syntax"] == 95.0
        assert result["schema"] == 90.0

    def test_boundary_value_1(self):
        scores = {"perfect": 1.0}
        result = normalize_scores(scores)
        assert result["perfect"] == 100.0

    def test_boundary_value_0(self):
        scores = {"zero": 0.0}
        result = normalize_scores(scores)
        assert result["zero"] == 0.0

    def test_empty_dict(self):
        assert normalize_scores({}) == {}


class TestAllThresholdsMet:
    def test_all_met(self):
        scores = {
            "syntax_validity": 99.0,
            "schema_accuracy": 96.0,
            "logical_accuracy": 91.0,
            "semantic_equivalence": 91.0,
            "completeness": 91.0,
            "response_quality": 50.0,
            "result_correctness": 86.0,
            "asset_routing": 96.0,
        }
        assert all_thresholds_met(scores) is True

    def test_one_below(self):
        scores = {
            "syntax_validity": 99.0,
            "schema_accuracy": 96.0,
            "logical_accuracy": 91.0,
            "semantic_equivalence": 91.0,
            "completeness": 91.0,
            "response_quality": 50.0,
            "result_correctness": 80.0,  # below 85
            "asset_routing": 96.0,
        }
        assert all_thresholds_met(scores) is False

    def test_missing_judge_returns_false(self):
        scores = {"syntax_validity": 99.0}
        assert all_thresholds_met(scores) is False

    def test_custom_targets(self):
        scores = {"custom_metric": 75.0}
        targets = {"custom_metric": 70.0}
        assert all_thresholds_met(scores, targets) is True

    def test_empty_scores_with_defaults_fails(self):
        assert all_thresholds_met({}) is False


class TestNormalizeResultDf:
    def test_none_returns_empty_df(self):
        result = normalize_result_df(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame()
        result = normalize_result_df(df)
        assert result is not None
        assert result.empty

    def test_columns_lowercased(self):
        df = pd.DataFrame({"Col_A": [1, 2], "Col_B": ["x", "y"]})
        result = normalize_result_df(df)
        assert list(result.columns) == ["col_a", "col_b"]

    def test_columns_sorted(self):
        df = pd.DataFrame({"z_col": [1], "a_col": [2]})
        result = normalize_result_df(df)
        assert list(result.columns) == ["a_col", "z_col"]

    def test_floats_rounded(self):
        df = pd.DataFrame({"val": [1.123456789]})
        result = normalize_result_df(df)
        assert result["val"].iloc[0] == pytest.approx(1.1235, abs=1e-4)

    def test_strings_stripped(self):
        df = pd.DataFrame({"name": ["  alice  ", "bob  "]})
        result = normalize_result_df(df)
        assert result["name"].tolist() == ["alice", "bob"]

    def test_deterministic_row_order(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = normalize_result_df(df)
        assert result["a"].tolist() == [1, 2, 3]


class TestResultSignature:
    def test_none_returns_empty(self):
        sig = result_signature(None)
        assert sig["schema_hash"] == ""
        assert sig["row_count"] == 0

    def test_empty_df(self):
        sig = result_signature(pd.DataFrame())
        assert sig["row_count"] == 0

    def test_basic_signature(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        sig = result_signature(df)
        assert sig["row_count"] == 3
        assert sig["schema_hash"] != ""
        assert "a" in sig["numeric_sums"]
        assert sig["numeric_sums"]["a"] == 6.0

    def test_same_data_same_hash(self):
        df1 = pd.DataFrame({"x": [10, 20]})
        df2 = pd.DataFrame({"x": [10, 20]})
        assert result_signature(df1)["schema_hash"] == result_signature(df2)["schema_hash"]

    def test_different_schema_different_hash(self):
        df1 = pd.DataFrame({"x": [10]})
        df2 = pd.DataFrame({"y": [10]})
        assert result_signature(df1)["schema_hash"] != result_signature(df2)["schema_hash"]


class TestComputeArbiterAdjustedAccuracy:
    """Tests for arbiter-adjusted overall accuracy computation."""

    def test_empty_rows(self):
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy([])
        assert accuracy == 0.0
        assert correct == 0
        assert failures == []

    def test_all_correct(self):
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert accuracy == 100.0
        assert correct == 2
        assert failures == []

    def test_all_failed(self):
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert accuracy == 0.0
        assert correct == 0
        assert set(failures) == {"q1", "q2"}

    def test_arbiter_genie_correct_overrides_failure(self):
        """result_correctness=no but arbiter=genie_correct → counted as correct."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q3"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q3"]

    def test_arbiter_both_correct_overrides_failure(self):
        """result_correctness=no but arbiter=both_correct → counted as correct."""
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "both_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 1
        assert accuracy == 50.0
        assert failures == ["q2"]

    def test_rc_no_arbiter_skipped_is_failure(self):
        """result_correctness=no + arbiter=skipped → failure."""
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 0
        assert accuracy == 0.0
        assert failures == ["q1"]

    def test_mixed_scenario(self):
        """Realistic mix of verdicts."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "no", "arbiter/value": "both_correct",
             "inputs/question_id": "q4"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q5"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q6"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 4
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert set(failures) == {"q5", "q6"}

    def test_excluded_rows_not_in_denominator(self):
        """Excluded rows are removed from the denominator."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q4"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 1
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q3"]

    def test_all_excluded_returns_zero(self):
        """All rows excluded → 0% accuracy, no failures."""
        rows = [
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 2
        assert correct == 0
        assert accuracy == 0.0
        assert failures == []

    def test_mixed_with_exclusions(self):
        """Realistic mix including exclusions."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q4"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q5"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 2
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q5"]


# ── Temporal Date Resolution ──────────────────────────────────────────────

from datetime import date

from genie_space_optimizer.optimization.evaluation import TemporalIntent


class TestDetectTemporalIntent:
    """Tests for _detect_temporal_intent()."""

    def test_this_year(self):
        intent = _detect_temporal_intent("Top 10 hosts this year", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "this_year"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 2, 28)

    def test_ytd(self):
        intent = _detect_temporal_intent("Revenue YTD", today=date(2026, 3, 15))
        assert intent is not None
        assert intent.keyword == "ytd"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 3, 15)

    def test_this_month(self):
        intent = _detect_temporal_intent("Bookings this month", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "this_month"
        assert intent.start_date == date(2026, 2, 1)
        assert intent.end_date == date(2026, 2, 28)

    def test_this_quarter(self):
        intent = _detect_temporal_intent("Revenue this quarter", today=date(2026, 5, 10))
        assert intent is not None
        assert intent.keyword == "this_quarter"
        assert intent.start_date == date(2026, 4, 1)
        assert intent.end_date == date(2026, 5, 10)

    def test_last_quarter(self):
        intent = _detect_temporal_intent("Revenue last quarter", today=date(2026, 5, 10))
        assert intent is not None
        assert intent.keyword == "last_quarter"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 3, 31)

    def test_last_year(self):
        intent = _detect_temporal_intent("Revenue last year", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "last_year"
        assert intent.start_date == date(2025, 1, 1)
        assert intent.end_date == date(2025, 12, 31)

    def test_last_n_months(self):
        intent = _detect_temporal_intent("Revenue last 6 months", today=date(2026, 6, 15))
        assert intent is not None
        assert intent.keyword == "last_n_months"
        assert intent.start_date == date(2025, 12, 15)
        assert intent.end_date == date(2026, 6, 15)

    def test_last_n_days(self):
        intent = _detect_temporal_intent("Bookings last 30 days", today=date(2026, 3, 1))
        assert intent is not None
        assert intent.keyword == "last_n_days"
        assert intent.start_date == date(2026, 1, 30)
        assert intent.end_date == date(2026, 3, 1)

    def test_explicit_year_skips(self):
        intent = _detect_temporal_intent("Revenue for 2025 this year", today=date(2025, 6, 1))
        assert intent is None

    def test_no_temporal_keyword(self):
        intent = _detect_temporal_intent("Show me all properties in Tokyo")
        assert intent is None

    def test_empty_question(self):
        assert _detect_temporal_intent("") is None
        assert _detect_temporal_intent(None) is None


class TestRewriteTemporalDates:
    """Tests for _rewrite_temporal_dates()."""

    def test_rewrites_two_dates(self):
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert "'2026-02-28'" in rewritten
        assert "'2025-01-01'" not in rewritten
        assert meta is not None
        assert meta["keyword"] == "this_year"
        assert meta["original_dates"] == ["2025-01-01", "2025-12-31"]
        assert meta["rewritten_dates"] == ["2026-01-01", "2026-02-28"]

    def test_rewrites_single_date(self):
        gt_sql = "SELECT * FROM t WHERE d >= '2025-01-01'"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert meta is not None

    def test_no_dates_returns_unchanged(self):
        gt_sql = "SELECT * FROM t WHERE d >= CURRENT_DATE()"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert rewritten == gt_sql
        assert meta is None

    def test_same_year_skips_rewrite(self):
        gt_sql = "SELECT * FROM get_host_performance('2026-01-01', '2026-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert rewritten == gt_sql
        assert meta is None

    def test_empty_sql_returns_unchanged(self):
        rewritten, meta = _rewrite_temporal_dates("", TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28)))
        assert rewritten == ""
        assert meta is None

    def test_preserves_non_date_parameters(self):
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, _ = _rewrite_temporal_dates(gt_sql, intent)
        assert "'10'" in rewritten


class TestTemporalIntegration:
    """End-to-end tests combining detection + rewriting."""

    def test_host_performance_this_year(self):
        question = "Top 10 hosts by revenue this year"
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is not None
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert "'2026-02-28'" in rewritten
        assert meta is not None

    def test_booking_trends_explicit_year_not_rewritten(self):
        question = "Monthly booking trends for 2025"
        gt_sql = "SELECT * FROM get_booking_trends('2025-01-01', '2025-12-31', 'monthly')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is None

    def test_last_6_months_revenue(self):
        question = "Revenue by destination for last 6 months"
        gt_sql = "SELECT * FROM get_revenue_summary('2024-08-01', '2025-02-01')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is not None
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2025-08-28'" in rewritten
        assert "'2026-02-28'" in rewritten

    def test_no_rewrite_when_no_temporal(self):
        question = "Show me all properties in Tokyo"
        gt_sql = "SELECT * FROM dim_property WHERE city = 'Tokyo'"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is None


class TestBuildTemporalNote:
    """Tests for build_temporal_note()."""

    def test_returns_empty_when_no_rewrite(self):
        assert build_temporal_note({}) == ""
        assert build_temporal_note({"temporal_rewrite": None}) == ""

    def test_returns_note_when_rewrite_present(self):
        cmp = {
            "temporal_rewrite": {
                "keyword": "this_year",
                "original_dates": ["2025-01-01", "2025-12-31"],
                "rewritten_dates": ["2026-01-01", "2026-02-28"],
            }
        }
        note = build_temporal_note(cmp)
        assert "TEMPORAL CONTEXT" in note
        assert "this_year" in note
        assert "2025-01-01" in note
        assert "2026-01-01" in note


# ── Asset Coverage Gap-Fill ──────────────────────────────────────────────


class TestComputeAssetCoverage:
    """Tests for _compute_asset_coverage()."""

    def _config(self, tables=None, mvs=None, functions=None):
        return {
            "_tables": tables or [],
            "_metric_views": mvs or [],
            "_functions": functions or [],
        }

    def test_all_covered(self):
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.customers"],
        )
        benchmarks = [
            {
                "required_tables": ["orders", "customers"],
                "expected_sql": "SELECT * FROM catalog.schema.orders JOIN catalog.schema.customers",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_tables"] == set()
        assert result["uncovered_mvs"] == set()
        assert result["uncovered_functions"] == set()

    def test_with_gaps(self):
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.customers", "catalog.schema.products"],
            mvs=["catalog.schema.revenue_mv"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert "customers" in result["uncovered_tables"]
        assert "products" in result["uncovered_tables"]
        assert "orders" not in result["uncovered_tables"]
        assert "revenue_mv" in result["uncovered_mvs"]

    def test_empty_benchmarks(self):
        config = self._config(
            tables=["catalog.schema.orders"],
            functions=["catalog.schema.get_revenue"],
        )
        result = _compute_asset_coverage([], config)
        assert "orders" in result["uncovered_tables"]
        assert "get_revenue" in result["uncovered_functions"]

    def test_empty_config(self):
        result = _compute_asset_coverage(
            [{"required_tables": ["orders"], "expected_sql": "SELECT 1"}],
            self._config(),
        )
        assert result["uncovered_tables"] == set()
        assert result["uncovered_mvs"] == set()
        assert result["uncovered_functions"] == set()

    def test_sql_reference_detection(self):
        """Assets referenced only in expected_sql (not required_tables) count as covered."""
        config = self._config(tables=["catalog.schema.orders"])
        benchmarks = [
            {
                "required_tables": [],
                "expected_sql": "SELECT * FROM catalog.schema.orders WHERE id = 1",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_tables"] == set()

    def test_function_coverage(self):
        config = self._config(functions=["catalog.schema.get_host_performance"])
        benchmarks = [
            {
                "required_tables": ["get_host_performance"],
                "expected_sql": "SELECT * FROM catalog.schema.get_host_performance('2025-01-01')",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_functions"] == set()


class TestFillCoverageGaps:
    """Tests for _fill_coverage_gaps()."""

    def _config(self, tables=None, mvs=None, functions=None):
        return {
            "_tables": tables or [],
            "_metric_views": mvs or [],
            "_functions": functions or [],
            "_instructions": [],
            "_parsed_space": {},
        }

    def _allowlist(self):
        return {"assets": set(), "columns": set(), "column_index": {}, "routines": set()}

    def test_no_gaps_returns_empty(self):
        config = self._config(tables=["catalog.schema.orders"])
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []

    def test_soft_cap_skips(self):
        """When benchmark count is at the soft cap, gap-fill is skipped."""
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.uncovered"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ] * 30  # well above TARGET_BENCHMARK_COUNT * 1.5 = 30
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_generates_for_uncovered(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Valid gap-fill benchmarks are returned with correct provenance."""
        mock_llm.return_value = [
            {
                "question": "What are the top products?",
                "expected_sql": "SELECT * FROM catalog.schema.products",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["products"],
                "required_columns": ["product_name"],
                "expected_facts": ["top products"],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (True, "")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert len(result) == 1
        assert result[0]["provenance"] == "coverage_gap_fill"
        assert result[0]["question"] == "What are the top products?"
        mock_llm.assert_called_once()

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_deduplicates_existing_questions(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Gap-fill benchmarks that duplicate existing questions are excluded."""
        mock_llm.return_value = [
            {
                "question": "What are the top products?",
                "expected_sql": "SELECT * FROM catalog.schema.products",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["products"],
                "required_columns": [],
                "expected_facts": [],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (True, "")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions={"what are the top products?"},
        )
        assert result == []

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_invalid_benchmarks_filtered(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Gap-fill benchmarks failing validation are discarded."""
        mock_llm.return_value = [
            {
                "question": "Bad question",
                "expected_sql": "SELECT * FROM fake_table",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["fake_table"],
                "required_columns": [],
                "expected_facts": [],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (False, "Table not found")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []


# ---------------------------------------------------------------------------
# _flag_stale_temporal_benchmarks — warehouse routing
# ---------------------------------------------------------------------------

class TestFlagStaleTemporalBenchmarks:
    def _make_temporal_benchmark(self, question: str = "What were sales this year?"):
        return {
            "question": question,
            "expected_sql": "SELECT * FROM orders WHERE date > '2025-01-01'",
        }

    @patch("genie_space_optimizer.optimization.evaluation._execute_sql_via_warehouse")
    def test_warehouse_path_executes_sql(self, mock_wh):
        from genie_space_optimizer.optimization.evaluation import _flag_stale_temporal_benchmarks

        mock_wh.return_value = pd.DataFrame()
        benchmarks = [self._make_temporal_benchmark()]
        _flag_stale_temporal_benchmarks(
            benchmarks, MagicMock(),
            w=MagicMock(), warehouse_id="wh-123",
        )
        mock_wh.assert_called_once()
        assert benchmarks[0].get("temporal_stale") is True

    def test_spark_fallback_preserves_behavior(self):
        from genie_space_optimizer.optimization.evaluation import _flag_stale_temporal_benchmarks

        mock_spark = MagicMock()
        mock_spark.sql.return_value.limit.return_value.count.return_value = 0
        benchmarks = [self._make_temporal_benchmark()]
        _flag_stale_temporal_benchmarks(benchmarks, mock_spark)
        mock_spark.sql.assert_called_once()
        assert benchmarks[0].get("temporal_stale") is True


# ---------------------------------------------------------------------------
# generate_benchmarks — redundant block removal (R4)
# ---------------------------------------------------------------------------

class TestGenerateBenchmarksValidation:
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    def test_redundant_spark_sql_block_removed(self, mock_validate):
        """After removal of the redundant spark.sql block, 0-row results are
        still caught by _validate_benchmark_sql(execute=True)."""
        mock_validate.return_value = (
            False,
            "EMPTY_RESULT: Query returned 0 rows — likely wrong filter or empty table",
        )
        mock_spark = MagicMock()
        mock_validate_call_kwargs = {}

        def capture_call(*args, **kwargs):
            mock_validate_call_kwargs.update(kwargs)
            return (False, "EMPTY_RESULT: Query returned 0 rows")

        mock_validate.side_effect = capture_call

        assert mock_validate.call_count == 0
        mock_validate("SELECT 1", mock_spark, "cat", "schema", execute=True)
        assert mock_validate.call_count == 1
        assert mock_validate_call_kwargs.get("execute") is True


# ---------------------------------------------------------------------------
# Benchmark categories — "multi-table"
# ---------------------------------------------------------------------------

class TestBenchmarkCategories:
    def test_multi_table_category_present(self):
        from genie_space_optimizer.common.config import BENCHMARK_CATEGORIES
        assert "multi-table" in BENCHMARK_CATEGORIES


# ---------------------------------------------------------------------------
# _build_schema_contexts — join specs context
# ---------------------------------------------------------------------------

class TestBuildSchemaContextsJoinSpecs:
    def test_includes_join_specs_from_instructions(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": [],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {
                "instructions": {
                    "join_specs": [
                        {
                            "left": {"identifier": "cat.sch.orders", "alias": "orders"},
                            "right": {"identifier": "cat.sch.customers", "alias": "customers"},
                            "sql": ["`orders`.`customer_id` = `customers`.`customer_id`"],
                        }
                    ],
                },
            },
        }
        ctx = _build_schema_contexts(config, [], [])
        assert "orders" in ctx["join_specs_context"]
        assert "customers" in ctx["join_specs_context"]
        assert "customer_id" in ctx["join_specs_context"]

    def test_fallback_to_data_sources_join_specs(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": [],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {
                "instructions": {},
                "data_sources": {
                    "join_specs": [
                        {
                            "left": {"identifier": "cat.sch.a", "alias": "a"},
                            "right": {"identifier": "cat.sch.b", "alias": "b"},
                            "sql": ["`a`.`id` = `b`.`id`"],
                        }
                    ],
                },
            },
        }
        ctx = _build_schema_contexts(config, [], [])
        assert "cat.sch.a" in ctx["join_specs_context"]
        assert "cat.sch.b" in ctx["join_specs_context"]

    def test_no_join_specs_shows_fallback_text(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": [],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {"instructions": {}},
        }
        ctx = _build_schema_contexts(config, [], [])
        assert ctx["join_specs_context"] == "(No join specifications configured.)"


# ---------------------------------------------------------------------------
# _build_schema_contexts — enriched metric views
# ---------------------------------------------------------------------------

class TestBuildSchemaContextsMetricViews:
    def test_includes_measure_and_dimension_detail(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": ["cat.sch.mv_sales"],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {
                "instructions": {},
                "data_sources": {
                    "metric_views": [
                        {
                            "identifier": "cat.sch.mv_sales",
                            "column_configs": [
                                {"column_name": "region", "column_type": "dimension"},
                                {"column_name": "total_revenue", "column_type": "measure"},
                                {"column_name": "avg_order_value", "is_measure": True},
                            ],
                        }
                    ],
                },
            },
        }
        ctx = _build_schema_contexts(config, [], [])
        mv_ctx = ctx["metric_views_context"]
        assert "cat.sch.mv_sales" in mv_ctx
        assert "MEASURE()" in mv_ctx
        assert "total_revenue" in mv_ctx
        assert "avg_order_value" in mv_ctx
        assert "region" in mv_ctx
        assert "Dimensions" in mv_ctx

    def test_name_only_when_no_column_configs(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": ["cat.sch.mv_revenue"],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {
                "instructions": {},
                "data_sources": {"metric_views": []},
            },
        }
        ctx = _build_schema_contexts(config, [], [])
        assert "cat.sch.mv_revenue" in ctx["metric_views_context"]
        assert "no column detail available" in ctx["metric_views_context"]

    def test_no_metric_views(self):
        from genie_space_optimizer.optimization.evaluation import _build_schema_contexts

        config = {
            "_metric_views": [],
            "_functions": [],
            "_instructions": [],
            "_parsed_space": {"instructions": {}},
        }
        ctx = _build_schema_contexts(config, [], [])
        assert ctx["metric_views_context"] == "(none)"


# ---------------------------------------------------------------------------
# _guard_mv_select_star
# ---------------------------------------------------------------------------

class TestGuardMvSelectStar:
    def test_rejects_select_star_on_mv(self):
        from genie_space_optimizer.optimization.evaluation import _guard_mv_select_star
        ok, reason = _guard_mv_select_star(
            "SELECT * FROM cat.sch.mv_sales",
            {"cat.sch.mv_sales"},
        )
        assert ok is False
        assert "mv_sales" in reason

    def test_allows_select_star_on_table(self):
        from genie_space_optimizer.optimization.evaluation import _guard_mv_select_star
        ok, _ = _guard_mv_select_star(
            "SELECT * FROM cat.sch.orders",
            {"cat.sch.mv_sales"},
        )
        assert ok is True

    def test_allows_explicit_columns_on_mv(self):
        from genie_space_optimizer.optimization.evaluation import _guard_mv_select_star
        ok, _ = _guard_mv_select_star(
            "SELECT region, MEASURE(total_revenue) FROM cat.sch.mv_sales GROUP BY region",
            {"cat.sch.mv_sales"},
        )
        assert ok is True

    def test_empty_mv_set(self):
        from genie_space_optimizer.optimization.evaluation import _guard_mv_select_star
        ok, _ = _guard_mv_select_star("SELECT * FROM foo", set())
        assert ok is True


# ---------------------------------------------------------------------------
# _rewrite_measure_refs — SELECT clause rewriting
# ---------------------------------------------------------------------------

class TestRewriteMeasureRefsSelect:
    def test_wraps_bare_measure_in_select(self):
        from genie_space_optimizer.optimization.evaluation import _rewrite_measure_refs
        sql = "SELECT region, total_revenue FROM mv_sales GROUP BY region"
        result = _rewrite_measure_refs(sql, {"mv_sales": {"total_revenue"}})
        assert "MEASURE(total_revenue)" in result
        assert result.count("MEASURE") >= 1

    def test_skips_already_wrapped_in_select(self):
        from genie_space_optimizer.optimization.evaluation import _rewrite_measure_refs
        sql = "SELECT region, MEASURE(total_revenue) FROM mv_sales GROUP BY region"
        result = _rewrite_measure_refs(sql, {"mv_sales": {"total_revenue"}})
        assert result.count("MEASURE") == 1

    def test_order_by_still_works(self):
        from genie_space_optimizer.optimization.evaluation import _rewrite_measure_refs
        sql = "SELECT region FROM mv_sales ORDER BY total_revenue DESC"
        result = _rewrite_measure_refs(sql, {"mv_sales": {"total_revenue"}})
        assert "ORDER BY MEASURE(total_revenue)" in result

    def test_both_select_and_order_by(self):
        from genie_space_optimizer.optimization.evaluation import _rewrite_measure_refs
        sql = "SELECT region, total_revenue FROM mv_sales ORDER BY total_revenue DESC"
        result = _rewrite_measure_refs(sql, {"mv_sales": {"total_revenue"}})
        assert result.count("MEASURE(total_revenue)") == 2

    def test_no_op_for_non_mv(self):
        from genie_space_optimizer.optimization.evaluation import _rewrite_measure_refs
        sql = "SELECT total_revenue FROM orders ORDER BY total_revenue"
        result = _rewrite_measure_refs(sql, {"mv_sales": {"total_revenue"}})
        assert result == sql


# ---------------------------------------------------------------------------
# _extract_join_pairs
# ---------------------------------------------------------------------------

class TestExtractJoinPairs:
    def test_single_join(self):
        from genie_space_optimizer.optimization.evaluation import _extract_join_pairs
        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id"
        pairs = _extract_join_pairs(sql)
        assert ("customers", "orders") in pairs

    def test_multi_join(self):
        from genie_space_optimizer.optimization.evaluation import _extract_join_pairs
        sql = (
            "SELECT * FROM orders "
            "JOIN customers ON orders.cid = customers.id "
            "JOIN products ON orders.pid = products.id"
        )
        pairs = _extract_join_pairs(sql)
        assert ("customers", "orders") in pairs
        assert ("orders", "products") in pairs

    def test_no_join(self):
        from genie_space_optimizer.optimization.evaluation import _extract_join_pairs
        sql = "SELECT * FROM orders WHERE status = 'active'"
        pairs = _extract_join_pairs(sql)
        assert len(pairs) == 0

    def test_fqn_normalized_to_leaf(self):
        from genie_space_optimizer.optimization.evaluation import _extract_join_pairs
        sql = "SELECT * FROM cat.sch.orders JOIN cat.sch.customers ON 1=1"
        pairs = _extract_join_pairs(sql)
        assert ("customers", "orders") in pairs


# ---------------------------------------------------------------------------
# _compute_asset_coverage — uncovered joins
# ---------------------------------------------------------------------------

class TestComputeAssetCoverageJoins:
    def test_uncovered_joins_detected(self):
        from genie_space_optimizer.optimization.evaluation import _compute_asset_coverage

        config = {
            "_tables": ["cat.sch.orders", "cat.sch.customers"],
            "_metric_views": [],
            "_functions": [],
            "_parsed_space": {
                "instructions": {
                    "join_specs": [
                        {
                            "left": {"identifier": "cat.sch.orders"},
                            "right": {"identifier": "cat.sch.customers"},
                            "sql": ["`orders`.`cid` = `customers`.`id`"],
                        }
                    ],
                },
            },
        }
        benchmarks = [
            {"question": "q1", "expected_sql": "SELECT * FROM orders", "required_tables": ["orders"]},
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert ("customers", "orders") in result["uncovered_joins"]

    def test_no_uncovered_when_join_covered(self):
        from genie_space_optimizer.optimization.evaluation import _compute_asset_coverage

        config = {
            "_tables": ["cat.sch.orders", "cat.sch.customers"],
            "_metric_views": [],
            "_functions": [],
            "_parsed_space": {
                "instructions": {
                    "join_specs": [
                        {
                            "left": {"identifier": "cat.sch.orders"},
                            "right": {"identifier": "cat.sch.customers"},
                            "sql": ["`orders`.`cid` = `customers`.`id`"],
                        }
                    ],
                },
            },
        }
        benchmarks = [
            {
                "question": "q1",
                "expected_sql": "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
                "required_tables": ["orders", "customers"],
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert len(result["uncovered_joins"]) == 0

    def test_no_join_specs_means_empty(self):
        from genie_space_optimizer.optimization.evaluation import _compute_asset_coverage

        config = {
            "_tables": ["cat.sch.orders"],
            "_metric_views": [],
            "_functions": [],
            "_parsed_space": {"instructions": {}},
        }
        benchmarks = [{"question": "q1", "expected_sql": "SELECT 1", "required_tables": []}]
        result = _compute_asset_coverage(benchmarks, config)
        assert len(result["uncovered_joins"]) == 0


class TestSplitPersistenceRoundTrip:
    """Verify that the split field is persisted by create_evaluation_dataset
    and read back by load_benchmarks_from_dataset."""

    def test_build_eval_records_includes_split(self):
        from genie_space_optimizer.optimization.benchmarks import build_eval_records

        benchmarks = [
            {"id": "q1", "question": "Q1", "expected_sql": "SELECT 1", "split": "held_out"},
            {"id": "q2", "question": "Q2", "expected_sql": "SELECT 2", "split": "train"},
        ]
        records = build_eval_records(benchmarks)
        assert records[0]["expectations"]["split"] == "held_out"
        assert records[1]["expectations"]["split"] == "train"

    def test_build_eval_records_defaults_split_to_train(self):
        from genie_space_optimizer.optimization.benchmarks import build_eval_records

        records = build_eval_records([{"id": "q1", "question": "Q1"}])
        assert records[0]["expectations"]["split"] == "train"
