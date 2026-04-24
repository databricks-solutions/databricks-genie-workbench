"""F10 — gate the MEASURE() hint on true measure errors only.

The original implementation in ``benchmarks.validate_ground_truth_sql``
unconditionally appended

    (hint: use MEASURE({col_name}) for metric view measures in ORDER BY)

to every ``UNRESOLVED_COLUMN`` Spark error. In the field log this
produced misleading feedback: when the LLM emitted a bare table stem
(``FROM dim_date`` instead of ``FROM cat.sch.mv_esr_dim_date``),
Spark surfaced an ``UNRESOLVED_COLUMN: dim_date`` error, and the
hint then told the correction LLM to wrap ``dim_date`` in
``MEASURE()`` — when the actual fix was to qualify the table name.

The gate now fires only when there is evidence the unresolved
identifier is actually a metric-view measure:
  1. Spark reports ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``, OR
  2. The unresolved column matches a known measure name in any
     metric view declared in ``config``.

These tests patch out the Spark EXPLAIN call site so we exercise
only the error-classification branch — no live warehouse needed.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.benchmarks import (
    validate_ground_truth_sql,
)


def _mv_config(measures: list[str]) -> dict:
    """Minimal config with a single metric view declaring ``measures``.

    Shape mirrors ``config["_parsed_space"]["data_sources"]["metric_views"]``
    that the production code walks. Kept intentionally small to keep the
    test surface focused on the classifier branch.
    """
    return {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [
                    {
                        "identifier": "cat.sch.mv_esr_sales",
                        "measures": [{"name": n} for n in measures],
                    },
                ],
            },
        },
    }


def _raise_spark_error(message: str):
    """Return a callable that raises ``RuntimeError(message)`` on call.

    ``spark.sql`` and ``_execute_sql_via_warehouse`` are the two call
    sites ``validate_ground_truth_sql`` dispatches to for the EXPLAIN.
    We patch ``spark.sql`` since the test passes a mock spark without a
    warehouse.
    """
    def _call(*_args, **_kwargs):
        raise RuntimeError(message)
    return _call


class _FakeSpark:
    """Stand-in for a Spark session — only ``sql`` is exercised here.

    We attach the failure-raising callable in the test so each test
    can pick its own Spark error message.
    """
    def __init__(self, raise_on_sql):
        self._raise = raise_on_sql

    def sql(self, *a, **kw):
        self._raise()


@pytest.fixture(autouse=True)
def _patch_quiet_grpc_logs():
    """Silence the grpc context manager — it tries to reach into Spark
    internals which aren't needed for the classifier branch.
    """
    import contextlib as _ctx
    with patch(
        "genie_space_optimizer.optimization.benchmarks._quiet_grpc_logs",
        return_value=_ctx.nullcontext(),
    ):
        yield


@pytest.fixture(autouse=True)
def _skip_set_sql_context():
    """Bypass ``_set_sql_context`` so ``spark.sql`` is the only
    interaction with the fake spark.
    """
    with patch(
        "genie_space_optimizer.optimization.benchmarks._set_sql_context",
        return_value=None,
    ):
        yield


class TestMeasureHintSuppressedOnTableMiss:
    """Bare-stem table references surface as UNRESOLVED_COLUMN but are
    NOT measure failures. The hint must be suppressed so the
    correction LLM doesn't chase the wrong fix.
    """

    def test_unresolved_table_stem_no_measure_hint(self):
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `dim_date` cannot be resolved. "
            "Did you mean one of the following? ["
            "cat.sch.mv_esr_dim_date.day_of_week, "
            "cat.sch.mv_esr_dim_date.month]"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT day_of_week FROM dim_date",
            spark=fake, catalog="cat", gold_schema="sch",
            config=_mv_config(measures=["sales_total", "sales_count"]),
        )
        assert is_valid is False
        assert "UNRESOLVED_COLUMN" in err
        assert "MEASURE(dim_date)" not in err
        assert "hint: use MEASURE" not in err

    def test_unresolved_column_on_base_table_no_measure_hint(self):
        """Column that isn't a measure and isn't a metric-view table
        reference either — most common UNRESOLVED_COLUMN case in the
        field log. Hint must stay off.
        """
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `prodcut_id` cannot be resolved. "
            "Did you mean one of the following? [product_id, prod_id]"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT prodcut_id FROM sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=_mv_config(measures=["sales_total"]),
        )
        assert is_valid is False
        assert "hint: use MEASURE" not in err


class TestMeasureHintSurfacedOnTrueMeasureError:
    """Real measure errors SHOULD still get the hint — the gate is
    opt-in, not a blanket removal.
    """

    def test_metric_view_missing_measure_function_marker_surfaces_hint(self):
        """Spark's explicit marker — the canonical positive signal."""
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `sales_total` cannot be resolved. "
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] The usage of measure "
            "column [sales_total] must be wrapped in MEASURE(). "
            "Did you mean one of the following? [cat.sch.mv_esr_sales.region]"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT sales_total FROM cat.sch.mv_esr_sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=_mv_config(measures=["sales_total"]),
        )
        assert is_valid is False
        assert "hint: use MEASURE(sales_total)" in err

    def test_measure_name_in_config_surfaces_hint_even_without_marker(self):
        """Spark sometimes collapses the failure into a plain
        UNRESOLVED_COLUMN without the ``METRIC_VIEW_MISSING_MEASURE_FUNCTION``
        marker. Falling back to the config's measure catalog still
        catches the real measure-wrap case so we don't lose the hint
        in its legitimate use.
        """
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `sales_total` cannot be resolved. "
            "Did you mean one of the following? [cat.sch.mv_esr_sales.region]"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT sales_total FROM cat.sch.mv_esr_sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=_mv_config(measures=["sales_total", "sales_count"]),
        )
        assert is_valid is False
        assert "hint: use MEASURE(sales_total)" in err

    def test_measure_match_is_case_insensitive(self):
        """Guard against a future regression where config names are
        stored lowercased but Spark echoes the original case.
        """
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `Sales_Total` cannot be resolved. "
            "Did you mean one of the following? [region]"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT Sales_Total FROM cat.sch.mv_esr_sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=_mv_config(measures=["sales_total"]),
        )
        assert is_valid is False
        assert "hint: use MEASURE(Sales_Total)" in err


class TestMeasureHintWithMissingConfig:
    """Edge case: ``config`` can be ``None`` (preflight / benchmark
    paths that haven't plumbed it). The function must fall back
    cleanly to the marker-only check without crashing.
    """

    def test_no_config_no_marker_no_hint(self):
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `foo` cannot be resolved."
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT foo FROM sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=None,
        )
        assert is_valid is False
        assert "hint: use MEASURE" not in err

    def test_no_config_with_marker_still_surfaces_hint(self):
        """Marker wins even without config — don't regress on the
        explicit signal.
        """
        spark_err = (
            "[UNRESOLVED_COLUMN] A column, variable, or function "
            "parameter with name `rev` cannot be resolved. "
            "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] wrap in MEASURE()"
        )
        fake = _FakeSpark(_raise_spark_error(spark_err))
        is_valid, err = validate_ground_truth_sql(
            "SELECT rev FROM cat.sch.mv_esr_sales",
            spark=fake, catalog="cat", gold_schema="sch",
            config=None,
        )
        assert is_valid is False
        assert "hint: use MEASURE(rev)" in err
