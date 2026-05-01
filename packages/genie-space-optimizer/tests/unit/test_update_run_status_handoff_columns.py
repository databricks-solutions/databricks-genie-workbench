"""update_run_status must accept and persist the 3 handoff columns."""
import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.state import update_run_status


def _capture_sql(spark_mock):
    """Return a list of SQL strings spark.sql was called with."""
    return [str(c.args[0]) for c in spark_mock.sql.call_args_list]


def test_update_run_status_accepts_warehouse_id():
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.optimization.state._lookup_run_space_id",
        return_value="space-abc",
    ):
        update_run_status(
            spark, "run-001", "cat", "sch", warehouse_id="wh-xyz",
        )
    sqls = _capture_sql(spark)
    assert any("warehouse_id" in s and "wh-xyz" in s for s in sqls), sqls


def test_update_run_status_accepts_human_corrections_list():
    spark = MagicMock()
    corrections = [{"qid": "q1", "fix": "use SUM"}]
    with patch(
        "genie_space_optimizer.optimization.state._lookup_run_space_id",
        return_value="space-abc",
    ):
        update_run_status(
            spark, "run-001", "cat", "sch", human_corrections=corrections,
        )
    sqls = _capture_sql(spark)
    persisted = next(
        s for s in sqls if "human_corrections_json" in s
    )
    # JSON must be embedded as a Spark SQL string literal
    assert json.dumps(corrections) in persisted


def test_update_run_status_accepts_max_benchmark_count():
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.optimization.state._lookup_run_space_id",
        return_value="space-abc",
    ):
        update_run_status(
            spark, "run-001", "cat", "sch", max_benchmark_count=42,
        )
    sqls = _capture_sql(spark)
    assert any(
        "max_benchmark_count" in s and "42" in s for s in sqls
    ), sqls


def test_update_run_status_omits_unset_handoff_fields():
    """If the caller doesn't pass any handoff arg, none must appear in SQL."""
    spark = MagicMock()
    with patch(
        "genie_space_optimizer.optimization.state._lookup_run_space_id",
        return_value="space-abc",
    ):
        update_run_status(spark, "run-001", "cat", "sch", status="IN_PROGRESS")
    sqls = " ".join(_capture_sql(spark))
    assert "warehouse_id" not in sqls
    assert "human_corrections_json" not in sqls
    assert "max_benchmark_count" not in sqls
