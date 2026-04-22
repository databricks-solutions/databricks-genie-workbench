"""Tests for the postflight IQ scan helper (PR 6).

The helper is soft-failing: every exception path should be caught so a failed
scan never blocks the harness's terminal status write that follows. Also
verifies the feature-flag gate shares GSO_ENABLE_IQ_SCAN_PREFLIGHT with the
preflight sub-step.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization import scan_snapshots


@pytest.fixture
def capturing_spark():
    spark = MagicMock()
    spark._captured_sql = []
    spark.sql.side_effect = lambda sql, *a, **kw: spark._captured_sql.append(sql) or MagicMock()
    return spark


def _sample_scan_result() -> dict:
    return {
        "score": 9,
        "total": 12,
        "maturity": "Ready to Optimize",
        "checks": [{"label": "Data sources exist", "passed": True, "severity": "pass"}],
        "findings": [],
        "warnings": [],
        "scanned_at": "2026-04-22T12:00:00+00:00",
    }


def test_flag_off_returns_false_and_does_not_fetch(monkeypatch, capturing_spark):
    """When the shared flag is unset, postflight is a pure no-op."""
    monkeypatch.delenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", raising=False)
    w = MagicMock()
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config"
    ) as fetch_mock, patch(
        "genie_space_optimizer.iq_scan.scoring.calculate_score"
    ) as score_mock:
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-1", "space-1", "cat", "sch",
            best_accuracy=82.0,
        )
    assert result is False
    assert fetch_mock.call_count == 0
    assert score_mock.call_count == 0
    assert capturing_spark._captured_sql == []


def test_flag_on_happy_path_persists_postflight_row(monkeypatch, capturing_spark):
    """With flag on, scan runs and a phase='postflight' row is persisted."""
    monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
    w = MagicMock()
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={"_parsed_space": {"tables": [{"name": "t1"}]}},
    ), patch(
        "genie_space_optimizer.iq_scan.scoring.calculate_score",
        return_value=_sample_scan_result(),
    ) as score_mock:
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-1", "space-1", "cat", "sch",
            best_accuracy=88.5,
        )
    assert result is True
    # MERGE statement was issued for phase='postflight'.
    merge_sqls = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
    assert len(merge_sqls) == 1
    assert "'postflight'" in merge_sqls[0]
    # best_accuracy was forwarded to calculate_score via optimization_run.
    assert score_mock.call_args.kwargs["optimization_run"] == {"accuracy": 88.5}


def test_fetch_space_config_failure_is_soft(monkeypatch, capturing_spark):
    """If fetching the space config raises, postflight returns False without re-raising."""
    monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
    w = MagicMock()
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=RuntimeError("Genie API down"),
    ):
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-2", "space-2", "cat", "sch",
        )
    assert result is False
    # No MERGE attempted because we never made it to the writer.
    assert not any("MERGE INTO" in s for s in capturing_spark._captured_sql)


def test_calculate_score_failure_is_soft(monkeypatch, capturing_spark):
    """If scoring raises, postflight returns False without re-raising."""
    monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
    w = MagicMock()
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={"_parsed_space": {}},
    ), patch(
        "genie_space_optimizer.iq_scan.scoring.calculate_score",
        side_effect=ValueError("bad config"),
    ):
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-3", "space-3", "cat", "sch",
        )
    assert result is False


def test_write_snapshot_failure_is_soft(monkeypatch, capturing_spark):
    """If MERGE fails, postflight returns False without re-raising."""
    monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
    w = MagicMock()

    def raise_on_merge(sql, *a, **kw):
        if "MERGE INTO" in sql:
            raise RuntimeError("Delta write failed")
        capturing_spark._captured_sql.append(sql)
        return MagicMock()

    capturing_spark.sql.side_effect = raise_on_merge
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={"_parsed_space": {}},
    ), patch(
        "genie_space_optimizer.iq_scan.scoring.calculate_score",
        return_value=_sample_scan_result(),
    ):
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-4", "space-4", "cat", "sch",
        )
    # write_scan_snapshot itself catches and returns False.
    assert result is False


def test_best_accuracy_optional(monkeypatch, capturing_spark):
    """Omitting best_accuracy should not crash; optimization_run is None."""
    monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", "true")
    w = MagicMock()
    with patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value={"_parsed_space": {}},
    ), patch(
        "genie_space_optimizer.iq_scan.scoring.calculate_score",
        return_value=_sample_scan_result(),
    ) as score_mock:
        result = scan_snapshots.run_postflight_scan(
            w, capturing_spark, "run-5", "space-5", "cat", "sch",
        )
    assert result is True
    assert score_mock.call_args.kwargs["optimization_run"] is None


def test_flag_various_truthy_values(monkeypatch, capturing_spark):
    """Flag parsing accepts the standard truthy shapes."""
    w = MagicMock()
    for truthy in ("1", "true", "YES", "On"):
        monkeypatch.setenv("GSO_ENABLE_IQ_SCAN_PREFLIGHT", truthy)
        with patch(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            return_value={"_parsed_space": {}},
        ), patch(
            "genie_space_optimizer.iq_scan.scoring.calculate_score",
            return_value=_sample_scan_result(),
        ):
            result = scan_snapshots.run_postflight_scan(
                w, capturing_spark, f"run-{truthy}", "space", "cat", "sch",
            )
        assert result is True, f"flag={truthy!r} should be truthy"
