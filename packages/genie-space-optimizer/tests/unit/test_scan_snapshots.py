"""Tests for the IQ scan snapshot writer.

Uses a spark mock that captures emitted SQL so we can assert idempotent MERGE
behavior without needing a live Delta environment.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.common.config import TABLE_SCAN_SNAPSHOTS
from genie_space_optimizer.optimization.scan_snapshots import (
    _ensure_scan_snapshot_table,
    write_scan_snapshot,
)


@pytest.fixture
def capturing_spark():
    """A MagicMock spark that records every SQL string passed to ``spark.sql``."""
    spark = MagicMock()
    spark._captured_sql = []
    spark.sql.side_effect = lambda sql, *a, **kw: spark._captured_sql.append(sql) or MagicMock()
    return spark


def _sample_scan_result() -> dict:
    return {
        "score": 7,
        "total": 12,
        "maturity": "Ready to Optimize",
        "checks": [{"label": "Data sources exist", "passed": True, "severity": "pass"}],
        "findings": ["Only 5 example SQL questions"],
        "warnings": ["Column descriptions at 60%"],
        "scanned_at": "2025-01-15T12:00:00+00:00",
    }


class TestEnsureTable:
    def test_emits_create_if_not_exists(self, capturing_spark):
        _ensure_scan_snapshot_table(capturing_spark, "main", "gso")
        sql = "\n".join(capturing_spark._captured_sql)
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert f"main.gso.{TABLE_SCAN_SNAPSHOTS}" in sql
        assert "USING DELTA" in sql
        assert "run_id" in sql and "phase" in sql

    def test_failure_is_swallowed(self):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("table already exists")
        _ensure_scan_snapshot_table(spark, "main", "gso")


class TestWriteScanSnapshot:
    def test_persists_one_row_for_new_key(self, capturing_spark):
        ok = write_scan_snapshot(
            capturing_spark, "run-1", "space-1", "preflight",
            _sample_scan_result(), "main", "gso",
        )
        assert ok is True
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert len(merges) == 1
        assert "t.run_id = s.run_id AND t.phase = s.phase" in merges[0]
        assert "'run-1'" in merges[0]
        assert "'preflight'" in merges[0]

    def test_same_run_and_phase_yields_single_merge_each_time(self, capturing_spark):
        """Calling write twice with the same (run_id, phase) still only emits MERGE.

        The MERGE-on-match-update-else-insert pattern guarantees idempotency at
        the SQL level; this test guards that the writer doesn't accidentally
        switch to INSERT.
        """
        for _ in range(2):
            write_scan_snapshot(
                capturing_spark, "run-1", "space-1", "preflight",
                _sample_scan_result(), "main", "gso",
            )
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        inserts = [s for s in capturing_spark._captured_sql if s.strip().startswith("INSERT")]
        assert len(merges) == 2
        assert inserts == []

    def test_different_phases_emit_distinct_merges(self, capturing_spark):
        write_scan_snapshot(
            capturing_spark, "run-1", "space-1", "preflight",
            _sample_scan_result(), "main", "gso",
        )
        write_scan_snapshot(
            capturing_spark, "run-1", "space-1", "postflight",
            _sample_scan_result(), "main", "gso",
        )
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert len(merges) == 2
        assert any("'preflight'" in m for m in merges)
        assert any("'postflight'" in m for m in merges)

    def test_checks_json_roundtrips(self, capturing_spark):
        scan = _sample_scan_result()
        write_scan_snapshot(
            capturing_spark, "run-1", "space-1", "preflight",
            scan, "main", "gso",
        )
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert merges
        merge_sql = merges[0]
        # Pull out the checks_json literal from the SQL and round-trip it.
        # The string form in SQL wraps the JSON in single quotes and doubles any
        # embedded quotes; undo that to get the original JSON.
        label = "Data sources exist"
        # Loose assertion: the checks json appears verbatim somewhere in the SQL.
        assert label in merge_sql
        # And the serialized form round-trips.
        serialized = json.dumps(scan["checks"])
        assert serialized.replace("'", "''") in merge_sql

    def test_escapes_sql_quotes_in_findings(self, capturing_spark):
        scan = _sample_scan_result()
        scan["findings"] = ["It's a problem"]
        write_scan_snapshot(
            capturing_spark, "run-1", "space-1", "preflight",
            scan, "main", "gso",
        )
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert merges
        # Single quote must be escaped (doubled) so the SQL is valid.
        assert "It''s a problem" in merges[0]

    def test_rejects_invalid_phase(self, capturing_spark):
        with pytest.raises(ValueError, match="phase"):
            write_scan_snapshot(
                capturing_spark, "run-1", "space-1", "unknown",
                _sample_scan_result(), "main", "gso",
            )

    def test_requires_run_id_and_space_id(self, capturing_spark):
        with pytest.raises(ValueError):
            write_scan_snapshot(
                capturing_spark, "", "space-1", "preflight",
                _sample_scan_result(), "main", "gso",
            )
        with pytest.raises(ValueError):
            write_scan_snapshot(
                capturing_spark, "run-1", "", "preflight",
                _sample_scan_result(), "main", "gso",
            )

    def test_returns_false_on_spark_failure(self):
        spark = MagicMock()
        spark.sql.side_effect = [MagicMock(), RuntimeError("write failed")]
        ok = write_scan_snapshot(
            spark, "run-1", "space-1", "preflight",
            _sample_scan_result(), "main", "gso",
        )
        assert ok is False

    def test_handles_missing_optional_fields(self, capturing_spark):
        minimal = {"score": 0, "total": 12, "maturity": "Not Ready"}
        ok = write_scan_snapshot(
            capturing_spark, "run-2", "space-2", "preflight",
            minimal, "main", "gso",
        )
        assert ok is True
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert merges
        # Empty list should serialize as [] not null.
        assert "'[]'" in merges[0]

    def test_handles_null_score(self, capturing_spark):
        ok = write_scan_snapshot(
            capturing_spark, "run-3", "space-1", "preflight",
            {"score": None, "total": None, "maturity": ""},
            "main", "gso",
        )
        assert ok is True
        merges = [s for s in capturing_spark._captured_sql if "MERGE INTO" in s]
        assert merges
        assert "NULL" in merges[0]
