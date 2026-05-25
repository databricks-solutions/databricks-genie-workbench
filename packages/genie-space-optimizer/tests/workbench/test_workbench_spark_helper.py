"""Workbench V1.5 — Spark session helper for live-databricks mode.

The workbench needs a real ``SparkSession`` only when constructing
``make_predict_fn`` + ``make_all_scorers``. In sm-tape and stage1-only
modes the helper is never called. Failing fast with an actionable
error when Databricks Connect is not installed is preferable to a
late ``AttributeError`` deep inside ``make_predict_fn``.
"""
from __future__ import annotations

import sys

import pytest

from local_lever_workbench.local_runner import _build_workbench_spark


@pytest.mark.workbench
def test_build_workbench_spark_returns_none_when_not_required() -> None:
    """Caller can opt out of Spark when not needed (sm-tape, stage1-only)."""
    result = _build_workbench_spark(profile=None, profile_required=False)
    assert result is None


@pytest.mark.workbench
def test_build_workbench_spark_raises_when_dbconnect_missing(monkeypatch) -> None:
    """A missing databricks-connect install must surface as a clear,
    actionable RuntimeError, not an opaque ImportError mid-eval."""
    # Force the import to fail by shadowing databricks.connect in sys.modules
    # so the import path resolves as unavailable.
    monkeypatch.setitem(sys.modules, "databricks.connect", None)
    with pytest.raises(RuntimeError, match="databricks-connect"):
        _build_workbench_spark(profile="fevm-prashanth", profile_required=True)
