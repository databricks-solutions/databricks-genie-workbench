"""Tests for the SQL-warehouse stage writer + advice-scan reader (MV-D31).

The interactive advice path has no SparkSession, so it cannot call the Spark
``state.write_stage``. ``wh_write_stage`` is its twin (the MV-D21 pin: the two
writers must agree on table, columns, and the terminal-duration rule), and
``wh_load_latest_advice_scan`` is the hydrate-on-mount reader that derives the
panel's "last scanned … — N proposals" summary from the advice run's terminal
stage row — a derivation from existing state (``detail_json``), never a new
``genie_opt_runs`` column.
"""

from __future__ import annotations

import base64
import json

import pandas as pd
import pytest

from genie_space_optimizer.common import warehouse
from genie_space_optimizer.optimization.ddl import _GENIE_OPT_STAGES_DDL


class _FakeWorkspaceClient:
    pass


@pytest.fixture
def executed(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: statements.append(sql),
    )
    return statements


def _write(executed, **overrides):
    kwargs = {
        "run_id": "run-adv-1",
        "stage": "mv_advisor",
        "status": "STARTED",
        "catalog": "main",
        "schema": "genie_space_optimizer",
    }
    kwargs.update(overrides)
    warehouse.wh_write_stage(_FakeWorkspaceClient(), "wh1", **kwargs)
    return executed[-1]


# ── wh_write_stage: the MV-D21 twin ─────────────────────────────────────────


def test_started_row_has_no_completed_at_or_duration(executed):
    sql = _write(executed, status="STARTED")
    assert sql.startswith(
        "INSERT INTO main.genie_space_optimizer.genie_opt_stages"
    )
    # A STARTED row opens the interval: started now, no end, no duration yet.
    assert "current_timestamp()" in sql
    # The VALUES carry NULL for completed_at and duration_seconds (no diff yet).
    values = sql.split("VALUES", 1)[1]
    assert "NULL" in values


def test_terminal_row_diffs_the_started_row_for_duration(executed):
    sql = _write(executed, status="COMPLETE", detail={"status": "COMPLETE"})
    # Duration is computed in-engine against THIS stage's STARTED row, so both
    # endpoints use the warehouse clock (the Spark writer's rule, twinned).
    assert "unix_timestamp(current_timestamp())" in sql
    assert "status = 'STARTED'" in sql
    assert "WHERE run_id = 'run-adv-1' AND stage = 'mv_advisor'" in sql


def test_writes_only_columns_the_table_declares(executed):
    sql = _write(executed, status="COMPLETE", detail={"status": "COMPLETE"})
    written = {
        column for column in (
            "run_id", "stage", "status", "started_at", "completed_at",
            "duration_seconds", "detail_json", "error_message",
        )
        if column in sql
    }
    # Every column the twin writes must exist in the DDL — no drift, no
    # job-only columns (task_key / lever / iteration) the advice path never sets.
    assert len(written) == 8
    for column in written:
        assert column in _GENIE_OPT_STAGES_DDL


def test_detail_json_is_base64_routed_so_nested_json_survives(executed):
    detail = {"status": "SKIPPED", "skip_reason": "NO_CANDIDATES", "measures_found": 4}
    sql = _write(executed, status="SKIPPED", detail=detail)
    # The nested JSON rides through unbase64, like the consent/candidate JSON
    # columns, so the warehouse's string-escape mode cannot mangle it.
    encoded = base64.b64encode(json.dumps(detail).encode("utf-8")).decode("ascii")
    assert f"unbase64('{encoded}')" in sql


def test_empty_run_id_or_stage_is_rejected(executed):
    with pytest.raises(ValueError):
        _write(executed, run_id="")
    with pytest.raises(ValueError):
        _write(executed, stage="")


# ── wh_load_latest_advice_scan: the hydration reader ────────────────────────


def _stub_query(monkeypatch, df: pd.DataFrame):
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query", lambda ws, warehouse_id, sql: df
    )


def test_scan_summary_derives_skip_and_measures_from_detail_json(monkeypatch):
    detail = {"status": "SKIPPED", "skip_reason": "NO_CANDIDATES", "measures_found": 4}
    df = pd.DataFrame([
        {
            "run_id": "run-adv-9",
            "scanned_at": "2026-08-25T05:00:00Z",
            "status": "SKIPPED",
            "duration_seconds": 252.0,
            "detail_json": json.dumps(detail),
        }
    ])
    _stub_query(monkeypatch, df)

    out = warehouse.wh_load_latest_advice_scan(
        _FakeWorkspaceClient(), "wh1",
        catalog="main", schema="genie_space_optimizer", space_id="space-1",
    )
    assert out is not None
    assert out["run_id"] == "run-adv-9"
    assert out["status"] == "SKIPPED"
    assert out["duration_seconds"] == 252.0
    # The note-2 source: skip_reason / measures_found are DERIVED from the stage
    # detail, not read from a new genie_opt_runs column.
    assert out["skip_reason"] == "NO_CANDIDATES"
    assert out["measures_found"] == 4


def test_never_scanned_space_is_none(monkeypatch):
    _stub_query(monkeypatch, pd.DataFrame())
    out = warehouse.wh_load_latest_advice_scan(
        _FakeWorkspaceClient(), "wh1",
        catalog="main", schema="genie_space_optimizer", space_id="space-1",
    )
    # The panel renders never-scanned as a first-class Scan affordance, not an
    # empty result — so the reader must say "None", not an empty summary.
    assert out is None


def test_scan_summary_tolerates_a_missing_stage_row(monkeypatch):
    """The LEFT JOIN can return the run with a NULL stage half (a run that
    crashed before its terminal row). The reader must not blow up on the missing
    detail_json — it returns what it has."""
    df = pd.DataFrame([
        {
            "run_id": "run-adv-10",
            "scanned_at": "2026-08-25T06:00:00Z",
            "status": None,
            "duration_seconds": None,
            "detail_json": None,
        }
    ])
    _stub_query(monkeypatch, df)
    out = warehouse.wh_load_latest_advice_scan(
        _FakeWorkspaceClient(), "wh1",
        catalog="main", schema="genie_space_optimizer", space_id="space-1",
    )
    assert out is not None
    assert out["run_id"] == "run-adv-10"
    assert out["skip_reason"] is None
    assert out["measures_found"] is None
    assert out["duration_seconds"] is None
