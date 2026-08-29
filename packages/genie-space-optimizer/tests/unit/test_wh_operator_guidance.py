"""Tests for the operator free-text guidance artifact handoff (Semantic Blueprint §7).

Free-text guidance the human typed in the run-config panel rides into a run as
ADVICE the optimize loop injects into the LLM prompt — never a config edit and
never persisted per-space. It travels through the generic ``genie_opt_artifacts``
table (kind=``operator_guidance``), keyed by run_id, exactly like the join seeds.
These pin the write shape and the fail-open read.
"""

from __future__ import annotations

import base64
import json

import pandas as pd
import pytest

from genie_space_optimizer.common import warehouse


class _FakeWorkspaceClient:
    pass


_GUIDANCE = "Prefer orders_v2; revenue is net of refunds."


@pytest.fixture
def executed(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: statements.append(sql),
    )
    return statements


def test_write_targets_the_artifacts_table_with_the_guidance_kind(executed):
    warehouse.wh_write_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer", text=_GUIDANCE,
    )
    assert len(executed) == 1
    sql = executed[0]
    assert sql.startswith("INSERT INTO main.genie_space_optimizer.genie_opt_artifacts")
    assert warehouse.OPERATOR_GUIDANCE_ARTIFACT_KIND == "operator_guidance"
    assert "'operator_guidance'" in sql
    assert "'run-1'" in sql
    # The text payload is base64-routed via unbase64 so free-text survives the
    # warehouse's string-literal escape mode.
    encoded = base64.b64encode(json.dumps({"text": _GUIDANCE}).encode("utf-8")).decode("ascii")
    assert f"unbase64('{encoded}')" in sql


def test_write_is_a_noop_for_blank_text(executed):
    warehouse.wh_write_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer", text="   ",
    )
    warehouse.wh_write_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer", text=None,
    )
    assert executed == []


def test_read_roundtrips_the_guidance_payload(monkeypatch):
    df = pd.DataFrame([{"artifact_json": json.dumps({"text": _GUIDANCE})}])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: df)
    out = warehouse.wh_read_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == _GUIDANCE


def test_read_is_empty_when_no_artifact_row(monkeypatch):
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: pd.DataFrame())
    out = warehouse.wh_read_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == ""


def test_read_fails_open_on_query_error(monkeypatch):
    def boom(ws, wid, sql):
        raise RuntimeError("warehouse down")

    monkeypatch.setattr(warehouse, "sql_warehouse_query", boom)
    out = warehouse.wh_read_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == ""


def test_read_tolerates_malformed_payload(monkeypatch):
    df = pd.DataFrame([{"artifact_json": "not json {["}])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: df)
    out = warehouse.wh_read_operator_guidance(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == ""
