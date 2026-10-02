"""Tests for the operator-proposed-join artifact handoff (Semantic Blueprint §7).

Seeded Join Advisor candidates ride into a run as ADVICE the optimize loop
validates and adds itself (``add_join_spec``) — never a declared ``join_spec``
written by the Workbench. The seeds travel through the generic
``genie_opt_artifacts`` table (kind=``operator_proposed_joins``), keyed by
run_id, so the optimize task reads durable Delta state rather than a new job
parameter. These pin the write shape and the fail-open read.
"""

from __future__ import annotations

import base64
import json

import pandas as pd
import pytest

from genie_space_optimizer.common import warehouse


class _FakeWorkspaceClient:
    pass


_SEEDS = [
    {
        "id": "jc:orders.customer_id->customer.customer_id",
        "from": "c.s.orders",
        "fromCol": "customer_id",
        "to": "c.s.customer",
        "toCol": "customer_id",
        "rel": "N:1",
        "match": "name-type",
        "probe": 0.97,
        "note": None,
    }
]


@pytest.fixture
def executed(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: statements.append(sql),
    )
    return statements


def test_write_targets_the_artifacts_table_with_the_join_advice_kind(executed):
    warehouse.wh_write_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer", seeds=_SEEDS,
    )
    assert len(executed) == 1
    sql = executed[0]
    assert sql.startswith("INSERT INTO main.genie_space_optimizer.genie_opt_artifacts")
    assert warehouse.JOIN_ADVICE_ARTIFACT_KIND == "operator_proposed_joins"
    assert "'operator_proposed_joins'" in sql
    assert "'run-1'" in sql
    # The seed payload is base64-routed via unbase64 so nested JSON survives the
    # warehouse's string-literal escape mode.
    encoded = base64.b64encode(json.dumps({"seeds": _SEEDS}).encode("utf-8")).decode("ascii")
    assert f"unbase64('{encoded}')" in sql


def test_write_is_a_noop_for_empty_seeds(executed):
    warehouse.wh_write_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer", seeds=[],
    )
    assert executed == []


def test_read_roundtrips_the_seed_payload(monkeypatch):
    df = pd.DataFrame([{"artifact_json": json.dumps({"seeds": _SEEDS})}])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: df)
    out = warehouse.wh_read_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == _SEEDS


def test_read_is_empty_when_no_artifact_row(monkeypatch):
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: pd.DataFrame())
    out = warehouse.wh_read_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == []


def test_read_fails_open_on_query_error(monkeypatch):
    def boom(ws, wid, sql):
        raise RuntimeError("warehouse down")

    monkeypatch.setattr(warehouse, "sql_warehouse_query", boom)
    out = warehouse.wh_read_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == []


def test_read_tolerates_malformed_payload(monkeypatch):
    df = pd.DataFrame([{"artifact_json": "not json {["}])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", lambda ws, wid, sql: df)
    out = warehouse.wh_read_join_advice(
        _FakeWorkspaceClient(), "wh1",
        run_id="run-1", catalog="main", schema="genie_space_optimizer",
    )
    assert out == []
