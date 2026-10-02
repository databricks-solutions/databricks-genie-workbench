"""Tests for the SQL-warehouse metric view consent accessors (MV-D7).

These exist because the FastAPI backend records consent and has no
SparkSession, so ``mv_state.upsert_mv_consent`` cannot serve that path. What
matters here is that the warehouse twin writes the *same* table with the same
key and the same column set — a drift between the two writers would show up as
a consent the job cannot re-verify.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from genie_space_optimizer.common import warehouse
from genie_space_optimizer.optimization.ddl import _GENIE_OPT_MV_CONSENTS_DDL


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


def _upsert(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "genie_space_optimizer",
        "probe_id": "p1",
        "granted_by": "analyst@example.com",
        "target_catalog": "finance",
        "target_schema": "sales",
        "verdict": "SUFFICIENT",
    }
    kwargs.update(overrides)
    result = warehouse.wh_upsert_mv_consent(
        _FakeWorkspaceClient(), "wh1", **kwargs,
    )
    return result, executed[-1]


def test_upsert_merges_on_probe_id(executed):
    probe_id, sql = _upsert(executed)

    assert probe_id == "p1"
    assert sql.startswith("MERGE INTO main.genie_space_optimizer.genie_opt_mv_consents")
    assert "ON t.probe_id = s.probe_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_upsert_writes_only_columns_the_table_declares(executed):
    _, sql = _upsert(executed)

    written = {
        column for column in (
            "probe_id", "run_id", "granted_by", "granted_at", "target_catalog",
            "target_schema", "materialize_consented", "probe_results_json",
            "verdict", "downgrade_reason", "updated_at",
        )
        if column in sql
    }
    assert len(written) == 11
    for column in written:
        assert column in _GENIE_OPT_MV_CONSENTS_DDL


def test_upsert_never_stamps_reverified_at_trigger(executed):
    """Only the pre-write re-verification may set it, so a stale probe cannot pass."""
    _, sql = _upsert(executed)

    assert "reverified_at_trigger" not in sql


def test_run_id_is_null_until_a_run_claims_the_consent(executed):
    _, sql = _upsert(executed)
    assert "t.run_id = NULL" in sql

    _, with_run = _upsert(executed, run_id="r1")
    assert "t.run_id = 'r1'" in with_run


def test_materialize_consent_defaults_to_false(executed):
    _, sql = _upsert(executed)
    assert "t.materialize_consented = false" in sql

    _, opted_in = _upsert(executed, materialize_consented=True)
    assert "t.materialize_consented = true" in opted_in


def test_granted_at_defaults_to_now_and_accepts_an_iso_string(executed):
    _, sql = _upsert(executed)
    assert "t.granted_at = current_timestamp()" in sql

    _, pinned = _upsert(executed, granted_at="2026-08-23T09:14:00+00:00")
    assert "t.granted_at = CAST('2026-08-23T09:14:00+00:00' AS TIMESTAMP)" in pinned


def test_probe_results_are_base64_encoded_json(executed):
    payload = {"capabilities": [{"capability": "mv_nested_joins", "status": "UNKNOWN"}]}
    _, sql = _upsert(executed, probe_results=payload)

    assert "unbase64(" in sql
    import base64
    import re

    encoded = re.search(r"unbase64\('([^']+)'\)", sql).group(1)
    assert json.loads(base64.b64decode(encoded).decode("utf-8")) == payload


def test_absent_probe_results_write_null(executed):
    _, sql = _upsert(executed)
    assert "t.probe_results_json = NULL" in sql


def test_quotes_in_free_text_cannot_break_out_of_the_literal(executed):
    _, sql = _upsert(executed, downgrade_reason="user's grant was revoked")

    assert "t.downgrade_reason = 'user''s grant was revoked'" in sql


@pytest.mark.parametrize("verdict", ["SUFFICIENT", "INSUFFICIENT", "UNKNOWN"])
def test_every_declared_verdict_is_accepted(executed, verdict):
    _, sql = _upsert(executed, verdict=verdict)
    assert f"t.verdict = '{verdict}'" in sql


def test_unknown_verdict_value_is_rejected(executed):
    with pytest.raises(ValueError, match="verdict must be one of"):
        _upsert(executed, verdict="MAYBE")
    assert executed == []


def test_empty_probe_id_is_rejected(executed):
    with pytest.raises(ValueError, match="probe_id is required"):
        _upsert(executed, probe_id="")
    assert executed == []


# ── Re-verification stamp (Prompt 15.5, Scenario B) ──────────────────────


def _reverify(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "genie_space_optimizer",
        "probe_id": "p1",
    }
    kwargs.update(overrides)
    warehouse.wh_mark_mv_consent_reverified(_FakeWorkspaceClient(), "wh1", **kwargs)
    return executed[-1]


def test_reverify_is_update_only_and_stamps_the_trigger_time(executed):
    """A stale authorization must never masquerade as a fresh consent, so this
    only UPDATEs a row the probe already wrote — never INSERTs one."""
    sql = _reverify(executed)

    assert sql.startswith("MERGE INTO main.genie_space_optimizer.genie_opt_mv_consents")
    assert "ON t.probe_id = s.probe_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" not in sql
    assert "t.reverified_at_trigger = current_timestamp()" in sql
    assert "t.updated_at = current_timestamp()" in sql


def test_reverify_closes_the_consent_to_run_loop(executed):
    """Scenario B's fix: the downgraded run's id, verdict and reason are stamped
    so /mv-created (which reads the consent by run) stops surfacing NULL."""
    sql = _reverify(
        executed,
        run_id="r1",
        verdict="INSUFFICIENT",
        downgrade_reason="grant revoked before trigger",
    )

    assert "t.run_id = 'r1'" in sql
    assert "t.verdict = 'INSUFFICIENT'" in sql
    assert "t.downgrade_reason = 'grant revoked before trigger'" in sql


def test_reverify_omits_columns_left_unset(executed):
    """The success path stamps a verdict but no downgrade_reason; an omitted
    field is left untouched rather than nulled, so a prior value survives."""
    sql = _reverify(executed, verdict="SUFFICIENT")

    assert "t.verdict = 'SUFFICIENT'" in sql
    assert "downgrade_reason" not in sql
    assert "t.run_id" not in sql


def test_reverify_rejects_an_undeclared_verdict(executed):
    with pytest.raises(ValueError, match="verdict must be one of"):
        _reverify(executed, verdict="MAYBE")
    assert executed == []


def test_reverify_requires_a_probe_id(executed):
    with pytest.raises(ValueError, match="probe_id is required"):
        _reverify(executed, probe_id="")
    assert executed == []


def test_reverify_quotes_cannot_break_out_of_the_literal(executed):
    sql = _reverify(executed, downgrade_reason="user's grant was revoked")
    assert "t.downgrade_reason = 'user''s grant was revoked'" in sql


# ── Reads ────────────────────────────────────────────────────────────────


def test_load_decodes_probe_results_like_the_spark_reader(monkeypatch):
    payload = {"verdict": "SUFFICIENT", "capabilities": []}
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {"probe_id": "p1", "probe_results_json": json.dumps(payload)},
        ]),
    )

    row = warehouse.wh_load_mv_consent(_FakeWorkspaceClient(), "wh1", "p1", "main", "gso")

    assert row is not None
    assert row["probe_results"] == payload
    assert "probe_results_json" not in row


def test_load_surfaces_unparseable_json_as_raw_text(monkeypatch):
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {"probe_id": "p1", "probe_results_json": "{not json"},
        ]),
    )

    row = warehouse.wh_load_mv_consent(_FakeWorkspaceClient(), "wh1", "p1", "main", "gso")

    assert row is not None
    assert row["probe_results"] == "{not json"


def test_load_returns_none_for_a_missing_probe(monkeypatch):
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame(),
    )

    assert warehouse.wh_load_mv_consent(
        _FakeWorkspaceClient(), "wh1", "p1", "main", "gso",
    ) is None


def test_load_returns_none_when_the_read_fails(monkeypatch):
    def _boom(ws, warehouse_id, sql):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(warehouse, "sql_warehouse_query", _boom)

    assert warehouse.wh_load_mv_consent(
        _FakeWorkspaceClient(), "wh1", "p1", "main", "gso",
    ) is None


def test_load_escapes_the_probe_id(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )

    warehouse.wh_load_mv_consent(
        _FakeWorkspaceClient(), "wh1", "p1' OR '1'='1", "main", "gso",
    )

    assert "WHERE probe_id = 'p1'' OR ''1''=''1'" in seen[0]
