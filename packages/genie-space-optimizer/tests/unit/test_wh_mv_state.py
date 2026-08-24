"""Tests for the SQL-warehouse metric view state accessors (MV-D21).

The FastAPI backend has no SparkSession, so ``mv_state``'s Spark accessors cannot
serve the Prompt 9 routes. These warehouse twins must write the *same* tables
with the *same* keys and the *same* column set — a drift between the two writers
would surface as a candidate the job cannot re-read or a created object the
lifecycle cannot track. Every write test therefore pins its columns against the
authoritative DDL, and the read tests pin the JSON decode against the Spark
reader's behavior.
"""

from __future__ import annotations

import base64
import json
import re

import pandas as pd
import pytest

from genie_space_optimizer.common import warehouse
from genie_space_optimizer.optimization.ddl import (
    ADDITIVE_COLUMN_MIGRATIONS,
    _GENIE_OPT_MV_CANDIDATES_DDL,
    _GENIE_OPT_MV_CREATED_OBJECTS_DDL,
)
from genie_space_optimizer.common.config import (
    TABLE_MV_CANDIDATES,
    TABLE_MV_CREATED_OBJECTS,
)


def _additive_columns(table: str) -> set[str]:
    """Column names the additive migrations add to ``table`` (MV-D21).

    Additive columns (yaml_text, provenance, lift_report_json) are not in the
    CREATE DDL string — they land via ADDITIVE_COLUMN_MIGRATIONS — so the pin
    checks their membership here rather than against the CREATE body.
    """
    return {col for tbl, col, _decl in ADDITIVE_COLUMN_MIGRATIONS if tbl == table}


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


# ── Decisions ────────────────────────────────────────────────────────────


def _decide(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "gso",
        "target_space_id": "space-1",
        "dedup_fingerprint": "fp1",
        "decision": "approved",
        "decided_by": "analyst@example.com",
    }
    kwargs.update(overrides)
    warehouse.wh_record_mv_candidate_decision(_FakeWorkspaceClient(), "wh1", **kwargs)
    return executed[-1]


def test_decision_updates_on_the_candidate_key(executed):
    sql = _decide(executed)
    assert sql.startswith("UPDATE main.gso.genie_opt_mv_candidates SET")
    assert "WHERE target_space_id = 'space-1' AND dedup_fingerprint = 'fp1'" in sql


def test_decision_writes_only_columns_the_table_declares(executed):
    sql = _decide(executed)
    written = {
        col for col in (
            "decision", "decided_by", "decided_at", "suppressed_until",
            "approved_for_rerun", "updated_at",
        )
        if col in sql
    }
    assert len(written) == 6
    for col in written:
        assert col in _GENIE_OPT_MV_CANDIDATES_DDL


def test_approval_flips_approved_for_rerun_true(executed):
    assert "approved_for_rerun = true" in _decide(executed, decision="approved")


def test_rejection_leaves_approved_for_rerun_false(executed):
    assert "approved_for_rerun = false" in _decide(executed, decision="rejected")


def test_rejection_carries_a_suppression_window(executed):
    sql = _decide(
        executed, decision="rejected", suppressed_until="2026-09-01T00:00:00+00:00",
    )
    assert "suppressed_until = CAST('2026-09-01T00:00:00+00:00' AS TIMESTAMP)" in sql


def test_absent_suppression_writes_null(executed):
    assert "suppressed_until = NULL" in _decide(executed)


def test_unknown_decision_is_rejected(executed):
    with pytest.raises(ValueError, match="decision must be one of"):
        _decide(executed, decision="maybe")
    assert executed == []


def test_decision_escapes_quotes_in_the_key(executed):
    sql = _decide(executed, target_space_id="s' OR '1'='1")
    assert "target_space_id = 's'' OR ''1''=''1'" in sql


# ── Created objects: upsert ────────────────────────────────────────────────


def _upsert_created(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "gso",
        "run_id": "r1",
        "suggestion_id": "sug1",
        "full_name": "finance.sales.revenue_metrics",
        "created_by": "analyst@example.com",
    }
    kwargs.update(overrides)
    result = warehouse.wh_upsert_mv_created_object(
        _FakeWorkspaceClient(), "wh1", **kwargs,
    )
    return result, executed[-1]


def test_created_object_merges_on_run_and_suggestion(executed):
    full_name, sql = _upsert_created(executed)
    assert full_name == "finance.sales.revenue_metrics"
    assert sql.startswith("MERGE INTO main.gso.genie_opt_mv_created_objects")
    assert "ON t.run_id = s.run_id AND t.suggestion_id = s.suggestion_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_created_object_writes_only_columns_the_table_declares(executed):
    _, sql = _upsert_created(executed)
    written = {
        col for col in (
            "run_id", "suggestion_id", "full_name", "created_by", "created_at",
            "status", "attach_patch_id", "baseline_eval_run_id",
            "post_attach_eval_run_id", "on_regression_action", "updated_at",
        )
        if col in sql
    }
    assert len(written) == 11
    for col in written:
        assert col in _GENIE_OPT_MV_CREATED_OBJECTS_DDL
    # provenance (MV-D24) is additive: written by the writer, present via
    # ADDITIVE_COLUMN_MIGRATIONS, and NOT in the CREATE body.
    assert "provenance" in sql
    assert "provenance" in _additive_columns(TABLE_MV_CREATED_OBJECTS)
    assert "provenance" not in _GENIE_OPT_MV_CREATED_OBJECTS_DDL


def test_created_object_defaults_to_created_and_never_drop(executed):
    _, sql = _upsert_created(executed)
    assert "t.status = 'CREATED'" in sql
    assert "t.on_regression_action = 'DETACH_ONLY_NEVER_DROP'" in sql


def test_created_object_provenance_defaults_to_obo_created(executed):
    _, sql = _upsert_created(executed)
    assert "t.provenance = 'OBO_CREATED'" in sql


def test_created_object_records_user_created_provenance(executed):
    _, sql = _upsert_created(executed, provenance="USER_CREATED")
    assert "t.provenance = 'USER_CREATED'" in sql


def test_created_object_rejects_an_unknown_status(executed):
    with pytest.raises(ValueError, match="status must be one of"):
        _upsert_created(executed, status="LIVE")
    assert executed == []


def test_created_object_rejects_an_unknown_regression_action(executed):
    with pytest.raises(ValueError, match="on_regression_action must be one of"):
        _upsert_created(executed, on_regression_action="AUTO_DROP")
    assert executed == []


def test_created_object_requires_a_full_name(executed):
    with pytest.raises(ValueError, match="full_name is required"):
        _upsert_created(executed, full_name="")
    assert executed == []


# ── Candidates: upsert (MV-D23 standalone advice write side) ───────────────


def _upsert_candidate(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "gso",
        "run_id": "r1",
        "target_space_id": "space-1",
        "suggestion_id": "sug1",
        "dedup_fingerprint": "fp1",
        "candidate_type": "NEW_METRIC_VIEW",
    }
    kwargs.update(overrides)
    result = warehouse.wh_upsert_mv_candidate(_FakeWorkspaceClient(), "wh1", **kwargs)
    return result, executed[-1]


def test_upsert_candidate_merges_on_space_and_fingerprint(executed):
    fp, sql = _upsert_candidate(executed)
    assert fp == "fp1"
    assert sql.startswith("MERGE INTO main.gso.genie_opt_mv_candidates")
    assert (
        "ON t.target_space_id = s.target_space_id "
        "AND t.dedup_fingerprint = s.dedup_fingerprint" in sql
    )
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_upsert_candidate_writes_only_columns_the_table_declares(executed):
    _, sql = _upsert_candidate(
        executed,
        confidence_score=82.0,
        tier="HIGH",
        proposed_object="finance.sales.revenue_metrics",
        score_components={"L": 40},
        evidence={"benchmark_ids": ["q1"]},
        provenance={"kind": "generated"},
        alternatives=[{"expr": "x"}],
        conflicts=[{"with": "y"}],
        requested_mode="suggest_only",
        effective_mode="suggest_only",
        yaml_text="version: '1.1'\n",
    )
    written = {
        col for col in (
            "target_space_id", "dedup_fingerprint", "suggestion_id", "run_id",
            "candidate_type", "confidence_score", "tier", "proposed_object",
            "score_components_json", "evidence_json", "provenance_json",
            "alternatives_json", "conflicts_json", "requested_mode",
            "effective_mode", "yaml_text", "updated_at", "created_at",
            "approved_for_rerun",
        )
        if col in sql
    }
    assert len(written) == 19
    additive = _additive_columns(TABLE_MV_CANDIDATES)
    assert "yaml_text" in additive
    for col in written:
        assert col in _GENIE_OPT_MV_CANDIDATES_DDL or col in additive


def test_upsert_candidate_never_updates_the_decision_columns(executed):
    _, sql = _upsert_candidate(executed)
    # approved_for_rerun is insert-only; a re-proposing run must not resurrect a
    # candidate the user rejected. decision/decided_by/suppressed_until are never
    # written by the proposer at all.
    update_clause = sql.split("WHEN MATCHED THEN UPDATE SET", 1)[1].split(
        "WHEN NOT MATCHED", 1
    )[0]
    assert "approved_for_rerun" not in update_clause
    for col in ("decision", "decided_by", "decided_at", "suppressed_until"):
        assert col not in sql


def test_upsert_candidate_base64_encodes_yaml_and_json(executed):
    _, sql = _upsert_candidate(
        executed,
        yaml_text="version: '1.1'\nsource: sales.orders\n",
        score_components={"L": 40, "Y": 30},
    )
    encoded = re.findall(r"unbase64\('([^']+)'\)", sql)
    decoded = {base64.b64decode(e).decode("utf-8") for e in encoded}
    assert "version: '1.1'\nsource: sales.orders\n" in decoded
    assert json.dumps({"L": 40, "Y": 30}) in decoded


def test_upsert_candidate_rejects_an_unknown_type(executed):
    with pytest.raises(ValueError, match="candidate_type must be one of"):
        _upsert_candidate(executed, candidate_type="MADE_UP")
    assert executed == []


def test_upsert_candidate_requires_a_fingerprint(executed):
    with pytest.raises(ValueError, match="dedup_fingerprint is required"):
        _upsert_candidate(executed, dedup_fingerprint="")
    assert executed == []


def test_upsert_candidate_escapes_quotes_in_the_key(executed):
    _, sql = _upsert_candidate(executed, target_space_id="s' OR '1'='1")
    assert "'s'' OR ''1''=''1'" in sql


# ── Created objects: status transition ─────────────────────────────────────


def _update_status(executed, **overrides):
    kwargs = {
        "catalog": "main",
        "schema": "gso",
        "run_id": "r1",
        "suggestion_id": "sug1",
        "status": "DETACHED",
    }
    kwargs.update(overrides)
    warehouse.wh_update_mv_created_object_status(
        _FakeWorkspaceClient(), "wh1", **kwargs,
    )
    return executed[-1]


def test_status_update_targets_the_created_object_key(executed):
    sql = _update_status(executed)
    assert sql.startswith("UPDATE main.gso.genie_opt_mv_created_objects SET")
    assert "status = 'DETACHED'" in sql
    assert "WHERE run_id = 'r1' AND suggestion_id = 'sug1'" in sql


def test_status_update_writes_only_the_fields_it_was_given(executed):
    sql = _update_status(executed)
    # attach/eval columns are omitted unless passed, so a status flip cannot
    # blank a previously-set attach_patch_id.
    assert "attach_patch_id" not in sql
    assert "baseline_eval_run_id" not in sql
    assert "lift_report_json" not in sql


def test_status_update_base64_encodes_the_lift_report(executed):
    payload = json.dumps({"kept": True, "delta": 0.1})
    sql = _update_status(executed, status="ATTACHED", lift_report_json=payload)
    encoded = re.search(r"unbase64\('([^']+)'\)", sql).group(1)
    assert base64.b64decode(encoded).decode("utf-8") == payload


def test_status_update_rejects_an_unknown_status(executed):
    with pytest.raises(ValueError, match="status must be one of"):
        _update_status(executed, status="GONE")
    assert executed == []


# ── Reads ──────────────────────────────────────────────────────────────────


def test_load_candidates_requires_a_scope():
    with pytest.raises(ValueError, match="target_space_id or run_id"):
        warehouse.wh_load_mv_candidates(
            _FakeWorkspaceClient(), "wh1", "main", "gso",
        )


def test_load_candidates_decodes_json_columns_like_the_spark_reader(monkeypatch):
    components = {"L": 40, "Y": 30}
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {
                "suggestion_id": "sug1",
                "dedup_fingerprint": "fp1",
                "score_components_json": json.dumps(components),
                "evidence_json": json.dumps({"benchmark_ids": ["q1"]}),
                "confidence_score": 82.0,
            },
        ]),
    )
    rows = warehouse.wh_load_mv_candidates(
        _FakeWorkspaceClient(), "wh1", "main", "gso", run_id="r1",
    )
    assert rows[0]["score_components"] == components
    assert rows[0]["evidence"] == {"benchmark_ids": ["q1"]}
    assert "score_components_json" not in rows[0]


def test_load_candidates_orders_and_filters(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )
    warehouse.wh_load_mv_candidates(
        _FakeWorkspaceClient(), "wh1", "main", "gso",
        target_space_id="space-1", approved_for_rerun=True,
    )
    assert "WHERE target_space_id = 'space-1' AND approved_for_rerun = true" in seen[0]
    assert "ORDER BY confidence_score DESC NULLS LAST, created_at DESC" in seen[0]


def test_load_candidates_returns_empty_on_read_failure(monkeypatch):
    def _boom(ws, warehouse_id, sql):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(warehouse, "sql_warehouse_query", _boom)
    assert warehouse.wh_load_mv_candidates(
        _FakeWorkspaceClient(), "wh1", "main", "gso", run_id="r1",
    ) == []


def test_load_created_object_returns_the_row_and_decodes_lift(monkeypatch):
    lift = {"kept": False, "delta": -0.2}
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {
                "run_id": "r1", "suggestion_id": "sug1",
                "full_name": "finance.sales.revenue_metrics",
                "created_by": "analyst@example.com", "status": "DETACHED",
                "lift_report_json": json.dumps(lift),
            },
        ]),
    )
    row = warehouse.wh_load_mv_created_object(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso",
        run_id="r1", suggestion_id="sug1",
    )
    assert row is not None
    assert row["full_name"] == "finance.sales.revenue_metrics"
    assert row["lift_report"] == lift
    assert "lift_report_json" not in row


def test_load_created_object_is_none_when_absent(monkeypatch):
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame(),
    )
    assert warehouse.wh_load_mv_created_object(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso",
        run_id="r1", suggestion_id="sug1",
    ) is None


def test_load_created_object_escapes_the_key(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )
    warehouse.wh_load_mv_created_object(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso",
        run_id="r1' OR '1'='1", suggestion_id="sug1",
    )
    assert "run_id = 'r1'' OR ''1''=''1'" in seen[0]


# ── Created objects: plural read (run output / results screen) ─────────────


def test_load_created_objects_returns_all_rows_and_decodes_lift(monkeypatch):
    lift_a = {"delta_affected": -0.07, "needs_review_count": 3}
    lift_b = {"delta_affected": 0.02, "needs_review_count": 0}
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {
                "run_id": "r1", "suggestion_id": "sugA",
                "full_name": "finance.sales.a", "status": "DETACHED",
                "lift_report_json": json.dumps(lift_a),
            },
            {
                "run_id": "r1", "suggestion_id": "sugB",
                "full_name": "finance.sales.b", "status": "ATTACHED",
                "lift_report_json": json.dumps(lift_b),
            },
        ]),
    )
    rows = warehouse.wh_load_mv_created_objects(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    )
    assert [r["suggestion_id"] for r in rows] == ["sugA", "sugB"]
    assert rows[0]["lift_report"] == lift_a
    assert rows[1]["lift_report"] == lift_b
    assert all("lift_report_json" not in r for r in rows)


def test_load_created_objects_orders_newest_first_and_scopes_to_the_run(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )
    warehouse.wh_load_mv_created_objects(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    )
    assert "WHERE run_id = 'r1'" in seen[0]
    assert "ORDER BY updated_at DESC" in seen[0]


def test_load_created_objects_returns_empty_on_read_failure(monkeypatch):
    def _boom(ws, warehouse_id, sql):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(warehouse, "sql_warehouse_query", _boom)
    assert warehouse.wh_load_mv_created_objects(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    ) == []


def test_load_created_objects_escapes_the_run_key(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )
    warehouse.wh_load_mv_created_objects(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso",
        run_id="r1' OR '1'='1",
    )
    assert "run_id = 'r1'' OR ''1''=''1'" in seen[0]


# ── Consent: read by run (downgrade_reason for the results screen) ─────────


def test_load_consent_by_run_returns_row_and_decodes_probe_results(monkeypatch):
    probe = {"verdict": "SUFFICIENT"}
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame([
            {
                "probe_id": "probe_1", "run_id": "r1",
                "downgrade_reason": "grant revoked before trigger",
                "probe_results_json": json.dumps(probe),
            },
        ]),
    )
    row = warehouse.wh_load_mv_consent_by_run(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    )
    assert row is not None
    assert row["downgrade_reason"] == "grant revoked before trigger"
    assert row["probe_results"] == probe
    assert "probe_results_json" not in row


def test_load_consent_by_run_orders_newest_first_and_scopes_to_the_run(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (seen.append(sql), pd.DataFrame())[1],
    )
    warehouse.wh_load_mv_consent_by_run(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    )
    assert "WHERE run_id = 'r1'" in seen[0]
    assert "ORDER BY updated_at DESC LIMIT 1" in seen[0]


def test_load_consent_by_run_is_none_when_absent(monkeypatch):
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: pd.DataFrame(),
    )
    assert warehouse.wh_load_mv_consent_by_run(
        _FakeWorkspaceClient(), "wh1", catalog="main", schema="gso", run_id="r1",
    ) is None
