"""Tests for the metric view create-and-attach service and its routes (Prompt 9).

Two things matter and are tested at the seam, not end to end (no Databricks):

- The **MV-D22 abort guard**: when a fresh probe would force the stored YAML to a
  lower ladder rung than it was rendered for, the create path drops that
  suggestion instead of installing the wrong artifact. This passes vacuously
  today (MV-D13 pins both compute paths to the same floor), so it is asserted
  against a *synthetically* stricter revalidation to prove the guard is wired,
  not merely dormant.
- The **lifecycle routes**: proposals list, DDL artifact, approve/reject
  decision, and the OBO-gated drop that refuses a non-owner or a non-DETACHED
  object.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_create, mv_entitlement
from genie_space_optimizer.common import warehouse
from genie_space_optimizer.common.config import (
    MV_CAPABILITY_NESTED_JOINS,
    MV_PROVEN_JOIN_STRATEGIES,
    MV_RENDER_VERSION,
)
from genie_space_optimizer.optimization import mv_yaml

_REAL_LOAD_RUN_ENVELOPE = auto_optimize._load_run_envelope
_REAL_OBJECT_EXISTS = mv_create._object_exists
_REAL_CONFIRM_METRIC_VIEW = mv_create._confirm_metric_view

# ── Service: create_and_attach_for_run ─────────────────────────────────────


def _verification(
    effective_mode="create_and_attach", downgrade_reason=None, verdict="SUFFICIENT", capabilities=(),
):
    fresh = SimpleNamespace(capabilities=list(capabilities), checked_as="analyst@example.com")
    return SimpleNamespace(
        effective_mode=effective_mode,
        downgrade_reason=downgrade_reason,
        verdict=verdict,
        fresh_probe=fresh,
    )


_CONSENT = {
    "target_catalog": "finance", "target_schema": "sales", "probe_id": "p1",
    "probe_results": {"privileges": [{"privilege": "SELECT", "securable": "finance.sales.orders"}]},
}
_ARTIFACT = {
    "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
    "join_strategy": "direct",
    "proposed_object": "warehouse.raw.revenue_metrics",
    "render_version": MV_RENDER_VERSION,
}


_OBO_WS = MagicMock(name="obo_ws")
_SP_WS = MagicMock(name="sp_ws")


class _Calls(list):
    """Captured calls, with ``on`` holding the workspace client each ran on."""

    def __init__(self):
        super().__init__()
        self.on: list = []
        self.described_on: list = []


@pytest.fixture
def create_env(monkeypatch):
    """Wire the create path down to captured warehouse calls."""
    executed = _Calls()
    upserts = _Calls()

    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: _SP_WS)
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: _OBO_WS)
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (_verification(), dict(_CONSENT)),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact", lambda *a, **k: dict(_ARTIFACT),
    )
    monkeypatch.setattr(
        mv_create, "_object_exists",
        lambda ws, *a, **k: (executed.described_on.append(ws) or False),
    )
    monkeypatch.setattr(
        mv_create, "_confirm_metric_view",
        lambda ws, *a, **k: (executed.described_on.append(ws) or True),
    )
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute",
        lambda ws, warehouse_id, sql: (executed.on.append(ws), executed.append(sql)),
    )
    monkeypatch.setattr(
        warehouse, "wh_upsert_mv_created_object",
        lambda ws, warehouse_id, **kw: (upserts.on.append(ws), upserts.append(kw))
        and kw["full_name"],
    )
    # The real _ledger_row_exists re-read after a failed write: no row, a proven
    # absence. Records (client, SQL).
    upserts.reads = []
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query",
        lambda ws, warehouse_id, sql: (upserts.reads.append((ws, sql)), pd.DataFrame())[1],
    )
    # An existing object at the name is someone else's unless a test says so.
    monkeypatch.setattr(
        mv_create, "_adopt_existing_view",
        lambda *a, **k: mv_create.ExistingView(False, False, "", "differs"),
    )
    return executed, upserts


def _run_create():
    return mv_create.create_and_attach_for_run(
        "run-1",
        space_id="space-1",
        probe_id="p1",
        approved_suggestion_ids=["sug1"],
        catalog="main",
        schema="gso",
        warehouse_id="wh1",
    )


def test_happy_path_creates_and_attaches_at_the_consented_name(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.action_mode == "create_and_attach"
    # Re-targeted to the CONSENTED catalog/schema, not the render-time
    # proposed_object (warehouse.raw.*). Base name is preserved.
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"
    assert upserts[0]["created_by"] == "analyst@example.com"


def test_candidate_yaml_text_is_the_fallback_when_no_artifact(create_env, monkeypatch):
    """MV-D23 severs coupling 3: a standalone advice candidate carries the
    replay body on the row, so an absent run-partitioned artifact is not a skip."""
    executed, upserts = create_env
    # No run-keyed artifact (the standalone advice path never wrote one) — the
    # candidate row carries yaml_text + evidence.join_strategy instead.
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1",
            "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "direct", "render_version": MV_RENDER_VERSION},
        }],
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"


def test_no_artifact_and_no_candidate_body_skips(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(mv_create, "_load_ddl_artifact", lambda *a, **k: None)
    # Candidate row has no yaml_text either — nothing replayable, so it skips.
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_run_hook_skips_an_unstamped_body(create_env, monkeypatch, caplog):
    """MV-D113: an unstamped (or render_version=0) body is skipped with a log;
    the loop continues and still creates a later stamped suggestion."""
    executed, upserts = create_env
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [
            {"suggestion_id": "sug1", "dedup_fingerprint": "fp1"},
            {"suggestion_id": "sug2", "dedup_fingerprint": "fp2"},
        ],
    )
    names = {
        "fp1": "warehouse.raw.revenue_metrics",
        "fp2": "warehouse.raw.order_counts",
    }

    def load_artifact(*a, fingerprint, **k):
        if fingerprint == "fp1":
            return {
                "yaml_text": "version: 0.1\nsource: finance.sales.orders\n",
                "join_strategy": "direct",
                "proposed_object": names[fingerprint],
                # absent render_version — pre-M3 body
            }
        return {**_ARTIFACT, "proposed_object": names[fingerprint]}

    monkeypatch.setattr(mv_create, "_load_ddl_artifact", load_artifact)
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    body0, reason0 = mv_create._replay_body(
        {"yaml_text": "x", "render_version": 0}, {},
    )
    assert body0 is None and reason0 == mv_create.STALE_BODY_REASON

    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _run_create_selecting(["sug1", "sug2"])

    assert any(
        "sug1" in r.getMessage() and "earlier version" in r.getMessage()
        for r in caplog.records
    )
    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.order_counts"]
    assert not any("revenue_metrics" in s for s in executed)
    assert any("CREATE VIEW `finance`.`sales`.`order_counts`" in s for s in executed)
    assert [kw["suggestion_id"] for kw in upserts] == ["sug2"]


def test_run_hook_prefers_a_stamped_candidate_over_a_stale_artifact(
    create_env, monkeypatch,
):
    """When the artifact is unstamped but the candidate row is stamped, replay
    the candidate body (MV-D113) rather than refusing the whole suggestion."""
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            "yaml_text": "version: 0.1\nsource: stale.artifact.body\n",
            "join_strategy": "direct",
            "proposed_object": "warehouse.raw.revenue_metrics",
        },
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1",
            "dedup_fingerprint": "fp1",
            "yaml_text": "version: 0.1\nsource: finance.sales.orders\n# candidate\n",
            "proposed_object": "warehouse.raw.revenue_metrics",
            "evidence": {"join_strategy": "direct", "render_version": MV_RENDER_VERSION},
        }],
    )
    seen_bodies: list[str] = []

    def validate(text, **kw):
        seen_bodies.append(text)
        return mv_yaml.ValidationReport(ok=True, downgrade_to=None)

    monkeypatch.setattr(mv_yaml, "validate", validate)

    handoff = _run_create()

    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("# candidate" in t for t in seen_bodies)
    assert not any("stale.artifact.body" in t for t in seen_bodies)
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"


def test_run_hook_refuses_a_non_plain_target_name(create_env, monkeypatch, caplog):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            **_ARTIFACT,
            "proposed_object": "main.sales.revenue metrics",
        },
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("DESCRIBE" in s or "CREATE VIEW" in s for s in executed)
    assert upserts == []
    assert any(
        "sug1" in r.getMessage() and "not a plain three-part name" in r.getMessage()
        for r in caplog.records
    )


def test_every_statement_naming_the_view_quotes_it(create_env, monkeypatch):
    """DESCRIBE / SELECT / CREATE / DROP issued by the create path quote every
    view-name part (MV-D113)."""
    import pandas as pd

    executed, upserts = create_env
    monkeypatch.setattr(mv_create, "_object_exists", _REAL_OBJECT_EXISTS)
    monkeypatch.setattr(mv_create, "_confirm_metric_view", _REAL_CONFIRM_METRIC_VIEW)
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    queries: list[str] = []

    def fake_query(ws, warehouse_id, sql):
        queries.append(sql)
        if sql.startswith("DESCRIBE TABLE ") and "EXTENDED" not in sql:
            return pd.DataFrame()
        if "DESCRIBE EXTENDED" in sql:
            return pd.DataFrame({"col_name": ["Type"], "data_type": ["METRIC_VIEW"]})
        if sql.startswith("SELECT 1 FROM"):
            return pd.DataFrame({"ok": [1]})
        return pd.DataFrame()

    monkeypatch.setattr(warehouse, "sql_warehouse_query", fake_query)

    handoff = _run_create()

    quoted = "`finance`.`sales`.`revenue_metrics`"
    bare = "finance.sales.revenue_metrics"
    naming = [s for s in list(executed) + queries if bare in s or quoted in s]
    assert naming, "expected at least one statement naming the view"
    assert all(quoted in s for s in naming)
    assert not any(
        bare in s.replace(quoted, "") for s in naming
    )
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert upserts and upserts[0]["status"] == "CREATED"


def test_claim_matches_a_quoted_generated_body(monkeypatch):
    """A quoted M3 body still claim-matches the candidate fingerprint (MV-D113)."""
    from genie_space_optimizer.optimization.mv_fingerprint import canonicalize_expr
    from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

    space = "space-1"
    fp = mv_candidate_fingerprint(
        space, canonicalize_expr("SUM(amount)"), ("main.sales.orders",),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": fp}],
    )
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: _SP_WS)
    yaml_text = (
        'version: "1.1"\n'
        "source: '`main`.`sales`.`orders`'\n"
        "measures:\n"
        "- name: total\n"
        "  expr: SUM(source.`amount`)\n"
    )
    ok, reason = mv_create._claim_matches_view(
        _SP_WS, "wh1",
        catalog="main", schema="gso",
        space_id=space, suggestion_id="sug1", yaml_text=yaml_text,
    )
    assert ok is True and reason is None


def test_claim_still_matches_a_pre_m3_body(monkeypatch):
    """Pre-M3 unquoted bodies keep matching after the AS-source fix."""
    from genie_space_optimizer.optimization.mv_fingerprint import canonicalize_expr
    from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

    space = "space-1"
    fp = mv_candidate_fingerprint(
        space, canonicalize_expr("SUM(amount)"), ("main.sales.orders",),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": fp}],
    )
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: _SP_WS)
    yaml_text = (
        'version: "1.1"\n'
        "source: main.sales.orders\n"
        "measures:\n"
        "- name: total\n"
        "  expr: SUM(source.amount)\n"
    )
    ok, reason = mv_create._claim_matches_view(
        _SP_WS, "wh1",
        catalog="main", schema="gso",
        space_id=space, suggestion_id="sug1", yaml_text=yaml_text,
    )
    assert ok is True and reason is None


def _claim_fp(expr: str) -> str:
    from genie_space_optimizer.optimization.mv_fingerprint import canonicalize_expr
    from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint

    return mv_candidate_fingerprint(
        "space-1", canonicalize_expr(expr), ("main.sales.orders",),
    )


def _claim(monkeypatch, row: dict, measures: list[str]):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", **row}],
    )
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: _SP_WS)
    yaml_text = (
        'version: "1.1"\n'
        "source: '`main`.`sales`.`orders`'\n"
        "measures:\n"
        + "".join(
            f"- name: m{i}\n  expr: {expr}\n" for i, expr in enumerate(measures)
        )
    )
    return mv_create._claim_matches_view(
        _SP_WS, "wh1",
        catalog="main", schema="gso",
        space_id="space-1", suggestion_id="sug1", yaml_text=yaml_text,
    )


def test_claim_matches_a_row_count(monkeypatch):
    """COUNT(*) keys on the view's source table, as the corpus scan does (MV-D117)."""
    ok, reason = _claim(
        monkeypatch, {"dedup_fingerprint": _claim_fp("COUNT(*)")}, ["COUNT(*)"],
    )
    assert ok is True and reason is None


def _bundle_row() -> dict:
    return {
        "dedup_fingerprint": "bundle_fp",
        "evidence": {
            "render_version": MV_RENDER_VERSION,
            "measures": [
                {"dedup_fingerprint": _claim_fp("SUM(amount)"), "role": "anchor"},
                {"dedup_fingerprint": _claim_fp("COUNT(*)"), "role": "anchor"},
                {"dedup_fingerprint": _claim_fp("MAX(amount)"), "role": "supporting"},
            ],
        },
    }


def test_claim_matches_a_bundle_by_its_anchors(monkeypatch):
    """A bundle's key is not a measure fingerprint; its anchors are (MV-D30)."""
    ok, reason = _claim(
        monkeypatch, _bundle_row(), ["SUM(source.`amount`)", "COUNT(*)"],
    )
    assert ok is True and reason is None


def test_claim_refuses_a_bundle_missing_an_anchor(monkeypatch):
    ok, reason = _claim(monkeypatch, _bundle_row(), ["SUM(source.`amount`)"])
    assert ok is False
    assert "fingerprint mismatch" in (reason or "")


def test_claim_refuses_a_stale_proposal(monkeypatch):
    """A body older than the renderer is never claimed, even when it matches."""
    ok, reason = _claim(
        monkeypatch,
        {
            "dedup_fingerprint": _claim_fp("COUNT(*)"),
            "proposed_object": "main.sales.orders_metrics",
            "evidence": {},
        },
        ["COUNT(*)"],
    )
    assert ok is False
    assert "earlier version" in (reason or "")
    assert mv_create.STALE_BODY_REASON.split(";")[0] in (reason or "")


def test_claim_matches_a_current_proposal(monkeypatch):
    """A rendered proposal at the current render version is claimable."""
    ok, reason = _claim(
        monkeypatch,
        {
            "dedup_fingerprint": _claim_fp("SUM(amount)"),
            "proposed_object": "main.sales.orders_metrics",
            "evidence": {"render_version": MV_RENDER_VERSION},
        },
        ["SUM(source.`amount`)"],
    )
    assert ok is True and reason is None


def test_claim_counts_a_bundle_member_without_a_role_as_an_anchor(monkeypatch):
    row = _bundle_row()
    for member in row["evidence"]["measures"]:
        member.pop("role")

    ok, reason = _claim(
        monkeypatch, row, ["SUM(source.`amount`)", "COUNT(*)", "MAX(source.`amount`)"],
    )
    assert ok is True and reason is None

    ok, reason = _claim(monkeypatch, row, ["SUM(source.`amount`)", "COUNT(*)"])
    assert ok is False
    assert "fingerprint mismatch" in (reason or "")


def test_revalidation_downgrade_aborts_the_create(create_env, monkeypatch):
    """MV-D22: a rung below the stored one drops the suggestion, never creates."""
    executed, upserts = create_env
    # Stored strategy is "direct"; a stricter probe forces "subquery_source".
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(
            ok=True, downgrade_to="subquery_source",
        ),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


def test_a_body_with_an_unknown_join_strategy_is_not_created(create_env, monkeypatch, caplog):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: dict(_ARTIFACT, join_strategy="cross"),
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("CREATE VIEW" in s for s in executed)
    assert handoff.attach_views == []
    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "needs a join strategy not yet proven in Unity Catalog (1)"
    )
    assert mv_create.UNPROVEN_RUNG_REASON in caplog.text
    assert upserts == []


# ── The join strategies proven live in Unity Catalog (MV-D124) ─────────────

_RUNG_PROOF = (
    Path(__file__).resolve().parents[2]
    / "packages/genie-space-optimizer/tests/unit/data/mv_rung_proof_7eeb5f5b.json"
)
_REAL_VALIDATE = mv_yaml.validate
# How mv_entitlement reports the runtime: a SQL warehouse gives a DBSQL version,
# so every floor is UNKNOWN; a DBR 17.3 cluster grants the nested-join floor.
_WAREHOUSE_CAPABILITIES = mv_entitlement._capability_rows("DBSQL", "2026.36", "wh1")
_NESTED_GRANTED_CAPABILITIES = mv_entitlement._capability_rows("DBR", "17.3", "wh1")


def _proven_body(strategy):
    rungs = json.loads(_RUNG_PROOF.read_text())["rungs"]
    return next(r for r in rungs if r["strategy"] == strategy)


def _run_proven(monkeypatch, strategy, *, stored_strategy, capabilities):
    """The run hook over a golden body, its tables covered, under the real validate."""
    rung = _proven_body(strategy)
    consent = _consent_covering(*rung["profiling"]["table_columns"])
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            **_ARTIFACT, "yaml_text": rung["yaml_text"], "join_strategy": stored_strategy,
        },
    )
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (_verification(capabilities=capabilities), consent),
    )
    monkeypatch.setattr(mv_yaml, "validate", _REAL_VALIDATE)
    return rung, _run_create()


def test_the_probe_capability_rows_resolve_as_the_pins_assume():
    nested = MV_CAPABILITY_NESTED_JOINS
    assert {r.capability: r.status for r in _WAREHOUSE_CAPABILITIES}[nested] == "UNKNOWN"
    assert {r.capability: r.status for r in _NESTED_GRANTED_CAPABILITIES}[nested] == "GRANTED"


@pytest.mark.parametrize(
    "strategy, capabilities",
    [
        ("denormalized", _WAREHOUSE_CAPABILITIES),
        ("nested", _NESTED_GRANTED_CAPABILITIES),
        ("subquery_source", _WAREHOUSE_CAPABILITIES),
    ],
)
def test_run_hook_creates_a_body_at_each_proven_join_strategy(
    create_env, monkeypatch, strategy, capabilities
):
    executed, upserts = create_env
    assert strategy in MV_PROVEN_JOIN_STRATEGIES

    rung, handoff = _run_proven(
        monkeypatch, strategy, stored_strategy=strategy, capabilities=capabilities,
    )

    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert [s for s in executed if "CREATE VIEW" in s] == [
        mv_yaml.create_ddl("finance.sales.revenue_metrics", rung["yaml_text"])
    ]
    assert upserts and upserts[0]["status"] == "CREATED"


def test_run_hook_reads_a_missing_join_strategy_as_direct(create_env, monkeypatch):
    executed, _upserts = create_env

    rung, handoff = _run_proven(
        monkeypatch, "direct", stored_strategy=None, capabilities=_WAREHOUSE_CAPABILITIES,
    )

    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert [s for s in executed if "CREATE VIEW" in s] == [
        mv_yaml.create_ddl("finance.sales.revenue_metrics", rung["yaml_text"])
    ]


@pytest.mark.parametrize("stored_strategy", ["nested", "subquery_source"])
def test_run_hook_refuses_a_nested_body_on_a_warehouse_below_the_rendered_rung(
    create_env, monkeypatch, caplog, stored_strategy
):
    """The nested body needs a capability a warehouse does not grant, so it is
    refused under its own label and under a ``subquery_source`` label alike."""
    executed, upserts = create_env

    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        _rung, handoff = _run_proven(
            monkeypatch, "nested", stored_strategy=stored_strategy,
            capabilities=_WAREHOUSE_CAPABILITIES,
        )

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "re-validation demands a lower join strategy (1)"
    )
    assert mv_create.UNPROVEN_RUNG_REASON not in caplog.text
    assert (
        f"demands join strategy subquery_source (stored {stored_strategy}); "
        "aborting create (MV-D22)"
    ) in caplog.text
    assert " below " not in caplog.text
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []


@pytest.mark.parametrize(
    "strategy, unproven",
    [
        (None, False), ("", False), ("direct", False), ("denormalized", False),
        ("nested", False), ("subquery_source", False), ("cross", True),
    ],
)
def test_unproven_rung_refuses_only_a_strategy_outside_the_proven_set(strategy, unproven):
    assert mv_create._unproven_rung(strategy) is unproven


def test_revalidation_failure_drops_the_suggestion(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=False, errors=("bad yaml",)),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)


def test_no_consent_downgrades_without_probing(monkeypatch):
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "verify_consent", lambda **kw: (None, None))

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []


def test_verify_downgrade_returns_suggest_only(monkeypatch):
    monkeypatch.setattr(mv_create, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(mv_create, "require_obo_workspace_client", lambda: MagicMock())
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (_verification(effective_mode="suggest_only",
                                    downgrade_reason="revoked"), dict(_CONSENT)),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == "revoked"


def test_downgrade_stamps_the_consent_with_run_and_reason(create_env, monkeypatch):
    """Prompt 15.5 / Scenario B: an auto-downgraded run stamps ``run_id`` +
    ``downgrade_reason`` (and the re-verified verdict) onto the consent, so
    ``/mv-created`` — which reads the consent BY run — stops surfacing NULL. The
    stamp is the missing warehouse twin of ``mark_mv_consent_reverified``."""
    _executed, _upserts = create_env
    stamps: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )
    monkeypatch.setattr(
        mv_create, "verify_consent",
        lambda **kw: (
            _verification(
                effective_mode="suggest_only",
                downgrade_reason="grant revoked before trigger",
                verdict="INSUFFICIENT",
            ),
            dict(_CONSENT),
        ),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == "grant revoked before trigger"
    assert len(stamps) == 1
    assert stamps[0]["probe_id"] == "p1"
    assert stamps[0]["run_id"] == "run-1"
    assert stamps[0]["verdict"] == "INSUFFICIENT"
    assert stamps[0]["downgrade_reason"] == "grant revoked before trigger"


def test_success_stamps_the_consent_run_without_a_downgrade_reason(create_env, monkeypatch):
    """The success path binds the run to the consent (so ``/mv-created`` can find
    it) and records the re-verified verdict, but leaves ``downgrade_reason``
    unset — a create-and-attach is not a downgrade."""
    _executed, _upserts = create_env
    stamps: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert handoff.action_mode == "create_and_attach"
    assert len(stamps) == 1
    assert stamps[0]["run_id"] == "run-1"
    assert stamps[0]["verdict"] == "SUFFICIENT"
    assert stamps[0].get("downgrade_reason") is None


def test_existing_object_is_not_clobbered(create_env, monkeypatch):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        mv_create, "_adopt_existing_view",
        lambda *a, **k: mv_create.ExistingView(False, False, "", "differs"),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)


def _ok_validate(monkeypatch):
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )


def _adopt_returns(monkeypatch, existing):
    """Stub the existing-view check; returns the captured (client, kwargs) calls."""
    calls: list = []
    monkeypatch.setattr(
        mv_create, "_adopt_existing_view",
        lambda ws, warehouse_id, **kw: (calls.append((ws, kw)), existing)[1],
    )
    return calls


def _create_raises(monkeypatch, executed, exc):
    """The CREATE raises ``exc``; every other statement is captured."""
    def execute(ws, warehouse_id, sql):
        executed.on.append(ws)
        executed.append(sql)
        if sql.startswith("CREATE VIEW"):
            raise exc

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", execute)


def _assert_type_only_logs(records, type_name):
    assert any(type_name in r.getMessage() for r in records)
    assert all("zq_secret" not in r.getMessage() for r in records)
    assert all(not r.exc_info for r in records)


def test_an_owned_matching_view_at_the_name_is_adopted(create_env, monkeypatch, caplog):
    """MV-D120: a view at the consented name that is this proposal and the
    caller's own is recorded and attached, never re-created."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    adopt = _adopt_returns(
        monkeypatch, mv_create.ExistingView(True, True, "analyst@example.com", None),
    )

    with caplog.at_level("INFO", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("CREATE VIEW" in s for s in executed)
    assert not any("DROP VIEW" in s for s in executed)
    assert [ws for ws, _ in adopt] == [_OBO_WS]
    assert adopt[0][1]["full_name"] == "finance.sales.revenue_metrics"
    assert adopt[0][1]["caller"] == "analyst@example.com"
    assert upserts.on == [_SP_WS]
    assert upserts[0]["status"] == "CREATED"
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_OBO_CREATED
    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert handoff.created[0].provenance == "OBO_CREATED"
    assert any(
        "Adopted existing metric view finance.sales.revenue_metrics for run run-1"
        in r.getMessage() for r in caplog.records
    )


def test_a_matching_view_someone_else_owns_is_refused(create_env, monkeypatch):
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    _adopt_returns(
        monkeypatch, mv_create.ExistingView(True, False, "other@example.com", None),
    )

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "already exists in the consented schema (1)"
    )
    assert not any(s.startswith(("CREATE VIEW", "DROP VIEW")) for s in executed)
    assert upserts == [] and upserts.on == []


def test_a_failed_create_whose_view_exists_is_recorded(create_env, monkeypatch, caplog):
    """MV-D120: a CREATE that raised may still have committed; the view is looked
    up, and the caller's own matching view is recorded and attached."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _create_raises(monkeypatch, executed, RuntimeError("zq_secret"))
    adopt = _adopt_returns(
        monkeypatch, mv_create.ExistingView(True, True, "analyst@example.com", None),
    )

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    view_ddl = [s for s in executed if s.startswith(("CREATE VIEW", "DROP VIEW"))]
    assert len(view_ddl) == 1 and view_ddl[0].startswith("CREATE VIEW")
    assert [ws for ws, _ in adopt] == [_OBO_WS]
    # Only the _object_exists DESCRIBE: the helper confirmed it, no second confirm.
    assert executed.described_on == [_OBO_WS]
    assert upserts.on == [_SP_WS] and upserts[0]["status"] == "CREATED"
    assert upserts[0]["provenance"] == mv_create.MV_PROVENANCE_OBO_CREATED
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert handoff.created[0].provenance == "OBO_CREATED"
    _assert_type_only_logs(caplog.records, "RuntimeError")


def test_a_failed_create_with_no_view_is_an_error_skip(create_env, monkeypatch, caplog):
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _create_raises(monkeypatch, executed, RuntimeError("zq_secret"))
    _adopt_returns(
        monkeypatch, mv_create.ExistingView(False, False, "", "absent", exists=False),
    )

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("DROP VIEW" in s for s in executed)
    assert upserts == [] and upserts.on == []
    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "failed with an error (1)"
    )
    _assert_type_only_logs(caplog.records, "RuntimeError")
    assert any(
        "was not found after a failed create" in r.getMessage() for r in caplog.records
    )


def test_a_raising_lookup_after_a_failed_create_is_an_error_skip(create_env, monkeypatch, caplog):
    """The lookup raising inside the CREATE ``except`` reads as not found: the
    ``error`` skip, logged by type only, and nothing is dropped or recorded."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _create_raises(monkeypatch, executed, RuntimeError("zq_secret"))

    def adopt(*a, **k):
        raise ValueError("zq_secret")

    monkeypatch.setattr(mv_create, "_adopt_existing_view", adopt)

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("DROP VIEW" in s for s in executed)
    assert upserts == [] and upserts.on == []
    assert handoff.attach_views == []
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "failed with an error (1)"
    )
    _assert_type_only_logs(caplog.records, "ValueError")
    assert any(
        "Could not look up finance.sales.revenue_metrics after a failed create (ValueError)"
        in r.getMessage() for r in caplog.records
    )


def test_an_unexpected_failure_in_the_hook_logs_the_type_only(create_env, monkeypatch, caplog):
    """MV-D121: a per-suggestion step that raises outside the CREATE is the
    ``error`` skip, and the hook's catch-all logs the exception type only."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)

    def confirm(*a, **k):
        raise RuntimeError("zq_secret")

    monkeypatch.setattr(mv_create, "_confirm_metric_view", confirm)

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert upserts == []
    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "failed with an error (1)"
    )
    _assert_type_only_logs(caplog.records, "RuntimeError")
    assert any(
        "Create failed for suggestion sug1; dropping it from the run (RuntimeError)"
        in r.getMessage() for r in caplog.records
    )


@pytest.mark.parametrize(
    "existing",
    [
        mv_create.ExistingView(True, False, "other@example.com", None),
        mv_create.ExistingView(False, False, "", "differs"),
    ],
    ids=["someone_elses", "different"],
)
def test_a_view_found_after_a_failed_create_that_is_not_the_callers_is_an_exists_skip(
    create_env, monkeypatch, caplog, existing,
):
    """MV-D120: the lookup found an object at the name that the run won't adopt.
    It exists, so it is the ``exists`` skip, not an ``error``."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _create_raises(monkeypatch, executed, RuntimeError("zq_secret"))
    _adopt_returns(monkeypatch, existing)

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("DROP VIEW" in s for s in executed)
    assert upserts == [] and upserts.on == []
    assert handoff.attach_views == []
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "already exists in the consented schema (1)"
    )
    _assert_type_only_logs(caplog.records, "RuntimeError")


def _upsert_raises(monkeypatch, upserts, exc):
    def upsert(ws, warehouse_id, **kw):
        upserts.on.append(ws)
        raise exc

    monkeypatch.setattr(warehouse, "wh_upsert_mv_created_object", upsert)


def _ledger_query_returns(monkeypatch, result):
    """Stub the SELECT the real ``_ledger_row_exists`` runs; ``result`` is a
    DataFrame or an exception to raise. Returns the captured (client, SQL)."""
    reads: list = []

    def query(ws, warehouse_id, sql):
        reads.append((ws, sql))
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(warehouse, "sql_warehouse_query", query)
    return reads


def _assert_ledger_read(reads):
    assert len(reads) == 1 and reads[0][0] is _SP_WS
    sql = reads[0][1]
    assert sql.startswith("SELECT 1 FROM main.gso.genie_opt_mv_created_objects")
    assert "run_id = 'run-1'" in sql and "suggestion_id = 'sug1'" in sql


def test_a_ledger_write_that_landed_is_kept(create_env, monkeypatch, caplog):
    """MV-D120: a MERGE that raised but committed leaves a row; the re-read sees
    it, so the view is kept and attached as recorded."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _upsert_raises(monkeypatch, upserts, RuntimeError("zq_secret"))
    reads = _ledger_query_returns(monkeypatch, pd.DataFrame([{"1": 1}]))

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("DROP VIEW" in s for s in executed)
    _assert_ledger_read(reads)
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert [c.suggestion_id for c in handoff.created] == ["sug1"]
    _assert_type_only_logs(caplog.records, "RuntimeError")


def test_a_ledger_proven_absent_keeps_the_fresh_view(create_env, monkeypatch, caplog):
    """MV-D120 (owner ruling): an absent re-read doesn't prove the MERGE won't
    commit, so the view is kept and left for the next run to adopt."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _upsert_raises(monkeypatch, upserts, RuntimeError("zq_secret"))
    reads = _ledger_query_returns(monkeypatch, pd.DataFrame())

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    _assert_ledger_read(reads)
    assert not any("DROP VIEW" in s for s in executed)
    assert _view_ddl_clients(executed) == [_OBO_WS]
    assert upserts.on == [_SP_WS]
    assert handoff.attach_views == [] and handoff.created == []
    assert handoff.downgrade_reason == _UNRECORDED_KEPT_REASON
    _assert_type_only_logs(caplog.records, "RuntimeError")
    assert any(
        "finance.sales.revenue_metrics" in r.getMessage()
        and "leaving it for the next run to adopt" in r.getMessage()
        for r in caplog.records
    )


def test_an_unreadable_ledger_keeps_the_view(create_env, monkeypatch, caplog):
    """The real reader lets a read failure raise, so it is never a proven absence."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _upsert_raises(monkeypatch, upserts, ValueError("zq_secret"))
    reads = _ledger_query_returns(monkeypatch, RuntimeError("zq_secret"))

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    _assert_ledger_read(reads)
    assert not any("DROP VIEW" in s for s in executed)
    assert handoff.attach_views == [] and handoff.created == []
    assert handoff.downgrade_reason == _UNRECORDED_KEPT_REASON
    assert any(
        "finance.sales.revenue_metrics" in r.getMessage()
        and "ValueError" in r.getMessage() and "RuntimeError" in r.getMessage()
        for r in caplog.records
    )
    assert all("zq_secret" not in r.getMessage() for r in caplog.records)
    assert all(not r.exc_info for r in caplog.records)


_UNRECORDED_KEPT_REASON = (
    "no metric view could be created for the selected candidates: "
    "could not be recorded; left in place for the next run to adopt (1)"
)


def test_an_adopted_view_whose_record_fails_is_never_dropped(create_env, monkeypatch, caplog):
    """MV-D120: the cleanup DROP is only for a view this run freshly created; an
    adopted view may be the caller's own, so it is kept even on a proven absence."""
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    monkeypatch.setattr(mv_create, "_object_exists", lambda *a, **k: True)
    _adopt_returns(
        monkeypatch, mv_create.ExistingView(True, True, "analyst@example.com", None),
    )
    _upsert_raises(monkeypatch, upserts, RuntimeError("zq_secret"))
    _ledger_query_returns(monkeypatch, pd.DataFrame())

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any(s.startswith(("CREATE VIEW", "DROP VIEW")) for s in executed)
    assert upserts.on == [_SP_WS]
    assert handoff.attach_views == [] and handoff.created == []
    assert handoff.downgrade_reason == _UNRECORDED_KEPT_REASON
    _assert_type_only_logs(caplog.records, "RuntimeError")


def test_a_view_found_after_a_failed_create_whose_record_fails_is_kept(
    create_env, monkeypatch, caplog,
):
    executed, upserts = create_env
    _ok_validate(monkeypatch)
    _create_raises(monkeypatch, executed, RuntimeError("zq_secret"))
    _adopt_returns(
        monkeypatch, mv_create.ExistingView(True, True, "analyst@example.com", None),
    )
    _upsert_raises(monkeypatch, upserts, RuntimeError("zq_secret"))
    _ledger_query_returns(monkeypatch, pd.DataFrame())

    with caplog.at_level("DEBUG", logger="backend.services.mv_create"):
        handoff = _run_create()

    assert not any("DROP VIEW" in s for s in executed)
    assert handoff.attach_views == [] and handoff.created == []
    assert handoff.downgrade_reason == _UNRECORDED_KEPT_REASON
    _assert_type_only_logs(caplog.records, "RuntimeError")


def _run_create_selecting(ids):
    return mv_create.create_and_attach_for_run(
        "run-1", space_id="space-1", probe_id="p1",
        approved_suggestion_ids=ids,
        catalog="main", schema="gso", warehouse_id="wh1",
    )


def _view_ddl_clients(executed):
    return [ws for ws, sql in zip(executed.on, executed) if sql.startswith(("CREATE VIEW", "DROP VIEW"))]


def _two_candidates(monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [
            {"suggestion_id": "sug1", "dedup_fingerprint": "fp1"},
            {"suggestion_id": "sug2", "dedup_fingerprint": "fp2"},
        ],
    )
    names = {"fp1": "warehouse.raw.revenue_metrics", "fp2": "warehouse.raw.order_counts"}
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, fingerprint, **k: {**_ARTIFACT, "proposed_object": names[fingerprint]},
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )


@pytest.mark.parametrize("ids", [[], None])
def test_create_and_attach_with_nothing_selected_creates_nothing(create_env, monkeypatch, ids):
    """MV-D112: an empty selection means none, never "every approved candidate"."""
    executed, upserts = create_env
    loads: list[dict] = []
    stamps: list[dict] = []
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: (loads.append(k) or []))
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append((ws, kw)),
    )

    handoff = _run_create_selecting(ids)

    assert handoff.action_mode == "suggest_only"
    assert handoff.attach_views == []
    assert handoff.consent_id == "p1"
    assert handoff.downgrade_reason == "no metric views were selected for this run"
    assert executed == [] and upserts == [] and loads == []
    assert executed.described_on == []
    assert len(stamps) == 1
    assert stamps[0][0] is _SP_WS
    assert stamps[0][1]["downgrade_reason"] == "no metric views were selected for this run"


def test_only_the_selected_candidates_are_created(create_env, monkeypatch):
    executed, upserts = create_env
    _two_candidates(monkeypatch)

    handoff = _run_create_selecting(["sug2"])

    creates = [s for s in executed if "CREATE VIEW" in s]
    assert handoff.attach_views == ["finance.sales.order_counts"]
    assert len(creates) == 1 and "CREATE VIEW `finance`.`sales`.`order_counts`" in creates[0]
    assert not any("revenue_metrics" in s for s in executed)
    assert _view_ddl_clients(executed) == [_OBO_WS]
    assert executed.described_on and all(ws is _OBO_WS for ws in executed.described_on)
    assert [kw["suggestion_id"] for kw in upserts] == ["sug2"] and upserts.on == [_SP_WS]


def test_an_unrecorded_create_is_kept_and_the_run_moves_on(create_env, monkeypatch):
    """MV-D120 (owner ruling, amending MV-D112 (3)): a view whose ledger row cannot
    be written is never dropped; the run re-reads the ledger and moves on."""
    executed, upserts = create_env
    _two_candidates(monkeypatch)

    def upsert(ws, warehouse_id, **kw):
        upserts.on.append(ws)
        if kw["suggestion_id"] == "sug1":
            raise RuntimeError("delta write failed")
        upserts.append(kw)

    monkeypatch.setattr(warehouse, "wh_upsert_mv_created_object", upsert)
    events: list = []
    real_execute = warehouse.sql_warehouse_execute

    def execute(ws, warehouse_id, sql):
        events.append(sql.split(" ")[0])
        real_execute(ws, warehouse_id, sql)

    def read(ws, warehouse_id, sql):
        events.append("READ")
        upserts.reads.append((ws, sql))
        return pd.DataFrame()

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", execute)
    monkeypatch.setattr(warehouse, "sql_warehouse_query", read)

    handoff = _run_create_selecting(["sug1", "sug2"])

    # The re-read follows sug1's failed write; sug2's CREATE follows with no DROP.
    assert events[:3] == ["CREATE", "READ", "CREATE"]
    assert "DROP" not in events
    assert len(upserts.reads) == 1 and upserts.reads[0][0] is _SP_WS
    assert "suggestion_id = 'sug1'" in upserts.reads[0][1]
    assert not any("DROP VIEW" in s for s in executed)
    assert _view_ddl_clients(executed) == [_OBO_WS] * 2
    assert upserts.on == [_SP_WS, _SP_WS]
    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.order_counts"]
    assert [c.suggestion_id for c in handoff.created] == ["sug2"]


def test_a_failed_confirm_cleanup_drop_is_logged(create_env, monkeypatch, caplog):
    """The only hook DROP left is the cleanup of a fresh create that is not a usable
    metric view; a failed cleanup is logged naming the view."""
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(mv_create, "_confirm_metric_view", lambda *a, **k: False)

    drops_on: list = []

    def execute(ws, warehouse_id, sql):
        if sql.startswith("DROP VIEW"):
            drops_on.append(ws)
            raise RuntimeError("drop failed")
        executed.on.append(ws)
        executed.append(sql)

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", execute)

    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _run_create_selecting(["sug1"])

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "was not confirmed as a metric view after create (1)"
    )
    assert _view_ddl_clients(executed) == [_OBO_WS] and drops_on == [_OBO_WS]
    assert upserts == []
    assert any(
        r.levelname == "WARNING"
        and "Could not clean up finance.sales.revenue_metrics" in r.getMessage()
        for r in caplog.records
    )


_UNSTAMPED_BODY = "version: 0.1\nsource: finance.sales.orders\n"


def _stale_and_taken(monkeypatch, *, taken_exists: bool) -> list[dict]:
    """``sug_old`` has only unstamped bodies; ``sug_taken`` is stamped, and its
    name already exists when ``taken_exists``. Returns the captured consent stamps."""
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [
            {
                "suggestion_id": "sug_old", "dedup_fingerprint": "fp_old",
                "yaml_text": _UNSTAMPED_BODY,
                "proposed_object": "warehouse.raw.old_metrics",
            },
            {"suggestion_id": "sug_taken", "dedup_fingerprint": "fp_taken"},
        ],
    )
    artifacts = {
        "fp_old": {
            "yaml_text": _UNSTAMPED_BODY, "join_strategy": "direct",
            "proposed_object": "warehouse.raw.old_metrics",
        },
        "fp_taken": {**_ARTIFACT, "proposed_object": "warehouse.raw.taken_metrics"},
    }
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, fingerprint, **k: dict(artifacts[fingerprint]),
    )
    monkeypatch.setattr(
        mv_create, "_object_exists",
        lambda ws, wh, full_name: taken_exists and full_name.endswith(".taken_metrics"),
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    stamps: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )
    return stamps


_STALE_AND_TAKEN_REASON = (
    "no metric view could be created for the selected candidates: "
    "rendered by an earlier version of the advisor, re-scan the Agent for a current "
    "suggestion (1); already exists in the consented schema (1)"
)


def test_nothing_built_names_each_skip_reason(create_env, monkeypatch):
    """MV-D117 (C-1): an empty create names why, as counts in a fixed order."""
    executed, upserts = create_env
    stamps = _stale_and_taken(monkeypatch, taken_exists=True)

    handoff = _run_create_selecting(["sug_old", "sug_taken"])

    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == _STALE_AND_TAKEN_REASON
    assert len(stamps) == 1
    assert stamps[0]["downgrade_reason"] == _STALE_AND_TAKEN_REASON
    assert not any("CREATE VIEW" in s for s in executed) and upserts == []


def test_an_approved_id_with_no_candidate_is_no_longer_available(create_env, monkeypatch):
    """MV-D117: an id the loaded candidates no longer carry is counted, not lost."""
    stamps: list[dict] = []
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    monkeypatch.setattr(
        warehouse, "wh_mark_mv_consent_reverified",
        lambda ws, warehouse_id, **kw: stamps.append(kw),
    )

    handoff = _run_create_selecting(["sug1"])

    reason = (
        "no metric view could be created for the selected candidates: "
        "no longer available (1)"
    )
    assert handoff.downgrade_reason == reason
    assert stamps[0]["downgrade_reason"] == reason


def test_unavailable_ids_lead_the_summary(create_env, monkeypatch):
    _stale_and_taken(monkeypatch, taken_exists=True)

    reason = _run_create_selecting(["sug_gone", "sug_old", "sug_taken"]).downgrade_reason

    assert reason == (
        "no metric view could be created for the selected candidates: "
        "no longer available (1); "
        "rendered by an earlier version of the advisor, re-scan the Agent for a current "
        "suggestion (1); already exists in the consented schema (1)"
    )


def test_the_stale_reason_does_not_promise_a_refresh():
    assert mv_create.STALE_BODY_REASON == (
        "this proposal was rendered by an earlier version of the advisor; "
        "re-scan the Agent for a current suggestion"
    )


@pytest.mark.parametrize(
    "key,label",
    [
        ("unavailable", "no longer available"),
        ("stale", "rendered by an earlier version of the advisor, re-scan the Agent for a current suggestion"),
        ("no_body", "no rendered body"),
        ("unproven_rung", "needs a join strategy not yet proven in Unity Catalog"),
        ("invalid_name", "not a plain Unity Catalog name"),
        ("uncovered", "reads a table the access check did not cover"),
        ("revalidation", "failed re-validation"),
        ("rung_below", "re-validation demands a lower join strategy"),
        ("exists", "already exists in the consented schema"),
        ("not_confirmed", "was not confirmed as a metric view after create"),
        ("unrecorded_kept", "could not be recorded; left in place for the next run to adopt"),
        ("error", "failed with an error"),
    ],
)
def test_each_skip_key_names_its_label(key, label):
    assert mv_create._nothing_built_reason({key: 2}) == (
        f"no metric view could be created for the selected candidates: {label} (2)"
    )


def test_the_skip_labels_are_pinned_in_order():
    assert [key for key, _ in mv_create._SKIP_ORDER] == [
        "unavailable", "stale", "no_body", "unproven_rung", "invalid_name", "uncovered",
        "revalidation", "rung_below", "exists", "not_confirmed", "unrecorded_kept", "error",
    ]


# ── _ledger_row_exists: the strict re-read after a failed ledger write ──────


def _ledger_row_exists(run_id="run-1", suggestion_id="sug1"):
    return mv_create._ledger_row_exists(
        _SP_WS, "wh1", catalog="main", schema="gso",
        run_id=run_id, suggestion_id=suggestion_id,
    )


@pytest.mark.parametrize(
    "frame,expected",
    [(pd.DataFrame([{"1": 1}]), True), (pd.DataFrame(), False)],
    ids=["present", "empty"],
)
def test_ledger_row_exists_reads_the_row_on_the_sp(monkeypatch, frame, expected):
    reads = _ledger_query_returns(monkeypatch, frame)

    assert _ledger_row_exists() is expected
    _assert_ledger_read(reads)
    assert reads[0][1].endswith("LIMIT 1")


def test_ledger_row_exists_lets_a_read_failure_raise(monkeypatch):
    _ledger_query_returns(monkeypatch, RuntimeError("zq_secret"))

    with pytest.raises(RuntimeError):
        _ledger_row_exists()


def test_ledger_row_exists_quotes_its_ids_through_wh_literal(monkeypatch):
    reads = _ledger_query_returns(monkeypatch, pd.DataFrame())
    quoted: list = []
    real_literal = warehouse._wh_literal
    monkeypatch.setattr(
        warehouse, "_wh_literal", lambda v, **k: (quoted.append(v), real_literal(v, **k))[1],
    )
    run_id, suggestion_id = "run' OR '1'='1", "sug\\1"

    _ledger_row_exists(run_id=run_id, suggestion_id=suggestion_id)

    sql = reads[0][1]
    assert quoted == [run_id, suggestion_id]
    assert f"run_id = {real_literal(run_id)} " in sql
    assert sql.endswith(f"suggestion_id = {real_literal(suggestion_id)} LIMIT 1")
    assert f"'{run_id}'" not in sql and f"'{suggestion_id}'" not in sql


def test_partial_success_stamps_no_reason(create_env, monkeypatch):
    stamps = _stale_and_taken(monkeypatch, taken_exists=False)

    handoff = _run_create_selecting(["sug_old", "sug_taken"])

    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.taken_metrics"]
    assert handoff.downgrade_reason is None
    assert len(stamps) == 1 and stamps[0].get("downgrade_reason") is None


def test_the_summary_names_no_view_or_sql(create_env, monkeypatch):
    _stale_and_taken(monkeypatch, taken_exists=True)

    reason = _run_create_selecting(["sug_old", "sug_taken"]).downgrade_reason or ""

    assert reason.startswith("no metric view could be created")
    for leaked in ("old_metrics", "taken_metrics", "finance", "warehouse.raw", "SELECT"):
        assert leaked not in reason


@pytest.mark.parametrize(
    "downgrade_to,expected",
    [
        ("subquery_source", True),   # the body needs a capability the probe lacks
        (None, False),               # no downgrade demanded
        ("", False),
        ("denormalized", True),      # any downgrade is refused, not only today's one
    ],
)
def test_rung_below(downgrade_to, expected):
    assert mv_create._rung_below(downgrade_to) is expected


# ── Routes ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _run_envelope_for_run_keyed_routes(monkeypatch):
    """Run-keyed gates resolve the run's space before Can Edit; stub the envelope."""

    async def run_envelope(run_id):
        return {"run_id": run_id, "space_id": "space-1", "status": "CONVERGED"}

    monkeypatch.setattr(auto_optimize, "_load_run_envelope", run_envelope)


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_list_mv_proposals_returns_the_rows(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
            "confidence_score": 82.0, "approved_for_rerun": True,
        }],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["proposals"][0]["suggestion_id"] == "sug1"
    assert data["proposals"][0]["approved_for_rerun"] is True


def test_list_space_mv_proposals_filters_by_space_and_approved(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return [{
            "suggestion_id": "sug_space", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
            "confidence_score": 91.0, "approved_for_rerun": True,
        }]

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true")
    assert resp.status_code == 200
    data = resp.json()
    assert data["space_id"] == "space-1"
    assert data["proposals"][0]["suggestion_id"] == "sug_space"
    # The gate is space-scoped; run_id must NOT stand in for it (MV-D23).
    assert captured.get("target_space_id") == "space-1"
    assert captured.get("approved_for_rerun") is True
    assert captured.get("run_id") is None


def test_list_space_mv_proposals_defaults_approved_to_none(client, monkeypatch):
    captured: dict = {}

    def fake_load(*args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", fake_load)
    # MV-D31: the unfiltered panel load also reads the last-scan summary; stub it
    # so this test pins the candidate read + response shape (last_scan hydrates on
    # its own, and is None here — this space has no advice run in the stub).
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")
    assert resp.status_code == 200
    assert resp.json() == {"space_id": "space-1", "proposals": [], "last_scan": None}
    assert captured.get("approved_for_rerun") is None


def test_list_space_mv_proposals_hydrates_last_scan(client, monkeypatch):
    """MV-D31 hydrate-on-mount: the unfiltered panel load returns the last scan's
    timestamp, real duration, and empty/skip state, so the surface opens showing
    "last scanned … — N proposals" instead of a bare button."""
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    monkeypatch.setattr(
        warehouse, "wh_load_latest_advice_scan",
        lambda *a, **k: {
            "run_id": "run-adv-9",
            "scanned_at": "2026-08-25T05:00:00Z",
            "status": "SKIPPED",
            "duration_seconds": 252.0,
            "skip_reason": "NO_CANDIDATES",
            "measures_found": 4,
        },
    )
    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")
    assert resp.status_code == 200
    scan = resp.json()["last_scan"]
    assert scan["status"] == "SKIPPED"
    assert scan["skip_reason"] == "NO_CANDIDATES"
    assert scan["measures_found"] == 4
    assert scan["duration_seconds"] == 252.0
    assert scan["proposal_count"] == 0


def test_list_space_mv_proposals_rerun_gate_skips_last_scan(client, monkeypatch):
    """The re-run gate query (approved_for_rerun=true) asks a different question
    ("what has this Agent had approved?") and never wants the last-scan framing,
    so the summary read is not issued on that path."""
    read = {"called": False}

    def _scan(*a, **k):
        read["called"] = True
        return None

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", _scan)
    resp = client.get(
        "/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true"
    )
    assert resp.status_code == 200
    assert resp.json()["last_scan"] is None
    assert read["called"] is False


def _stale_and_current_rows() -> list[dict]:
    base = {
        "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
        "approved_for_rerun": True, "conflicts": [],
    }
    return [
        {
            **base, "suggestion_id": "sug_stale", "dedup_fingerprint": "fp_stale",
            "proposed_object": "finance.sales.old_metrics", "evidence": {},
        },
        {
            **base, "suggestion_id": "sug_current", "dedup_fingerprint": "fp_current",
            "proposed_object": "finance.sales.new_metrics",
            "evidence": {"render_version": MV_RENDER_VERSION},
        },
    ]


def test_a_stale_proposal_is_flagged_and_claims_no_body_checks(client, monkeypatch):
    """MV-D117 (C-2): a body rendered before MV_RENDER_VERSION proves neither
    validated nor executable, and the card is told it is stale."""
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: _stale_and_current_rows())
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)

    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")

    assert resp.status_code == 200
    by_id = {p["suggestion_id"]: p for p in resp.json()["proposals"]}
    stale, current = by_id["sug_stale"], by_id["sug_current"]
    assert stale["stale_body"] is True
    assert stale["checks"] == {"no_overlap": "PASS"}
    assert current["stale_body"] is False
    assert current["checks"] == {"validated": "PASS", "executable": "PASS", "no_overlap": "PASS"}


def test_the_rerun_gate_excludes_a_stale_proposal(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: _stale_and_current_rows())

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true")

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_the_unfiltered_space_list_keeps_a_stale_proposal(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: _stale_and_current_rows())
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert resp.status_code == 200
    flags = {p["suggestion_id"]: p["stale_body"] for p in resp.json()["proposals"]}
    assert flags == {"sug_stale": True, "sug_current": False}


def _stale_beside_its_successor(stale_object: str, **stale_extra) -> list[dict]:
    """A re-scan moved the bundle key, so the stale row outlived its refresh."""
    stale, current = _stale_and_current_rows()
    return [{**stale, "proposed_object": stale_object, **stale_extra}, current]


@pytest.mark.parametrize(
    "stale_object", ["finance.sales.new_metrics", "`Finance`.`Sales`.`NEW_METRICS`"],
)
def test_a_stale_proposal_with_a_current_sibling_leaves_the_list(client, monkeypatch, stale_object):
    """MV-D117: the current card of the same view is the live suggestion."""
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: _stale_beside_its_successor(stale_object),
    )
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_the_run_keyed_list_drops_a_stale_sibling(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: _stale_beside_its_successor("finance.sales.new_metrics"),
    )
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")
    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_an_approved_stale_proposal_with_a_current_sibling_leaves_the_list(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: _stale_beside_its_successor(
            "finance.sales.new_metrics", decision="approved",
        ),
    )
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_a_stale_proposal_with_a_current_sibling_leaves_the_gate(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: _stale_beside_its_successor("finance.sales.new_metrics"),
    )

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_the_sibling_rule_drops_only_a_stale_row_a_current_row_names():
    current = {"proposed_object": "c.s.v", "evidence": {"render_version": MV_RENDER_VERSION}}
    rows = [
        {"suggestion_id": "stale_v", "proposed_object": "`C`.`S`.`V`", "evidence": {}},
        {**current, "suggestion_id": "current_v1"},
        {**current, "suggestion_id": "current_v2"},
        {"suggestion_id": "stale_w1", "proposed_object": "c.s.w", "evidence": {}},
        {"suggestion_id": "stale_w2", "proposed_object": "c.s.w", "evidence": {}},
        {"suggestion_id": "blank", "proposed_object": None, "evidence": {}},
    ]

    kept = auto_optimize._drop_stale_with_current_sibling(rows)

    assert [r["suggestion_id"] for r in kept] == [
        "current_v1", "current_v2", "stale_w1", "stale_w2", "blank",
    ]


# ── MV-D122: a reshaped bundle is not listed beside its successor ──────────

_RESHAPED_VIEW = "main.sales.orders_metrics"
_SUG_OLD = "sug_" + "a" * 12
_SUG_NEW = "sug_" + "b" * 12
_SUG_MID = "sug_" + "c" * 12


def _reshaped_row(suggestion_id: str, fingerprint: str, updated_at, **extra) -> dict:
    return {
        "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
        "approved_for_rerun": False, "conflicts": [],
        "suggestion_id": suggestion_id, "dedup_fingerprint": fingerprint,
        "proposed_object": _RESHAPED_VIEW, "decision": None, "updated_at": updated_at,
        "evidence": {"render_version": MV_RENDER_VERSION},
        **extra,
    }


def _reshaped_pair(old_at="2026-10-01T10:00:00", new_at="2026-10-01T11:00:00", **old_extra):
    return [
        _reshaped_row(_SUG_OLD, "a" * 64, old_at, **old_extra),
        _reshaped_row(_SUG_NEW, "b" * 64, new_at),
    ]


def _none_created(suggestion_ids) -> set[str]:
    return set()


class _RecordingLookup:
    def __init__(self, created=()):
        self.created = set(created)
        self.calls: list[list[str]] = []

    def __call__(self, suggestion_ids):
        self.calls.append(list(suggestion_ids))
        return set(self.created)


def _ids(rows) -> list[str]:
    return [r["suggestion_id"] for r in rows]


def _stub_space_list(monkeypatch, rows) -> None:
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: rows)
    monkeypatch.setattr(warehouse, "wh_load_latest_advice_scan", lambda *a, **k: None)
    monkeypatch.setattr(auto_optimize, "_mv_fetch_space_config", lambda space_id: None)


def _ledger_query(ledger: dict[str, str]):
    """A fake warehouse read over a created-objects ledger of id -> provenance."""

    def _query(ws, warehouse_id, sql):
        import re as _re

        assert "genie_opt_mv_created_objects" in sql
        asked = set(_re.findall(r"'(sug_[0-9a-f]{12})'", sql))
        return pd.DataFrame({"suggestion_id": sorted(asked & set(ledger))})

    return _query


def test_the_older_undecided_row_of_a_view_leaves_the_list(client, monkeypatch):
    _stub_space_list(monkeypatch, _reshaped_pair())
    monkeypatch.setattr(warehouse, "sql_warehouse_query", _ledger_query({}))

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_NEW]


def test_an_approved_row_stays_beside_a_newer_undecided_one():
    kept = auto_optimize._live_proposal_rows(
        _reshaped_pair(decision="approved"), created_lookup=_none_created,
    )
    assert _ids(kept) == [_SUG_OLD, _SUG_NEW]


def test_a_rejected_row_stays_beside_a_newer_undecided_one():
    kept = auto_optimize._live_proposal_rows(
        _reshaped_pair(decision="rejected"), created_lookup=_none_created,
    )
    assert _ids(kept) == [_SUG_OLD, _SUG_NEW]


def test_a_created_row_stays_beside_a_newer_undecided_one(client, monkeypatch):
    """Create-at-approval records no decision (Ruling 7 amendment); the
    created-objects ledger is what keeps the row."""
    _stub_space_list(monkeypatch, _reshaped_pair())
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query", _ledger_query({_SUG_OLD: "OBO_CREATED"}),
    )

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_OLD, _SUG_NEW]


def test_a_claimed_row_stays_beside_a_newer_undecided_one(client, monkeypatch):
    _stub_space_list(monkeypatch, _reshaped_pair())
    monkeypatch.setattr(
        warehouse, "sql_warehouse_query", _ledger_query({_SUG_OLD: "USER_CREATED"}),
    )

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_OLD, _SUG_NEW]


def test_a_created_row_hides_no_other_row():
    rows = [
        _reshaped_row(_SUG_OLD, "a" * 64, "2026-10-01T12:00:00"),
        _reshaped_row(_SUG_MID, "c" * 64, "2026-10-01T10:00:00"),
        _reshaped_row(_SUG_NEW, "b" * 64, "2026-10-01T11:00:00"),
    ]

    kept = auto_optimize._live_proposal_rows(
        rows, created_lookup=_RecordingLookup(created={_SUG_OLD}),
    )

    assert _ids(kept) == [_SUG_OLD, _SUG_NEW]


def test_the_newest_wins_across_timestamp_types():
    later_naive = datetime(2026, 10, 1, 12, 0)  # noqa: DTZ001 - a naive cell
    rows = _reshaped_pair(old_at=later_naive, new_at="2026-10-01T11:30:00Z")
    assert _ids(auto_optimize._live_proposal_rows(rows, created_lookup=_none_created)) == [
        _SUG_OLD
    ]

    earlier_aware = datetime(2026, 10, 1, 11, 0, tzinfo=UTC)
    rows = _reshaped_pair(old_at=earlier_aware, new_at="2026-10-01 11:30:00")
    assert _ids(auto_optimize._live_proposal_rows(rows, created_lookup=_none_created)) == [
        _SUG_NEW
    ]


def test_equal_timestamps_keep_the_greater_fingerprint():
    same = "2026-10-01T11:00:00"
    rows = [
        _reshaped_row(_SUG_NEW, "b" * 64, same),
        _reshaped_row(_SUG_OLD, "a" * 64, same),
    ]
    assert _ids(auto_optimize._live_proposal_rows(rows, created_lookup=_none_created)) == [
        _SUG_NEW
    ]
    assert _ids(
        auto_optimize._live_proposal_rows(list(reversed(rows)), created_lookup=_none_created)
    ) == [_SUG_NEW]


def test_names_compare_case_and_backtick_insensitively():
    rows = _reshaped_pair(proposed_object="`Main`.sales.ORDERS_metrics")

    assert _ids(auto_optimize._live_proposal_rows(rows, created_lookup=_none_created)) == [
        _SUG_NEW
    ]


@pytest.mark.parametrize(
    "url",
    [
        "/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals",
        "/api/auto-optimize/spaces/space-1/mv-proposals",
    ],
    ids=["run-list", "space-list"],
)
def test_the_newest_undecided_drop_applies_at_every_list_site(client, monkeypatch, url):
    """The run and space lists; the suggest and stream reloads are pinned in
    ``test_mv_suggest.py`` and the semantic graph in ``test_semantic_graph.py``."""
    _stub_space_list(monkeypatch, _reshaped_pair())
    lookup_ws: list = []

    def _created(ws, warehouse_id, *, catalog, schema, suggestion_ids):
        lookup_ws.append((warehouse_id, catalog, schema, sorted(suggestion_ids)))
        return set()

    monkeypatch.setattr(warehouse, "wh_created_suggestion_ids", _created)

    resp = client.get(url)

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_NEW]
    assert lookup_ws == [("wh-test", "main", "gso_test", [_SUG_OLD, _SUG_NEW])]


def test_the_rerun_gate_is_unchanged(client, monkeypatch):
    rows = _reshaped_pair(decision="approved", approved_for_rerun=True)
    rows[1].update(decision="approved", approved_for_rerun=True)
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: rows)
    lookup = _RecordingLookup()
    monkeypatch.setattr(
        warehouse, "wh_created_suggestion_ids", lambda *a, **k: lookup(k["suggestion_ids"]),
    )

    resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals?approved_for_rerun=true")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_OLD, _SUG_NEW]
    assert lookup.calls == []


def test_the_lookup_is_not_called_when_no_view_has_two_undecided_rows():
    rows = [
        _reshaped_row(_SUG_OLD, "a" * 64, "2026-10-01T10:00:00", decision="approved"),
        _reshaped_row(_SUG_NEW, "b" * 64, "2026-10-01T11:00:00"),
        _reshaped_row(
            _SUG_MID, "c" * 64, "2026-10-01T12:00:00",
            proposed_object="main.sales.returns_metrics",
        ),
        _reshaped_row(
            "sug_" + "d" * 12, "d" * 64, "2026-10-01T09:00:00",
            proposed_object="main.sales.returns_metrics", evidence={},
        ),
    ]
    lookup = _RecordingLookup()

    kept = auto_optimize._live_proposal_rows(rows, created_lookup=lookup)

    assert lookup.calls == []
    assert _ids(kept) == [_SUG_OLD, _SUG_NEW, _SUG_MID]


def test_the_lookup_is_asked_once_and_only_about_contested_rows():
    rows = [
        _reshaped_row(_SUG_OLD, "a" * 64, "2026-10-01T10:00:00"),
        _reshaped_row(_SUG_NEW, "b" * 64, "2026-10-01T11:00:00"),
        _reshaped_row("sug_" + "e" * 12, "e" * 64, "2026-10-01T09:00:00", decision="approved"),
        _reshaped_row(
            _SUG_MID, "c" * 64, "2026-10-01T12:00:00",
            proposed_object="main.sales.returns_metrics",
        ),
        _reshaped_row(
            "sug_" + "f" * 12, "f" * 64, "2026-10-01T13:00:00",
            proposed_object="main.sales.refunds_metrics",
        ),
        _reshaped_row(
            "sug_" + "9" * 12, "9" * 64, "2026-10-01T14:00:00",
            proposed_object="main.sales.refunds_metrics",
        ),
    ]
    lookup = _RecordingLookup()

    kept = auto_optimize._live_proposal_rows(rows, created_lookup=lookup)

    assert [sorted(call) for call in lookup.calls] == [
        sorted([_SUG_OLD, _SUG_NEW, "sug_" + "f" * 12, "sug_" + "9" * 12])
    ]
    assert _ids(kept) == [_SUG_NEW, "sug_" + "e" * 12, _SUG_MID, "sug_" + "9" * 12]


def test_a_failed_ledger_read_drops_no_undecided_row_and_logs_its_type_only(
    client, monkeypatch, caplog,
):
    _stub_space_list(monkeypatch, _reshaped_pair())

    def _boom(*args, **kwargs):
        raise RuntimeError("zq_ledger_sentinel")

    monkeypatch.setattr(warehouse, "wh_created_suggestion_ids", _boom)

    with caplog.at_level(logging.DEBUG):
        resp = client.get("/api/auto-optimize/spaces/space-1/mv-proposals")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_OLD, _SUG_NEW]
    assert not any("zq_ledger_sentinel" in r.getMessage() for r in caplog.records)
    assert all(not r.exc_info for r in caplog.records)
    assert any("RuntimeError" in r.getMessage() for r in caplog.records)


def test_a_failed_ledger_read_still_drops_a_stale_sibling():
    stale = _reshaped_row(_SUG_MID, "c" * 64, "2026-10-01T09:00:00", evidence={})

    def _boom(suggestion_ids):
        raise RuntimeError("zq_ledger_sentinel")

    kept = auto_optimize._live_proposal_rows([stale, *_reshaped_pair()], created_lookup=_boom)

    assert _ids(kept) == [_SUG_OLD, _SUG_NEW]


def test_live_proposal_rows_requires_an_explicit_lookup():
    with pytest.raises(TypeError):
        auto_optimize._live_proposal_rows(_reshaped_pair())


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-10-01T11:00:00Z", (2026, 10, 1, 11, 0)),
        ("2026-10-01T11:00:00+00:00", (2026, 10, 1, 11, 0)),
        ("2026-10-01T06:00:00-05:00", (2026, 10, 1, 11, 0)),
        ("2026-10-01T11:00:00", (2026, 10, 1, 11, 0)),
        ("2026-10-01 11:00:00.123", (2026, 10, 1, 11, 0)),
    ],
)
def test_the_proposal_instant_reads_an_iso_string_naive_as_utc(value, expected):
    instant = auto_optimize._proposal_instant(value)

    assert instant.tzinfo is not None
    assert instant.astimezone(UTC).timetuple()[:5] == expected


def test_the_proposal_instant_reads_a_datetime_naive_as_utc():
    naive = auto_optimize._proposal_instant(datetime(2026, 10, 1, 11, 0))  # noqa: DTZ001
    aware = auto_optimize._proposal_instant(
        datetime(2026, 10, 1, 6, 0, tzinfo=timezone(timedelta(hours=-5)))
    )

    assert naive == aware == datetime(2026, 10, 1, 11, 0, tzinfo=UTC)


@pytest.mark.parametrize("value", [None, "", "not a time", float("nan"), 17, object()])
def test_an_unparsable_instant_sorts_oldest(value):
    oldest = auto_optimize._proposal_instant(value)

    assert oldest < auto_optimize._proposal_instant(datetime(1970, 1, 1, tzinfo=UTC))
    assert oldest == auto_optimize._proposal_instant(None)


def test_get_mv_ddl_surfaces_yaml_and_grant(client, monkeypatch):
    # Deployed-review fix: the card GRANT names the GSO service principal (the one
    # grant that matters functionally — the optimizer must read the view on a
    # create-and-attach run), not the broad space audience. Never a `<grantee>`.
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e",
    )
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "join_strategy": "subquery_source",
            "yaml_text": "version: 0.1\n",
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
            "render_version": MV_RENDER_VERSION,
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    data = resp.json()
    assert data["yaml_text"] == "version: 0.1\n"
    assert (
        "GRANT SELECT ON VIEW `finance`.`sales`.`revenue_metrics` TO "
        "`a803ebc5-232f-44c0-9ed6-fb17d7c77f9e`;" in data["grant_sql"]
    )
    assert "<grantee>" not in data["grant_sql"]
    # Exactly one GRANT statement now — the "why so many grants?" noise is gone.
    assert data["grant_sql"].count("GRANT SELECT") == 1


def test_get_mv_ddl_parses_source_tables_from_yaml(client, monkeypatch):
    """Deployed review #2/#3: the card's Source column is fed serve-time from the
    rendered YAML's ``source:`` (base + each join), so an existing candidate shows
    its sources with no re-scan and no extra column on the row."""
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "join_strategy": "denormalized",
            "yaml_text": (
                "version: '1.1'\n"
                "source: finance.sales.fact_orders\n"
                "joins:\n"
                "  - name: customer\n"
                "    source: `finance`.`sales`.`dim_customer`\n"
                "    on: fact_orders.customer_id = dim_customer.id\n"
            ),
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
            "render_version": MV_RENDER_VERSION,
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    assert resp.json()["source_tables"] == [
        "finance.sales.fact_orders",
        "finance.sales.dim_customer",
    ]


def test_get_mv_ddl_grant_says_so_in_words_when_no_sp_resolves(client, monkeypatch):
    """No resolvable optimizer SP → the GRANT says so in words rather than emitting
    a placeholder that cannot run (a commented instruction, no runnable statement
    with a fake principal)."""
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "yaml_text": "version: 0.1\n",
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
            "render_version": MV_RENDER_VERSION,
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    grant = resp.json()["grant_sql"]
    assert "<grantee>" not in grant
    assert "Could not resolve the optimizer service principal" in grant
    # Nothing executable with an invented principal — every SQL line is commented.
    assert not any(
        line.strip() and not line.strip().startswith("--") for line in grant.splitlines()
    )


def test_get_mv_ddl_404_when_absent(client, monkeypatch):
    # No artifact AND no candidate fallback (an advice run with nothing rendered):
    # the route still 404s. The fallback is stubbed to None so this pins the
    # empty case, not the warehouse read.
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_fallback", lambda run_id, suggestion_id: None
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 404


def test_get_mv_ddl_pins_artifact_by_suggestion_id(client, monkeypatch):
    """A multi-bundle run writes one mv_candidate_ddl artifact per view, so blind
    latest-wins can only surface one card's DDL. When ``suggestion_id`` is given,
    the route must return THAT bundle's artifact — not whichever was written last —
    so every card fetches its own body."""
    import json as _json

    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")

    def _artifact(sug: str, obj: str) -> dict:
        return {
            "suggestion_id": sug, "dedup_fingerprint": f"fp_{sug}",
            "target_space_id": "space-1", "proposed_object": obj,
            "join_strategy": "direct", "yaml_text": f"version: 0.1  # {sug}\n",
            "ddl": f"CREATE VIEW {obj} ...", "validation": {"ok": True},
            "render_version": MV_RENDER_VERSION,
        }

    # Rows come back created_at DESC — sugB (franchises) is the LATEST artifact,
    # sugA (transactions) the earlier one. Asking for sugA must still get sugA.
    rows = [
        {"artifact_json": _json.dumps(_artifact("sugB", "cat.sch.sales_franchises_metrics"))},
        {"artifact_json": _json.dumps(_artifact("sugA", "cat.sch.sales_transactions_metrics"))},
    ]
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda *a, **k: rows)
    # If suggestion pinning silently missed, the route would fall through here.
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_fallback",
        lambda run_id, suggestion_id: None,
    )

    run = "11111111-1111-4111-8111-111111111111"
    resp = client.get(f"/api/auto-optimize/runs/{run}/mv-ddl?suggestion_id=sugA")
    assert resp.status_code == 200
    assert resp.json()["suggestion_id"] == "sugA"
    assert resp.json()["proposed_object"] == "cat.sch.sales_transactions_metrics"


def test_get_mv_ddl_falls_back_to_candidate_yaml_text(client, monkeypatch):
    """MV-D23 / Prompt 15.1: with no run-partitioned artifact (an advice run),
    route 7 renders the DDL from the candidate row's yaml_text — best-wins on the
    wh_load_mv_candidates ordering — so a never-optimized space serves copy-ready
    DDL instead of 404ing. validation is None on this preview path (documented)."""
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    # Deployed-review fix: the advice-run fallback GRANT also names the GSO SP.
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e",
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sugA", "dedup_fingerprint": "fpA",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "yaml_text": "version: 0.1\n",
            "evidence": {"join_strategy": "subquery_source", "render_version": MV_RENDER_VERSION},
        }],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    data = resp.json()
    assert data["suggestion_id"] == "sugA"
    assert data["yaml_text"] == "version: 0.1\n"
    assert data["join_strategy"] == "subquery_source"
    assert "CREATE VIEW `finance`.`sales`.`revenue_metrics`" in data["ddl"]
    assert (
        "GRANT SELECT ON VIEW `finance`.`sales`.`revenue_metrics` TO "
        "`a803ebc5-232f-44c0-9ed6-fb17d7c77f9e`;" in data["grant_sql"]
    )
    assert data["validation"] is None


def test_mv_ddl_refuses_a_stale_artifact(client, monkeypatch):
    """MV-D117 (C-2): the app no longer serves a body it would not create."""
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: {
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "join_strategy": "direct", "yaml_text": "version: 0.1\n",
            "ddl": "CREATE VIEW finance.sales.revenue_metrics ...",
            "validation": {"ok": True},
        },
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 409
    assert resp.json()["detail"] == mv_create.STALE_BODY_REASON


def test_mv_ddl_refuses_a_stale_candidate_row(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "sugA", "dedup_fingerprint": "fpA",
            "target_space_id": "space-1",
            "proposed_object": "finance.sales.revenue_metrics",
            "yaml_text": "version: 0.1\n",
            "evidence": {"join_strategy": "direct"},
        }],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 409
    assert resp.json()["detail"] == mv_create.STALE_BODY_REASON


def _ddl_payload(**extra):
    return {
        "suggestion_id": "sug1", "dedup_fingerprint": "fp1", "target_space_id": "space-1",
        "proposed_object": "finance.sales.revenue_metrics", "yaml_text": "version: 0.1\n",
        "ddl": "CREATE VIEW finance.sales.revenue_metrics ...", "validation": {"ok": True},
        **extra,
    }


def test_mv_ddl_falls_back_to_a_current_candidate_body(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(auto_optimize, "_load_candidate_ddl_artifact", lambda *a: _ddl_payload())
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_fallback",
        lambda *a: _ddl_payload(yaml_text="version: '1.1'\n", render_version=MV_RENDER_VERSION),
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    assert resp.json()["yaml_text"] == "version: '1.1'\n"


def test_mv_ddl_refuses_when_both_bodies_are_stale(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_load_candidate_ddl_artifact", lambda *a: _ddl_payload())
    monkeypatch.setattr(auto_optimize, "_load_candidate_ddl_fallback", lambda *a: _ddl_payload())
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 409


def test_mv_ddl_does_not_read_the_fallback_for_a_current_artifact(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_artifact",
        lambda *a: _ddl_payload(render_version=MV_RENDER_VERSION),
    )
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_fallback",
        lambda *a: pytest.fail("fallback read for a current artifact"),
    )
    assert client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl").status_code == 200


def _ddl_candidate(sug: str, *, current: bool = True) -> dict:
    evidence = {"join_strategy": "direct"}
    if current:
        evidence["render_version"] = MV_RENDER_VERSION
    return {
        "suggestion_id": sug, "dedup_fingerprint": f"fp_{sug}", "target_space_id": "space-1",
        "proposed_object": f"finance.sales.{sug}_metrics",
        "yaml_text": f"version: '1.1'  # {sug}\n", "evidence": evidence,
    }


def test_an_unpinned_stale_artifact_falls_back_to_its_own_proposal(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: _ddl_payload(suggestion_id="sug_a"),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [_ddl_candidate("sug_b"), _ddl_candidate("sug_a")],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    assert resp.json()["suggestion_id"] == "sug_a"
    assert resp.json()["yaml_text"] == "version: '1.1'  # sug_a\n"


def test_an_unpinned_stale_artifact_whose_proposal_is_stale_is_refused(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: _ddl_payload(suggestion_id="sug_a"),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [_ddl_candidate("sug_b"), _ddl_candidate("sug_a", current=False)],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 409
    assert resp.json()["detail"] == mv_create.STALE_BODY_REASON
    assert "sug_b" not in resp.text


def test_an_unpinned_stale_artifact_with_no_suggestion_id_is_refused(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_latest_artifact",
        lambda run_id, kind: _ddl_payload(suggestion_id=None),
    )
    calls = []

    def fallback(run_id, suggestion_id):
        calls.append(suggestion_id)
        return _ddl_payload(suggestion_id="sug_b", render_version=MV_RENDER_VERSION)

    monkeypatch.setattr(auto_optimize, "_load_candidate_ddl_fallback", fallback)
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 409
    assert resp.json()["detail"] == mv_create.STALE_BODY_REASON
    assert calls == []


def test_an_unpinned_run_with_no_artifact_serves_the_best_row(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(auto_optimize, "_load_latest_artifact", lambda run_id, kind: None)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [_ddl_candidate("sug_b"), _ddl_candidate("sug_a")],
    )
    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl")
    assert resp.status_code == 200
    assert resp.json()["suggestion_id"] == "sug_b"


def test_the_grant_quotes_a_spaced_view_name():
    grant = auto_optimize._mv_optimizer_grant_sql(
        "main.sales.order revenue", "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e"
    )
    assert "`main`.`sales`.`order revenue`" in grant


_CORRUPT_NAME = "main..orders_metrics"


def _assert_no_name_and_no_traceback(records) -> None:
    assert all(_CORRUPT_NAME not in r.getMessage() for r in records)
    assert all(not r.exc_info for r in records)


def test_the_grant_helper_returns_none_for_a_refused_name(caplog):
    """MV-D122: a name ``quote_fqn`` refuses offers no GRANT rather than raising."""
    with caplog.at_level("DEBUG", logger="backend.routers.auto_optimize"):
        grant = auto_optimize._mv_optimizer_grant_sql(
            _CORRUPT_NAME, "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e"
        )
    assert grant is None
    assert caplog.records
    assert "ValueError" in caplog.text
    _assert_no_name_and_no_traceback(caplog.records)


def test_a_corrupt_stored_name_yields_no_grant_on_mv_ddl(client, monkeypatch, caplog):
    monkeypatch.setattr(
        auto_optimize, "_gso_sp_application_id",
        lambda: "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e",
    )
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_artifact",
        lambda *a: _ddl_payload(
            proposed_object=_CORRUPT_NAME, render_version=MV_RENDER_VERSION
        ),
    )
    with caplog.at_level("DEBUG"):
        resp = client.get(
            "/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl"
        )
    assert resp.status_code == 200
    assert resp.json()["grant_sql"] is None
    _assert_no_name_and_no_traceback(caplog.records)


def test_the_grant_helper_offers_no_text_for_a_refused_name_with_no_sp(caplog):
    """F8: the no-SP branch's copyable comment never echoes a name ``quote_fqn`` refuses."""
    with caplog.at_level("DEBUG", logger="backend.routers.auto_optimize"):
        grant = auto_optimize._mv_optimizer_grant_sql(_CORRUPT_NAME, "")
    assert grant is None
    assert "ValueError" in caplog.text
    _assert_no_name_and_no_traceback(caplog.records)


def test_a_corrupt_stored_name_yields_no_grant_on_mv_ddl_with_no_sp(client, monkeypatch, caplog):
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(
        auto_optimize, "_load_candidate_ddl_artifact",
        lambda *a: _ddl_payload(
            proposed_object=_CORRUPT_NAME, render_version=MV_RENDER_VERSION
        ),
    )
    with caplog.at_level("DEBUG"):
        resp = client.get(
            "/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl"
        )
    assert resp.status_code == 200
    assert resp.json()["grant_sql"] is None
    _assert_no_name_and_no_traceback(caplog.records)


def test_the_no_sp_grant_text_names_the_quoted_view():
    grant = auto_optimize._mv_optimizer_grant_sql("main.sales.order revenue", "")
    assert "`main`.`sales`.`order revenue`" in grant
    assert not any(
        line.strip() and not line.strip().startswith("--") for line in grant.splitlines()
    )


def test_the_run_list_returns_empty_when_the_sp_client_cannot_be_built(client, monkeypatch):
    """F6: as before M7e-1, a client that cannot be built lists nothing; it never 500s."""
    def _no_client():
        raise RuntimeError("no service principal")

    loads: list = []
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", _no_client)
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: loads.append(a) or [])

    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")

    assert resp.status_code == 200
    assert resp.json()["proposals"] == []
    assert loads == []


def test_the_run_list_builds_one_sp_client(client, monkeypatch):
    clients: list = []

    def _client():
        clients.append(MagicMock())
        return clients[-1]

    seen: list = []
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", _client)
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda ws, *a, **k: seen.append(ws) or [{
            "suggestion_id": "sug1", "dedup_fingerprint": "fp1",
            "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
        }],
    )

    resp = client.get("/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals")

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug1"]
    assert len(clients) == 1
    assert seen == clients


def test_decision_records_and_flips_rerun(client, monkeypatch):
    recorded: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{"suggestion_id": "sug1", "dedup_fingerprint": "fp1"}],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: recorded.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/sug1/decision",
        json={"space_id": "space-1", "decision": "approved"},
        headers={"x-forwarded-email": "analyst@example.com"},
    )
    assert resp.status_code == 200
    assert resp.json()["approved_for_rerun"] is True
    assert recorded[0]["dedup_fingerprint"] == "fp1"
    assert recorded[0]["decided_by"] == "analyst@example.com"


def test_reject_fans_out_to_per_measure_suppression(client, monkeypatch):
    """MV-D30 as-implemented (Prompt 15.3): rejecting a view-grained bundle must
    suppress each member measure, not just the bundle key. The bundle's
    dedup_fingerprint is membership-sensitive, so recording the bundle decision
    alone lets a rejected measure resurface inside a differently-membered bundle.
    The router fans the rejection out to the per-measure suppression ledger,
    reading member fingerprints from the row's evidence.measures[]."""
    recorded: list[dict] = []
    suppressed: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "bundle1",
            "dedup_fingerprint": "bundle-fp",
            "evidence": {"measures": [
                {"dedup_fingerprint": "m-fp-1"},
                {"dedup_fingerprint": "m-fp-2"},
            ]},
        }],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: recorded.append(kw),
    )
    monkeypatch.setattr(
        warehouse, "wh_suppress_mv_measures",
        lambda ws, warehouse_id, **kw: suppressed.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/bundle1/decision",
        json={"space_id": "space-1", "decision": "rejected"},
        headers={"x-forwarded-email": "analyst@example.com"},
    )
    assert resp.status_code == 200
    assert resp.json()["approved_for_rerun"] is False
    # The bundle-row decision is still recorded, keyed on the view-grained key.
    assert recorded[0]["dedup_fingerprint"] == "bundle-fp"
    # AND every member fingerprint is written to the suppression ledger,
    # tagged with the originating bundle so the fan-out is auditable.
    assert len(suppressed) == 1
    assert set(suppressed[0]["measure_fingerprints"]) == {"m-fp-1", "m-fp-2"}
    assert suppressed[0]["originating_suggestion_id"] == "bundle1"
    assert suppressed[0]["target_space_id"] == "space-1"


def test_approve_does_not_suppress_members(client, monkeypatch):
    """Approval stays bundle-grained: approving a bundle records the decision but
    never writes to the suppression ledger (per-measure partial approval is out
    of scope — MV-D30 future note)."""
    suppressed: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates",
        lambda *a, **k: [{
            "suggestion_id": "bundle1",
            "dedup_fingerprint": "bundle-fp",
            "evidence": {"measures": [{"dedup_fingerprint": "m-fp-1"}]},
        }],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: None,
    )
    monkeypatch.setattr(
        warehouse, "wh_suppress_mv_measures",
        lambda ws, warehouse_id, **kw: suppressed.append(kw),
    )
    resp = client.post(
        "/api/auto-optimize/mv/proposals/bundle1/decision",
        json={"space_id": "space-1", "decision": "approved"},
    )
    assert resp.status_code == 200
    assert suppressed == []


def test_decision_404_when_suggestion_not_in_space(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])
    resp = client.post(
        "/api/auto-optimize/mv/proposals/sug1/decision",
        json={"space_id": "space-1", "decision": "rejected"},
    )
    assert resp.status_code == 404


_DROP_RUN = "33333333-3333-4333-8333-333333333333"


def _created_row(status="DETACHED", created_by="analyst@example.com"):
    return {
        "run_id": _DROP_RUN, "suggestion_id": "sug1",
        "full_name": "finance.sales.revenue_metrics",
        "created_by": created_by, "status": status,
    }


def _obo_as(email):
    ws = MagicMock()
    ws.current_user.me.return_value = SimpleNamespace(user_name=email)
    return ws


def test_drop_requires_confirm(client):
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": False},
    )
    assert resp.status_code == 400


def test_drop_happy_path(client, monkeypatch):
    executed: list[str] = []
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row())
    monkeypatch.setattr(warehouse, "sql_warehouse_execute",
                        lambda ws, warehouse_id, sql: executed.append(sql))
    monkeypatch.setattr(warehouse, "wh_update_mv_created_object_status",
                        lambda *a, **k: None)
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 200
    assert resp.json()["dropped"] is True
    assert any("DROP VIEW IF EXISTS `finance`.`sales`.`revenue_metrics`" in s for s in executed)


def test_drop_accepts_a_backticked_three_part_name(client, monkeypatch):
    executed: list[str] = []
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: {**_created_row(), "full_name": "`main`.`sales`.`revenue_metrics`"})
    monkeypatch.setattr(warehouse, "sql_warehouse_execute",
                        lambda ws, warehouse_id, sql: executed.append(sql))
    monkeypatch.setattr(warehouse, "wh_update_mv_created_object_status",
                        lambda *a, **k: None)
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 200
    assert resp.json()["dropped"] is True
    assert executed == ["DROP VIEW IF EXISTS `main`.`sales`.`revenue_metrics`"]


@pytest.mark.parametrize(
    "full_name", ["finance.sales.revenue metrics", "finance.sales", "finance.sales.x; DROP TABLE y"],
)
def test_drop_refuses_a_recorded_name_that_is_not_plain(client, monkeypatch, full_name):
    executed: list[str] = []
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: {**_created_row(), "full_name": full_name})
    monkeypatch.setattr(warehouse, "sql_warehouse_execute",
                        lambda ws, warehouse_id, sql: executed.append(sql))
    monkeypatch.setattr(warehouse, "wh_update_mv_created_object_status",
                        lambda *a, **k: None)
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 409
    assert "SQL editor" in resp.json()["detail"]
    assert executed == []


def test_drop_forbidden_for_non_owner(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("someone-else@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row(created_by="owner@example.com"))
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 403


def test_drop_refuses_a_non_detached_object(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client",
                        lambda: _obo_as("analyst@example.com"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object",
                        lambda *a, **k: _created_row(status="ATTACHED"))
    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop", json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 409


def test_drop_asks_run_space_access_before_owner_check(client, monkeypatch):
    """Denied Can Edit on the run's space returns 403 before current_user.me."""
    obo_ws = MagicMock()
    me = MagicMock()
    obo_ws.current_user.me = me
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", lambda: obo_ws)
    ledger = MagicMock(side_effect=AssertionError("ledger load ran before the space gate"))
    monkeypatch.setattr(warehouse, "wh_load_mv_created_object", ledger)

    async def deny(space_id, level):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "space_access_denied",
                "required": "edit",
                "message": "You need Can Edit permission on this Genie Agent.",
                "platform_message": 'You need "Can Edit" permission to perform this action',
            },
        )

    monkeypatch.setattr(auto_optimize, "require_space_access", deny)

    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop",
        json={"run_id": _DROP_RUN, "confirm": True},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"]["code"] == "space_access_denied"
    me.assert_not_called()
    ledger.assert_not_called()


def test_drop_rejects_a_malformed_run_id_before_any_io(client, monkeypatch):
    """A non-uuid body run_id is a 422 before the run gate touches Lakebase or Delta."""
    monkeypatch.setattr(auto_optimize, "_load_run_envelope", _REAL_LOAD_RUN_ENVELOPE)
    touched: list[str] = []

    async def lakebase_miss(run_id):
        touched.append(f"lakebase:{run_id}")
        return None

    def delta(sql, *, strict=False):
        touched.append(sql)
        return []

    monkeypatch.setattr(auto_optimize.gso_lakebase, "load_gso_run", lakebase_miss)
    monkeypatch.setattr(auto_optimize, "_delta_query", delta)

    resp = client.post(
        "/api/auto-optimize/mv/created/sug1/drop",
        json={"run_id": "x' UNION SELECT 'a', 'attacker-space', 'X' --", "confirm": True},
    )
    assert resp.status_code == 422
    assert touched == []


# ── Created-object results read (Prompt 13 step 0) ─────────────────────────

_RUN_UUID = "22222222-2222-4222-8222-222222222222"

_LIFT = {
    "delta_affected": -0.07, "delta_suite": -0.03,
    "regressed_question_ids": ["bq_0007"], "needs_review_count": 3,
    "pre_eval_run_id": "eval_a1", "post_eval_run_id": "eval_b2",
    "question_subset": ["bq_0007", "bq_0019"],
    "pre_accuracy_affected": 0.78, "post_accuracy_affected": 0.71,
    "pre_accuracy_suite": 0.80, "post_accuracy_suite": 0.77,
    "needs_review_question_ids": ["bq_0022", "bq_0033", "bq_0041"],
    "graded_affected_count": 12, "graded_suite_count": 40,
}


def test_list_mv_created_returns_objects_with_lift(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [{
            "run_id": "r1", "suggestion_id": "sug1",
            "full_name": "finance.sales.order_revenue",
            "created_by": "analyst@example.com", "status": "DETACHED",
            "baseline_eval_run_id": "eval_a1", "post_attach_eval_run_id": "eval_b2",
            "on_regression_action": "DETACH_ONLY_NEVER_DROP",
            "lift_report": _LIFT,
        }],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    data = resp.json()
    obj = data["created"][0]
    assert obj["full_name"] == "finance.sales.order_revenue"
    assert obj["status"] == "DETACHED"
    # The 14-key lift shape is mirrored verbatim, not reshaped.
    assert obj["lift_report"]["pre_accuracy_affected"] == 0.78
    assert obj["lift_report"]["post_accuracy_affected"] == 0.71
    assert obj["lift_report"]["needs_review_count"] == 3
    assert obj["lift_report"]["pre_eval_run_id"] == "eval_a1"
    assert data["downgrade_reason"] is None


def test_list_mv_created_returns_provenance(client, monkeypatch):
    # Prompt 14.1 (exposure-matrix GAP 1): route 10 surfaces provenance so a
    # reloaded UI can hide the Drop affordance on USER_CREATED views. A row with
    # no provenance column reads as the legacy OBO_CREATED (NULL convention).
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [
            {
                "run_id": "r1", "suggestion_id": "byo1",
                "full_name": "finance.sales.net_revenue",
                "created_by": "prashanth@example.com", "status": "CREATED",
                "provenance": "USER_CREATED",
            },
            {
                "run_id": "r1", "suggestion_id": "obo1",
                "full_name": "finance.sales.order_revenue",
                "created_by": "analyst@example.com", "status": "ATTACHED",
            },
        ],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    by_id = {o["suggestion_id"]: o for o in resp.json()["created"]}
    assert by_id["byo1"]["provenance"] == "USER_CREATED"
    assert by_id["obo1"]["provenance"] == "OBO_CREATED"


def test_list_mv_created_surfaces_downgrade_reason(client, monkeypatch):
    monkeypatch.setattr(warehouse, "wh_load_mv_created_objects", lambda *a, **k: [])
    monkeypatch.setattr(
        warehouse, "wh_load_mv_consent_by_run",
        lambda *a, **k: {"run_id": "r1", "downgrade_reason": "grant revoked before trigger"},
    )
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    data = resp.json()
    assert data["created"] == []
    assert data["downgrade_reason"] == "grant revoked before trigger"


def test_list_mv_created_tolerates_a_missing_lift_report(client, monkeypatch):
    monkeypatch.setattr(
        warehouse, "wh_load_mv_created_objects",
        lambda *a, **k: [{
            "run_id": "r1", "suggestion_id": "sug1",
            "full_name": "finance.sales.order_revenue",
            "status": "ATTACHED",
        }],
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    obj = resp.json()["created"][0]
    assert obj["status"] == "ATTACHED"
    assert obj["lift_report"] is None


def test_list_mv_created_returns_empty_on_read_failure(client, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(warehouse, "wh_load_mv_created_objects", _boom)
    monkeypatch.setattr(warehouse, "wh_load_mv_consent_by_run", lambda *a, **k: None)
    resp = client.get(f"/api/auto-optimize/runs/{_RUN_UUID}/mv-created")
    assert resp.status_code == 200
    assert resp.json() == {"run_id": _RUN_UUID, "created": [], "downgrade_reason": None}


# ── Trigger threading ──────────────────────────────────────────────────────


@pytest.fixture
def trigger_client(monkeypatch):
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(auto_optimize, "get_workspace_client", lambda: MagicMock())

    async def _allow_space_access(space_id, level):
        return None

    monkeypatch.setattr(auto_optimize, "require_space_access", _allow_space_access)

    captured: dict = {}

    def _fake_trigger(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            run_id="run-xyz", job_run_id="42", job_url=None, status="IN_PROGRESS",
        )

    monkeypatch.setattr(auto_optimize, "trigger_optimization", _fake_trigger)
    from backend.services.version_control.platform.identity import RUN_AS_VERIFIED

    app = FastAPI()
    app.state.gso_run_as = RUN_AS_VERIFIED
    app.include_router(auto_optimize.router)
    return TestClient(app), captured


def test_trigger_threads_mv_params_and_builds_hook(trigger_client):
    client, captured = trigger_client
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={
            "space_id": "space-1",
            "enable_metric_view_suggestions": True,
            "mv_action_mode": "create_and_attach",
            "mv_min_confidence": 80,
            "mv_approved_suggestion_ids": ["sug1"],
            "mv_consent": {
                "granted_by": "analyst@example.com",
                "granted_at": "2026-08-24T00:00:00+00:00",
                "probe_id": "p1",
            },
        },
    )
    assert resp.status_code == 200
    assert captured["enable_metric_view_suggestions"] is True
    assert captured["mv_action_mode"] == "create_and_attach"
    assert captured["mv_min_confidence"] == 80
    assert callable(captured["mv_attach_hook"])


def test_trigger_omits_hook_when_suggest_only(trigger_client):
    client, captured = trigger_client
    resp = client.post(
        "/api/auto-optimize/trigger",
        json={"space_id": "space-1", "enable_metric_view_suggestions": True},
    )
    assert resp.status_code == 200
    assert captured["mv_attach_hook"] is None
    assert captured["mv_action_mode"] == "suggest_only"


def test_run_hook_refuses_a_body_reading_a_table_the_consent_did_not_cover(create_env, monkeypatch, caplog):
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {**_ARTIFACT, "yaml_text": "version: 0.1\nsource: finance.hr.salaries\n"},
    )
    monkeypatch.setattr(
        mv_yaml, "validate", lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _run_create()
    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []
    assert "salaries" not in caplog.text


@pytest.mark.parametrize("spelling", ["'`finance`.`sales`.`orders`'", "FINANCE.SALES.ORDERS"])
def test_run_hook_matches_covered_tables_by_normalized_name(create_env, monkeypatch, spelling):
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {**_ARTIFACT, "yaml_text": f"version: 0.1\nsource: {spelling}\n"},
    )
    monkeypatch.setattr(
        mv_yaml, "validate", lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    executed, _upserts = create_env
    handoff = _run_create()
    assert handoff.action_mode == "create_and_attach"
    assert any("CREATE VIEW" in s for s in executed)


def test_the_uncovered_skip_is_named_when_nothing_builds(create_env, monkeypatch):
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {**_ARTIFACT, "yaml_text": "version: 0.1\nsource: finance.hr.salaries\n"},
    )
    monkeypatch.setattr(
        mv_yaml, "validate", lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    handoff = _run_create()
    assert "reads a table the access check did not cover" in (handoff.downgrade_reason or "")
    assert "salaries" not in (handoff.downgrade_reason or "")


def test_uncovered_tables_reads_joins_and_refuses_an_unreadable_body():
    consent = dict(_CONSENT)
    body = (
        "version: '1.1'\nsource: finance.sales.orders\njoins:\n"
        "  - name: c\n    source: finance.sales.customers\n    on: source.cid = c.id\n"
    )
    assert mv_create._uncovered_tables(body, consent) == ["finance.sales.customers"]
    assert mv_create._uncovered_tables("version: 0.1\nsource: finance.sales.orders\n", consent) == []
    assert mv_create._uncovered_tables(": not yaml :", consent) is None
    assert mv_create._uncovered_tables("version: 0.1\njoins: [1, 2]\nsource: finance.sales.orders\n", consent) is None
    assert mv_create._uncovered_tables("version: 0.1\njoins: 5\nsource: finance.sales.orders\n", consent) is None
    nested_list = (
        "version: 0.1\nsource: finance.sales.orders\njoins:\n"
        "  - name: c\n    source: finance.sales.orders\n    joins:\n"
        "      - - source: finance.hr.salaries\n"
    )
    assert mv_create._uncovered_tables(nested_list, consent) is None
    assert mv_create._uncovered_tables("version: 0.1\nsource: SELECT 1\n", consent) is None


def _consent_covering(*securables):
    return {
        **_CONSENT,
        "probe_results": {"privileges": [{"privilege": "SELECT", "securable": s} for s in securables]},
    }


@pytest.mark.parametrize(
    "source, read_as", [
        ("'`finance.sales`.orders'", "`finance.sales`.orders"),
        ("sales.orders", "sales.orders"),
        ("orders", "orders"),
    ],
)
def test_a_name_that_is_not_three_parts_is_uncovered(source, read_as):
    body = f"version: 0.1\nsource: {source}\n"
    assert mv_create._uncovered_tables(body, _consent_covering("finance.sales.orders")) == [read_as]


def test_a_quoted_consent_securable_covers_by_its_parts():
    body = "version: 0.1\nsource: finance.sales.orders\n"
    assert mv_create._uncovered_tables(body, _consent_covering("`finance`.`sales`.`orders`")) == []
    assert mv_create._uncovered_tables(body, _consent_covering("`finance.sales`.orders")) == [
        "finance.sales.orders"
    ]
    hyphenated = "version: 0.1\nsource: '`finance`.`sales-eu`.`orders`'\n"
    assert mv_create._uncovered_tables(hyphenated, _consent_covering("finance.sales-eu.orders")) == []


# ── Coverage reads the tables inside a subquery source ──────────────────────

_SUBQUERY_SOURCE = (
    "SELECT fact.*, dim_branch.`branch_name` AS `branch_name`, dim_area.`area_name` AS `area_name`\n"
    "FROM `finance`.`sales`.`orders` AS fact\n"
    "LEFT JOIN (SELECT `branch_id`, MAX(`area_id`) AS `area_id`, MAX(`branch_name`) AS `branch_name` "
    "FROM `finance`.`sales`.`branch` WHERE `is_current` = true GROUP BY `branch_id`) AS dim_branch "
    "ON fact.`branch_id` = dim_branch.`branch_id`\n"
    "LEFT JOIN (SELECT `area_id`, MAX(`area_name`) AS `area_name` FROM `finance`.`sales`.`area` "
    "GROUP BY `area_id`) AS dim_area ON dim_branch.`area_id` = dim_area.`area_id`\n"
)
_SUBQUERY_TABLES = ("finance.sales.orders", "finance.sales.branch", "finance.sales.area")


def _subquery_body(source=_SUBQUERY_SOURCE):
    import yaml

    return yaml.safe_dump({
        "version": "1.1",
        "source": source,
        "dimensions": [
            {"name": "branch_name", "expr": "source.`branch_name`"},
            {"name": "area_name", "expr": "source.`area_name`"},
        ],
        "measures": [{"name": "total_amount", "expr": "SUM(source.`amount`)"}],
    }, sort_keys=False)


def _subquery_run(monkeypatch, consent, source=_SUBQUERY_SOURCE):
    """The run hook over a stored ``subquery_source`` body; the rung is proven, so
    the coverage gate is reached."""
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: {
            **_ARTIFACT, "yaml_text": _subquery_body(source), "join_strategy": "subquery_source",
        },
    )
    monkeypatch.setattr(mv_create, "verify_consent", lambda **kw: (_verification(), consent))
    monkeypatch.setattr(
        mv_yaml, "validate", lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    return _run_create()


def test_uncovered_tables_reads_every_table_inside_a_subquery_source():
    body = _subquery_body()
    assert mv_create._uncovered_tables(body, _consent_covering(*_SUBQUERY_TABLES)) == []
    assert mv_create._uncovered_tables(
        body, _consent_covering("finance.sales.orders", "finance.sales.branch")
    ) == ["`finance`.`sales`.`area`"]
    assert mv_create._uncovered_tables(
        _subquery_body("SELECT fact.* FROM ((("), _consent_covering(*_SUBQUERY_TABLES)
    ) is None


def test_a_subquery_body_still_governs_no_table():
    import yaml
    from genie_space_optimizer.optimization.mv_scoring import _definition_tables

    assert _definition_tables(yaml.safe_load(_subquery_body())) == ()


def test_uncovered_tables_reads_a_query_in_a_nested_join():
    body = (
        "version: '1.1'\nsource: finance.sales.orders\njoins:\n"
        "  - name: b\n    source: finance.sales.branch\n    on: source.branch_id = b.branch_id\n"
        "    joins:\n"
        "      - name: a\n"
        "        source: SELECT area_id, area_name FROM finance.geo.area\n"
        "        on: b.area_id = a.area_id\n"
    )
    assert mv_create._uncovered_tables(
        body, _consent_covering("finance.sales.orders", "finance.sales.branch")
    ) == ["`finance`.`geo`.`area`"]
    assert mv_create._uncovered_tables(
        body, _consent_covering("finance.sales.orders", "finance.sales.branch", "finance.geo.area")
    ) == []


@pytest.mark.parametrize(
    "source",
    [
        "SELECT * FROM sales.orders",
        "SELECT * FROM finance.sales.orders AS o JOIN range(10) AS r ON 1 = 1",
        "SELECT 1",
    ],
)
def test_a_subquery_source_with_an_unreadable_table_is_refused(source):
    body = f"version: '1.1'\nsource: {source}\n"
    assert mv_create._uncovered_tables(body, _consent_covering("finance.sales.orders")) is None


def test_a_query_join_under_an_empty_base_source_is_refused():
    body = (
        "version: '1.1'\njoins:\n"
        "  - name: b\n    source: SELECT * FROM finance.sales.branch\n    on: source.id = b.id\n"
    )
    assert mv_create._uncovered_tables(body, _consent_covering("finance.sales.branch")) is None


def test_run_hook_passes_coverage_for_a_subquery_body_the_consent_covers(create_env, monkeypatch):
    executed, upserts = create_env
    handoff = _subquery_run(monkeypatch, _consent_covering(*_SUBQUERY_TABLES))
    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.revenue_metrics"]
    assert any("CREATE VIEW `finance`.`sales`.`revenue_metrics`" in s for s in executed)
    assert upserts and upserts[0]["status"] == "CREATED"
    assert "access check" not in (handoff.downgrade_reason or "")


def test_run_hook_refuses_a_subquery_body_with_an_inner_table_uncovered(
    create_env, monkeypatch, caplog
):
    executed, upserts = create_env
    consent = _consent_covering("finance.sales.orders", "finance.sales.branch")
    assert mv_create._uncovered_tables(_subquery_body(), consent) == ["`finance`.`sales`.`area`"]
    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _subquery_run(monkeypatch, consent)
    assert handoff.action_mode == "suggest_only"
    assert handoff.downgrade_reason == (
        "no metric view could be created for the selected candidates: "
        "reads a table the access check did not cover (1)"
    )
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []
    assert mv_create.UNCOVERED_TABLES_REASON in caplog.text
    assert "SELECT" not in caplog.text and "area" not in caplog.text


def test_run_hook_refuses_an_unparsable_subquery_body(create_env, monkeypatch, caplog):
    executed, upserts = create_env
    with caplog.at_level("WARNING", logger="backend.services.mv_create"):
        handoff = _subquery_run(
            monkeypatch, _consent_covering(*_SUBQUERY_TABLES), source="SELECT fact.* FROM (((",
        )
    assert handoff.action_mode == "suggest_only"
    assert "reads a table the access check did not cover (1)" in (handoff.downgrade_reason or "")
    assert not any("CREATE VIEW" in s for s in executed)
    assert upserts == []
    assert "SELECT" not in caplog.text and "(((" not in caplog.text


@pytest.mark.parametrize(
    "full_name, valid", [
        ("`finance.sales`.orders", False),
        ("`a.b`.c.d", False),
        ("`finance.sales.orders`", False),
        ("`main`.`sales`.`revenue_metrics`", True),
        ("`main`.`sales-eu`.`revenue_metrics`", False),
        ("main.sales.revenue_metrics", True),
    ],
)
def test_valid_uc_identifier_splits_backtick_aware(full_name, valid):
    assert mv_create._valid_uc_identifier(full_name) is valid


@pytest.mark.parametrize(
    "name, parts", [
        ("`a.b`.c.d", None),
        ("`finance.sales.orders`", None),
        ("`finance`.`sales`.`orders`", ("finance", "sales", "orders")),
    ],
)
def test_uc_name_parts_refuses_a_dot_inside_a_quoted_part(name, parts):
    assert mv_create._uc_name_parts(name) == parts


# ── MV-D123 Ruling 9: approved and created proposals keep working across identity v2 ──

_ORDERS = "main.sales.orders"
_ORDERS_VIEW = "main.sales.orders_metrics"
_ORDERS_SPACE = {"data_sources": {"tables": [{"identifier": _ORDERS}]}}
_RUN_LIST = "/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-proposals"
_SPACE_LIST = "/api/auto-optimize/spaces/space-1/mv-proposals"
_CREATED_RUN = "44444444-4444-4444-8444-444444444444"


def _v1_member_keys(*statements: str) -> list[str]:
    """The member keys a v1 scan wrote: the frozen v1 grouping over the names as spelled."""
    from genie_space_optimizer.optimization.mv_fingerprint import extract_measures
    from genie_space_optimizer.optimization.mv_identity_v1 import v1_member_keys

    refs = [ref for sql in statements for ref in extract_measures(sql)]
    return sorted(set(v1_member_keys("space-1", dict(enumerate(refs))).values()))


def _v2_member_keys(*statements: str) -> list[str]:
    """The member keys a v2 scan writes: the live grouping over the space's tables."""
    from genie_space_optimizer.optimization.mv_fingerprint import corpus_scan
    from genie_space_optimizer.optimization.mv_state import mv_candidate_fingerprint
    from genie_space_optimizer.optimization.mv_tables import TableResolver

    scan = corpus_scan(
        [(sql, f"p{i}") for i, sql in enumerate(statements)],
        resolver=TableResolver.from_config(_ORDERS_SPACE),
    )
    return sorted(
        mv_candidate_fingerprint("space-1", m.canonical_expr, m.source_tables) for m in scan.measures
    )


def _orders_bundle(member_keys: list[str], updated_at: str, **extra) -> dict:
    from genie_space_optimizer.optimization.mv_scoring import suggestion_id_for
    from genie_space_optimizer.optimization.mv_state import mv_bundle_fingerprint

    bundle_key = mv_bundle_fingerprint("space-1", member_keys, [_ORDERS])
    suggestion_id = suggestion_id_for(bundle_key)
    return {
        "target_space_id": "space-1", "candidate_type": "NEW_METRIC_VIEW",
        "approved_for_rerun": False, "conflicts": [], "decision": None,
        "suggestion_id": suggestion_id, "dedup_fingerprint": bundle_key,
        "proposed_object": _ORDERS_VIEW, "updated_at": updated_at,
        "yaml_text": f"version: '1.1'  # {suggestion_id}\n",
        "evidence": {
            "render_version": MV_RENDER_VERSION, "join_strategy": "direct",
            "source_tables": [_ORDERS],
            "measures": [{"dedup_fingerprint": k, "role": "anchor"} for k in member_keys],
        },
        **extra,
    }


def _approved_beside_its_successor() -> tuple[dict, dict, dict]:
    """An approved v1 bundle, an older pending v1 row and a v2 re-scan's reshaped row of one view."""
    approved = _orders_bundle(
        _v1_member_keys(f"SELECT SUM(amount) FROM {_ORDERS}"), "2026-10-01T09:00:00",
        decision="approved", approved_for_rerun=True,
    )
    pending = _orders_bundle(
        _v1_member_keys(f"SELECT SUM(amount) FROM {_ORDERS}", f"SELECT COUNT(*) FROM {_ORDERS}"),
        "2026-10-01T10:00:00",
    )
    successor = _orders_bundle(
        _v2_member_keys(
            "SELECT SUM(amount) FROM orders",
            "SELECT COUNT(*) FROM orders",
            "SELECT MAX(amount) FROM sales.orders",
        ),
        "2026-10-01T11:00:00",
    )
    assert len({approved["suggestion_id"], pending["suggestion_id"], successor["suggestion_id"]}) == 3
    return approved, pending, successor


def _created_ledger(rows: list[dict]):
    """A fake warehouse read over created-objects ledger rows, asked by id or by (run, id)."""

    def _query(ws, warehouse_id, sql):
        import re as _re

        assert "genie_opt_mv_created_objects" in sql
        pair = _re.search(r"run_id = '([^']*)' AND suggestion_id = '([^']*)'", sql)
        if pair:
            return pd.DataFrame(
                [r for r in rows if (r["run_id"], r["suggestion_id"]) == pair.groups()]
            )
        asked = set(_re.findall(r"'(sug_[0-9a-f]{12})'", sql))
        return pd.DataFrame({"suggestion_id": sorted(asked & {r["suggestion_id"] for r in rows})})

    return _query


@pytest.mark.parametrize("url", [_RUN_LIST, _SPACE_LIST], ids=["run-list", "space-list"])
def test_an_approved_v1_bundle_is_listed_beside_its_v2_successor(client, monkeypatch, url):
    """The approved row stays; the older pending row leaves beside the newer one."""
    approved, pending, successor = _approved_beside_its_successor()
    _stub_space_list(monkeypatch, [approved, pending, successor])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", _created_ledger([]))

    resp = client.get(url)

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [
        approved["suggestion_id"], successor["suggestion_id"],
    ]


@pytest.mark.parametrize("decision", ["approved", "rejected"])
def test_the_approved_v1_bundle_resolves_in_the_decision_route(client, monkeypatch, decision):
    approved, pending, successor = _approved_beside_its_successor()
    recorded: list[dict] = []
    suppressed: list[dict] = []
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates", lambda *a, **k: [successor, pending, approved],
    )
    monkeypatch.setattr(
        warehouse, "wh_record_mv_candidate_decision",
        lambda ws, warehouse_id, **kw: recorded.append(kw),
    )
    monkeypatch.setattr(
        warehouse, "wh_suppress_mv_measures",
        lambda ws, warehouse_id, **kw: suppressed.append(kw),
    )

    resp = client.post(
        f"/api/auto-optimize/mv/proposals/{approved['suggestion_id']}/decision",
        json={"space_id": "space-1", "decision": decision},
    )

    assert resp.status_code == 200
    assert [r["dedup_fingerprint"] for r in recorded] == [approved["dedup_fingerprint"]]
    fanned_out = [fp for call in suppressed for fp in call["measure_fingerprints"]]
    stored_members = [m["dedup_fingerprint"] for m in approved["evidence"]["measures"]]
    assert fanned_out == ([] if decision == "approved" else stored_members)


@pytest.mark.parametrize("with_artifacts", [True, False], ids=["artifact", "candidate-row"])
def test_the_approved_v1_bundle_resolves_in_mv_ddl(client, monkeypatch, with_artifacts):
    approved, pending, successor = _approved_beside_its_successor()
    rows = [successor, pending, approved]
    artifacts = [
        {"artifact_json": json.dumps({
            **{k: r[k] for k in ("suggestion_id", "dedup_fingerprint", "proposed_object", "yaml_text")},
            "render_version": MV_RENDER_VERSION,
        })}
        for r in rows
    ] if with_artifacts else []
    monkeypatch.setattr(auto_optimize, "_gso_sp_application_id", lambda: "")
    monkeypatch.setattr(auto_optimize, "_delta_query", lambda sql, *, strict=False: artifacts)
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: rows)

    resp = client.get(
        "/api/auto-optimize/runs/11111111-1111-4111-8111-111111111111/mv-ddl",
        params={"suggestion_id": approved["suggestion_id"]},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["suggestion_id"] == approved["suggestion_id"]
    assert body["dedup_fingerprint"] == approved["dedup_fingerprint"]
    assert body["yaml_text"] == approved["yaml_text"]


def test_a_created_v1_row_resolves_by_its_stored_suggestion_id(client, monkeypatch):
    """Create-at-approval records no decision, so the ledger's stored id is what
    keeps the row listed beside its successor and what the drop finds. The
    successor's id holds no ledger row: nothing is carried to the new key."""
    approved, _, successor = _approved_beside_its_successor()
    created = {**approved, "decision": None, "approved_for_rerun": False}
    ledger = [{
        "run_id": _CREATED_RUN, "suggestion_id": created["suggestion_id"],
        "full_name": _ORDERS_VIEW, "created_by": "analyst@example.com",
        "status": "DETACHED", "provenance": "OBO_CREATED",
    }]
    executed: list[str] = []
    _stub_space_list(monkeypatch, [created, successor])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", _created_ledger(ledger))
    monkeypatch.setattr(
        warehouse, "sql_warehouse_execute", lambda ws, warehouse_id, sql: executed.append(sql),
    )
    monkeypatch.setattr(
        auto_optimize, "require_obo_workspace_client", lambda: _obo_as("analyst@example.com"),
    )

    listed = client.get(_SPACE_LIST)
    assert [p["suggestion_id"] for p in listed.json()["proposals"]] == [
        created["suggestion_id"], successor["suggestion_id"],
    ]

    stray = client.post(
        f"/api/auto-optimize/mv/created/{successor['suggestion_id']}/drop",
        json={"run_id": _CREATED_RUN, "confirm": True},
    )
    assert stray.status_code == 404
    assert executed == []

    dropped = client.post(
        f"/api/auto-optimize/mv/created/{created['suggestion_id']}/drop",
        json={"run_id": _CREATED_RUN, "confirm": True},
    )
    assert dropped.status_code == 200
    assert executed[0] == "DROP VIEW IF EXISTS `main`.`sales`.`orders_metrics`"
    assert f"suggestion_id = '{created['suggestion_id']}'" in executed[1]
    assert "'DROPPED'" in executed[1]


@pytest.mark.parametrize("source", [_ORDERS, "'`main`.`sales`.`orders`'"])
def test_the_claim_over_a_three_part_source_recomputes_the_stored_member_keys(monkeypatch, source):
    """Ruling 8: the claim keys the view's measures over its own ``source:``, which
    is already three-part, so the v1 and v2 member keys are the ones it recomputes."""
    v1 = _v1_member_keys(*(
        f"SELECT {aggregate} FROM {_ORDERS}" for aggregate in ("SUM(amount)", "COUNT(*)", "MAX(amount)")
    ))
    v2 = _v2_member_keys(
        "SELECT SUM(amount) FROM orders",
        "SELECT COUNT(*) FROM sales.orders",
        "SELECT MAX(amount) FROM `Main`.`Sales`.`Orders`",
    )
    assert v1 == v2 and len(v2) == 3
    row = _orders_bundle(v2, "2026-10-01T11:00:00")
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [row])
    yaml_text = (
        'version: "1.1"\n'
        f"source: {source}\n"
        "measures:\n"
        "- name: total\n  expr: SUM(source.amount)\n"
        "- name: orders\n  expr: COUNT(*)\n"
        "- name: biggest\n  expr: MAX(source.`amount`)\n"
    )

    ok, reason = mv_create._claim_matches_view(
        _SP_WS, "wh1", catalog="main", schema="gso",
        space_id="space-1", suggestion_id=row["suggestion_id"], yaml_text=yaml_text,
    )

    assert ok is True and reason is None


def test_the_claim_refuses_a_stored_member_key_v2_cannot_reproduce(monkeypatch):
    """Ruling 8's negative control: a v1 key over a spelled union (``main.sales.orders``
    and ``orders`` in one leaf bucket) is not what the view's ``source:`` recomputes."""
    (drifted,) = _v1_member_keys(f"SELECT SUM(amount) FROM {_ORDERS}", "SELECT SUM(amount) FROM orders")
    (reproducible,) = _v2_member_keys(f"SELECT SUM(amount) FROM {_ORDERS}")
    assert drifted != reproducible
    yaml_text = (
        'version: "1.1"\n'
        f"source: {_ORDERS}\n"
        "measures:\n"
        "- name: total\n  expr: SUM(source.amount)\n"
    )

    def claim(member_key: str) -> tuple[bool, str | None]:
        row = _orders_bundle([member_key], "2026-10-01T11:00:00")
        monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [row])
        return mv_create._claim_matches_view(
            _SP_WS, "wh1", catalog="main", schema="gso",
            space_id="space-1", suggestion_id=row["suggestion_id"], yaml_text=yaml_text,
        )

    assert claim(reproducible) == (True, None)
    ok, reason = claim(drifted)
    assert ok is False
    assert "measure fingerprint mismatch" in reason


@pytest.mark.parametrize("url", [_RUN_LIST, _SPACE_LIST], ids=["run-list", "space-list"])
def test_a_v1_pending_row_beside_its_v2_successor_is_not_listed(client, monkeypatch, url):
    _, pending, successor = _approved_beside_its_successor()
    _stub_space_list(monkeypatch, [pending, successor])
    monkeypatch.setattr(warehouse, "sql_warehouse_query", _created_ledger([]))

    resp = client.get(url)

    assert resp.status_code == 200
    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [successor["suggestion_id"]]
