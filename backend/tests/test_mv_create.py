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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_create
from genie_space_optimizer.common import warehouse
from genie_space_optimizer.common.config import MV_RENDER_VERSION
from genie_space_optimizer.optimization import mv_yaml

_REAL_LOAD_RUN_ENVELOPE = auto_optimize._load_run_envelope
_REAL_OBJECT_EXISTS = mv_create._object_exists
_REAL_CONFIRM_METRIC_VIEW = mv_create._confirm_metric_view

# ── Service: create_and_attach_for_run ─────────────────────────────────────


def _verification(effective_mode="create_and_attach", downgrade_reason=None, verdict="SUFFICIENT"):
    fresh = SimpleNamespace(capabilities=[], checked_as="analyst@example.com")
    return SimpleNamespace(
        effective_mode=effective_mode,
        downgrade_reason=downgrade_reason,
        verdict=verdict,
        fresh_probe=fresh,
    )


_CONSENT = {"target_catalog": "finance", "target_schema": "sales", "probe_id": "p1"}
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


@pytest.mark.parametrize("strategy", ["nested", "subquery_source", "denormalized"])
def test_a_body_needing_an_unproven_join_is_not_created(create_env, monkeypatch, strategy):
    """MV-D117 (C-8): only a ``direct`` body is created until the join rungs are
    proven in Unity Catalog."""
    executed, upserts = create_env
    monkeypatch.setattr(
        mv_create, "_load_ddl_artifact",
        lambda *a, **k: dict(_ARTIFACT, join_strategy=strategy),
    )
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )

    handoff = _run_create()

    assert not any("CREATE VIEW" in s for s in executed)
    assert handoff.attach_views == []
    assert handoff.action_mode == "suggest_only"
    assert upserts == []


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

    handoff = _run_create()

    assert handoff.action_mode == "suggest_only"
    assert not any("CREATE VIEW" in s for s in executed)


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


def test_an_unrecorded_create_is_dropped_and_the_run_moves_on(create_env, monkeypatch):
    """MV-D112: a view whose ledger row cannot be written is dropped at once.
    Left in place, the next run refuses its name and no drop route can see it."""
    executed, upserts = create_env
    _two_candidates(monkeypatch)

    def upsert(ws, warehouse_id, **kw):
        upserts.on.append(ws)
        if kw["suggestion_id"] == "sug1":
            raise RuntimeError("delta write failed")
        upserts.append(kw)

    monkeypatch.setattr(warehouse, "wh_upsert_mv_created_object", upsert)

    handoff = _run_create_selecting(["sug1", "sug2"])

    assert "DROP VIEW IF EXISTS `finance`.`sales`.`revenue_metrics`" in executed
    assert _view_ddl_clients(executed) == [_OBO_WS] * 3
    assert upserts.on == [_SP_WS, _SP_WS]
    assert handoff.action_mode == "create_and_attach"
    assert handoff.attach_views == ["finance.sales.order_counts"]
    assert [c.suggestion_id for c in handoff.created] == ["sug2"]


def test_a_failed_cleanup_drop_is_logged_for_manual_removal(create_env, monkeypatch, caplog):
    executed, _ = create_env
    monkeypatch.setattr(
        mv_yaml, "validate",
        lambda text, **kw: mv_yaml.ValidationReport(ok=True, downgrade_to=None),
    )
    monkeypatch.setattr(
        warehouse, "wh_upsert_mv_created_object",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("delta write failed")),
    )

    drops_on: list = []

    def execute(ws, warehouse_id, sql):
        if sql.startswith("DROP VIEW"):
            drops_on.append(ws)
            raise RuntimeError("drop failed")
        executed.on.append(ws)
        executed.append(sql)

    monkeypatch.setattr(warehouse, "sql_warehouse_execute", execute)

    with caplog.at_level("ERROR", logger="backend.services.mv_create"):
        handoff = _run_create_selecting(["sug1"])

    assert handoff.action_mode == "suggest_only"
    assert _view_ddl_clients(executed) == [_OBO_WS] and drops_on == [_OBO_WS]
    assert any(
        r.levelname == "ERROR"
        and "finance.sales.revenue_metrics" in r.getMessage()
        and "must be dropped by hand" in r.getMessage()
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
        ("revalidation", "failed re-validation"),
        ("rung_below", "re-validation demands a lower join strategy"),
        ("exists", "already exists in the consented schema"),
        ("not_confirmed", "was not confirmed as a metric view after create"),
        ("unrecorded", "could not be recorded, so it was dropped"),
        ("error", "failed with an error"),
    ],
)
def test_each_skip_key_names_its_label(key, label):
    assert mv_create._nothing_built_reason({key: 2}) == (
        f"no metric view could be created for the selected candidates: {label} (2)"
    )


def test_the_skip_labels_are_pinned_in_order():
    assert [key for key, _ in mv_create._SKIP_ORDER] == [
        "unavailable", "stale", "no_body", "unproven_rung", "invalid_name",
        "revalidation", "rung_below", "exists", "not_confirmed", "unrecorded", "error",
    ]


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
    "downgrade_to,stored,expected",
    [
        ("subquery_source", "nested", True),   # forced below where it was rendered
        (None, "nested", False),               # no downgrade demanded
        ("subquery_source", "subquery_source", False),  # already at that rung
    ],
)
def test_rung_below(downgrade_to, stored, expected):
    assert mv_create._rung_below(downgrade_to, stored) is expected


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


def test_the_grant_quotes_a_spaced_view_name():
    grant = auto_optimize._mv_optimizer_grant_sql(
        "main.sales.order revenue", "a803ebc5-232f-44c0-9ed6-fb17d7c77f9e"
    )
    assert "`main`.`sales`.`order revenue`" in grant


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
