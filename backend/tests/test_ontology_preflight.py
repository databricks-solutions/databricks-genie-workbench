"""Ontology preflight (spec §11): with SP grants present the signals/tag_graph
tiers are ok and can_render_taxonomy=True; with a permission error injected on the
tag read the tag_graph tier is blocked, can_render_taxonomy=False, and the page
does not raise."""

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import OntologySettings
from backend.ontology.routers import preflight as preflight_mod
from backend.ontology.routers.preflight import router as preflight_router
from backend.ontology.services import grants, inventory, ont_settings, tag_graph
from backend.watch.services import system_tables


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(preflight_router)
    return TestClient(app)


def _patch_settings(monkeypatch, allowlist, read_identity="obo"):
    async def _fake():
        return OntologySettings(
            company_name="Northwind", catalog_allowlist=allowlist, read_identity=read_identity
        )
    monkeypatch.setattr(ont_settings, "get_settings", _fake)


def _patch_membership(monkeypatch, sp_seen: int, obo_seen: int):
    """Stub the BROWSE differential probes so preflight never hits a warehouse."""
    monkeypatch.setattr(tag_graph, "sp_assignment_count", lambda *_a, **_k: sp_seen)
    monkeypatch.setattr(inventory, "governed_tag_count", lambda _client, _allow: obo_seen)
    # Avoid constructing a real default client in the no-auth test env.
    monkeypatch.setattr(preflight_mod, "get_workspace_client", lambda: object())


def test_preflight_all_read_tiers_ok(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    _patch_membership(monkeypatch, sp_seen=7, obo_seen=7)  # SP sees members → ok

    data = _client().get("/api/ontology/preflight").json()
    assert data["can_render_taxonomy"] is True

    tiers = {t["id"]: t for t in data["tiers"]}
    assert len(tiers) == 5
    assert tiers["inventory"]["status"] == "ok"
    assert tiers["signals"]["status"] == "ok"
    assert tiers["tag_graph"]["status"] == "ok"
    # Phase 5 (17i) Stage 2: the write tier is REAL now. Off-platform (no DATABRICKS_HOST in
    # the test env) the OBO probe is indeterminate and fails soft to "ok" with informational
    # grant lines (the exact per-change grants are re-checked at preview time).
    assert tiers["membership_write"]["status"] == "ok"
    assert tiers["membership_write"]["grants"]  # informational apply-grant guidance present
    assert tiers["external_enrichment"]["status"] == "not_exercised"


def test_preflight_tag_graph_blocked_does_not_raise(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: False)  # permission error → blocked
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)

    resp = _client().get("/api/ontology/preflight")
    assert resp.status_code == 200  # page-does-not-raise
    data = resp.json()
    assert data["can_render_taxonomy"] is False
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["tag_graph"]["status"] == "blocked"
    # The blocked tier surfaces the copy-ready grant.
    assert any("governed_tags" in g for g in tiers["tag_graph"]["grants"])
    assert tiers["tag_graph"]["reason"]


def test_preflight_signals_degraded_when_grants_missing(monkeypatch):
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: False)
    _patch_membership(monkeypatch, sp_seen=7, obo_seen=7)

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["signals"]["status"] == "degraded"
    # signals never gates rendering.
    assert data["can_render_taxonomy"] is True


def test_preflight_tag_graph_degraded_when_browse_missing(monkeypatch):
    """Tree renders (governed_tags readable) but the SP is blind to assignments
    while the admin sees them → BROWSE-needed: degraded tier + copy-ready grant,
    and the taxonomy still renders (member counts just read 0). The BROWSE
    differential only runs under the opt-in SP read identity (MV-D50)."""
    _patch_settings(monkeypatch, ["finance", "sales"], read_identity="sp")
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-app-42")
    _patch_membership(monkeypatch, sp_seen=0, obo_seen=9)  # SP blind, OBO sees tags

    data = _client().get("/api/ontology/preflight").json()
    assert data["can_render_taxonomy"] is True  # tree still renders
    tag = {t["id"]: t for t in data["tiers"]}["tag_graph"]
    assert tag["status"] == "degraded"
    assert tag["reason"] and "BROWSE" in tag["reason"]
    # Copy-ready GRANT for each catalog, targeting the resolved SP.
    assert "GRANT BROWSE ON CATALOG `finance` TO `sp-app-42`" in tag["grants"]
    assert "GRANT BROWSE ON CATALOG `sales` TO `sp-app-42`" in tag["grants"]


def test_preflight_no_browse_nag_when_no_tags(monkeypatch):
    """Both SP and OBO see zero assignments → genuinely no tags, not a grant gap
    (exercised under the SP read identity where the differential runs)."""
    _patch_settings(monkeypatch, ["finance"], read_identity="sp")
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    _patch_membership(monkeypatch, sp_seen=0, obo_seen=0)

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}
    assert tiers["tag_graph"]["status"] == "ok"


def test_preflight_frames_sp_grants_as_optional_upgrade(monkeypatch):
    """§11 framing: signals/tag_graph tiers keep copy-ready grants, and a blocked
    SP grant no longer implies it is *required* to view — the reason frames it as an
    optional upgrade (the taxonomy renders as the OBO admin)."""
    _patch_settings(monkeypatch, ["finance"])  # default OBO
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: False)  # blocked read
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: False)

    data = _client().get("/api/ontology/preflight").json()
    tiers = {t["id"]: t for t in data["tiers"]}

    tag = tiers["tag_graph"]
    assert tag["status"] == "blocked"
    assert tag["grants"]  # copy-ready SP grant lines still present
    tag_reason = (tag["reason"] or "").lower()
    assert "optional upgrade" in tag_reason
    # …and explicitly framed as NOT required to view (vs the old "grant … to render").
    assert "not required" in tag_reason

    signals = tiers["signals"]
    assert signals["grants"]
    assert "optional upgrade" in (signals["reason"] or "").lower()


def test_preflight_membership_write_blocked_surfaces_copy_ready_grants(monkeypatch):
    """BUILD B: when the OBO viewer positively lacks the apply grants, the membership_write
    tier is 'blocked' with copy-ready GRANT lines (mirrors the enrichment tier's shape)."""
    _patch_settings(monkeypatch, ["finance"])
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    _patch_membership(monkeypatch, sp_seen=7, obo_seen=7)
    monkeypatch.setattr(
        grants,
        "membership_write_status",
        lambda *a, **k: ("blocked", ["GRANT APPLY TAG ON CATALOG `finance` TO `you`"], "needs a grant"),
    )

    data = _client().get("/api/ontology/preflight").json()
    tier = {t["id"]: t for t in data["tiers"]}["membership_write"]
    assert tier["status"] == "blocked"
    assert any("APPLY TAG" in g for g in tier["grants"])
    assert tier["reason"]
    # A blocked write tier never gates rendering (read-only page is unaffected).
    assert data["can_render_taxonomy"] is True


def test_membership_write_status_shapes():
    """The pure tier resolver (BUILD B): no catalogs → not_exercised; no client (off-platform)
    → ok fail-soft; a probe that blocks → blocked + copy-ready lines."""
    # No catalogs to probe yet.
    status, lines, reason = grants.membership_write_status(None, [], None)
    assert status == "not_exercised" and lines and reason

    # Off-platform / indeterminate → ok (grants re-checked per change at preview).
    status, lines, reason = grants.membership_write_status(None, ["finance"], None)
    assert status == "ok" and lines

    # A client whose effective read shows privileges WITHOUT APPLY TAG → blocked + grants.
    priv = types.SimpleNamespace(privilege="USE_CATALOG")
    assignment = types.SimpleNamespace(privileges=[priv])
    fake = types.SimpleNamespace(
        grants=types.SimpleNamespace(
            get_effective=lambda **k: types.SimpleNamespace(privilege_assignments=[assignment])
        )
    )
    status, lines, reason = grants.membership_write_status(fake, ["finance"], None, principal="u@x")
    assert status == "blocked"
    assert any("APPLY TAG" in ln for ln in lines)
    assert any("`u@x`" in ln for ln in lines)


def test_write_grant_lines_escape_identifiers():
    lines = grants.write_grant_lines("cat.sch.orders", "biz`tag", asset_type="table", principal="u@x")
    joined = "\n".join(lines)
    assert "GRANT APPLY TAG ON TABLE `cat`.`sch`.`orders` TO `u@x`" in joined
    assert "GRANT USE SCHEMA ON SCHEMA `cat`.`sch` TO `u@x`" in joined
    assert "GRANT USE CATALOG ON CATALOG `cat` TO `u@x`" in joined
    # tag_key with a backtick is doubled (injection-safe) and not a create/alter/drop.
    assert "GRANT ASSIGN ON GOVERNED TAG `biz``tag` TO `u@x`" in joined


def test_preflight_default_obo_does_not_touch_sp(monkeypatch):
    """Default OBO: preflight resolves the tag read as the viewer and never calls
    the BROWSE differential (which is the only SP read here)."""
    _patch_settings(monkeypatch, ["finance"])  # read_identity="obo"
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)

    def _boom(*_a, **_k):
        raise AssertionError("sp_assignment_count must not run under the OBO default")

    monkeypatch.setattr(tag_graph, "sp_assignment_count", _boom)
    monkeypatch.setattr(preflight_mod, "get_workspace_client", lambda: object())

    data = _client().get("/api/ontology/preflight").json()
    assert {t["id"]: t for t in data["tiers"]}["tag_graph"]["status"] == "ok"
