"""Phase 4 Stage A (17h): Context Sources registry + firewall-by-class + capability
probe + real tier-5 (spec §1 Stage A, §4/§7/§11). NO egress: nothing here calls a web
search, an AI Gateway, or resolves a Context Pack — those are Stage B."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.models import ExternalContext, OntologySettings
from backend.ontology.routers import preflight as preflight_mod
from backend.ontology.routers.preflight import router as preflight_router
from backend.ontology.services import context_sources as cs
from backend.ontology.services import ont_settings, tag_graph
from backend.watch.services import system_tables

# The naming-family targets every source may reach, and the structural targets only
# an internal UC-backed source may add. membership/measure/certification are NEVER
# reachable (the structural firewall, §7).
_NAMING_TARGETS = ["naming", "description", "synonym", "gap_hypothesis", "recent_context"]
_STRUCTURAL_TARGETS = ["structural_signal", "validation"]
_FORBIDDEN_TARGETS = ["membership", "measure", "certification"]


# ── Registry classification (§11) ────────────────────────────────────────────


def test_registry_entries_all_carry_valid_class_tier_influence():
    assert cs.CONTEXT_SOURCES, "registry must not be empty"
    for e in cs.CONTEXT_SOURCES:
        assert e.klass in ("internal", "external"), e.id
        assert e.provenance_tier in ("T0", "T1", "T2", "T3"), e.id
        assert isinstance(e.influence, str) and e.influence.strip(), e.id
        assert e.mcp_fqn and e.label and e.id, e.id
        assert isinstance(e.default_enabled, bool), e.id


def test_registry_matches_mv_d47_lead():
    by_id = {e.id: e for e in cs.CONTEXT_SOURCES}
    # T3 web: AI-Gateway web search + You.com, both external + opt-in (default off).
    assert by_id["web_search"].mcp_fqn == "system.ai.web_search"
    assert by_id["web_search"].klass == "external" and by_id["web_search"].provenance_tier == "T3"
    assert by_id["youcom"].mcp_fqn == "myyoumcp"
    assert by_id["youcom"].klass == "external" and by_id["youcom"].provenance_tier == "T3"
    for wid in ("web_search", "youcom"):
        assert by_id[wid].default_enabled is False
    # T1 company docs: Confluence / Google Drive / Microsoft 365, external + opt-in.
    for did in ("confluence", "google_drive", "microsoft_365"):
        assert by_id[did].klass == "external" and by_id[did].provenance_tier == "T1"
        assert by_id[did].default_enabled is False
    # T0 verified internal: Genie One + Databricks SQL.
    assert by_id["genie_one"].mcp_fqn == "/api/2.0/mcp/genie"
    assert by_id["databricks_sql"].mcp_fqn == "/api/2.0/mcp/sql"
    for iid in ("genie_one", "databricks_sql"):
        assert by_id[iid].klass == "internal" and by_id[iid].provenance_tier == "T0"


def test_gmail_slack_calendar_are_excluded():
    ids = {e.id for e in cs.CONTEXT_SOURCES}
    for excluded in cs.EXCLUDED_SOURCE_IDS:
        assert excluded not in ids
    assert cs.EXCLUDED_SOURCE_IDS == frozenset({"gmail", "slack", "calendar"})


# ── Firewall-by-class (positive guard, §7/§11) ───────────────────────────────


def _external():
    return next(e for e in cs.CONTEXT_SOURCES if e.klass == "external")


def _internal():
    return next(e for e in cs.CONTEXT_SOURCES if e.klass == "internal")


def test_external_source_reaches_naming_family_but_never_a_structural_writer():
    ext = _external()
    for t in _NAMING_TARGETS:
        assert cs.influence_allows(ext, t) is True, t
    # External may NOT reach the structural targets (those are internal-only).
    for t in _STRUCTURAL_TARGETS:
        assert cs.influence_allows(ext, t) is False, t
    # …and NEVER a membership/measure/certification writer.
    for t in _FORBIDDEN_TARGETS:
        assert cs.influence_allows(ext, t) is False, t


def test_internal_uc_source_adds_structural_signal_and_validation():
    it = _internal()
    for t in _NAMING_TARGETS + _STRUCTURAL_TARGETS:
        assert cs.influence_allows(it, t) is True, t
    # Even an internal UC-backed source can NEVER reach the forbidden writers.
    for t in _FORBIDDEN_TARGETS:
        assert cs.influence_allows(it, t) is False, t


def test_no_source_of_any_class_can_reach_a_forbidden_target():
    """The structural carve is universal — proven across the whole registry."""
    for e in cs.CONTEXT_SOURCES:
        for t in _FORBIDDEN_TARGETS:
            assert cs.influence_allows(e, t) is False, (e.id, t)


# ── Capability probe (MV-D43/D45, §11) ───────────────────────────────────────


def test_probe_with_no_client_is_unavailable_with_a_copy_ready_grant():
    entry = _external()
    st = cs.probe_source(entry, None, sp="sp-app-42")
    assert st.execute_status == "unavailable"
    assert st.grant_line == f"GRANT EXECUTE ON `{entry.mcp_fqn}` TO `sp-app-42`"
    assert st.reason  # a plain reason, never an exception
    assert st.klass == entry.klass and st.provenance_tier == entry.provenance_tier


def test_probe_ok_carries_no_grant_line(monkeypatch):
    monkeypatch.setattr(cs, "_resolve_execute", lambda e, c: ("ok", ""))
    st = cs.probe_source(_external(), object(), sp="sp-app-42")
    assert st.execute_status == "ok"
    assert st.grant_line is None


def test_probe_missing_carries_grant_line(monkeypatch):
    monkeypatch.setattr(cs, "_resolve_execute", lambda e, c: ("missing", "no EXECUTE"))
    entry = _external()
    st = cs.probe_source(entry, object(), sp="sp-app-42")
    assert st.execute_status == "missing"
    assert st.grant_line == f"GRANT EXECUTE ON `{entry.mcp_fqn}` TO `sp-app-42`"


def test_probe_never_raises_on_a_broken_client():
    class _Boom:
        @property
        def grants(self):
            raise RuntimeError("kaboom")

    # A dotted securable would attempt get_effective; the broken client degrades.
    entry = next(e for e in cs.CONTEXT_SOURCES if e.id == "web_search")
    st = cs.probe_source(entry, _Boom(), sp="sp-app-42")
    assert st.execute_status in ("unavailable", "blocked")
    assert st.grant_line


# ── Real tier-5 (§11) ─────────────────────────────────────────────────────────


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(preflight_router)
    return TestClient(app)


def _patch_common(monkeypatch):
    monkeypatch.setattr(tag_graph, "probe", lambda *a, **k: True)
    monkeypatch.setattr(system_tables, "system_tables_status", lambda: True)
    monkeypatch.setattr(preflight_mod, "get_workspace_client", lambda: object())


def _patch_settings(monkeypatch, external: ExternalContext):
    async def _fake():
        return OntologySettings(
            company_name="Northwind", catalog_allowlist=["finance"], external_context=external
        )
    monkeypatch.setattr(ont_settings, "get_settings", _fake)


def test_tier5_disabled_by_default_does_not_probe(monkeypatch):
    """DEFAULT OFF (MV-D44): the tier is a plain disabled reason and NO probe runs —
    the byte-identical estate-only path."""
    _patch_common(monkeypatch)
    _patch_settings(monkeypatch, ExternalContext())  # enabled=false

    def _boom(*_a, **_k):
        raise AssertionError("probe_source must not run when external_context is off")

    monkeypatch.setattr(cs, "probe_source", _boom)

    data = _client().get("/api/ontology/preflight").json()
    tier = {t["id"]: t for t in data["tiers"]}["external_enrichment"]
    assert tier["status"] == "not_exercised"
    assert tier["sources"] == []
    assert "off" in (tier["reason"] or "").lower()


def test_tier5_enabled_all_missing_is_degraded_with_grants(monkeypatch):
    _patch_common(monkeypatch)
    # An explicit False overrides a source's registry default_enabled=True (the
    # internal T0 sources default on) — so only web_search is enabled here.
    _patch_settings(
        monkeypatch,
        ExternalContext(
            enabled=True,
            sources={"web_search": True, "genie_one": False, "databricks_sql": False},
        ),
    )
    monkeypatch.setattr(preflight_mod, "get_service_principal_client", lambda: object())
    monkeypatch.setattr(
        cs, "_resolve_execute", lambda e, c: ("missing", "no EXECUTE")
    )

    data = _client().get("/api/ontology/preflight").json()
    tier = {t["id"]: t for t in data["tiers"]}["external_enrichment"]
    assert tier["status"] == "degraded"
    # Exactly the one enabled source is reported, with its per-source status + GRANT.
    assert [s["id"] for s in tier["sources"]] == ["web_search"]
    row = tier["sources"][0]
    assert row["klass"] == "external" and row["provenance_tier"] == "T3"
    assert row["execute_status"] == "missing" and row["grant_line"]
    assert any("GRANT EXECUTE" in g for g in tier["grants"])


def test_tier5_enabled_with_one_ok_source_is_ok(monkeypatch):
    _patch_common(monkeypatch)
    _patch_settings(monkeypatch, ExternalContext(enabled=True, sources={"web_search": True}))
    monkeypatch.setattr(preflight_mod, "get_service_principal_client", lambda: object())
    monkeypatch.setattr(cs, "_resolve_execute", lambda e, c: ("ok", ""))

    data = _client().get("/api/ontology/preflight").json()
    tier = {t["id"]: t for t in data["tiers"]}["external_enrichment"]
    assert tier["status"] == "ok"
    assert tier["sources"][0]["execute_status"] == "ok"


def test_tier5_never_raises_even_if_the_probe_client_explodes(monkeypatch):
    _patch_common(monkeypatch)
    _patch_settings(monkeypatch, ExternalContext(enabled=True, sources={"web_search": True}))

    def _boom():
        raise RuntimeError("no SP client")

    monkeypatch.setattr(preflight_mod, "get_service_principal_client", _boom)

    resp = _client().get("/api/ontology/preflight")
    assert resp.status_code == 200  # page-does-not-raise (MV-D43)
    tier = {t["id"]: t for t in resp.json()["tiers"]}["external_enrichment"]
    # No SP client ⇒ probe degrades to unavailable ⇒ tier degraded, still rendered.
    assert tier["status"] == "degraded"
    assert tier["sources"][0]["execute_status"] == "unavailable"
