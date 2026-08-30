"""Ontology Phase-2 backend tests (spec §11): contract-frozen guard, freshness
states, trigger idempotency, and the mirror-vs-live reader swap."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology import models
from backend.ontology.models import OntologySettings
from backend.ontology.routers import tags as tags_router
from backend.ontology.routers import taxonomy as taxonomy_router
from backend.ontology.routers.refresh import router as refresh_router
from backend.ontology.services import mirror, ont_settings, refresh

_NOW = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)


# ── Contract-frozen guard: Phase-1 models are byte-identical ────────────────

_PHASE1_FIELDS = {
    "PermissionTier": {"id", "label", "identity", "status", "grants", "reason"},
    "OntologyPreflight": {"tiers", "can_render_taxonomy", "company_name", "catalog_allowlist", "as_of"},
    "OntologyInventory": {"catalogs_scanned", "metric_view_count", "genie_agent_count", "governed_tag_count", "as_of"},
    "MemberAsset": {"fqn", "asset_type"},
    "SubDomainNode": {"tag_value", "name", "member_count", "members"},
    "DomainNode": {"tag_key", "name", "member_count", "subdomains", "members"},
    "UngroupedBucket": {"metric_views", "genie_agents"},
    "OntologyTaxonomy": {"domains", "ungrouped", "as_of"},
    "GovernedTag": {"tag_key", "allowed_values", "assignment_count", "acts_as_domain", "acts_as_subdomain"},
    "TagCollision": {"kind", "members", "suggestion"},
    "TagCleanup": {"tag_key", "flag", "detail"},
    "TagLens": {"tags", "collisions", "cleanup", "as_of"},
    "OntologySettings": {"company_name", "catalog_allowlist"},
}


def test_phase1_model_fields_are_frozen():
    for name, expected in _PHASE1_FIELDS.items():
        model = getattr(models, name)
        assert set(model.model_fields) == expected, f"{name} field set changed (contract broken)"


def test_only_new_model_is_refresh_status():
    # OntologyRefreshStatus is the sole Phase-2 addition to the model surface.
    assert set(models.OntologyRefreshStatus.model_fields) == {
        "state", "source", "mirror_as_of", "last_run_id",
        "last_run_state", "freshness_window_hours", "message",
    }


# ── Freshness state resolution (pure) ───────────────────────────────────────


def _run_row(state, as_of, run_id="r1"):
    return {"run_id": run_id, "state": state, "as_of": as_of.isoformat()}


def test_status_cold_when_no_runs():
    s = refresh.compute_status(None, None, now=_NOW)
    assert s.state == "cold" and s.source == "live" and s.last_run_state == "none"


def test_status_fresh_within_window():
    succ = _run_row("succeeded", _NOW - timedelta(hours=3))
    s = refresh.compute_status(succ, succ, now=_NOW)
    assert s.state == "fresh" and s.source == "mirror"
    assert "Updated" in s.message and "3 hour" in s.message


def test_status_stale_beyond_window():
    succ = _run_row("succeeded", _NOW - timedelta(hours=30))
    s = refresh.compute_status(succ, succ, now=_NOW)
    assert s.state == "stale" and s.source == "live"


def test_status_running_overlays():
    running = _run_row("running", _NOW, run_id="r2")
    succ = _run_row("succeeded", _NOW - timedelta(hours=1), run_id="r1")
    s = refresh.compute_status(running, succ, now=_NOW)
    assert s.state == "running" and s.last_run_state == "running"
    # A fresh prior snapshot still backs the reads while the new run is in flight.
    assert s.source == "mirror"


def test_status_failed_without_prior_success():
    failed = _run_row("failed", _NOW)
    s = refresh.compute_status(failed, None, now=_NOW)
    assert s.state == "failed" and s.source == "live"


# ── Trigger idempotency (mock the launcher) ─────────────────────────────────


def _client_refresh() -> TestClient:
    app = FastAPI()
    app.include_router(refresh_router)
    return TestClient(app)


def test_trigger_while_running_does_not_launch_duplicate(monkeypatch):
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")

    async def _latest_run(ws):
        return {"run_id": "r2", "state": "running", "as_of": _NOW.isoformat()}

    async def _latest_succeeded(ws):
        return {"run_id": "r1", "state": "succeeded", "as_of": (_NOW - timedelta(hours=1)).isoformat()}

    launched = {"n": 0}

    def _boom_launch(*a, **k):
        launched["n"] += 1
        return "should-not-happen"

    monkeypatch.setattr(mirror, "latest_run", _latest_run)
    monkeypatch.setattr(mirror, "latest_succeeded_run", _latest_succeeded)
    monkeypatch.setattr(refresh, "_launch", _boom_launch)

    resp = _client_refresh().post("/api/ontology/refresh")
    assert resp.status_code == 200
    assert resp.json()["state"] == "running"
    assert launched["n"] == 0  # no duplicate launch


def test_trigger_launches_when_idle(monkeypatch):
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.setenv("GSO_ONT_JOB_ID", "12345")

    async def _none(ws):
        return None

    async def _settings():
        return OntologySettings(company_name=None, catalog_allowlist=["finance"])

    launched = {"n": 0}

    def _launch(job_id, *, workspace_id, allowlist):
        launched["n"] += 1
        launched["args"] = (job_id, workspace_id, allowlist)
        return "job-run-1"

    monkeypatch.setattr(mirror, "latest_run", _none)
    monkeypatch.setattr(mirror, "latest_succeeded_run", _none)
    monkeypatch.setattr(ont_settings, "get_settings", _settings)
    monkeypatch.setattr(refresh, "_launch", _launch)

    resp = _client_refresh().post("/api/ontology/refresh")
    assert resp.status_code == 200
    assert resp.json()["state"] == "queued"
    assert launched["n"] == 1
    assert launched["args"] == ("12345", "ws1", ["finance"])


def test_trigger_reports_not_configured_without_job_id(monkeypatch):
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    monkeypatch.delenv("GSO_ONT_JOB_ID", raising=False)

    async def _none(ws):
        return None

    monkeypatch.setattr(mirror, "latest_run", _none)
    monkeypatch.setattr(mirror, "latest_succeeded_run", _none)
    resp = _client_refresh().post("/api/ontology/refresh")
    assert resp.status_code == 200
    assert "set up" in (resp.json()["message"] or "").lower()


# ── Reader swap: mirror when fresh, live fallback otherwise ─────────────────


def _taxonomy_client() -> TestClient:
    app = FastAPI()
    app.include_router(taxonomy_router.router)
    return app


def _tags_client() -> TestClient:
    app = FastAPI()
    app.include_router(tags_router.router)
    return app


def _patch_settings(monkeypatch):
    async def _settings():
        return OntologySettings(company_name=None, catalog_allowlist=["finance"])
    monkeypatch.setattr(ont_settings, "get_settings", _settings)
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")


def test_taxonomy_serves_mirror_when_fresh(monkeypatch):
    _patch_settings(monkeypatch)

    async def _fresh(ws):
        return True

    async def _tree(ws):
        return {
            "domains": [{"tag_key": "Finance", "name": "Finance", "member_count": 1, "subdomains": [], "members": []}],
            "ungrouped": {"metric_views": [], "genie_agents": []},
            "as_of": "2026-08-30T09:00:00+00:00",
        }

    monkeypatch.setattr(taxonomy_router.refresh, "mirror_is_fresh", _fresh)
    monkeypatch.setattr(taxonomy_router.mirror, "read_taxonomy_tree", _tree)
    # If the live path were taken this would blow up — proving the mirror served.
    monkeypatch.setattr(taxonomy_router.tag_graph, "build_graph", lambda a: (_ for _ in ()).throw(AssertionError("live path used")))

    data = TestClient(_taxonomy_client()).get("/api/ontology/taxonomy").json()
    assert [d["tag_key"] for d in data["domains"]] == ["Finance"]
    assert data["as_of"] == "2026-08-30T09:00:00+00:00"  # mirror materialization time


def test_taxonomy_falls_back_to_live_when_cold(monkeypatch):
    _patch_settings(monkeypatch)

    async def _not_fresh(ws):
        return False

    monkeypatch.setattr(taxonomy_router.refresh, "mirror_is_fresh", _not_fresh)
    monkeypatch.setattr(taxonomy_router, "get_workspace_client", lambda: object())
    monkeypatch.setattr(
        taxonomy_router.tag_graph, "build_graph",
        lambda a: {"tags": [{"tag_key": "Finance", "allowed_values": [], "assignment_count": 1,
                             "members": [{"fqn": "finance.core.ledger", "asset_type": "table"}]},
                            {"tag_key": "Finance/Tax", "allowed_values": [], "assignment_count": 1,
                             "members": [{"fqn": "finance.tax.filings", "asset_type": "table"}]}],
                   "as_of": "2026-08-30T12:00:00+00:00"},
    )
    monkeypatch.setattr(taxonomy_router.inventory, "metric_view_fqns", lambda c, a: ["finance.rep.mv"])
    monkeypatch.setattr(taxonomy_router.genie_client, "list_genie_spaces", lambda: [])

    data = TestClient(_taxonomy_client()).get("/api/ontology/taxonomy").json()
    assert [d["tag_key"] for d in data["domains"]] == ["Finance"]
    assert {m["fqn"] for m in data["ungrouped"]["metric_views"]} == {"finance.rep.mv"}


def test_tags_serves_mirror_graph_when_fresh(monkeypatch):
    _patch_settings(monkeypatch)

    async def _fresh(ws):
        return True

    async def _graph(ws):
        return {"tags": [
            {"tag_key": "Finance", "allowed_values": [], "assignment_count": 2, "members": []},
            {"tag_key": "Finance/Tax", "allowed_values": [], "assignment_count": 1, "members": []},
        ], "as_of": "2026-08-30T09:00:00+00:00"}

    monkeypatch.setattr(tags_router.refresh, "mirror_is_fresh", _fresh)
    monkeypatch.setattr(tags_router.mirror, "read_tag_graph", _graph)
    monkeypatch.setattr(tags_router.tag_graph, "build_graph",
                        lambda a: (_ for _ in ()).throw(AssertionError("live path used")))

    data = TestClient(_tags_client()).get("/api/ontology/tags").json()
    by_key = {t["tag_key"]: t for t in data["tags"]}
    assert by_key["Finance"]["acts_as_domain"] is True
    assert by_key["Finance/Tax"]["acts_as_subdomain"] is True
    assert data["as_of"] == "2026-08-30T09:00:00+00:00"


def test_tags_falls_back_to_live_when_cold(monkeypatch):
    _patch_settings(monkeypatch)

    async def _not_fresh(ws):
        return False

    monkeypatch.setattr(tags_router.refresh, "mirror_is_fresh", _not_fresh)
    monkeypatch.setattr(tags_router.tag_graph, "build_graph",
                        lambda a: {"tags": [{"tag_key": "finance", "allowed_values": [], "assignment_count": 0, "members": []}],
                                   "as_of": "2026-08-30T12:00:00+00:00"})

    data = TestClient(_tags_client()).get("/api/ontology/tags").json()
    assert [t["tag_key"] for t in data["tags"]] == ["finance"]
    # orphan (0 assignments) surfaces in cleanup via the live path.
    assert any(c["flag"] == "orphan" for c in data["cleanup"])


# ── Mirror reconstruction (Delta rows → graph) closes the parity loop ───────


async def test_mirror_read_tag_graph_reconstructs_from_delta_rows(monkeypatch):
    from genie_space_optimizer.ontology import transforms

    # Rows shaped like the materializer's genie_ont_tag_graph writes.
    delta_rows = [
        {"tag_key": "Finance", "allowed_values": [], "assignment_count": 2, "as_of": "2026-08-30T09:00:00+00:00"},
        {"tag_key": "Finance/Tax", "allowed_values": '["x"]', "assignment_count": 1, "as_of": "2026-08-30T09:00:00+00:00"},
    ]
    monkeypatch.setattr(mirror, "_synced_pool", lambda: None)
    monkeypatch.setattr(mirror, "_delta_query", lambda sql, params=None: delta_rows)

    graph = await mirror.read_tag_graph("ws1")
    assert [t["tag_key"] for t in graph["tags"]] == ["Finance", "Finance/Tax"]
    assert graph["tags"][1]["allowed_values"] == ["x"]  # JSON-array string parsed
    # The same transforms the live route runs yield a consistent lens off the mirror.
    gtags = transforms.governed_tag_rows(graph)
    by_key = {g["tag_key"]: g for g in gtags}
    assert by_key["Finance"]["acts_as_domain"] is True
    assert by_key["Finance/Tax"]["acts_as_subdomain"] is True


async def test_mirror_read_taxonomy_tree_parses_json(monkeypatch):
    tree = {"domains": [], "ungrouped": {"metric_views": [], "genie_agents": []}, "as_of": "2026-08-30T09:00:00+00:00"}
    import json as _json

    monkeypatch.setattr(mirror, "_synced_pool", lambda: None)
    monkeypatch.setattr(mirror, "_delta_query", lambda sql, params=None: [{"tree": _json.dumps(tree)}])
    got = await mirror.read_taxonomy_tree("ws1")
    assert got == tree


async def test_mirror_returns_none_without_warehouse(monkeypatch):
    # No synced pool and no warehouse (empty _delta_query) → None → route degrades.
    monkeypatch.setattr(mirror, "_synced_pool", lambda: None)
    monkeypatch.setattr(mirror, "_delta_query", lambda sql, params=None: [])
    assert await mirror.read_tag_graph("ws1") is None
    assert await mirror.read_taxonomy_tree("ws1") is None
    assert await mirror.latest_succeeded_run("ws1") is None
