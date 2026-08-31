"""Ontology drafts serve + decision route (Phase 3d §11, items 5 + 9).

Covers the decision route (approve → consents, dismiss / reassign_reject →
suppressions, reassign_accept → consents; idempotent MERGE keyed on
(metastore_id, kind, proposal_id); decided_by = OBO email; **no SET TAG**), the
GET /drafts serve shape, and degrade-not-hang (a mirror failure yields a typed
empty ``source="cold"`` payload, never a 500).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology.routers.drafts import router as drafts_router
from backend.ontology.services import decisions, mirror, ont_settings, refresh


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    app = FastAPI()
    app.include_router(drafts_router)
    return TestClient(app)


# ── record_decision: the ledger MERGE (service level) ───────────────────────


class _FakeStmtExec:
    def __init__(self, captured):
        self._captured = captured

    def execute_statement(self, *, warehouse_id, statement, parameters, wait_timeout):
        self._captured["sql"] = statement
        self._captured["params"] = {p.name: p.value for p in parameters}
        return SimpleNamespace(
            statement_id="s1",
            status=SimpleNamespace(state=_SUCCEEDED, error=None),
        )

    def get_statement(self, *, statement_id):  # pragma: no cover — never polled (already SUCCEEDED)
        return SimpleNamespace(statement_id=statement_id, status=SimpleNamespace(state=_SUCCEEDED))


# Resolve the real enum once so the fake reports the true SUCCEEDED value.
from databricks.sdk.service.sql import StatementState as _StatementState  # noqa: E402

_SUCCEEDED = _StatementState.SUCCEEDED


def _wire_warehouse(monkeypatch, captured):
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "genie_space_optimizer")
    fake_client = SimpleNamespace(statement_execution=_FakeStmtExec(captured))
    monkeypatch.setattr(decisions, "get_workspace_client", lambda: fake_client)
    return captured


@pytest.mark.parametrize(
    "action,expected,table",
    [
        ("approve", "consent", "genie_ont_consents"),
        ("reassign_accept", "consent", "genie_ont_consents"),
        ("dismiss", "suppression", "genie_ont_suppressions"),
        ("reassign_reject", "suppression", "genie_ont_suppressions"),
    ],
)
def test_decision_routes_to_correct_ledger(monkeypatch, action, expected, table):
    captured: dict = {}
    _wire_warehouse(monkeypatch, captured)
    recorded = decisions.record_decision(
        kind="reassign" if "reassign" in action else "domain",
        proposal_id="sug_x", action=action, metastore_id="ms1",
        workspace_id="ws1", decided_by="alice@example.com",
    )
    assert recorded == expected
    assert f"MERGE INTO main.genie_space_optimizer.{table}" in captured["sql"]
    # Idempotency key is (metastore_id, proposal_kind, proposal_id).
    assert "t.metastore_id = s.metastore_id" in captured["sql"]
    assert "t.proposal_kind = s.proposal_kind" in captured["sql"]
    assert "t.proposal_id = s.proposal_id" in captured["sql"]
    # decided_by is the OBO email; NO governed-tag write anywhere.
    assert captured["params"]["decided_by"] == "alice@example.com"
    assert captured["params"]["proposal_id"] == "sug_x"
    assert "set tag" not in captured["sql"].lower()
    assert "governed tag" not in captured["sql"].lower()


def test_unknown_action_is_rejected(monkeypatch):
    _wire_warehouse(monkeypatch, {})
    with pytest.raises(ValueError):
        decisions.record_decision(
            kind="domain", proposal_id="x", action="frobnicate",
            metastore_id="ms1", workspace_id="ws1", decided_by="a@b.c",
        )


# ── POST /decision route: OBO email + response shape ────────────────────────


def test_post_decision_uses_obo_email_header_and_records(client, monkeypatch):
    seen: dict = {}

    def _fake_record(*, kind, proposal_id, action, metastore_id, workspace_id, decided_by):
        seen.update(locals())
        return "suppression"

    monkeypatch.setattr(decisions, "record_decision", _fake_record)
    resp = client.post(
        "/api/ontology/decision",
        json={"kind": "domain", "proposal_id": "sug_1", "action": "dismiss"},
        headers={"x-forwarded-email": "curator@acme.com"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True and body["recorded"] == "suppression"
    assert seen["decided_by"] == "curator@acme.com"
    assert seen["metastore_id"] == "ms1" and seen["proposal_id"] == "sug_1"


def test_post_decision_rejects_unknown_enum(client):
    resp = client.post(
        "/api/ontology/decision",
        json={"kind": "domain", "proposal_id": "x", "action": "not_an_action"},
    )
    assert resp.status_code == 422  # pydantic enum validation


# ── GET /drafts: serve shape + degrade-not-hang ─────────────────────────────


def test_get_drafts_serves_ranked_mirror_payload(client, monkeypatch):
    async def _fresh(ms):
        return True

    async def _domains(ms):
        return [{
            "proposal_id": "sug_d", "kind": "reassign", "name": "Finance",
            "description": "d", "tag_decision": "reassign", "conflict_tag": "finance",
            "subdomains": ["Tax"], "members": [{"fqn": "c.s.t", "asset_type": "table"}],
            "why": "worth confirming", "evidence": [{"label": "Overlaps the “finance” tag", "kind": "conflict"}],
            "tier": "high",
        }]

    async def _pages(ms):
        return [{
            "proposal_id": "pg_1", "archetype": "Routing", "title": "[Routing] total_revenue",
            "reason": "Answer from the governed metric view.", "body": "Description: ...",
            "synonyms": ["TR", "net sales", "revenue"], "related_fqns": [], "source_fqns": ["c.s.mv"],
            "certify": True, "evidence": [{"label": "Backed by 2 sources", "kind": "corroboration"}],
            "tier": "medium",
        }]

    async def _run(ms):
        return {"as_of": "2026-08-31T00:00:00+00:00"}

    monkeypatch.setattr(refresh, "mirror_is_fresh", _fresh)
    monkeypatch.setattr(mirror, "read_domain_drafts", _domains)
    monkeypatch.setattr(mirror, "read_page_drafts", _pages)
    monkeypatch.setattr(mirror, "latest_succeeded_run", _run)

    body = client.get("/api/ontology/drafts").json()
    assert body["source"] == "mirror"
    assert body["domains"][0]["kind"] == "reassign" and body["domains"][0]["conflict_tag"] == "finance"
    assert body["pages"][0]["archetype"] == "Routing" and body["pages"][0]["certify"] is True
    assert body["as_of"] == "2026-08-31T00:00:00+00:00"


def test_get_drafts_cold_when_mirror_not_fresh(client, monkeypatch):
    async def _stale(ms):
        return False

    monkeypatch.setattr(refresh, "mirror_is_fresh", _stale)
    body = client.get("/api/ontology/drafts").json()
    assert body == {"domains": [], "pages": [], "source": "cold", "as_of": body["as_of"]}


def test_get_drafts_degrades_to_cold_on_mirror_failure(client, monkeypatch):
    async def _fresh(ms):
        return True

    async def _boom(ms):
        raise RuntimeError("warehouse down")

    monkeypatch.setattr(refresh, "mirror_is_fresh", _fresh)
    monkeypatch.setattr(mirror, "read_domain_drafts", _boom)
    resp = client.get("/api/ontology/drafts")
    assert resp.status_code == 200  # never a 500
    assert resp.json()["source"] == "cold"
