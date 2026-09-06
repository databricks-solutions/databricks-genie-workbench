"""Ontology apply — Phase 5 (17i) plan builder + consent gate + mirror hydration.

Covers the OFFLINE-testable surface of the subsystem's ONLY governed-tag write path:

- ``build_apply_plan`` expands APPROVED consents to the three write shapes
  (``reuse`` → SET TAG; ``create`` → CREATE GOVERNED TAG + SET TAG; ``reassign`` →
  UNSET + SET), excludes page consents (MV-D27), and produces a deterministic
  ``plan_hash`` — writing nothing (dry-run purity).
- The execute route enforces the two-step consent gate: ``confirm=true`` AND the
  preview's ``plan_hash`` (409 on mismatch, and the 409 must surface, not degrade),
  and attributes the write to the OBO caller (MV-D50).
- The mirror readers hydrate a bare consent key from its ``genie_ont_domains``
  proposal row (metastore grain, MV-D49) and scope reassign moves conservatively.

No warehouse, no UC write, no OBO round trip — the live apply is deploy-gated.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology import models
from backend.ontology.routers.apply import router as apply_router
from backend.ontology.services import apply as apply_service
from backend.ontology.services import mirror, ont_settings


# ── helpers ──────────────────────────────────────────────────────────────────


def _patch_mirror(monkeypatch, *, consents, members_by_domain, tag_members=None):
    """Stub the three Phase-5 mirror readers apply_service calls."""

    async def _consents(_ms):
        return list(consents)

    async def _domain_members(_ms, domain_id):
        return list(members_by_domain.get(domain_id, []))

    async def _tag_members(_ms, conflict_tag):
        return list((tag_members or {}).get(conflict_tag, []))

    monkeypatch.setattr(mirror, "read_approved_consents", _consents)
    monkeypatch.setattr(mirror, "read_domain_members", _domain_members)
    monkeypatch.setattr(mirror, "read_tag_members", _tag_members)


def _statements(plan):
    return [i.statement for i in plan.items]


# ── build_apply_plan: the three write shapes ──────────────────────────────────


async def test_reuse_domain_emits_set_tag_only(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "domain",
                "proposal_id": "sug_a",
                "name": "Revenue",
                "tag_decision": "reuse",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}, {"asset_fqn": "cat.sch.line_items"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    assert not any(s.startswith("CREATE GOVERNED TAG") for s in stmts)
    assert "ALTER ASSET `cat.sch.orders` SET TAG `business_domain` = 'Revenue'" in stmts
    assert "ALTER ASSET `cat.sch.line_items` SET TAG `business_domain` = 'Revenue'" in stmts
    assert plan.executable_count == 2
    assert plan.source == "mirror"


async def test_create_subdomain_emits_create_then_set(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "subdomain",
                "proposal_id": "sug_b",
                "name": "Revenue/Billing",
                "tag_decision": "create",
                "tag_key": "business_subdomain",
                "tag_value": "Revenue/Billing",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_b": [{"asset_fqn": "cat.sch.invoices"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    assert stmts[0] == "CREATE GOVERNED TAG `business_subdomain` WITH ALLOWED_VALUES ('Revenue/Billing')"
    assert "ALTER ASSET `cat.sch.invoices` SET TAG `business_subdomain` = 'Revenue/Billing'" in stmts


async def test_reassign_emits_unset_then_set(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "reassign",
                "proposal_id": "sug_c",
                "name": "Revenue",
                "tag_decision": "reassign",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "legacy_domain",
            }
        ],
        members_by_domain={},
        tag_members={"legacy_domain": [{"asset_fqn": "cat.sch.txns"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    assert "ALTER ASSET `cat.sch.txns` UNSET TAG `legacy_domain`" in stmts
    assert "ALTER ASSET `cat.sch.txns` SET TAG `business_domain` = 'Revenue'" in stmts
    # order: unset precedes set for the moved asset
    assert stmts.index("ALTER ASSET `cat.sch.txns` UNSET TAG `legacy_domain`") < stmts.index(
        "ALTER ASSET `cat.sch.txns` SET TAG `business_domain` = 'Revenue'"
    )


async def test_page_consent_excluded(monkeypatch):
    # Even if a page consent slips into the list, build_apply_plan skips it (MV-D27).
    _patch_mirror(
        monkeypatch,
        consents=[{"proposal_kind": "page", "proposal_id": "pg_x", "tag_key": "", "tag_value": None}],
        members_by_domain={},
    )
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == []


async def test_plan_hash_is_deterministic(monkeypatch):
    cfg = dict(
        consents=[
            {
                "proposal_kind": "domain",
                "proposal_id": "sug_a",
                "name": "Revenue",
                "tag_decision": "reuse",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}]},
    )
    _patch_mirror(monkeypatch, **cfg)
    h1 = (await apply_service.build_apply_plan("ms1")).plan_hash
    _patch_mirror(monkeypatch, **cfg)
    h2 = (await apply_service.build_apply_plan("ms1")).plan_hash
    assert h1 and h1 == h2


async def test_no_consents_yields_empty_mirror_plan(monkeypatch):
    _patch_mirror(monkeypatch, consents=[], members_by_domain={})
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == [] and plan.source == "mirror"


async def test_reader_failure_degrades_to_cold(monkeypatch):
    async def _boom(_ms):
        raise RuntimeError("mirror down")

    monkeypatch.setattr(mirror, "read_approved_consents", _boom)
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == [] and plan.source == "cold"


# ── execute route: the two-step consent gate + OBO attribution ────────────────


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    app = FastAPI()
    app.include_router(apply_router)
    return TestClient(app)


def test_execute_requires_confirm(client):
    resp = client.post("/api/ontology/apply/execute", json={"plan_hash": "abc", "confirm": False})
    assert resp.status_code == 400


def test_execute_409_on_plan_hash_mismatch(client, monkeypatch):
    async def _plan(_ms):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="real", source="mirror", as_of="t"
        )

    monkeypatch.setattr(apply_service, "build_apply_plan", _plan)
    resp = client.post("/api/ontology/apply/execute", json={"plan_hash": "stale", "confirm": True})
    assert resp.status_code == 409


def test_execute_runs_and_attributes_to_obo_caller(client, monkeypatch):
    async def _plan(_ms):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
        )

    captured: dict = {}

    async def _exec(*, plan, metastore_id, workspace_id, applied_by):
        captured.update(metastore_id=metastore_id, workspace_id=workspace_id, applied_by=applied_by)
        return models.ApplyResult(as_of="t")

    monkeypatch.setattr(apply_service, "build_apply_plan", _plan)
    monkeypatch.setattr(apply_service, "execute_apply_plan", _exec)
    resp = client.post(
        "/api/ontology/apply/execute",
        json={"plan_hash": "h", "confirm": True},
        headers={"x-forwarded-email": "curator@databricks.com"},
    )
    assert resp.status_code == 200
    assert captured == {"metastore_id": "ms1", "workspace_id": "ws1", "applied_by": "curator@databricks.com"}


# ── mirror hydration: bare consent key → proposal tag fields ──────────────────


async def test_read_approved_consents_hydrates_from_domain_row(monkeypatch):
    captured: dict = {}

    def _delta(sql, params=None):
        captured["sql"] = sql
        return [{"proposal_kind": "reassign", "proposal_id": "sug_c"}]

    async def _read_table(table, _ms):
        assert table == "genie_ont_domains"
        return [
            {
                "domain_id": "sug_c",
                "name": "Revenue",
                "tag_decision": "reassign",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "evidence": {"conflict": {"existing_tag": "legacy_domain"}},
            }
        ]

    monkeypatch.setattr(mirror, "_delta_query", _delta)
    monkeypatch.setattr(mirror, "_read_table", _read_table)
    out = await mirror.read_approved_consents("ms1")
    # The read is metastore-scoped, approved-only, and page-excluded (the SQL filter).
    assert "state = 'approved'" in captured["sql"]
    assert "proposal_kind IN ('domain', 'subdomain', 'reassign')" in captured["sql"]
    assert out == [
        {
            "proposal_kind": "reassign",
            "proposal_id": "sug_c",
            "name": "Revenue",
            "tag_decision": "reassign",
            "tag_key": "business_domain",
            "tag_value": "Revenue",
            "conflict_tag": "legacy_domain",
        }
    ]


async def test_read_approved_consents_skips_aged_out_proposal(monkeypatch):
    monkeypatch.setattr(mirror, "_delta_query", lambda sql, params=None: [{"proposal_kind": "domain", "proposal_id": "gone"}])

    async def _empty(_table, _ms):
        return []

    monkeypatch.setattr(mirror, "_read_table", _empty)
    assert await mirror.read_approved_consents("ms1") == []


async def test_read_domain_members_filters_by_domain(monkeypatch):
    async def _rows(_table, _ms):
        return [
            {"domain_id": "sug_a", "asset_fqn": "cat.sch.a"},
            {"domain_id": "sug_b", "asset_fqn": "cat.sch.b"},
            {"domain_id": "sug_a", "asset_fqn": ""},  # empty fqn dropped
        ]

    monkeypatch.setattr(mirror, "_read_table", _rows)
    assert await mirror.read_domain_members("ms1", "sug_a") == [{"asset_fqn": "cat.sch.a"}]


async def test_read_tag_members_scopes_to_conflicted_proposals(monkeypatch):
    calls = {"n": 0}

    async def _read_table(table, _ms):
        calls["n"] += 1
        if table == "genie_ont_domains":
            return [
                {"domain_id": "sug_c", "evidence": {"conflict": {"existing_tag": "legacy_domain"}}},
                {"domain_id": "sug_d", "evidence": {"conflict": {"existing_tag": "other"}}},
            ]
        return [
            {"domain_id": "sug_c", "asset_fqn": "cat.sch.txns"},
            {"domain_id": "sug_d", "asset_fqn": "cat.sch.other"},
        ]

    monkeypatch.setattr(mirror, "_read_table", _read_table)
    out = await mirror.read_tag_members("ms1", "legacy_domain")
    assert out == [{"asset_fqn": "cat.sch.txns"}]  # only the conflicted proposal's members
    assert await mirror.read_tag_members("ms1", "") == []  # empty tag → no read
