"""Tests for the Join Advisor (Semantic Blueprint v4 §7) — advice, not config edit.

The Join Advisor proposes candidate joins the Auto-Optimize run re-validates and
adds itself; the Workbench never writes them into ``serialized_space``. These
tests pin the offline seams without a warehouse or Databricks:

- **Heuristics**: ``is_key_like`` / ``_norm_type`` (what may be a join key, and
  when two column types are join-compatible).
- **Config reads**: ``_table_identifiers`` (in-scope FQNs) and ``_declared_pairs``
  (existing join_specs the advisor must never re-propose).
- **Candidate generation** (``candidate_pairs``): pure, deterministic; only
  key-like, type-compatible, shared columns; oriented child→parent; declared
  pairs skipped.
- **Containment probe**: ``containment_sql`` is a single read-only SELECT;
  ``_probe_candidate`` maps its row to a probe ratio and upgrades N:1→1:1.
- **Discovery pipeline** (``discover_candidates``): the honest-empty ``status``
  discriminator (no_candidates / fully_connected / no_warehouse / ok) and the FK
  upgrade, driven by a stubbed ``execute_sql``.
- **Persistence**: ``lakebase.save_join_advice`` / ``get_join_advice`` round-trip
  and clear (in-memory fallback).
- **Endpoints**: ``/join-candidates`` and ``/join-advice`` (GET/POST) shapes,
  aliases, and the empty-clears contract.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import join_advisor
from backend.services import lakebase


# ── Heuristics ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "col,expected",
    [
        ("customer_id", True),
        ("order_key", True),
        ("product_code", True),
        ("id", True),
        ("KEY", True),
        ("line_no", True),
        ("customer_sk", True),
        ("amount", False),
        ("description", False),
        ("", False),
    ],
)
def test_is_key_like(col, expected):
    assert join_advisor.is_key_like(col) is expected


@pytest.mark.parametrize(
    "a,b,match",
    [
        ("BIGINT", "INT", True),
        ("bigint", "integer", True),
        ("VARCHAR(64)", "STRING", True),
        ("string", "char", True),
        ("DECIMAL(10,2)", "double", True),
        ("INT", "STRING", False),
        ("date", "timestamp", False),
    ],
)
def test_norm_type_join_compatibility(a, b, match):
    assert (join_advisor._norm_type(a) == join_advisor._norm_type(b)) is match


# ── Config reads ─────────────────────────────────────────────────────────────


def test_table_identifiers_only_three_part_names_deduped_in_order():
    space = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.orders"},
                {"identifier": "cat.sch.orders"},  # dup (case-insensitive)
                {"identifier": "cat.sch.customer"},
                {"identifier": "two.part"},  # not FQN → dropped
                {"nope": 1},
            ]
        }
    }
    assert join_advisor._table_identifiers(space) == [
        "cat.sch.orders",
        "cat.sch.customer",
    ]


def test_declared_pairs_extracts_equality_endpoints_short_name_lowercased():
    space = {
        "instructions": {
            "join_specs": [
                {"sql": ["`order_items`.`order_id` = `orders`.`order_id`"]},
            ]
        }
    }
    pairs = join_advisor._declared_pairs(space)
    assert frozenset({"order_items.order_id", "orders.order_id"}) in pairs


# ── Candidate generation (pure) ──────────────────────────────────────────────


def _cols(*names_types):
    return [{"name": n, "type": t} for n, t in names_types]


def test_candidate_pairs_emits_oriented_key_like_type_matched_only():
    columns = {
        "cat.sch.orders": _cols(
            ("order_id", "bigint"),
            ("customer_id", "bigint"),
            ("amount", "double"),  # not key-like → ignored
        ),
        "cat.sch.customer": _cols(
            ("customer_id", "int"),  # bigint vs int → compatible
            ("name", "string"),
        ),
    }
    cands = join_advisor.candidate_pairs(columns, declared=set())
    assert len(cands) == 1
    c = cands[0]
    # customer_id points to the customer table (child=orders, parent=customer).
    assert c["from"] == "cat.sch.orders"
    assert c["to"] == "cat.sch.customer"
    assert c["fromCol"] == "customer_id"
    assert c["toCol"] == "customer_id"
    assert c["match"] == "name-type"
    assert c["rel"] == "N:1"
    assert c["probe"] is None


def test_candidate_pairs_skips_type_mismatch_and_non_key_and_declared():
    columns = {
        "cat.sch.a": _cols(("thing_id", "string"), ("shared", "int")),
        "cat.sch.b": _cols(("thing_id", "bigint"), ("shared", "int")),
    }
    # thing_id: string vs bigint → type mismatch, dropped.
    # shared: type-matched but not key-like, dropped.
    assert join_advisor.candidate_pairs(columns, declared=set()) == []

    columns2 = {
        "cat.sch.a": _cols(("thing_id", "bigint")),
        "cat.sch.b": _cols(("thing_id", "bigint")),
    }
    # Undirected declared pair on short-name.col must suppress the candidate.
    declared = {frozenset({"a.thing_id", "b.thing_id"})}
    assert join_advisor.candidate_pairs(columns2, declared) == []
    # Without the declaration, the candidate returns.
    assert len(join_advisor.candidate_pairs(columns2, declared=set())) == 1


# ── Containment probe ────────────────────────────────────────────────────────


def test_containment_sql_is_a_single_read_only_select_with_quoted_idents():
    sql = join_advisor.containment_sql(
        "cat.sch.orders", "customer_id", "cat.sch.customer", "customer_id"
    )
    lowered = sql.lower()
    assert lowered.lstrip().startswith("select")
    # No mutation verbs leaked into the probe.
    for verb in ("insert", "update", "delete", "drop", "merge", "create"):
        assert verb not in lowered
    # Identifiers are backtick-quoted and fully-qualified.
    assert "`cat`.`sch`.`orders`" in sql
    assert "`cat`.`sch`.`customer`" in sql


def test_probe_candidate_maps_row_to_ratio_and_upgrades_to_one_to_one(monkeypatch):
    # matched / from_distinct = 90/100 = 0.9, parent not unique → stays N:1.
    monkeypatch.setattr(
        join_advisor, "execute_sql", lambda sql: {"data": [[100, 90, False]], "error": None}
    )
    cand = {
        "id": "x", "from": "c.s.f", "fromCol": "k", "to": "c.s.d", "toCol": "k",
        "rel": "N:1", "match": "name-type", "probe": None,
    }
    join_advisor._probe_candidate(cand)
    assert cand["probe"] == 0.9
    assert cand["rel"] == "N:1"

    # Full containment + unique parent key → 1:1.
    monkeypatch.setattr(
        join_advisor, "execute_sql", lambda sql: {"data": [[100, 100, True]], "error": None}
    )
    cand2 = dict(cand, probe=None, rel="N:1")
    join_advisor._probe_candidate(cand2)
    assert cand2["probe"] == 1.0
    assert cand2["rel"] == "1:1"


def test_probe_candidate_stays_none_on_error(monkeypatch):
    monkeypatch.setattr(
        join_advisor, "execute_sql", lambda sql: {"data": [], "error": "no warehouse"}
    )
    cand = {"id": "x", "from": "c.s.f", "fromCol": "k", "to": "c.s.d", "toCol": "k",
            "rel": "N:1", "match": "name-type", "probe": None}
    join_advisor._probe_candidate(cand)
    assert cand["probe"] is None


# ── Discovery pipeline ───────────────────────────────────────────────────────


def test_discover_needs_two_tables():
    assert join_advisor.discover_candidates({}) == {"status": "no_candidates", "candidates": []}
    one = {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
    assert join_advisor.discover_candidates(one) == {"status": "fully_connected", "candidates": []}


def test_discover_no_warehouse_when_columns_unavailable(monkeypatch):
    space = {"data_sources": {"tables": [{"identifier": "c.s.a"}, {"identifier": "c.s.b"}]}}
    monkeypatch.setattr(join_advisor, "_columns_by_table", lambda ids: {})
    assert join_advisor.discover_candidates(space) == {"status": "no_warehouse", "candidates": []}


def test_discover_ok_probes_candidates_and_upgrades_fk(monkeypatch):
    space = {
        "data_sources": {
            "tables": [{"identifier": "c.s.orders"}, {"identifier": "c.s.customer"}]
        }
    }
    monkeypatch.setattr(
        join_advisor, "_columns_by_table",
        lambda ids: {
            "c.s.orders": _cols(("customer_id", "bigint")),
            "c.s.customer": _cols(("customer_id", "bigint")),
        },
    )
    # A declared FK on this endpoint pair upgrades match to "fk".
    monkeypatch.setattr(
        join_advisor, "foreign_key_pairs",
        lambda ids: {frozenset({"orders.customer_id", "customer.customer_id"})},
    )
    monkeypatch.setattr(
        join_advisor, "execute_sql", lambda sql: {"data": [[50, 50, True]], "error": None}
    )
    result = join_advisor.discover_candidates(space)
    assert result["status"] == "ok"
    assert len(result["candidates"]) == 1
    c = result["candidates"][0]
    assert c["match"] == "fk"
    assert c["probe"] == 1.0
    assert c["rel"] == "1:1"


def test_discover_no_warehouse_when_candidates_unprobed(monkeypatch):
    space = {
        "data_sources": {
            "tables": [{"identifier": "c.s.orders"}, {"identifier": "c.s.customer"}]
        }
    }
    monkeypatch.setattr(
        join_advisor, "_columns_by_table",
        lambda ids: {
            "c.s.orders": _cols(("customer_id", "bigint")),
            "c.s.customer": _cols(("customer_id", "bigint")),
        },
    )
    monkeypatch.setattr(join_advisor, "foreign_key_pairs", lambda ids: set())
    # Probe fails for every candidate → honest "no_warehouse" but candidates kept.
    monkeypatch.setattr(
        join_advisor, "execute_sql", lambda sql: {"data": [], "error": "down"}
    )
    result = join_advisor.discover_candidates(space)
    assert result["status"] == "no_warehouse"
    assert len(result["candidates"]) == 1
    assert result["candidates"][0]["probe"] is None


# ── Persistence (in-memory fallback) ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_join_advice_roundtrip_and_clear():
    space_id = "jadv-space-roundtrip"
    seeds = [{
        "id": "jc:orders.customer_id->customer.customer_id",
        "from": "c.s.orders", "fromCol": "customer_id",
        "to": "c.s.customer", "toCol": "customer_id",
        "rel": "N:1", "match": "name-type", "probe": 0.97, "note": None,
    }]
    saved = await lakebase.save_join_advice(space_id, seeds, seeded_by="tester@example.com")
    assert saved["seeds"] == seeds
    got = await lakebase.get_join_advice(space_id)
    assert got is not None
    assert got["seeds"] == seeds
    assert got["seeded_by"] == "tester@example.com"

    # Empty seeds clears the advice.
    await lakebase.save_join_advice(space_id, [], seeded_by="tester@example.com")
    assert await lakebase.get_join_advice(space_id) is None


# ── Endpoints ────────────────────────────────────────────────────────────────


@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def test_join_candidates_endpoint_returns_aliased_candidates(client, monkeypatch):
    monkeypatch.setattr(auto_optimize, "get_serialized_space", lambda sid: {"data_sources": {}})
    monkeypatch.setattr(
        join_advisor, "discover_candidates",
        lambda space_data: {
            "status": "ok",
            "candidates": [{
                "id": "jc:orders.customer_id->customer.customer_id",
                "from": "c.s.orders", "fromCol": "customer_id",
                "to": "c.s.customer", "toCol": "customer_id",
                "rel": "N:1", "match": "fk", "probe": 0.99, "note": None,
            }],
        },
    )
    resp = client.get("/api/auto-optimize/spaces/space-x/join-candidates")
    assert resp.status_code == 200
    data = resp.json()
    assert data["space_id"] == "space-x"
    assert data["status"] == "ok"
    c = data["candidates"][0]
    # Serialized with the frontend aliases, not the python field names.
    assert c["from"] == "c.s.orders"
    assert c["fromCol"] == "customer_id"
    assert c["toCol"] == "customer_id"
    assert c["match"] == "fk"


def test_join_candidates_endpoint_502_on_config_read_failure(client, monkeypatch):
    def boom(sid):
        raise RuntimeError("no access")

    monkeypatch.setattr(auto_optimize, "get_serialized_space", boom)
    resp = client.get("/api/auto-optimize/spaces/space-x/join-candidates")
    assert resp.status_code == 502


def test_join_advice_post_then_get_roundtrip(client):
    space_id = "space-advice-endpoint"
    body = {
        "seeds": [{
            "id": "jc:orders.customer_id->customer.customer_id",
            "from": "c.s.orders", "fromCol": "customer_id",
            "to": "c.s.customer", "toCol": "customer_id",
            "rel": "N:1", "match": "name-type", "probe": 0.9,
        }]
    }
    post = client.post(f"/api/auto-optimize/spaces/{space_id}/join-advice", json=body)
    assert post.status_code == 200
    assert post.json()["seeds"][0]["fromCol"] == "customer_id"

    get = client.get(f"/api/auto-optimize/spaces/{space_id}/join-advice")
    assert get.status_code == 200
    assert len(get.json()["seeds"]) == 1

    # Empty seeds clears; GET then returns an empty advice set.
    clear = client.post(f"/api/auto-optimize/spaces/{space_id}/join-advice", json={"seeds": []})
    assert clear.status_code == 200
    assert clear.json()["seeds"] == []
    assert client.get(f"/api/auto-optimize/spaces/{space_id}/join-advice").json()["seeds"] == []
