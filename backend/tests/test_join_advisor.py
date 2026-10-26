"""Tests for the Join Advisor (Semantic Blueprint v4 §7) — advice, not config edit.

The Join Advisor proposes candidate joins the Auto-Optimize run re-validates and
adds itself; the Workbench never writes them into ``serialized_space``. These
tests pin the offline seams without a warehouse or Databricks:

- **Heuristics**: ``is_key_like`` / ``_norm_type`` (what may be a join key, and
  when two column types are join-compatible).
- **Config reads**: ``_table_identifiers`` (in-scope FQNs) and ``_declared_pairs``
  (table pairs already joined — keyed like the optimizer's applier, so aliased
  join_specs are recognised).
- **Table metadata**: ``_table_infos`` (parallel UC ``tables.get``, unreadable
  tables skipped) and the pure ``_columns_by_table`` / ``foreign_key_pairs``
  mappings over SDK ``TableInfo`` objects.
- **Candidate generation** (``candidate_pairs``): pure, deterministic; only
  key-like, type-compatible, shared columns; oriented child→parent; declared
  table pairs skipped entirely.
- **Containment probe**: ``containment_sql`` is a single read-only SELECT;
  ``_probe_candidate`` maps its row to a probe ratio and upgrades N:1→1:1.
- **Discovery pipeline** (``discover_candidates``): the honest-empty ``status``
  discriminator (no_candidates / fully_connected / no_metadata / no_warehouse /
  ok) and the FK upgrade, driven by stubbed ``_table_infos`` and ``execute_sql``.
- **Persistence**: ``lakebase.save_join_advice`` / ``get_join_advice`` round-trip
  and clear (in-memory fallback).
- **Endpoints**: ``/join-candidates`` and ``/join-advice`` (GET/POST) shapes,
  aliases, and the empty-clears contract.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    TableConstraint,
    TableInfo,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import join_advisor, lakebase

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


def test_declared_pairs_keys_aliased_spec_on_its_table_identifiers():
    # The Genie API shape: the ON predicate qualifies by alias (dim_destination_2),
    # so only the left/right identifiers name the tables actually joined.
    space = {
        "instructions": {
            "join_specs": [{
                "left": {"identifier": "Cat.Sch.dim_property", "alias": "dim_property"},
                "right": {"identifier": "`cat`.`sch`.`dim_destination`", "alias": "dim_destination_2"},
                "sql": [
                    "`dim_property`.`destination_id` = `dim_destination_2`.`destination_id`",
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                ],
            }]
        }
    }
    assert join_advisor._declared_pairs(space) == {
        frozenset({"cat.sch.dim_property", "cat.sch.dim_destination"})
    }


def test_declared_pairs_reads_legacy_table_name_fields():
    space = {
        "instructions": {
            "join_specs": [{"left_table_name": "c.s.orders", "right_table_name": "c.s.customer"}]
        }
    }
    assert join_advisor._declared_pairs(space) == {frozenset({"c.s.orders", "c.s.customer"})}


def test_declared_pairs_sql_only_spec_resolves_qualifiers_through_short_names():
    space = {
        "data_sources": {"tables": [{"identifier": "cat.sch.order_items"}, {"identifier": "cat.sch.orders"}]},
        "instructions": {
            "join_specs": [
                {"sql": ["`order_items`.`order_id` = `orders`.`order_id`"]},
                # An alias that names no in-scope table cannot be resolved → skipped.
                {"sql": ["`order_items`.`order_id` = `orders_2`.`order_id`"]},
            ]
        },
    }
    assert join_advisor._declared_pairs(space) == {
        frozenset({"cat.sch.order_items", "cat.sch.orders"})
    }


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
    # A declared (undirected) table pair must suppress the candidate.
    declared = {frozenset({"cat.sch.b", "cat.sch.a"})}
    assert join_advisor.candidate_pairs(columns2, declared) == []
    # Without the declaration, the candidate returns.
    assert len(join_advisor.candidate_pairs(columns2, declared=set())) == 1


def test_candidate_pairs_skips_every_key_of_an_already_joined_pair():
    # The optimizer's applier never adds a second join between two tables already
    # joined, so a different shared key on a declared pair is not a candidate —
    # while an unjoined pair sharing the same key still is.
    columns = {
        "cat.sch.dim_property": _cols(("destination_id", "bigint"), ("host_id", "bigint")),
        "cat.sch.fact_booking": _cols(("destination_id", "bigint"), ("host_id", "bigint")),
        "cat.sch.fact_review": _cols(("host_id", "bigint")),
    }
    declared = {frozenset({"cat.sch.dim_property", "cat.sch.fact_booking"})}
    cands = join_advisor.candidate_pairs(columns, declared)
    pairs = {frozenset({c["from"], c["to"]}) for c in cands}
    assert frozenset({"cat.sch.dim_property", "cat.sch.fact_booking"}) not in pairs
    assert pairs == {
        frozenset({"cat.sch.dim_property", "cat.sch.fact_review"}),
        frozenset({"cat.sch.fact_booking", "cat.sch.fact_review"}),
    }


# ── Table metadata (UC Tables API) ───────────────────────────────────────────


def _info(*columns, constraints=()):
    """A ``TableInfo`` with ``(name, type_text, type_name)`` columns."""
    return TableInfo(
        columns=[ColumnInfo(name=n, type_text=t, type_name=tn) for n, t, tn in columns],
        table_constraints=list(constraints),
    )


def _fk(child_columns, parent_table, parent_columns):
    return TableConstraint(foreign_key_constraint=ForeignKeyConstraint(
        name="fk", child_columns=child_columns, parent_table=parent_table, parent_columns=parent_columns,
    ))


def test_columns_by_table_prefers_type_text_and_falls_back_to_type_name():
    infos = {
        "c.s.orders": _info(
            ("order_id", "bigint", ColumnTypeName.LONG),
            ("customer_id", None, ColumnTypeName.LONG),  # no type text → type name
        ),
        "c.s.empty": _info(),  # no readable columns → dropped
    }
    cols = join_advisor._columns_by_table(infos)
    assert cols == {
        "c.s.orders": [
            {"name": "order_id", "type": "bigint"},
            {"name": "customer_id", "type": "LONG"},
        ]
    }
    # The type-name fallback still lands in the integer join family.
    assert join_advisor._norm_type(cols["c.s.orders"][1]["type"]) == join_advisor._norm_type("bigint")


def test_foreign_key_pairs_reads_single_column_constraints_only():
    infos = {
        "c.s.fact_booking": _info(constraints=[
            TableConstraint(primary_key_constraint=PrimaryKeyConstraint(name="pk", child_columns=["booking_id"])),
            _fk(["destination_id"], "c.s.dim_destination", ["destination_id"]),
            # Differently named key columns are still a declared FK.
            _fk(["check_in_date"], "c.s.dim_date", ["date"]),
            # Composite keys cannot match a one-column candidate → skipped.
            _fk(["a_id", "b_id"], "c.s.dim_ab", ["a_id", "b_id"]),
        ]),
        "c.s.dim_destination": _info(),
    }
    assert join_advisor.foreign_key_pairs(infos) == {
        frozenset({"fact_booking.destination_id", "dim_destination.destination_id"}),
        frozenset({"fact_booking.check_in_date", "dim_date.date"}),
    }


def test_table_infos_skips_unreadable_tables(monkeypatch):
    calls = []

    def get(full_name):
        calls.append(full_name)
        if full_name.endswith("secret"):
            raise PermissionError("no access")
        return _info(("id", "bigint", ColumnTypeName.LONG))

    client = SimpleNamespace(tables=SimpleNamespace(get=get))
    monkeypatch.setattr(join_advisor, "get_workspace_client", lambda: client)
    infos = join_advisor._table_infos(["c.s.orders", "`c`.`s`.`secret`"])
    assert sorted(calls) == ["c.s.orders", "c.s.secret"]  # backticks stripped
    assert list(infos) == ["c.s.orders"]


def test_table_infos_empty_without_a_client(monkeypatch):
    def boom():
        raise RuntimeError("no auth")

    monkeypatch.setattr(join_advisor, "get_workspace_client", boom)
    assert join_advisor._table_infos(["c.s.a", "c.s.b"]) == {}


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


def test_discover_no_metadata_when_no_table_is_readable(monkeypatch):
    space = {"data_sources": {"tables": [{"identifier": "c.s.a"}, {"identifier": "c.s.b"}]}}
    monkeypatch.setattr(join_advisor, "_table_infos", lambda ids: {})
    assert join_advisor.discover_candidates(space) == {"status": "no_metadata", "candidates": []}


def test_discover_fetches_metadata_once_for_columns_and_fks(monkeypatch):
    space = {"data_sources": {"tables": [{"identifier": "c.s.orders"}, {"identifier": "c.s.customer"}]}}
    calls = []

    def infos(ids):
        calls.append(list(ids))
        return {
            "c.s.orders": _info(("customer_id", "bigint", ColumnTypeName.LONG)),
            "c.s.customer": _info(("customer_id", "bigint", ColumnTypeName.LONG)),
        }

    monkeypatch.setattr(join_advisor, "_table_infos", infos)
    monkeypatch.setattr(join_advisor, "execute_sql", lambda sql: {"data": [[10, 10, True]], "error": None})
    assert join_advisor.discover_candidates(space)["status"] == "ok"
    assert calls == [["c.s.orders", "c.s.customer"]]


def test_discover_skips_an_aliased_declared_join(monkeypatch):
    # The live Wanderbricks shape that re-proposed declared joins: the ON predicate
    # qualifies the parent by alias, and the parent carries the matching UC FK.
    space = {
        "data_sources": {"tables": [{"identifier": "c.s.dim_property"}, {"identifier": "c.s.dim_destination"}]},
        "instructions": {"join_specs": [{
            "left": {"identifier": "c.s.dim_property", "alias": "dim_property"},
            "right": {"identifier": "c.s.dim_destination", "alias": "dim_destination_2"},
            "sql": ["`dim_property`.`destination_id` = `dim_destination_2`.`destination_id`"],
        }]},
    }
    monkeypatch.setattr(join_advisor, "_table_infos", lambda ids: {
        "c.s.dim_property": _info(
            ("destination_id", "bigint", ColumnTypeName.LONG),
            constraints=[_fk(["destination_id"], "c.s.dim_destination", ["destination_id"])],
        ),
        "c.s.dim_destination": _info(("destination_id", "bigint", ColumnTypeName.LONG)),
    })
    assert join_advisor.discover_candidates(space) == {"status": "fully_connected", "candidates": []}


def test_discover_ok_probes_candidates_and_upgrades_fk(monkeypatch):
    space = {
        "data_sources": {
            "tables": [{"identifier": "c.s.orders"}, {"identifier": "c.s.customer"}]
        }
    }
    # A declared UC FK on this endpoint pair upgrades match to "fk".
    monkeypatch.setattr(
        join_advisor, "_table_infos",
        lambda ids: {
            "c.s.orders": _info(
                ("customer_id", "bigint", ColumnTypeName.LONG),
                constraints=[_fk(["customer_id"], "c.s.customer", ["customer_id"])],
            ),
            "c.s.customer": _info(("customer_id", "bigint", ColumnTypeName.LONG)),
        },
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
        join_advisor, "_table_infos",
        lambda ids: {
            "c.s.orders": _info(("customer_id", "bigint", ColumnTypeName.LONG)),
            "c.s.customer": _info(("customer_id", "bigint", ColumnTypeName.LONG)),
        },
    )
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
