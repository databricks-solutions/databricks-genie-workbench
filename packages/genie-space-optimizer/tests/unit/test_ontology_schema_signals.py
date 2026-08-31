"""Pure structural-signal extraction — offline unit tests (Stage 1, MV-D52).

FK/PK + shared-join extraction from a fixture ``information_schema``, MV membership
from fixture MV YAML, and shared-schema affinity — all pure, no Spark.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import schema_signals as ss


# ── FK reconstruction from information_schema ───────────────────────────────


def _kcu(constraint, cat, sch, tbl, col):
    return {
        "constraint_catalog": "c", "constraint_schema": sch, "constraint_name": constraint,
        "table_catalog": cat, "table_schema": sch, "table_name": tbl, "column_name": col,
    }


def test_fk_edges_referencing_to_referenced():
    # FK fk_rev on revenue.fact_revenue(route_id) → revenue.dim_route(route_id).
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "revenue", "constraint_name": "fk_rev",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "revenue",
        "unique_constraint_name": "pk_route",
    }]
    key_col = [_kcu("fk_rev", "c", "revenue", "fact_revenue", "route_id")]
    constr_col = [_kcu("fk_rev", "c", "revenue", "dim_route", "route_id")]
    edges = ss.fk_edges(referential, key_col, constr_col)
    assert edges == [("c.revenue.fact_revenue", "c.revenue.dim_route")]


def test_fk_edges_falls_back_to_unique_constraint_kcu():
    # No constraint_column_usage row → resolve the referenced table via the linked
    # unique/PK constraint's key_column_usage entry.
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "res", "constraint_name": "fk_pnr",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "res",
        "unique_constraint_name": "pk_pnr",
    }]
    key_col = [
        _kcu("fk_pnr", "c", "res", "bookings", "pnr_id"),
        _kcu("pk_pnr", "c", "res", "pnr", "pnr_id"),
    ]
    edges = ss.fk_edges(referential, key_col, [])
    assert edges == [("c.res.bookings", "c.res.pnr")]


def test_fk_edges_drops_self_reference_and_dedupes():
    referential = [
        {"constraint_catalog": "c", "constraint_schema": "s", "constraint_name": "fk_self",
         "unique_constraint_catalog": "c", "unique_constraint_schema": "s", "unique_constraint_name": "pk_self"},
    ]
    key_col = [_kcu("fk_self", "c", "s", "t", "x")]
    constr_col = [_kcu("fk_self", "c", "s", "t", "x")]  # references itself
    assert ss.fk_edges(referential, key_col, constr_col) == []


# ── Shared-join-column proxy ────────────────────────────────────────────────


def _col(cat, sch, tbl, col):
    return {"table_catalog": cat, "table_schema": sch, "table_name": tbl, "column_name": col}


def test_shared_join_column_star_edges_and_suffix_filter():
    rows = [
        _col("c", "rev", "fact_revenue", "route_id"),
        _col("c", "route", "segments", "route_id"),
        _col("c", "sched", "flights", "route_id"),
        _col("c", "rev", "fact_revenue", "amount"),   # not a join suffix → ignored
    ]
    edges = ss.shared_join_column_edges(rows)
    # 3 tables share route_id → a 2-edge star from the sorted-first table.
    assert len(edges) == 2
    hub = sorted({"c.rev.fact_revenue", "c.route.segments", "c.sched.flights"})[0]
    assert all(a == hub and w == ss.SHARED_JOIN_WEIGHT and src == "shared_join_column" for (a, b, w, src) in edges)
    # A column on a single table produces no edge (min_tables).
    assert ss.shared_join_column_edges([_col("c", "rev", "t", "x_id")]) == []


def test_shared_join_column_skips_over_generic_columns():
    # A column touching more than MAX_TABLES_PER_SHARED_COLUMN tables is a generic unit
    # (currency_code) → skipped so it does not fuse the whole estate.
    rows = [_col("c", "s", f"t{i}", "currency_code") for i in range(ss.MAX_TABLES_PER_SHARED_COLUMN + 1)]
    assert ss.shared_join_column_edges(rows) == []


def test_join_key_edges_combines_fk_and_proxy():
    referential = [{
        "constraint_catalog": "c", "constraint_schema": "s", "constraint_name": "fk1",
        "unique_constraint_catalog": "c", "unique_constraint_schema": "s", "unique_constraint_name": "pk1",
    }]
    key_col = [_kcu("fk1", "c", "s", "child", "k_id")]
    constr_col = [_kcu("fk1", "c", "s", "parent", "k_id")]
    cols = [_col("c", "s", "child", "k_id"), _col("c", "s", "other", "k_id")]
    combined = ss.join_key_edges(referential, key_col, constr_col, cols)
    sources = {(e[3] if len(e) > 3 else "foreign_key") for e in combined}
    assert sources == {"foreign_key", "shared_join_column"}


# ── MV membership ───────────────────────────────────────────────────────────


def test_mv_membership_map_from_yaml_source():
    yamls = {
        "c.metrics.revenue_mv": {"source": "c.revenue.fact_revenue", "measures": []},
        "c.metrics.backtick_mv": {"source": "`c`.`revenue`.`bookings`"},
        "c.metrics.subquery_mv": {"source": "SELECT * FROM c.revenue.x"},  # subquery → skip
        "c.metrics.empty_mv": {"measures": []},                             # no source → skip
    }
    got = ss.mv_membership_map(yamls)
    assert got == {
        "c.metrics.revenue_mv": ["c.revenue.fact_revenue"],
        "c.metrics.backtick_mv": ["c.revenue.bookings"],
    }


# ── Schema affinity ─────────────────────────────────────────────────────────


def test_schema_affinity_map_groups_by_catalog_schema():
    rows = [
        {"table_catalog": "c", "table_schema": "revenue", "table_name": "fact_revenue"},
        {"table_catalog": "c", "table_schema": "revenue", "table_name": "dim_route"},
        {"table_catalog": "c", "table_schema": "loyalty", "table_name": "members"},
        {"table_catalog": "c", "table_schema": "loyalty", "table_name": None},  # skipped
    ]
    got = ss.schema_affinity_map(rows)
    assert got == {
        "c.loyalty": ["c.loyalty.members"],
        "c.revenue": ["c.revenue.dim_route", "c.revenue.fact_revenue"],
    }
